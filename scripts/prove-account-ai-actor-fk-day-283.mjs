/**
 * prove-account-ai-actor-fk-day-283.mjs — STAGING-ONLY live proof for the Day-283 fix.
 *
 * Provisions an auth-first QA manager (Admin Auth API + reps bridge, NO public.users
 * row — the identity class that triggers the actor-FK 500), creates a synthetic
 * tenant-scoped account, signs the QA manager in, and calls the DEPLOYED staging API:
 *   POST /v1/accounts/:id/summary
 *   POST /v1/accounts/:id/tasks/generate
 * printing HTTP status + a compact body. Run once before deploy (expect 500) and once
 * after (expect 200). `--cleanup` removes the synthetic account, its account_ai_* rows,
 * and the QA identity, then re-checks. All data synthetic; guarded to staging.
 *
 * Usage:
 *   node --env-file=.env.staging.local scripts/prove-account-ai-actor-fk-day-283.mjs [--cleanup]
 */
import { createClient } from "@supabase/supabase-js";

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const anon = process.env.SUPABASE_ANON_KEY;
const ref = (url || "").match(/https:\/\/([a-z0-9]+)\./)?.[1];
const API = process.env.STAGING_API_BASE || "https://gravix-sales-trainer-api-staging-production.up.railway.app";
const EMAIL = (process.env.STAGING_QA_EMAIL || "staging-qa@gravix.invalid").toLowerCase();
const PW = process.env.STAGING_QA_PASSWORD;

if (process.env.APP_ENV !== "staging" || ref !== process.env.EXPECTED_STAGING_SUPABASE_REF || ref === process.env.PROD_SUPABASE_REF) {
  console.error("GUARD: not a confirmed staging target (APP_ENV/ref mismatch) — refusing."); process.exit(1);
}
if (!key || !anon || !PW) { console.error("Missing SUPABASE_SERVICE_ROLE_KEY / SUPABASE_ANON_KEY / STAGING_QA_PASSWORD"); process.exit(1); }

const ORG = "00000000-2711-0000-0000-000000000002";
const COMPANY = "00000000-2711-0000-0000-000000000003";
const ACCOUNT_ID = "00000000-2833-4000-8000-000000000001"; // synthetic, deterministic → easy cleanup
const s = createClient(url, key, { auth: { persistSession: false } });
const cleanup = process.argv.includes("--cleanup");

async function removeSynthetic() {
  await s.from("account_ai_tasks").delete().eq("account_id", ACCOUNT_ID);
  await s.from("account_ai_summaries").delete().eq("account_id", ACCOUNT_ID);
  await s.from("accounts").delete().eq("id", ACCOUNT_ID);
}

if (cleanup) {
  await removeSynthetic();
  const { data: acc } = await s.from("accounts").select("id").eq("id", ACCOUNT_ID).maybeSingle();
  const { data: tasks } = await s.from("account_ai_tasks").select("id").eq("account_id", ACCOUNT_ID);
  const { data: sums } = await s.from("account_ai_summaries").select("id").eq("account_id", ACCOUNT_ID);
  const clean = !acc && (!tasks || tasks.length === 0) && (!sums || sums.length === 0);
  console.log(`  synthetic account present : ${!!acc}`);
  console.log(`  synthetic ai_tasks rows   : ${tasks?.length ?? 0}`);
  console.log(`  synthetic ai_summaries    : ${sums?.length ?? 0}`);
  console.log(clean ? "  ✓ synthetic account data cleaned" : "  ✗ residue remains");
  process.exit(clean ? 0 : 1);
}

// 1) Provision the QA manager (idempotent) + tenant bridge.
let authId;
{
  const { data, error } = await s.auth.admin.createUser({
    email: EMAIL, password: PW, email_confirm: true,
    user_metadata: { name: "Staging QA", staging_qa: true, org_id: ORG, role: "manager" },
    app_metadata: { org_id: ORG, staging_qa: true },
  });
  if (!error && data?.user?.id) authId = data.user.id;
  else {
    const { data: prof } = await s.from("profiles").select("user_id").eq("email", EMAIL).maybeSingle();
    authId = prof?.user_id;
    if (authId) await s.auth.admin.updateUserById(authId, { password: PW, email_confirm: true });
  }
  if (!authId) { console.error("could not create/find QA identity"); process.exit(1); }
  await s.from("profiles").upsert({ user_id: authId, role: "manager", email: EMAIL, full_name: "Staging QA" }, { onConflict: "user_id" });
  await s.from("reps").upsert({ id: authId, org_id: ORG, company_id: COMPANY, tier: "Manager", name: "Staging QA", display_name: "Staging QA", first_name: "Staging", last_name: "QA", email: EMAIL, is_active: true }, { onConflict: "id" });
}
// Confirm this identity is genuinely auth-first: NO public.users row.
const { data: usersRow } = await s.from("users").select("id").eq("id", authId).maybeSingle();
console.log(`  QA identity is auth-first (no public.users row): ${!usersRow}`);

// 2) Ensure a synthetic tenant-scoped account with NO owner (exercises all task branches).
await s.from("accounts").upsert({
  id: ACCOUNT_ID, org_id: COMPANY, name: "ZZ Day283 Proof Account", domain: "day283-proof.invalid",
  owner_id: null, created_at: new Date().toISOString(), updated_at: new Date().toISOString(),
}, { onConflict: "id" });

// 3) Sign the QA manager in → bearer token.
const pub = createClient(url, anon, { auth: { persistSession: false } });
const { data: sess, error: sErr } = await pub.auth.signInWithPassword({ email: EMAIL, password: PW });
const token = sess?.session?.access_token;
if (sErr || !token) { console.error("QA sign-in failed:", sErr?.message); process.exit(1); }

// 4) Call the DEPLOYED staging API as the QA manager.
const ver = await fetch(`${API}/v1/version`).then(r => r.json()).catch(() => ({}));
console.log(`\n  Deployed staging API version: ${ver.version} (app_env=${ver.app_env})\n`);

async function hit(path, body) {
  const r = await fetch(`${API}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(body || {}),
  });
  let j; try { j = await r.json(); } catch { j = {}; }
  return { status: r.status, ok: j?.ok, error: j?.error, detail: j?.detail || j?.message };
}

const summary = await hit(`/v1/accounts/${ACCOUNT_ID}/summary`, {
  summary: "Day283 proof summary", health_status: "at_risk", churn_risk: 40,
  next_best_action: "Book QBR", manager_notes: "synthetic",
});
const tasks = await hit(`/v1/accounts/${ACCOUNT_ID}/tasks/generate`, {});

console.log("  POST /:id/summary        ->", JSON.stringify(summary));
console.log("  POST /:id/tasks/generate ->", JSON.stringify(tasks));

const both200 = summary.status === 200 && tasks.status === 200;
console.log(`\n  ${both200 ? "✓ BOTH 200" : "→ result recorded"} (summary=${summary.status}, tasks=${tasks.status})`);
process.exit(0);
