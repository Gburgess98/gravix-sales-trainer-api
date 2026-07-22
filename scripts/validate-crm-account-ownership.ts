/**
 * validate-crm-account-ownership.ts
 *
 * Day 247 — locks in the crm_accounts org-scoping decision.
 *
 * crm_accounts is (id, org_id, name, domain, created_at): no user_id, no
 * owner column. The old per-user model (`user_id: requester`) referenced a
 * column that does not exist, so POST /v1/crm/accounts answered 500 and the
 * dedup/read path 42703'd. MVP decision: accounts are org-scoped — org_id is
 * the table's only tenant column — not user-owned.
 *
 * These checks exercise the create + dedup path, whose dedup step IS an
 * org-scoped read of crm_accounts, so it proves both that creation works and
 * that reads are isolated per org.
 *
 * Coverage:
 *   ✓ account creation works without user_id (200, was 500)
 *   ✓ response carries no user_id and the name is not a raw UUID
 *   ✓ the created row is stamped with the requester's org_id
 *   ✓ dedup within the same org returns the same account (org-scoped read)
 *   ✓ a different org creating the same name gets a SEPARATE account
 *     (org isolation — one org's dedup never resolves another org's row)
 *   ✓ each org ends up with exactly its own row
 *
 * Scope note: all reps currently share one org, so this proves ORG-level
 * isolation. Company-within-org isolation for accounts needs a
 * crm_accounts.company_id column (a deliberate future migration) — the two
 * fixtures below sit in DISTINCT orgs to exercise the boundary the schema
 * actually supports today.
 *
 * Self-cleaning: removes the accounts it creates plus its org/rep fixtures.
 * Never touches the demo companies.
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:crm-account-ownership
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY247::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function hit(method: string, path: string, userId: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json", "x-user-id": userId };
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(30_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

const ORG_A = uid("org-a");
const ORG_B = uid("org-b");
const REP_A = uid("rep-a");
const REP_B = uid("rep-b");
const ACCOUNT_NAME = "Day247 Shared Account Name";

async function seedFixtures() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [{ id: ORG_A, name: "Day247 Org A (validator)" }, { id: ORG_B, name: "Day247 Org B (validator)" }],
    { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: REP_A, name: "Day247 Rep A", tier: "Manager", org_id: ORG_A },
      { id: REP_B, name: "Day247 Rep B", tier: "Manager", org_id: ORG_B },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  // A clean slate for the shared account name in both orgs.
  await supa.from("crm_accounts").delete().in("org_id", [ORG_A, ORG_B]);
}

async function cleanup() {
  await supa.from("crm_accounts").delete().in("org_id", [ORG_A, ORG_B]);
  await supa.from("reps").delete().in("id", [REP_A, REP_B]);
  await supa.from("orgs").delete().in("id", [ORG_A, ORG_B]);
  console.log("\n  Cleanup: removed validator orgs, reps and accounts.");
}

async function rowsFor(orgId: string) {
  const { data } = await supa.from("crm_accounts").select("id, org_id, name").eq("org_id", orgId).eq("name", ACCOUNT_NAME);
  return (data ?? []) as any[];
}

async function main() {
  console.log("\nDay 247 — crm_accounts org ownership\n");

  await seedFixtures();

  // ── Create (org A) ─────────────────────────────────────────────────────────
  const createA = await hit("POST", "/v1/crm/accounts", REP_A, { name: ACCOUNT_NAME });
  c("account creation works without user_id (200, was 500)",
    createA.status === 200 && createA.data?.ok === true && createA.data?.created === true,
    `got ${createA.status} ${JSON.stringify(createA.data)?.slice(0, 160)}`);

  const accountA = createA.data?.account ?? {};
  c("create response carries no user_id",
    !("user_id" in accountA),
    `keys ${JSON.stringify(Object.keys(accountA))}`);

  c("account name is the supplied label, not a raw UUID",
    accountA.name === ACCOUNT_NAME && !UUID_RE.test(String(accountA.name ?? "")),
    `got ${JSON.stringify(accountA.name)}`);

  const dbA = await rowsFor(ORG_A);
  c("created row is stamped with the requester's org_id",
    dbA.length === 1 && String(dbA[0].id) === String(accountA.id),
    `got ${JSON.stringify(dbA)}`);

  // ── Dedup within org A ─────────────────────────────────────────────────────
  const dedupA = await hit("POST", "/v1/crm/accounts", REP_A, { name: ACCOUNT_NAME });
  c("dedup within the same org returns the same account (org-scoped read)",
    dedupA.status === 200 && dedupA.data?.created === false &&
    String(dedupA.data?.account?.id) === String(accountA.id),
    `got ${JSON.stringify(dedupA.data)?.slice(0, 160)}`);

  // ── Cross-org isolation ────────────────────────────────────────────────────
  const createB = await hit("POST", "/v1/crm/accounts", REP_B, { name: ACCOUNT_NAME });
  c("a different org creating the same name gets a SEPARATE account",
    createB.status === 200 && createB.data?.created === true &&
    String(createB.data?.account?.id) !== String(accountA.id),
    `got created=${createB.data?.created} id=${createB.data?.account?.id} (A was ${accountA.id})`);

  c("org B's create did not resolve or overwrite org A's account",
    String(createB.data?.account?.id) !== String(accountA.id),
    `B id ${createB.data?.account?.id}, A id ${accountA.id}`);

  const dbAAfter = await rowsFor(ORG_A);
  const dbBAfter = await rowsFor(ORG_B);
  c("each org ends up with exactly its own single row",
    dbAAfter.length === 1 && dbBAfter.length === 1 &&
    String(dbAAfter[0].id) === String(accountA.id) &&
    String(dbBAfter[0].id) === String(createB.data?.account?.id),
    `A=${JSON.stringify(dbAAfter.map((r) => r.id))} B=${JSON.stringify(dbBAfter.map((r) => r.id))}`);

  // org A dedup again — must still resolve A's row, never B's.
  const dedupAAgain = await hit("POST", "/v1/crm/accounts", REP_A, { name: ACCOUNT_NAME });
  c("org A's dedup still resolves org A's row after org B created one",
    String(dedupAAgain.data?.account?.id) === String(accountA.id),
    `got ${dedupAAgain.data?.account?.id}, expected ${accountA.id}`);
}

main()
  .catch((e) => {
    console.error("\n  Validator crashed:", e?.message || e);
    c("validator ran to completion", false, String(e?.message || e));
  })
  .finally(async () => {
    await cleanup();
    const passed = checks.filter((x) => x.passed).length;
    console.log(`\n  ${passed}/${checks.length} checks passed`);
    console.log(
      passed === checks.length
        ? "  CRM account ownership validation PASSED\n"
        : "  CRM account ownership validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
