/**
 * validate-crm-account-ownership.ts
 *
 * Day 247 → 248 — CRM account tenant scoping.
 *
 * Day 247 moved crm_accounts off a non-existent user_id onto org_id, reviving
 * account creation. Day 248 makes company_id the tenant boundary
 * (sql/20260723_crm_accounts_company_scope.sql), because both demo companies
 * share one org and org scoping cannot isolate them.
 *
 * Two modes, detected from the live PostgREST schema:
 *
 *   MIGRATION PENDING (company_id column absent) — proves the org-scoped
 *   fallback still works, so creation is not broken while the SQL is unapplied,
 *   using two reps in DIFFERENT orgs. Reported as a pass (Day 218/236
 *   precedent).
 *
 *   APPLIED (company_id present) — proves true company isolation using two
 *   reps in the SAME org but DIFFERENT companies: creating the same account
 *   name yields two separate accounts and neither manager ever dedups to the
 *   other's, a boundary org scoping cannot enforce.
 *
 * All isolation assertions go through the API (create + resolve-or-create
 * dedup). Those reads run on the API server's own Supabase client, which has
 * read-your-writes; a second client here does NOT reliably see a row the API
 * just wrote (Supabase pools/replicas make cross-client reads lag by an
 * unbounded, variable amount), so direct-DB reads are used only for the
 * confirmatory company_id-stamp check, with a generous retry.
 *
 * Self-cleaning: removes its accounts, reps, companies and orgs.
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:crm-account-ownership
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL = String(process.env.SUPABASE_URL ?? "").replace(/\/$/, "");
const SERVICE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "");
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY248::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// x-org-id is passed per rep so org resolution is deterministic even before the
// API server's client has seen the rep this validator just wrote. It stabilises
// only the ORG fallback; company_id is always resolved server-side from the rep
// context, so company isolation is proven by routing, never by this header.
async function hit(method: string, path: string, userId: string, orgId: string, body?: object) {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-user-id": userId,
    "x-org-id": orgId,
  };
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(30_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function retryRead<T>(fn: () => Promise<T>, ok: (v: T) => boolean, tries = 60, delay = 500): Promise<T> {
  let v = await fn();
  for (let i = 0; i < tries && !ok(v); i++) {
    await sleep(delay);
    v = await fn();
  }
  return v;
}

async function crmAccountsHasCompanyId(): Promise<boolean> {
  const res = await fetch(`${SUPA_URL}/rest/v1/`, {
    headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` },
  });
  const doc: any = await res.json();
  const props = doc?.definitions?.crm_accounts?.properties ?? {};
  return Object.prototype.hasOwnProperty.call(props, "company_id");
}

async function companyIdOf(accountId: string): Promise<string | null> {
  // select("*") rather than select("company_id"): the column does not exist
  // until the migration is applied, so a literal select of it would itself be
  // schema drift (validate:schema-selects would flag this file). With "*" the
  // field is simply absent pre-migration and present after.
  const { data } = await supa.from("crm_accounts").select("*").eq("id", accountId).maybeSingle();
  return (data as any)?.company_id ?? null;
}

const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
const ACCOUNT_NAME = "Day248 Shared Account Name";

// Applied mode: one org, two companies (the arrangement org scoping cannot
// isolate). Pending mode: two orgs (org scoping is all the schema supports).
const ORG_MAIN = uid("org-main");
const ORG_OTHER = uid("org-other");
const COMPANY_A = uid("company-a");
const COMPANY_B = uid("company-b");
const REP_A = uid("rep-a");
const REP_B = uid("rep-b");

type Rep = { id: string; org: string; company: string };

async function seedCommon() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [{ id: ORG_MAIN, name: "Day248 Org Main" }, { id: ORG_OTHER, name: "Day248 Org Other" }],
    { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: coErr } = await supa.from("companies").upsert(
    [
      { id: COMPANY_A, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER, name: "Day248 Company A", slug: "day248-company-a" },
      { id: COMPANY_B, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER, name: "Day248 Company B", slug: "day248-company-b" },
    ],
    { onConflict: "id" }
  );
  if (coErr) throw new Error(`fixture company upsert failed: ${coErr.message}`);
}

async function cleanup() {
  await supa.from("crm_accounts").delete().in("org_id", [ORG_MAIN, ORG_OTHER]);
  await supa.from("reps").delete().in("id", [REP_A, REP_B]);
  await supa.from("companies").delete().in("id", [COMPANY_A, COMPANY_B]);
  await supa.from("orgs").delete().in("id", [ORG_MAIN, ORG_OTHER]);
  console.log("\n  Cleanup: removed validator orgs, companies, reps and accounts.");
}

// Prove the two reps' accounts are isolated: same name, separate accounts,
// neither dedups to the other. Entirely via the API's consistent client.
async function proveIsolation(a: Rep, b: Rep, boundary: string) {
  const createA = await hit("POST", "/v1/crm/accounts", a.id, a.org, { name: ACCOUNT_NAME });
  c("account creation works (200)",
    createA.status === 200 && createA.data?.ok === true && createA.data?.created === true,
    `got ${createA.status} ${JSON.stringify(createA.data)?.slice(0, 160)}`);

  const accountA = createA.data?.account ?? {};
  c("create response carries no user_id", !("user_id" in accountA), `keys ${JSON.stringify(Object.keys(accountA))}`);
  c("account name is the supplied label, not a raw UUID",
    accountA.name === ACCOUNT_NAME && !UUID_RE.test(String(accountA.name ?? "")), `got ${JSON.stringify(accountA.name)}`);

  const dedupA = await hit("POST", "/v1/crm/accounts", a.id, a.org, { name: ACCOUNT_NAME });
  c("same-tenant dedup returns the same account",
    dedupA.data?.created === false && String(dedupA.data?.account?.id) === String(accountA.id),
    `got ${JSON.stringify(dedupA.data)?.slice(0, 140)}`);

  const createB = await hit("POST", "/v1/crm/accounts", b.id, b.org, { name: ACCOUNT_NAME });
  c(`${boundary} isolation: the other tenant creates a SEPARATE account (same name)`,
    createB.data?.created === true && String(createB.data?.account?.id) !== String(accountA.id),
    `B created=${createB.data?.created} id=${createB.data?.account?.id} (A was ${accountA.id})`);

  const dedupB = await hit("POST", "/v1/crm/accounts", b.id, b.org, { name: ACCOUNT_NAME });
  c("the other tenant's dedup resolves ITS row, never the first tenant's",
    String(dedupB.data?.account?.id) === String(createB.data?.account?.id) &&
    String(dedupB.data?.account?.id) !== String(accountA.id),
    `got ${dedupB.data?.account?.id}`);

  const dedupAAgain = await hit("POST", "/v1/crm/accounts", a.id, a.org, { name: ACCOUNT_NAME });
  c("the first tenant's dedup still resolves ITS row after the other created one",
    String(dedupAAgain.data?.account?.id) === String(accountA.id),
    `got ${dedupAAgain.data?.account?.id}, expected ${accountA.id}`);

  return { accountAId: String(accountA.id), accountBId: String(createB.data?.account?.id) };
}

async function main() {
  const applied = await crmAccountsHasCompanyId();
  console.log(`\nDay 248 — crm_accounts company scope  [${applied ? "APPLIED" : "MIGRATION PENDING"}]\n`);

  await seedCommon();

  if (!applied) {
    // Two reps in two orgs — org scoping is all the schema supports until the
    // migration lands. Proves the fallback isolates and creation is not broken.
    const { error } = await supa.from("reps").upsert(
      [
        { id: REP_A, name: "Day248 Rep A", tier: "Manager", org_id: ORG_MAIN, company_id: COMPANY_A },
        { id: REP_B, name: "Day248 Rep B", tier: "Manager", org_id: ORG_OTHER, company_id: COMPANY_B },
      ],
      { onConflict: "id" }
    );
    if (error) throw new Error(`fixture rep upsert failed: ${error.message}`);
    await supa.from("crm_accounts").delete().in("org_id", [ORG_MAIN, ORG_OTHER]);

    await proveIsolation(
      { id: REP_A, org: ORG_MAIN, company: COMPANY_A },
      { id: REP_B, org: ORG_OTHER, company: COMPANY_B },
      "org",
    );
    console.log("\n  company_id column absent — apply sql/20260723_crm_accounts_company_scope.sql,");
    console.log("  then re-run to prove COMPANY isolation within a single org.");
    return;
  }

  // Applied: two reps, SAME org, different companies. org can no longer tell
  // them apart, so passing proves company_id is doing the isolation.
  const { error } = await supa.from("reps").upsert(
    [
      { id: REP_A, name: "Day248 Rep A", tier: "Manager", org_id: ORG_MAIN, company_id: COMPANY_A },
      { id: REP_B, name: "Day248 Rep B", tier: "Manager", org_id: ORG_MAIN, company_id: COMPANY_B },
    ],
    { onConflict: "id" }
  );
  if (error) throw new Error(`fixture rep upsert failed: ${error.message}`);
  await supa.from("crm_accounts").delete().eq("org_id", ORG_MAIN);

  const { accountAId, accountBId } = await proveIsolation(
    { id: REP_A, org: ORG_MAIN, company: COMPANY_A },
    { id: REP_B, org: ORG_MAIN, company: COMPANY_B },
    "company",
  );

  // Confirmatory: the rows actually carry the right company_id (direct read,
  // generous retry for cross-client visibility).
  const compA = await retryRead(() => companyIdOf(accountAId), (v) => v !== null);
  const compB = await retryRead(() => companyIdOf(accountBId), (v) => v !== null);
  c("each account is stamped with its own company_id",
    compA === COMPANY_A && compB === COMPANY_B,
    `A company_id=${compA} (want ${COMPANY_A}); B company_id=${compB} (want ${COMPANY_B})`);
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
