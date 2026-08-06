/**
 * validate-staging-qa-identity-day-273.ts
 *
 * Proves the Day-273 staging QA identity provisioning is SAFE and CORRECT without
 * any network call or secret. It imports the pure core of
 * `scripts/provision-staging-qa.ts` and asserts:
 *   • the staging guard refuses production / mismatched projects (non-vacuous),
 *   • the profile + rep tenant-bridge rows are well-formed (role/tier/ids),
 *   • the tenant-scope rule mirrors src/lib/callAccess.ts#canAccessCall,
 *   • the JWT issuer-host check and redaction behave,
 *   • the provisioning script never logs the password, a bearer token or keys.
 *
 * TWO LANES:
 *   • SELF-TEST (default, NO network, NO secrets) — CI/local runs this.
 *   • LIVE-CONFIG (opt-in): if APP_ENV=staging + SUPABASE_URL +
 *     EXPECTED_STAGING_SUPABASE_REF are present, asserts the REAL environment
 *     passes the same staging guard (still no network, no secrets printed).
 *     The full create → sign-in → tenant-read → delete lifecycle is proven
 *     separately by `npm run staging:qa -- verify` (that path needs staging
 *     secrets and is run by the operator, never here).
 *
 * Usage:
 *   npx tsx scripts/validate-staging-qa-identity-day-273.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import {
  refFromSupabaseUrl,
  assertSafeStagingTarget,
  tierForRole,
  buildQaAuthUserPayload,
  buildQaProfileRow,
  buildQaRepRow,
  evaluateTenantScope,
  issuerHostFromJwt,
  redact,
  STAGING_TENANT,
  DEFAULT_QA_EMAIL,
} from "./provision-staging-qa";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) {
  console.log(`\n── ${t} ──`);
}

// Synthetic, clearly-not-production identifiers for the guard self-test.
const CLEAN = {
  appEnv: "staging",
  supabaseRef: "stagingref00staging",
  expectedStagingRef: "stagingref00staging",
  prodRef: "prodref00production",
};

function b64url(obj: unknown): string {
  return Buffer.from(JSON.stringify(obj)).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function fakeJwt(iss: string): string {
  return `${b64url({ alg: "HS256", typ: "JWT" })}.${b64url({ iss })}.sig`;
}

function main() {
  console.log("Staging QA identity provisioning safety-check (Day 273) — self-test makes NO network call, prints NO secrets\n");

  // 1) Supabase ref parsing ----------------------------------------------------
  section("Supabase project-ref parsing");
  gate("parses <ref>.supabase.co", refFromSupabaseUrl("https://dnumqzxthfthsmfnzvdz.supabase.co") === "dnumqzxthfthsmfnzvdz");
  gate("parses without scheme", refFromSupabaseUrl("abcref000.supabase.co") === "abcref000");
  gate("returns null for a non-supabase host", refFromSupabaseUrl("https://api.gravixbots.com") === null);
  gate("returns null for empty input", refFromSupabaseUrl("") === null);

  // 2) Staging guard: clean passes, planted violations caught ------------------
  section("Staging guard — clean config passes");
  gate("clean staging config passes", assertSafeStagingTarget(CLEAN).length === 0, assertSafeStagingTarget(CLEAN).join("; "));

  section("NON-VACUITY — unsafe targets must be refused");
  const caught = (label: string, mutate: (c: typeof CLEAN) => void) => {
    const c = { ...CLEAN };
    mutate(c);
    gate(`caught: ${label}`, assertSafeStagingTarget(c).length > 0);
  };
  caught("APP_ENV not staging", (c) => { c.appEnv = "production"; });
  caught("APP_ENV blank", (c) => { c.appEnv = ""; });
  caught("unresolved supabase ref", (c) => { (c as any).supabaseRef = null; });
  caught("EXPECTED_STAGING_SUPABASE_REF unset", (c) => { (c as any).expectedStagingRef = null; });
  caught("SUPABASE_URL ref != expected staging ref", (c) => { c.supabaseRef = "someotherref"; });
  caught("staging ref EQUALS production ref", (c) => { c.supabaseRef = c.prodRef; c.expectedStagingRef = c.prodRef; });
  caught("expected staging ref EQUALS production ref", (c) => { c.expectedStagingRef = c.prodRef; });

  // 3) Row builders ------------------------------------------------------------
  section("Row builders — profile + rep tenant bridge");
  const AUTH = "11111111-2222-4333-8444-555555555555";
  const profM = buildQaProfileRow(AUTH, { email: DEFAULT_QA_EMAIL, role: "manager" });
  gate("profile.user_id bound to auth id", profM.user_id === AUTH);
  gate("profile.role satisfies role check (manager)", profM.role === "manager");
  gate("profile.role satisfies role check (rep)", buildQaProfileRow(AUTH, { email: DEFAULT_QA_EMAIL, role: "rep" }).role === "rep");

  gate("tier maps manager → Manager", tierForRole("manager") === "Manager");
  gate("tier maps rep → SalesRep", tierForRole("rep") === "SalesRep");

  const repM = buildQaRepRow(AUTH, { orgId: STAGING_TENANT.orgId, companyId: STAGING_TENANT.companyId, email: DEFAULT_QA_EMAIL, role: "manager" });
  gate("rep.id equals auth id (getRequesterOrgId resolves)", repM.id === AUTH);
  gate("rep bound to synthetic org", repM.org_id === STAGING_TENANT.orgId);
  gate("rep bound to synthetic company", repM.company_id === STAGING_TENANT.companyId);
  gate("rep tier satisfies reps_tier_check (Manager)", repM.tier === "Manager");
  gate("rep tier satisfies reps_tier_check (SalesRep)", buildQaRepRow(AUTH, { orgId: STAGING_TENANT.orgId, companyId: STAGING_TENANT.companyId, email: DEFAULT_QA_EMAIL, role: "rep" }).tier === "SalesRep");

  const authPayload = buildQaAuthUserPayload(DEFAULT_QA_EMAIL, STAGING_TENANT.orgId, "manager");
  gate("auth payload confirms email (admin-created)", authPayload.email_confirm === true);
  gate("auth payload stamps org in app_metadata", authPayload.app_metadata.org_id === STAGING_TENANT.orgId);
  gate("auth payload marks the identity as staging_qa", authPayload.app_metadata.staging_qa === true);

  // 4) Tenant-scope rule (mirror of canAccessCall) -----------------------------
  section("Tenant-scope rule — mirrors src/lib/callAccess.ts");
  const REQ = AUTH;
  const ORG = STAGING_TENANT.orgId;
  gate("owner can always see own call", evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: REQ, callOrgId: ORG }));
  gate("same-org member sees call (default 'everyone')", evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: ORG }));
  gate("foreign-org call NOT visible", !evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: "00000000-9999-4000-8000-000000009999" }));
  gate("call with no org NOT visible to non-owner", !evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: null }));
  gate("requester with no resolvable org NOT visible", !evaluateTenantScope({ requester: REQ, requesterOrgId: null, callUserId: "other", callOrgId: ORG }));
  gate("visibility 'disabled' hides even same-org call", !evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: ORG, visibility: "disabled" }));
  gate("visibility 'managers' needs assignments (denied without)", !evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: ORG, visibility: "managers", requesterHasAssignments: false }));
  gate("visibility 'managers' allows with assignments", evaluateTenantScope({ requester: REQ, requesterOrgId: ORG, callUserId: "other", callOrgId: ORG, visibility: "managers", requesterHasAssignments: true }));

  // 5) JWT issuer host + redaction ---------------------------------------------
  section("Issuer-host check + redaction");
  gate("extracts issuer host from a JWT", issuerHostFromJwt(fakeJwt("https://dnumqzxthfthsmfnzvdz.supabase.co/auth/v1")) === "dnumqzxthfthsmfnzvdz.supabase.co");
  gate("returns null for a malformed JWT", issuerHostFromJwt("not-a-jwt") === null);
  gate("redact hides a set value", redact("hunter2") === "«set»" && !redact("hunter2").includes("hunter2"));
  gate("redact reports an unset value", redact("") === "«unset»");

  // 6) No-secret-leak scan of the provisioning script --------------------------
  section("SAFETY — provisioning script never logs secrets");
  const src = readFileSync(join(__dirname, "provision-staging-qa.ts"), "utf8");
  const dangerous: Array<[string, RegExp]> = [
    ["raw ${env.password} interpolation", /\$\{\s*env\.password\s*\}/],
    ["raw ${token} interpolation", /\$\{\s*token\s*\}/],
    ["raw service key interpolation", /\$\{\s*env\.serviceKey\s*\}/],
    ["raw anon key interpolation", /\$\{\s*env\.anonKey\s*\}/],
    ["raw access_token interpolation", /\$\{[^}]*access_token[^}]*\}/],
  ];
  for (const [label, re] of dangerous) gate(`no ${label}`, !re.test(src));
  gate("password line is redacted", /redact\(env\.password\)/.test(src));
  gate("supabase project ref is never console.logged (compared, not printed)", !/console\.log\([^)]*refFromSupabaseUrl\(env\.url\)/.test(src));
  gate("guard is applied before any client work", /guardOrExit\(env\)/.test(src) && /assertSafeStagingTarget/.test(src));
  gate("uses the supported Admin Auth API (no direct auth.users INSERT)", /auth\.admin\.createUser/.test(src) && !/insert\s+into\s+auth\.users/i.test(src));

  // 7) Seed no longer writes auth.users ---------------------------------------
  section("SAFETY — SQL seed no longer inserts auth.users/profiles directly");
  const seed = readFileSync(join(__dirname, "..", "sql", "seed", "staging-day272-fixtures.sql"), "utf8");
  gate("seed does NOT insert into auth.users", !/insert\s+into\s+auth\.users/i.test(seed));
  gate("seed does NOT insert into public.profiles", !/insert\s+into\s+public\.profiles/i.test(seed));
  gate("seed points at the provisioning script", /provision-staging-qa/.test(seed));

  // 8) LIVE-CONFIG lane (opt-in; still no network, no secrets) ------------------
  const url = process.env.SUPABASE_URL;
  const expected = process.env.EXPECTED_STAGING_SUPABASE_REF;
  if (url && expected) {
    section("LIVE-CONFIG — the REAL environment passes the staging guard");
    const issues = assertSafeStagingTarget({
      appEnv: process.env.APP_ENV,
      supabaseRef: refFromSupabaseUrl(url),
      expectedStagingRef: expected,
      prodRef: process.env.PROD_SUPABASE_REF || null,
    });
    gate("real env is a confirmed staging target", issues.length === 0, issues.join("; "));
    gate("real env still hides SUPABASE_SERVICE_ROLE_KEY", redact(process.env.SUPABASE_SERVICE_ROLE_KEY) === "«set»" || redact(process.env.SUPABASE_SERVICE_ROLE_KEY) === "«unset»");
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
  console.log(
    url && expected
      ? "Ran the self-test + LIVE-CONFIG guard (no network, no secrets)."
      : "Self-test only (no SUPABASE_URL/EXPECTED_STAGING_SUPABASE_REF) — no network, no secrets printed."
  );
  process.exit(failures === 0 ? 0 : 1);
}

main();
