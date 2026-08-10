/**
 * provision-staging-qa.ts — Day 273
 *
 * Repeatable, STAGING-ONLY provisioning of a disposable QA login identity through
 * the SUPPORTED Supabase Admin Auth API — never by hand-writing auth.users rows.
 *
 * WHY THIS EXISTS
 * Earlier staging seeds INSERTed auth.users directly. Those rows are malformed for
 * GoTrue (they omit the many fields the Auth service normally populates), so
 * `auth.admin.listUsers`/read failed with "Database error loading user" and the
 * identity could not sign in reliably. Day 273 removes that path: this script
 * creates the auth user with `auth.admin.createUser`, then upserts its
 * `public.profiles` row and `public.reps` tenant bridge into the synthetic staging
 * org/company, so a fresh identity can sign in normally and see only synthetic
 * fixtures. Teardown deletes both the application rows and the Auth identity.
 *
 * ACTIONS
 *   create  — create/repair the QA auth user + profile + rep bridge (idempotent)
 *   verify  — prove password sign-in (staging issuer) + tenant-scoped fixture read
 *   status  — report presence of the identity + tenant rows (no writes, no login)
 *   delete  — remove the profile, rep bridge and Auth identity (explicit teardown)
 *
 * SAFETY
 *   • Refuses to run unless APP_ENV=staging AND the Supabase project ref equals
 *     EXPECTED_STAGING_SUPABASE_REF (and differs from PROD_SUPABASE_REF if given).
 *   • Never prints the password, any bearer token, or the service-role key.
 *   • All data is synthetic (0000…-2711 org/company, controlled *.invalid email).
 *   • Production and customer identities are never touched (guard + synthetic ids).
 *
 * ENV
 *   SUPABASE_URL                     (staging project URL)
 *   SUPABASE_SERVICE_ROLE_KEY | SUPABASE_SERVICE_KEY   (staging service role)
 *   SUPABASE_ANON_KEY                (staging anon key — verify/sign-in only)
 *   EXPECTED_STAGING_SUPABASE_REF    (REQUIRED guard — staging project ref)
 *   PROD_SUPABASE_REF                (optional — asserts staging != production)
 *   APP_ENV=staging                  (REQUIRED guard)
 *   STAGING_QA_PASSWORD              (REQUIRED for create/verify — never printed)
 *   STAGING_QA_EMAIL                 (default staging-qa@gravix.invalid)
 *   STAGING_QA_ROLE                  (manager | rep, default manager)
 *   STAGING_QA_FIXTURE_CALL_ID       (default Day-272 stub-proof call)
 *
 * Usage:
 *   APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<ref> STAGING_QA_PASSWORD=… \
 *     npm run staging:qa -- create
 *   … npm run staging:qa -- verify
 *   … npm run staging:qa -- status
 *   … npm run staging:qa -- delete
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

// ── synthetic staging tenant (MUST match sql/seed/staging-day272-fixtures.sql) ──

export const STAGING_TENANT = {
  tmcId: "00000000-2711-0000-0000-000000000001",
  orgId: "00000000-2711-0000-0000-000000000002",
  companyId: "00000000-2711-0000-0000-000000000003",
  // Day-272 synthetic call fixtures (owner is pure tenant data, no auth FK):
  stubProofCallId: "00000000-2722-4000-8000-000000000001",
  v1FallbackCallId: "00000000-2722-4000-8000-000000000002",
} as const;

export const DEFAULT_QA_EMAIL = "staging-qa@gravix.invalid";
export type QaRole = "manager" | "rep";

// ── pure, testable core (imported by the Day-273 validator) ────────────────────

/** Extract the Supabase project ref (`<ref>.supabase.co`) from a project URL. */
export function refFromSupabaseUrl(url: string | undefined | null): string | null {
  if (!url) return null;
  try {
    const host = new URL(/^https?:\/\//.test(url) ? url : `https://${url}`).host;
    const m = host.match(/^([a-z0-9]+)\.supabase\.(co|in|net)$/i);
    return m ? m[1].toLowerCase() : null;
  } catch {
    return null;
  }
}

export interface StagingGuardConfig {
  appEnv?: string;
  supabaseRef?: string | null;
  expectedStagingRef?: string | null;
  prodRef?: string | null;
}

/**
 * Field-level reasons the target is NOT a safe staging project. Empty ⇒ safe.
 * Compares SAFE identifiers only and returns generic reasons — never echoes a ref.
 */
export function assertSafeStagingTarget(cfg: StagingGuardConfig): string[] {
  const issues: string[] = [];
  if (cfg.appEnv !== "staging") issues.push("APP_ENV is not 'staging'");
  if (!cfg.supabaseRef) issues.push("could not resolve the Supabase project ref from SUPABASE_URL");
  if (!cfg.expectedStagingRef) issues.push("EXPECTED_STAGING_SUPABASE_REF is not set (required guard)");
  if (cfg.supabaseRef && cfg.expectedStagingRef && cfg.supabaseRef !== cfg.expectedStagingRef) {
    issues.push("SUPABASE_URL project ref does not match EXPECTED_STAGING_SUPABASE_REF");
  }
  if (cfg.prodRef && cfg.supabaseRef && cfg.prodRef === cfg.supabaseRef) {
    issues.push("SUPABASE_URL project ref EQUALS the production ref (refused)");
  }
  if (cfg.prodRef && cfg.expectedStagingRef && cfg.prodRef === cfg.expectedStagingRef) {
    issues.push("EXPECTED_STAGING_SUPABASE_REF EQUALS the production ref (refused)");
  }
  return issues;
}

/** reps.tier is CHECK-constrained; map the QA role onto an allowed tier. */
export function tierForRole(role: QaRole): "Manager" | "SalesRep" {
  return role === "manager" ? "Manager" : "SalesRep";
}

/** Admin-API create payload (metadata only; password is supplied at the call site). */
export function buildQaAuthUserPayload(email: string, orgId: string, role: QaRole) {
  return {
    email,
    email_confirm: true,
    user_metadata: { name: "Staging QA", staging_qa: true, org_id: orgId, role },
    app_metadata: { org_id: orgId, staging_qa: true },
  };
}

/** public.profiles row for the QA identity (user_id FKs to the new auth.users id). */
export function buildQaProfileRow(authId: string, opts: { email: string; role: QaRole }) {
  return {
    user_id: authId,
    role: opts.role, // profiles_role_check allows only 'rep' | 'manager'
    email: opts.email,
    full_name: "Staging QA",
  };
}

/** public.reps tenant bridge (id = auth id → getRequesterOrgId resolves the org). */
export function buildQaRepRow(
  authId: string,
  opts: { orgId: string; companyId: string; email: string; role: QaRole }
) {
  return {
    id: authId,
    org_id: opts.orgId,
    company_id: opts.companyId,
    tier: tierForRole(opts.role),
    name: "Staging QA",
    display_name: "Staging QA",
    first_name: "Staging",
    last_name: "QA",
    email: opts.email,
    is_active: true,
  };
}

export type CallVisibility = "everyone" | "managers" | "disabled";

/**
 * Pure mirror of src/lib/callAccess.ts#canAccessCall — the tenant-scope rule a
 * provisioned QA identity is subject to. Kept in lock-step so `verify` and the
 * self-test assert the SAME rule the API enforces.
 */
export function evaluateTenantScope(args: {
  requester: string;
  requesterOrgId: string | null;
  callUserId: string;
  callOrgId: string | null;
  visibility?: CallVisibility;
  requesterHasAssignments?: boolean;
}): boolean {
  if (args.callUserId === args.requester) return true;
  if (!args.callOrgId) return false;
  if (!args.requesterOrgId || args.requesterOrgId !== args.callOrgId) return false;
  const visibility = args.visibility ?? "everyone";
  if (visibility === "disabled") return false;
  if (visibility === "everyone") return true;
  if (visibility === "managers") return Boolean(args.requesterHasAssignments);
  return false;
}

/** Decode a JWT's `iss` host WITHOUT verifying the signature (host comparison only). */
export function issuerHostFromJwt(jwt: string): string | null {
  try {
    const payload = jwt.split(".")[1];
    if (!payload) return null;
    const json = Buffer.from(payload.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
    const iss = JSON.parse(json)?.iss;
    if (!iss) return null;
    return new URL(iss).host.toLowerCase();
  } catch {
    return null;
  }
}

/** Never print a secret's value — only whether it is present. */
export function redact(v: string | null | undefined): string {
  return v && String(v).trim() ? "«set»" : "«unset»";
}

export interface PresenceState {
  authPresent: boolean;
  repPresent: boolean;
  profilePresent: boolean;
}

/**
 * Day 278 — pure teardown verdict. A completed teardown must leave NOTHING behind:
 * no Auth identity, no reps bridge, no profile row. Returns `absent:true` only when
 * all three are gone, and names whatever remains. Kept pure so the validator can
 * prove the check fails non-vacuously for a simulated present state.
 */
export function evaluateTeardown(state: PresenceState): { absent: boolean; remaining: string[] } {
  const remaining: string[] = [];
  if (state.authPresent) remaining.push("auth identity");
  if (state.repPresent) remaining.push("reps bridge");
  if (state.profilePresent) remaining.push("profiles row");
  return { absent: remaining.length === 0, remaining };
}

// ── CLI (side-effecting; not imported by the validator) ────────────────────────

type Action = "create" | "verify" | "status" | "delete" | "teardown";

function readEnv() {
  const url = process.env.SUPABASE_URL || "";
  return {
    url,
    serviceKey: process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY || "",
    anonKey: process.env.SUPABASE_ANON_KEY || "",
    appEnv: process.env.APP_ENV || "",
    expectedStagingRef: process.env.EXPECTED_STAGING_SUPABASE_REF || "",
    prodRef: process.env.PROD_SUPABASE_REF || "",
    password: process.env.STAGING_QA_PASSWORD || "",
    email: (process.env.STAGING_QA_EMAIL || DEFAULT_QA_EMAIL).toLowerCase(),
    role: (process.env.STAGING_QA_ROLE || "manager").toLowerCase() as QaRole,
    fixtureCallId: process.env.STAGING_QA_FIXTURE_CALL_ID || STAGING_TENANT.stubProofCallId,
  };
}

function die(msg: string): never {
  console.error(`✗ ${msg}`);
  process.exit(1);
}

function guardOrExit(env: ReturnType<typeof readEnv>): void {
  if (!env.url || !env.serviceKey) die("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  const issues = assertSafeStagingTarget({
    appEnv: env.appEnv,
    supabaseRef: refFromSupabaseUrl(env.url),
    expectedStagingRef: env.expectedStagingRef,
    prodRef: env.prodRef,
  });
  if (issues.length) {
    console.error("✗ Refusing to run — target is not a confirmed staging project:");
    for (const i of issues) console.error(`  • ${i}`);
    process.exit(1);
  }
  if (env.role !== "manager" && env.role !== "rep") die("STAGING_QA_ROLE must be 'manager' or 'rep'");
}

/** Find an existing QA auth user id by email (paged admin listing). */
async function findAuthUserId(admin: SupabaseClient, email: string): Promise<string | null> {
  // Prefer the application-owned profile bridge. Older malformed auth.users seed
  // rows can make GoTrue's global listUsers endpoint fail before it reaches the
  // valid disposable QA identity, while profiles.user_id gives us its exact ID.
  const { data: profile } = await admin
    .from("profiles")
    .select("user_id")
    .eq("email", email)
    .maybeSingle();
  if (profile?.user_id) {
    const { data, error } = await admin.auth.admin.getUserById(profile.user_id);
    const foundEmail = (data?.user?.email || "").toLowerCase();
    if (!error && foundEmail === email.toLowerCase()) return profile.user_id;
  }

  let page = 1;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { data, error } = await admin.auth.admin.listUsers({ page, perPage: 1000 });
    if (error || !data?.users?.length) return null;
    const found = data.users.find((u) => (u.email || "").toLowerCase() === email.toLowerCase());
    if (found) return found.id;
    if (data.users.length < 1000) return null;
    page += 1;
  }
}

async function createIdentity(admin: SupabaseClient, env: ReturnType<typeof readEnv>): Promise<string> {
  if (!env.password) die("STAGING_QA_PASSWORD is required for `create` (value is never printed)");

  let authId: string | null = null;
  const { data, error } = await admin.auth.admin.createUser({
    ...buildQaAuthUserPayload(env.email, STAGING_TENANT.orgId, env.role),
    password: env.password,
  });
  if (!error && data?.user?.id) {
    authId = data.user.id;
    console.log("  ✓ Auth identity created via Admin Auth API");
  } else {
    authId = await findAuthUserId(admin, env.email);
    if (authId) {
      // Idempotent repair: reset the password so sign-in is deterministic.
      await admin.auth.admin.updateUserById(authId, { password: env.password, email_confirm: true });
      console.log("  ✓ Auth identity already existed — password/confirmation reset");
    } else {
      die(`Could not create or find the QA auth user (${error?.message ?? "unknown error"})`);
    }
  }

  const profile = buildQaProfileRow(authId!, { email: env.email, role: env.role });
  const { error: pErr } = await admin.from("profiles").upsert(profile, { onConflict: "user_id" });
  if (pErr) die(`profiles upsert failed: ${pErr.message}`);
  console.log("  ✓ profiles row upserted (tenant role bound)");

  const rep = buildQaRepRow(authId!, {
    orgId: STAGING_TENANT.orgId,
    companyId: STAGING_TENANT.companyId,
    email: env.email,
    role: env.role,
  });
  const { error: rErr } = await admin.from("reps").upsert(rep, { onConflict: "id" });
  if (rErr) die(`reps upsert failed: ${rErr.message}`);
  console.log("  ✓ reps tenant bridge upserted (org/company membership)");

  return authId!;
}

async function statusIdentity(admin: SupabaseClient, env: ReturnType<typeof readEnv>): Promise<void> {
  const authId = await findAuthUserId(admin, env.email);
  console.log(`  Auth identity : ${authId ? "present" : "absent"}`);
  if (!authId) return;
  const { data: prof } = await admin.from("profiles").select("role").eq("user_id", authId).maybeSingle();
  const { data: rep } = await admin.from("reps").select("org_id, company_id, tier").eq("id", authId).maybeSingle();
  console.log(`  profiles row  : ${prof ? `present (role=${prof.role})` : "absent"}`);
  console.log(
    `  reps bridge   : ${
      rep ? `present (org matches synthetic=${rep.org_id === STAGING_TENANT.orgId})` : "absent"
    }`
  );
}

async function verifyIdentity(admin: SupabaseClient, env: ReturnType<typeof readEnv>): Promise<void> {
  if (!env.anonKey) die("SUPABASE_ANON_KEY is required for `verify` (password sign-in)");
  if (!env.password) die("STAGING_QA_PASSWORD is required for `verify` (value is never printed)");

  // Ensure the identity exists (idempotent) before proving login.
  const authId = await createIdentity(admin, env);

  // 1) Password sign-in through the public anon client → prove a STAGING issuer.
  const anon = createClient(env.url, env.anonKey, { auth: { persistSession: false } });
  const { data: session, error: sErr } = await anon.auth.signInWithPassword({
    email: env.email,
    password: env.password,
  });
  const token = session?.session?.access_token || "";
  if (sErr || !token) die(`Password sign-in failed: ${sErr?.message ?? "no session"}`);
  const issuerHost = issuerHostFromJwt(token);
  const expectedHost = (() => {
    try {
      return new URL(env.url).host.toLowerCase();
    } catch {
      return "";
    }
  })();
  const issuerOk = !!issuerHost && issuerHost === expectedHost;
  console.log(`  ✓ Password sign-in succeeded — token issuer host is staging: ${issuerOk}`);
  if (!issuerOk) die("Sign-in token issuer host is not the staging project");

  // 2) Tenant-scoped fixture access, evaluated with the API's own rule.
  const { data: call, error: cErr } = await admin
    .from("calls")
    .select("user_id, org_id")
    .eq("id", env.fixtureCallId)
    .maybeSingle();
  if (cErr || !call) die(`Synthetic fixture call not found (${env.fixtureCallId}) — run the SQL seed first`);

  const { data: repRow } = await admin.from("reps").select("org_id").eq("id", authId).maybeSingle();
  const requesterOrgId = repRow?.org_id ?? null;

  const canSee = evaluateTenantScope({
    requester: authId,
    requesterOrgId,
    callUserId: call.user_id,
    callOrgId: call.org_id,
    visibility: "everyone",
  });
  const cannotSeeForeign = !evaluateTenantScope({
    requester: authId,
    requesterOrgId,
    callUserId: "00000000-0000-4000-8000-0000000000ff",
    callOrgId: "00000000-9999-4000-8000-000000009999", // a different (foreign) org
    visibility: "everyone",
  });
  console.log(`  ✓ Tenant-scoped fixture visible to QA identity : ${canSee}`);
  console.log(`  ✓ Foreign-org call correctly NOT visible       : ${cannotSeeForeign}`);
  if (!canSee || !cannotSeeForeign) die("Tenant-scope check failed");

  console.log("  ✓ verify complete — sign-in + tenant-scoped access proven (no secrets printed)");
}

async function deleteIdentity(admin: SupabaseClient, env: ReturnType<typeof readEnv>): Promise<void> {
  const authId = await findAuthUserId(admin, env.email);
  // Remove the tenant bridge first (no FK cascade covers reps.id).
  const { error: rErr } = await admin.from("reps").delete().eq("email", env.email);
  if (rErr) console.warn(`  ⚠ reps delete: ${rErr.message}`);
  else console.log("  ✓ reps tenant bridge deleted");

  if (authId) {
    // profiles.user_id → auth.users ON DELETE CASCADE removes the profile row too.
    const { error: dErr } = await admin.auth.admin.deleteUser(authId);
    if (dErr) die(`Auth identity delete failed: ${dErr.message}`);
    console.log("  ✓ Auth identity deleted (profiles row cascades)");
  } else {
    // Defensive: drop any orphan profile if the auth user was already gone.
    await admin.from("profiles").delete().eq("email", env.email);
    console.log("  • No Auth identity found — nothing to delete (idempotent)");
  }
}

/** Read the current presence of all three artefacts (used by teardown). */
async function probePresence(admin: SupabaseClient, email: string): Promise<PresenceState> {
  const authId = await findAuthUserId(admin, email);
  const { data: rep } = await admin.from("reps").select("id").eq("email", email).maybeSingle();
  let profilePresent = false;
  if (authId) {
    const { data: prof } = await admin.from("profiles").select("user_id").eq("user_id", authId).maybeSingle();
    profilePresent = !!prof;
  }
  return { authPresent: !!authId, repPresent: !!rep, profilePresent };
}

/**
 * Day 278 — teardown discipline. Always `delete`, then re-check and FAIL VISIBLY
 * (exit 1) if anything remains. This is the command an audit runs at the end so a
 * disposable identity is never silently left behind.
 */
async function teardownIdentity(admin: SupabaseClient, env: ReturnType<typeof readEnv>): Promise<void> {
  await deleteIdentity(admin, env);
  const state = await probePresence(admin, env.email);
  const verdict = evaluateTeardown(state);
  console.log(
    `  Post-delete presence: auth=${state.authPresent} reps=${state.repPresent} profile=${state.profilePresent}`
  );
  if (verdict.absent) {
    console.log("  ✓ TEARDOWN VERIFIED — identity absent (auth + reps + profile all gone)");
  } else {
    die(`TEARDOWN INCOMPLETE — still present: ${verdict.remaining.join(", ")}`);
  }
}

async function main() {
  const action = (process.argv[2] || "").toLowerCase() as Action;
  if (!["create", "verify", "status", "delete", "teardown"].includes(action)) {
    die("Usage: npm run staging:qa -- <create|verify|status|delete|teardown>");
  }

  const env = readEnv();
  guardOrExit(env);

  const admin = createClient(env.url, env.serviceKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  // Refs are treated as sensitive (compared, never printed) — mirror Day 271.
  const refMatches = refFromSupabaseUrl(env.url) === env.expectedStagingRef;
  console.log(`\n  Gravix Staging QA identity — ${action}`);
  console.log(`  Target        : confirmed staging project (ref matches expected=${refMatches}, APP_ENV=${env.appEnv})`);
  console.log(`  QA email      : ${env.email}  (role=${env.role})`);
  console.log(`  Password env  : STAGING_QA_PASSWORD ${redact(env.password)} (never printed)\n`);

  try {
    if (action === "create") await createIdentity(admin, env);
    else if (action === "verify") await verifyIdentity(admin, env);
    else if (action === "status") await statusIdentity(admin, env);
    else if (action === "delete") await deleteIdentity(admin, env);
    else if (action === "teardown") await teardownIdentity(admin, env);
    console.log("\n  ✓ Done.\n");
  } catch (err) {
    die(err instanceof Error ? err.message : String(err));
  }
}

// Only run the CLI when invoked directly (the validator imports the pure core).
const invokedDirectly =
  typeof process.argv[1] === "string" && /provision-staging-qa\.ts$/.test(process.argv[1]);
if (invokedDirectly) {
  void main();
}
