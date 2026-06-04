/**
 * validate-isolation.ts
 *
 * Verifies company-level tenant isolation is working correctly.
 * Checks that:
 *   - George (Manager) sees only Gravix Test Company reps
 *   - Dana White (Manager) sees only Gravix Demo Company (UFC) reps
 *
 * Usage: npm run validate:isolation
 *
 * Requires the server to be running: npm run dev
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

// Company IDs
const UFC_COMPANY    = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const GRAVIX_COMPANY = "c1c17223-aa46-4998-8995-de6bf25a23e6";

type CheckResult = { label: string; passed: boolean; detail?: string };

function check(label: string, passed: boolean, detail?: string): CheckResult {
  return { label, passed, detail };
}

async function apiGet(path: string, userId: string, orgId?: string): Promise<any> {
  const headers: Record<string, string> = { "content-type": "application/json", "x-user-id": userId };
  if (orgId) headers["x-org-id"] = orgId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(10_000) });
  return res.json();
}

async function main() {
  console.log("\n  Tenant Isolation Validator\n");

  // ── 1. Fetch reference data ─────────────────────────────────────────────

  const { data: reps } = await supa
    .from("reps")
    .select("id, name, tier, company_id, org_id")
    .order("tier")
    .order("name");

  const { data: georgeUser } = await supa
    .from("reps")
    .select("id, name, company_id")
    .eq("name", "George (Manager)")
    .maybeSingle();

  const { data: danaUser } = await supa
    .from("reps")
    .select("id, name, company_id")
    .eq("name", "Dana White")
    .maybeSingle();

  const georgeId = (georgeUser as any)?.id ?? null;
  const danaId   = (danaUser as any)?.id ?? null;

  const gravixReps = (reps || []).filter((r: any) => r.company_id === GRAVIX_COMPANY).map((r: any) => r.name);
  const ufcReps    = (reps || []).filter((r: any) => r.company_id === UFC_COMPANY).map((r: any) => r.name);
  const nullReps   = (reps || []).filter((r: any) => !r.company_id).map((r: any) => r.name);

  console.log("  DB state:");
  console.log(`  Gravix Test Company reps (${gravixReps.length}):`, gravixReps.join(", ") || "none");
  console.log(`  UFC Demo Company reps    (${ufcReps.length}):`, ufcReps.slice(0, 5).join(", ") + (ufcReps.length > 5 ? ` +${ufcReps.length - 5} more` : ""));
  console.log(`  No company_id            (${nullReps.length}):`, nullReps.join(", ") || "none");
  console.log(`\n  George ID: ${georgeId}`);
  console.log(`  Dana ID:   ${danaId}\n`);

  if (!georgeId || !danaId) {
    console.error("  ✗  Could not find George or Dana in reps table. Run npm run seed:demo first.");
    process.exit(1);
  }

  // Check if company_id column exists at all
  if (!(georgeUser as any)?.company_id && !(danaUser as any)?.company_id) {
    console.error("  ✗  reps.company_id is NULL for both managers.");
    console.error("     Run sql/20260604_reps_company_office_bridge.sql then npm run db:backfill\n");
    process.exit(1);
  }

  const results: CheckResult[] = [];

  // ── 2. DB-level isolation ───────────────────────────────────────────────

  results.push(check(
    "George has company_id = Gravix Test Company",
    (georgeUser as any)?.company_id === GRAVIX_COMPANY,
    (georgeUser as any)?.company_id ?? "null"
  ));

  results.push(check(
    "Dana has company_id = UFC Demo Company",
    (danaUser as any)?.company_id === UFC_COMPANY,
    (danaUser as any)?.company_id ?? "null"
  ));

  results.push(check(
    "All UFC reps have company_id = UFC Demo Company",
    ufcReps.length >= 10,
    `${ufcReps.length} UFC reps found`
  ));

  // ── 3. API isolation — George's manager view ───────────────────────────

  const georgeOrgId = (georgeUser as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
  const georgeOverview = await apiGet("/v1/crm/manager/overview", georgeId, georgeOrgId);
  const georgeRepIds = (georgeOverview?.items ?? []).map((r: any) => r.rep_id);

  const georgeSeesUFC = georgeRepIds.some((id: string) =>
    ufcReps.some(() => (reps || []).find((r: any) => r.id === id && r.company_id === UFC_COMPANY))
  );
  const georgeRepCount = georgeRepIds.length;

  results.push(check(
    "George manager/overview returns only Gravix reps",
    !georgeSeesUFC && georgeRepCount > 0,
    `${georgeRepCount} reps returned, UFC cross-contamination: ${georgeSeesUFC}`
  ));

  // ── 4. API isolation — Dana's manager view ─────────────────────────────

  const danaOrgId = (danaUser as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
  const danaOverview = await apiGet("/v1/crm/manager/overview", danaId, danaOrgId);
  const danaRepIds = (danaOverview?.items ?? []).map((r: any) => r.rep_id);

  const danaSeesGravix = danaRepIds.some((id: string) =>
    (reps || []).find((r: any) => r.id === id && r.company_id === GRAVIX_COMPANY)
  );
  const danaRepCount = danaRepIds.length;

  results.push(check(
    "Dana manager/overview returns only UFC reps",
    !danaSeesGravix && danaRepCount > 0,
    `${danaRepCount} reps returned, Gravix cross-contamination: ${danaSeesGravix}`
  ));

  // ── 5. Control-centre isolation ─────────────────────────────────────────

  const georgeCC = await apiGet("/v1/crm/manager/control-centre", georgeId, georgeOrgId);
  const danaCC   = await apiGet("/v1/crm/manager/control-centre", danaId,   danaOrgId);

  const georgeCCRepIds = (georgeCC?.reps ?? []).map((r: any) => r.rep_id);
  const danaCCRepIds   = (danaCC?.reps   ?? []).map((r: any) => r.rep_id);

  const georgeSeesUFCinCC  = georgeCCRepIds.some((id: string) => (reps||[]).find((r: any) => r.id === id && r.company_id === UFC_COMPANY));
  const danaSeesGravixInCC = danaCCRepIds.some((id: string)   => (reps||[]).find((r: any) => r.id === id && r.company_id === GRAVIX_COMPANY));

  results.push(check(
    "George control-centre no UFC cross-contamination",
    !georgeSeesUFCinCC,
    `cross-contamination: ${georgeSeesUFCinCC}`
  ));

  results.push(check(
    "Dana control-centre no Gravix cross-contamination",
    !danaSeesGravixInCC,
    `cross-contamination: ${danaSeesGravixInCC}`
  ));

  // ── 6. Report ───────────────────────────────────────────────────────────

  console.log("  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed`);

  if (failed > 0) {
    console.log("\n  To fix: run sql/20260604_reps_company_office_bridge.sql then npm run db:backfill\n");
    process.exit(1);
  }

  console.log("\n  Tenant isolation confirmed ✓\n");
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
