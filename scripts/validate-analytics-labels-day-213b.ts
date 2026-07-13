/**
 * validate-analytics-labels-day-213b.ts
 *
 * Day 213B — /v1/crm/analytics/activity-by-rep returns human rep labels.
 *
 * Coverage:
 *   ✓ endpoint responds ok for the demo org
 *   ✓ response shape preserved (ok/scope/rep_id/requester_id/reps[])
 *   ✓ every row keeps the full internal rep_id (UUID)
 *   ✓ no rep_name is UUID-shaped (human label, email local part, or null)
 *   ✓ UFC demo resolves real names (Nate Diaz, Anderson Silva)
 *   ✓ repId filter still works and stays humanised
 *   ✓ org isolation: foreign org id sees none of the demo org's reps
 *
 * Requirements: server running (npm run dev), UFC demo seed applied.
 * Usage: npm run validate:analytics-labels
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

async function hit(path: string, userId: string, orgId: string) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "x-user-id": userId, "x-org-id": orgId },
    signal: AbortSignal.timeout(15_000),
  });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function main() {
  console.log("\n  Analytics Labels Validator — Day 213B\n");

  const { data: dana } = await supa
    .from("reps")
    .select("id, org_id")
    .eq("email", "dana.white@ufcelite.demo")
    .maybeSingle();
  const { data: nate } = await supa
    .from("reps")
    .select("id")
    .eq("email", "nate.diaz@ufcelite.demo")
    .maybeSingle();
  if (!dana?.id || !(dana as any)?.org_id || !nate?.id) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }
  const danaId = String((dana as any).id);
  const orgId  = String((dana as any).org_id);

  // ── Team scope ────────────────────────────────────────────────────────────
  const team = await hit(`/v1/crm/analytics/activity-by-rep?days=90`, danaId, orgId);
  c("endpoint responds ok for demo org", team.status === 200 && team.data?.ok === true, `got ${team.status}`);

  const rows: any[] = team.data?.reps ?? [];
  c("response shape preserved (scope/rep_id/requester_id/reps)",
    team.data?.scope === "team" && "rep_id" in (team.data ?? {}) && "requester_id" in (team.data ?? {}) && Array.isArray(rows));

  c("reps returned for demo org", rows.length > 0, `got ${rows.length}`);

  c("every row keeps full internal rep_id (UUID)",
    rows.length > 0 && rows.every((r) => UUID_RE.test(String(r.rep_id))));

  const uuidNames = rows.filter((r) => r.rep_name != null && UUID_RE.test(String(r.rep_name)));
  c("no rep_name is UUID-shaped", uuidNames.length === 0, `${uuidNames.length} leaked`);

  c("rep_name is string-or-null with counts intact",
    rows.every((r) =>
      (r.rep_name === null || typeof r.rep_name === "string") &&
      Number.isFinite(r.activities_created) && Number.isFinite(r.activities_completed)));

  const names = new Set(rows.map((r) => r.rep_name));
  c("UFC demo resolves real names (Nate Diaz, Anderson Silva)",
    names.has("Nate Diaz") && names.has("Anderson Silva"),
    `got ${[...names].slice(0, 6).join(", ")}`);

  // ── Rep filter ────────────────────────────────────────────────────────────
  const single = await hit(
    `/v1/crm/analytics/activity-by-rep?days=90&repId=${String(nate.id)}`,
    danaId,
    orgId
  );
  const singleRows: any[] = single.data?.reps ?? [];
  c("repId filter works and stays humanised",
    single.status === 200 &&
    single.data?.scope === "rep" &&
    singleRows.length === 1 &&
    singleRows[0]?.rep_id === String(nate.id) &&
    singleRows[0]?.rep_name === "Nate Diaz",
    JSON.stringify(singleRows).slice(0, 120));

  // ── Org isolation ─────────────────────────────────────────────────────────
  const foreignOrg = "00000000-0000-4000-8000-0000000213b0".slice(0, 36);
  const foreign = await hit(`/v1/crm/analytics/activity-by-rep?days=90`, danaId, foreignOrg);
  const foreignRows: any[] = foreign.data?.reps ?? [];
  const demoIds = new Set(rows.map((r) => String(r.rep_id)));
  c("foreign org sees none of the demo org's reps",
    foreign.status === 200 && foreignRows.every((r) => !demoIds.has(String(r.rep_id))),
    `got ${foreignRows.length} rows`);

  const failed = checks.filter((x) => !x.passed);
  console.log(`\n  ${checks.length - failed.length}/${checks.length} checks passed\n`);
  if (failed.length) process.exit(1);
}

main().catch((e) => {
  console.error("  validator crashed:", e?.message || e);
  process.exit(1);
});
