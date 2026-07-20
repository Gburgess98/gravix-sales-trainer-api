/**
 * validate-coach-score-drift.ts
 *
 * Day 242 — locks in the coach.ts and call_scores schema-drift fixes.
 *
 * POST /v1/coach/assign was completely broken, not merely degraded. The
 * insert payload carried `source` and `meta`, which coach_assignments does
 * not have, so every request failed with PGRST204. The retry beside it only
 * fires for a missing org_id, so nothing recovered and the route answered
 * 400 for every coaching assignment. The duplicate check above it selected
 * the same two columns, failed with 42703, and was logged as a warning —
 * so even had the insert worked, duplicates would never have been detected.
 * A third bug sat in the linked crm_activities write: `summary` is not a
 * column there either, so that insert failed too and was swallowed by its
 * try/catch. Provenance now rides in meta, which is that row's freeform
 * jsonb.
 *
 * The call_scores family: `score` (calls.ts) and `total_score` (crm.ts) are
 * both the column `overall`. Reads are aliased so response keys are
 * unchanged; the score-history writer was inserting `score` and failing
 * behind a warn-only handler, so no snapshot was ever persisted.
 *
 * Coverage:
 *   ✓ coach/assign creates an assignment (200) and persists the row
 *   ✓ the linked crm_activities row is actually written (activity_debug.ok)
 *   ✓ provenance survives in the activity's meta
 *   ✓ the reporting block keeps its shape and honest values
 *   ✓ duplicate assignments are detected instead of silently duplicated
 *   ✓ ownership guards still answer 403/404
 *   ✓ call_scores history accepts the writer's payload shape
 *   ✓ crm coaching-history averages call_scores instead of reporting 0
 *
 * Self-cleaning: every row it creates is removed at the end. It never
 * writes to the UFC demo company (Day 224 rule).
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:coach-score-drift
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
  const h = crypto.createHash("sha256").update(`DAY242::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function post(path: string, userId: string, body: object) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-user-id": userId },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function get(path: string, userId: string, orgId?: string) {
  const headers: Record<string, string> = { "x-user-id": userId };
  if (orgId) headers["x-org-id"] = orgId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(30_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

const ORG = uid("org");
const OWNER = uid("owner");
const STRANGER = uid("stranger");
const CALL = uid("call");
const GHOST_CALL = uid("ghost-call");
const DRILL = "day242-validator-drill";

// Two snapshots -> a known average, so the assertion is exact rather than
// "greater than zero".
const SCORES = [40, 80];
const EXPECTED_AVG = 60;
const SCORE_IDS = SCORES.map((_, i) => uid(`score-${i}`));

async function seedFixtures() {
  const { error: orgErr } = await supa
    .from("orgs")
    .upsert([{ id: ORG, name: "Day242 Org (validator)" }], { onConflict: "id" });
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: OWNER, name: "Day242 Owner", tier: "SalesRep", org_id: ORG },
      { id: STRANGER, name: "Day242 Stranger", tier: "SalesRep", org_id: ORG },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  const { error: callErr } = await supa.from("calls").upsert(
    [{
      id: CALL,
      user_id: OWNER,
      org_id: ORG,
      filename: "day242.mp3",
      storage_path: `day242/${CALL}.mp3`,
      audio_path: `day242/${CALL}.mp3`,
      score_overall: 60,
      created_at: new Date().toISOString(),
    }],
    { onConflict: "id" }
  );
  if (callErr) throw new Error(`fixture call upsert failed: ${callErr.message}`);

  await supa.from("call_scores").delete().in("id", SCORE_IDS);
  const { error: scoreErr } = await supa.from("call_scores").upsert(
    SCORES.map((overall, i) => ({
      id: SCORE_IDS[i],
      call_id: CALL,
      user_id: OWNER,
      overall,
      created_at: new Date().toISOString(),
    })),
    { onConflict: "id" }
  );
  if (scoreErr) throw new Error(`fixture call_scores upsert failed: ${scoreErr.message}`);

  await cleanupAssignments();
}

async function cleanupAssignments() {
  await supa.from("crm_activities").delete().eq("call_id", CALL);
  await supa.from("coach_assignments").delete().eq("call_id", CALL);
}

async function cleanup() {
  await cleanupAssignments();
  await supa.from("call_scores").delete().in("id", SCORE_IDS);
  await supa.from("calls").delete().eq("id", CALL);
  await supa.from("reps").delete().in("id", [OWNER, STRANGER]);
  await supa.from("orgs").delete().eq("id", ORG);
  console.log("\n  Cleanup: removed validator org, rep, call, scores and assignments.");
}

const REPORTING_KEYS = [
  "assignment_origin", "flagged_call", "threshold_band",
  "review_flag_count", "needs_manager_review",
];

async function main() {
  console.log("\nDay 242 — coach assign + call_scores drift\n");

  await seedFixtures();

  // ── coach/assign: the create path ────────────────────────────────────────
  const created = await post("/v1/coach/assign", OWNER, {
    callId: CALL, assigneeUserId: OWNER, drillId: DRILL, notes: "day242",
  });

  c("coach/assign creates an assignment → 200",
    created.status === 200 && created.data?.ok === true,
    `got ${created.status} ${JSON.stringify(created.data)?.slice(0, 160)}`);

  c("coach/assign persists the coach_assignments row",
    Boolean(created.data?.item?.id) && String(created.data?.item?.call_id) === CALL,
    `got ${JSON.stringify(created.data?.item)?.slice(0, 160)}`);

  c("coach/assign writes the linked crm_activities row",
    created.data?.activity_debug?.attempted === true &&
    created.data?.activity_debug?.ok === true,
    `got ${JSON.stringify(created.data?.activity_debug)}`);

  c("coach/assign reporting block keeps its shape",
    REPORTING_KEYS.every((k) => k in (created.data?.reporting ?? {})),
    `got ${JSON.stringify(created.data?.reporting)}`);

  c("coach/assign reports honest provenance for an unflagged call",
    created.data?.reporting?.assignment_origin === "manual" &&
    created.data?.reporting?.flagged_call === false &&
    created.data?.reporting?.review_flag_count === 0,
    `got ${JSON.stringify(created.data?.reporting)}`);

  // Provenance is no longer stored on coach_assignments; it must still be
  // recoverable from the activity row, which does have source/meta.
  const { data: activity } = await supa
    .from("crm_activities")
    .select("source, meta")
    .eq("call_id", CALL)
    .eq("type", "coach_assignment")
    .maybeSingle();

  c("provenance survives on the activity row (source + meta)",
    String((activity as any)?.source) === "coach_assignment" &&
    String((activity as any)?.meta?.assignment_origin) === "manual" &&
    (activity as any)?.meta?.summary != null,
    `got ${JSON.stringify(activity)?.slice(0, 200)}`);

  // ── coach/assign: the duplicate path ─────────────────────────────────────
  const duplicate = await post("/v1/coach/assign", OWNER, {
    callId: CALL, assigneeUserId: OWNER, drillId: DRILL, notes: "day242 again",
  });

  c("coach/assign detects the duplicate instead of creating a second row",
    duplicate.status === 200 && duplicate.data?.deduped === true,
    `got ${duplicate.status} deduped=${duplicate.data?.deduped}`);

  c("deduped response keeps the reporting shape",
    REPORTING_KEYS.every((k) => k in (duplicate.data?.reporting ?? {})),
    `got ${JSON.stringify(duplicate.data?.reporting)}`);

  const { count: assignmentCount } = await supa
    .from("coach_assignments")
    .select("id", { count: "exact", head: true })
    .eq("call_id", CALL);

  c("exactly one assignment row exists after the duplicate attempt",
    assignmentCount === 1, `got ${assignmentCount}`);

  // ── Guards still hold ────────────────────────────────────────────────────
  const forbidden = await post("/v1/coach/assign", STRANGER, {
    callId: CALL, assigneeUserId: STRANGER, drillId: DRILL,
  });
  c("coach/assign rejects a non-owner → 403",
    forbidden.status === 403, `got ${forbidden.status}`);

  const missing = await post("/v1/coach/assign", OWNER, {
    callId: GHOST_CALL, assigneeUserId: OWNER, drillId: DRILL,
  });
  c("coach/assign rejects an unknown call → 404",
    missing.status === 404, `got ${missing.status}`);

  // ── call_scores writer shape ─────────────────────────────────────────────
  // Guards the POST /v1/calls/:id/score snapshot payload without invoking
  // the scoring model: the column is `overall`, and writing `score` failed
  // behind a warn-only handler so no history was ever kept.
  const writerProbe = uid("writer-probe");
  const { error: writerErr } = await supa.from("call_scores").insert({
    id: writerProbe, call_id: CALL, user_id: OWNER, overall: 55, rubric: {},
  });
  c("call_scores accepts the score-history writer payload",
    !writerErr, `got ${writerErr?.message}`);
  await supa.from("call_scores").delete().eq("id", writerProbe);

  // ── crm coaching-history reads call_scores ───────────────────────────────
  const history = await get(`/v1/crm/reps/${OWNER}/coaching-history`, OWNER, ORG);
  c("crm coaching-history returns 200", history.status === 200, `got ${history.status}`);

  c("crm coaching-history averages call_scores instead of reporting 0",
    Number(history.data?.avg_score) === EXPECTED_AVG,
    `got ${history.data?.avg_score}, expected ${EXPECTED_AVG} (mean of ${SCORES.join(", ")})`);
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
        ? "  Coach + call_scores drift validation PASSED\n"
        : "  Coach + call_scores drift validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
