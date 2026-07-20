/**
 * validate-dashboard-schema-drift.ts
 *
 * Day 241 — locks in the src/routes/dashboard.ts schema-drift fixes.
 *
 * Three live routes were reading columns their tables do not have. None
 * returned an error: every failure was swallowed, so each surface served
 * plausible-looking but wrong numbers.
 *
 *   GET /v1/dashboard/leaderboard
 *     Name hydration read profiles.display_name then users.full_name.
 *     profiles is keyed by user_id with the name in full_name, and `users`
 *     has no name column at all, so both 42703'd and every rep rendered as
 *     the "Rep" placeholder. Now profiles(user_id, full_name) → reps(name).
 *
 *   GET /v1/dashboard/reporting-summary
 *     Selected coach_assignments.source/meta, which live on the separate
 *     `assignments` table. The error message contains "does not exist", so
 *     the guard meant to tolerate a missing TABLE swallowed a missing
 *     COLUMN and every assignment metric reported zero.
 *
 *   GET /v1/dashboard/rep-summary
 *     Selected and filtered assignments.org_id; that table is scoped by
 *     office_id/company_id and has no org column, so drill XP silently
 *     never counted.
 *
 * Coverage:
 *   ✓ all three routes answer 200 and keep their response keys
 *   ✓ leaderboard shows human names, never the "Rep" placeholder or a
 *     raw UUID, for reps that have a name
 *   ✓ the reps fallback path specifically works (fixture reps are absent
 *     from the sparse profiles table, so they can only resolve via reps)
 *   ✓ reporting-summary counts assignments that exist, instead of zero
 *   ✓ rep-summary XP includes drill XP, at exactly the documented rate
 *   ✓ leaderboard org scoping still excludes another org's reps
 *
 * Self-cleaning: every org, rep, call and assignment it creates is removed
 * at the end. It never writes to the UFC demo company (Day 224 rule).
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:dashboard-schema-drift
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
  const h = crypto.createHash("sha256").update(`DAY241::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function hit(path: string, userId?: string) {
  const headers: Record<string, string> = {};
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(30_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

// ── Fixtures ────────────────────────────────────────────────────────────────
const ORG = uid("org");
const OTHER_ORG = uid("other-org");
const MANAGER = uid("manager");
const REP_A = uid("rep-a");
const REP_B = uid("rep-b");
const REP_OTHER = uid("rep-other");

const REP_A_NAME = "Day241 Alpha Rep";
const REP_B_NAME = "Day241 Beta Rep";
const REP_OTHER_NAME = "Day241 Other-Org Rep";

// REP_A: 3 scored calls, two at/above the 70 XP threshold -> 20 call XP.
// REP_B: 3 scored calls, none at threshold -> 0 call XP, lower average.
const CALLS: Array<[string, string, number]> = [
  [uid("call-a1"), REP_A, 80],
  [uid("call-a2"), REP_A, 80],
  [uid("call-a3"), REP_A, 50],
  [uid("call-b1"), REP_B, 60],
  [uid("call-b2"), REP_B, 60],
  [uid("call-b3"), REP_B, 60],
  [uid("call-o1"), REP_OTHER, 90],
  [uid("call-o2"), REP_OTHER, 90],
  [uid("call-o3"), REP_OTHER, 90],
];

// Two completed of three -> 10 drill XP for REP_A.
// "assigned"/"completed" and type "sparring" are the values already in use —
// assignments carries a check constraint on type.
const ASSIGNMENTS: Array<[string, string]> = [
  [uid("as-1"), "completed"],
  [uid("as-2"), "completed"],
  [uid("as-3"), "assigned"],
];

const COACH_ASSIGNMENTS = [uid("ca-1"), uid("ca-2")];

const EXPECTED_CALL_XP = 20;   // 2 calls >= 70, +10 each
const EXPECTED_DRILL_XP = 10;  // 2 completed assignments, +5 each
const EXPECTED_TOTAL_XP = EXPECTED_CALL_XP + EXPECTED_DRILL_XP;

const CALL_IDS = CALLS.map(([id]) => id);
const REP_IDS = [MANAGER, REP_A, REP_B, REP_OTHER];

function iso(daysAgo: number) {
  return new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000).toISOString();
}

async function seedFixtures() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [
      { id: ORG, name: "Day241 Org (validator)" },
      { id: OTHER_ORG, name: "Day241 Other Org (validator)" },
    ],
    { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: MANAGER, name: "Day241 Manager", tier: "Manager", org_id: ORG },
      { id: REP_A, name: REP_A_NAME, tier: "SalesRep", org_id: ORG },
      { id: REP_B, name: REP_B_NAME, tier: "SalesRep", org_id: ORG },
      { id: REP_OTHER, name: REP_OTHER_NAME, tier: "SalesRep", org_id: OTHER_ORG },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  await supa.from("calls").delete().in("id", CALL_IDS);
  const { error: callErr } = await supa.from("calls").upsert(
    CALLS.map(([id, repId, score], i) => ({
      id,
      user_id: repId,
      org_id: repId === REP_OTHER ? OTHER_ORG : ORG,
      filename: "day241.mp3",
      storage_path: `day241/${id}.mp3`,
      audio_path: `day241/${id}.mp3`,
      score_overall: score,
      created_at: iso(i + 2),
    })),
    { onConflict: "id" }
  );
  if (callErr) throw new Error(`fixture call upsert failed: ${callErr.message}`);

  const { error: asErr } = await supa.from("assignments").upsert(
    ASSIGNMENTS.map(([id, status]) => ({
      id,
      rep_id: REP_A,
      manager_id: MANAGER,
      type: "sparring",
      status,
      created_at: iso(3),
    })),
    { onConflict: "id" }
  );
  if (asErr) throw new Error(`fixture assignment upsert failed: ${asErr.message}`);

  const { error: caErr } = await supa.from("coach_assignments").upsert(
    COACH_ASSIGNMENTS.map((id, i) => ({
      id,
      assignee_user_id: REP_A,
      call_id: CALL_IDS[i],
      drill_id: "day241-drill", // free text in this table, not a FK
      org_id: ORG,
      status: i === 0 ? "completed" : "open",
      created_at: iso(3),
    })),
    { onConflict: "id" }
  );
  if (caErr) throw new Error(`fixture coach_assignment upsert failed: ${caErr.message}`);
}

async function cleanup() {
  await supa.from("coach_assignments").delete().in("id", COACH_ASSIGNMENTS);
  await supa.from("assignments").delete().in("id", ASSIGNMENTS.map(([id]) => id));
  await supa.from("calls").delete().in("id", CALL_IDS);
  await supa.from("reps").delete().in("id", REP_IDS);
  await supa.from("orgs").delete().in("id", [ORG, OTHER_ORG]);
  console.log("\n  Cleanup: removed validator orgs, reps, calls and assignments.");
}

const LEADERBOARD_KEYS = ["user_id", "name", "avg_score", "calls", "xp"];

async function main() {
  console.log("\nDay 241 — dashboard schema drift\n");

  await seedFixtures();

  // ── Leaderboard ──────────────────────────────────────────────────────────
  const lb = await hit(`/v1/dashboard/leaderboard?days=30&limit=50&minCalls=3&orgId=${ORG}`, MANAGER);
  c("leaderboard returns 200", lb.status === 200, `got ${lb.status} ${JSON.stringify(lb.data)?.slice(0, 120)}`);

  const items: any[] = lb.data?.items ?? [];
  c("leaderboard response shape preserved",
    items.length > 0 && LEADERBOARD_KEYS.every((k) => k in items[0]),
    `keys ${JSON.stringify(Object.keys(items[0] ?? {}))}`);

  const byId = new Map(items.map((i) => [String(i.user_id), i]));

  c("leaderboard resolves a human name via reps (profiles has no row for these reps)",
    byId.get(REP_A)?.name === REP_A_NAME && byId.get(REP_B)?.name === REP_B_NAME,
    `got ${JSON.stringify(items.map((i) => i.name))}`);

  c("leaderboard shows no 'Rep' placeholder where a name exists",
    items.every((i) => String(i.name) !== "Rep"),
    `got ${JSON.stringify(items.map((i) => i.name))}`);

  c("leaderboard shows no raw UUID as a name",
    items.every((i) => !UUID_RE.test(String(i.name ?? ""))),
    `got ${JSON.stringify(items.map((i) => i.name))}`);

  c("leaderboard aggregates scores correctly",
    Math.round(Number(byId.get(REP_A)?.avg_score)) === 70 &&
    Number(byId.get(REP_A)?.calls) === 3,
    `got ${JSON.stringify(byId.get(REP_A))}`);

  // Org scoping: the other org's rep outscores everyone but must not appear.
  c("leaderboard org scoping excludes another org's rep",
    !byId.has(REP_OTHER),
    `got ${JSON.stringify(items.map((i) => i.user_id))}`);

  const otherLb = await hit(`/v1/dashboard/leaderboard?days=30&limit=50&minCalls=3&orgId=${OTHER_ORG}`, MANAGER);
  const otherIds = (otherLb.data?.items ?? []).map((i: any) => String(i.user_id));
  c("leaderboard scoped to the other org excludes this org's reps",
    otherIds.includes(REP_OTHER) && !otherIds.includes(REP_A) && !otherIds.includes(REP_B),
    `got ${JSON.stringify(otherIds)}`);

  // ── Reporting summary ────────────────────────────────────────────────────
  const rs = await hit(`/v1/dashboard/reporting-summary?days=30`, MANAGER);
  c("reporting-summary returns 200", rs.status === 200, `got ${rs.status}`);

  const assignmentsCounted = Number(
    rs.data?.assignments ?? rs.data?.totals?.assignments ?? NaN
  );
  c("reporting-summary counts coach assignments instead of silently zero",
    Number.isFinite(assignmentsCounted) && assignmentsCounted >= COACH_ASSIGNMENTS.length,
    `got ${assignmentsCounted} (expected at least ${COACH_ASSIGNMENTS.length})`);

  c("reporting-summary keeps its assignment metric keys",
    [
      "auto_assignments_created", "manual_assignments_created",
      "assignments_from_critical_flags", "assignments_from_low_flags",
      "assignment_auto_rate", "assignment_completion_rate",
    ].every((k) => k in (rs.data ?? {})),
    `keys ${JSON.stringify(Object.keys(rs.data ?? {}).filter((k) => k.includes("assignment")))}`);

  // coach_assignments has no provenance columns, so every row counts as
  // manual and the auto rate is structurally 0. Asserted so the intent is
  // explicit: it is a documented consequence of the schema, not a wrong sum.
  c("reporting-summary attributes assignments as manual (no provenance on that table)",
    Number(rs.data?.auto_assignments_created) === 0 &&
    Number(rs.data?.manual_assignments_created) === assignmentsCounted &&
    Number(rs.data?.assignment_auto_rate) === 0,
    `auto ${rs.data?.auto_assignments_created}, manual ${rs.data?.manual_assignments_created}, rate ${rs.data?.assignment_auto_rate}`);

  // ── Rep summary ──────────────────────────────────────────────────────────
  const rsum = await hit(`/v1/dashboard/rep-summary?userId=${REP_A}&days=30`, MANAGER);
  c("rep-summary returns 200", rsum.status === 200, `got ${rsum.status}`);

  c("rep-summary response shape preserved",
    ["ok", "userId", "calls", "avg_score", "xp", "recent"].every((k) => k in (rsum.data ?? {})),
    `keys ${JSON.stringify(Object.keys(rsum.data ?? {}))}`);

  c("rep-summary XP includes drill XP from completed assignments",
    Number(rsum.data?.xp) === EXPECTED_TOTAL_XP,
    `got ${rsum.data?.xp}, expected ${EXPECTED_TOTAL_XP} (${EXPECTED_CALL_XP} call + ${EXPECTED_DRILL_XP} drill)`);

  c("rep-summary counts the rep's calls",
    Number(rsum.data?.calls) === 3, `got ${rsum.data?.calls}`);
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
        ? "  Dashboard schema drift validation PASSED\n"
        : "  Dashboard schema drift validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
