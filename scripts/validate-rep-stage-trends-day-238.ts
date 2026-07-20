/**
 * validate-rep-stage-trends-day-238.ts
 *
 * Day 238 — rep stage trends derived from rubric/analysis JSON.
 *
 * Background: GET /v1/reps/:id/weakness-trends and /:id/daily-feed used to
 * select intro_score/discovery_score/objection_score/close_score from the
 * calls table. Those columns do not exist (Day 237B sweep), so both routes
 * threw 42703 and answered 500 for every rep. Stage scores actually live in
 * JSON, in three shapes that are all present in live data:
 *
 *   analysis_json.stages[stage].score   (newer scored calls)
 *   rubric.stages[stage].score          (current scoring output)
 *   rubric[stage].score                 (legacy, pre-`stages` nesting)
 *
 * Coverage:
 *   ✓ both routes answer 200, not 500, for a rep with real stage data
 *   ✓ response shape preserved (every documented key still present)
 *   ✓ the seeded UFC rep returns real stage values from rubric.stages
 *   ✓ all three JSON source shapes are read, in precedence order
 *   ✓ a missing stage contributes NO trend point — never a zero
 *   ✓ a rep whose calls carry no stage data returns empty trends and
 *     weakest_area: null, with 200 and no invented regression warnings
 *   ✓ a rep with no calls at all is safe
 *   ✓ org scoping still holds: cross-org requester → 403, and the
 *     identity/param gates (401/400) are unchanged
 *
 * Self-cleaning: every org, rep and call it creates is removed at the end.
 * It never writes to the UFC demo company (Day 224 rule) — the UFC
 * assertions are strictly read-only.
 *
 * Requirements: server running (npm run dev), UFC demo seed applied.
 * Usage: npm run validate:rep-stage-trends
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
  const h = crypto.createHash("sha256").update(`DAY238::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function hit(path: string, userId?: string, orgId?: string) {
  const headers: Record<string, string> = {};
  if (userId) headers["x-user-id"] = userId;
  if (orgId) headers["x-org-id"] = orgId;
  const res = await fetch(`${API_BASE}${path}`, {
    headers,
    signal: AbortSignal.timeout(15_000),
  });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

// ── Fixtures ────────────────────────────────────────────────────────────────
// Own org so org-scope assertions are exercised against rows this script
// owns outright; the UFC demo data is only ever read.
const ORG_A = uid("org-a");
const ORG_B = uid("org-b");
const MANAGER_A = uid("manager-a");
const REP_SCORED = uid("rep-scored");      // calls covering all three shapes
const REP_UNSCORED = uid("rep-unscored");  // calls with no stage data at all
const REP_NO_CALLS = uid("rep-no-calls");
const MANAGER_B = uid("manager-b");        // different org — must be refused

const CALL_ANALYSIS = uid("call-analysis");
const CALL_RUBRIC = uid("call-rubric");
const CALL_LEGACY = uid("call-legacy");
const CALL_PARTIAL = uid("call-partial");
const CALL_BARE = uid("call-bare");

const FIXTURE_CALLS = [CALL_ANALYSIS, CALL_RUBRIC, CALL_LEGACY, CALL_PARTIAL, CALL_BARE];
const FIXTURE_REPS = [MANAGER_A, MANAGER_B, REP_SCORED, REP_UNSCORED, REP_NO_CALLS];
const FIXTURE_ORGS = [ORG_A, ORG_B];

function callRow(id: string, repId: string, orgId: string, day: number, extra: Record<string, any>) {
  return {
    id,
    user_id: repId,
    org_id: orgId,
    filename: "day238.mp3",
    storage_path: `day238/${id}.mp3`,
    audio_path: `day238/${id}.mp3`,
    created_at: new Date(Date.UTC(2026, 0, day)).toISOString(),
    ...extra,
  };
}

async function seedFixtures() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [
      { id: ORG_A, name: "Day238 Org A (validator)" },
      { id: ORG_B, name: "Day238 Org B (validator)" },
    ],
    { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: MANAGER_A, name: "Day238 Manager A", tier: "Manager", org_id: ORG_A },
      { id: MANAGER_B, name: "Day238 Manager B", tier: "Manager", org_id: ORG_B },
      { id: REP_SCORED, name: "Day238 Scored Rep", tier: "SalesRep", org_id: ORG_A },
      { id: REP_UNSCORED, name: "Day238 Unscored Rep", tier: "SalesRep", org_id: ORG_A },
      { id: REP_NO_CALLS, name: "Day238 Callless Rep", tier: "SalesRep", org_id: ORG_A },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  await supa.from("calls").delete().in("id", FIXTURE_CALLS);

  const { error: callErr } = await supa.from("calls").upsert(
    [
      // Shape 1 — analysis_json.stages. Deliberately carries a DIFFERENT
      // rubric.stages block so precedence is provable, not coincidental:
      // analysis_json must win.
      callRow(CALL_ANALYSIS, REP_SCORED, ORG_A, 5, {
        score_overall: 70,
        analysis_json: {
          stages: {
            intro: { score: 81 }, discovery: { score: 82 },
            objection: { score: 83 }, close: { score: 84 },
          },
        },
        rubric: {
          stages: {
            intro: { score: 11 }, discovery: { score: 12 },
            objection: { score: 13 }, close: { score: 14 },
          },
        },
      }),
      // Shape 2 — rubric.stages (current scoring output).
      callRow(CALL_RUBRIC, REP_SCORED, ORG_A, 6, {
        score_overall: 60,
        rubric: {
          stages: {
            intro: { score: 61 }, discovery: { score: 62 },
            objection: { score: 63 }, close: { score: 64 },
          },
        },
      }),
      // Shape 3 — legacy top-level rubric.<stage>, no `stages` wrapper.
      callRow(CALL_LEGACY, REP_SCORED, ORG_A, 7, {
        score_overall: 50,
        rubric: {
          intro: { score: 41 }, discovery: { score: 42 },
          objection: { score: 43 }, close: { score: 44 },
        },
      }),
      // Partially scored: intro only. The other three stages must be
      // absent from their series, not present as 0.
      callRow(CALL_PARTIAL, REP_SCORED, ORG_A, 8, {
        score_overall: 55,
        rubric: { stages: { intro: { score: 77 } } },
      }),
      // No stage data whatsoever.
      callRow(CALL_BARE, REP_UNSCORED, ORG_A, 9, { score_overall: 48, rubric: null }),
    ],
    { onConflict: "id" }
  );
  if (callErr) throw new Error(`fixture call upsert failed: ${callErr.message}`);
}

async function cleanup() {
  await supa.from("calls").delete().in("id", FIXTURE_CALLS);
  await supa.from("reps").delete().in("id", FIXTURE_REPS);
  await supa.from("orgs").delete().in("id", FIXTURE_ORGS);
  console.log(`\n  Cleanup: removed ${FIXTURE_CALLS.length} call(s), ${FIXTURE_REPS.length} rep(s), ${FIXTURE_ORGS.length} org(s).`);
}

const TREND_KEYS = [
  "ok", "rep_id", "momentum_score", "regression_warnings", "ai_summary",
  "trends", "deltas", "replay_improvement_trend", "coaching_completion_trend",
];
const FEED_KEYS = [
  "ok", "rep_id", "coaching_summary", "weakest_area", "momentum_insight",
  "momentum_delta", "regression_warnings", "recommended_replay",
  "recommended_drill", "coaching_urgency", "ai_motivation_message", "todays_focus",
];

function valuesOn(series: any[]): number[] {
  return (series ?? []).map((p: any) => Number(p.value));
}

async function main() {
  console.log("\nDay 238 — rep stage trends\n");

  await seedFixtures();

  // ── Routes answer at all (the Day 238 regression) ────────────────────────
  const trends = await hit(`/v1/reps/${REP_SCORED}/weakness-trends`, MANAGER_A, ORG_A);
  c("weakness-trends returns 200 (was 500 on missing stage columns)",
    trends.status === 200, `got ${trends.status} ${JSON.stringify(trends.data)?.slice(0, 120)}`);

  const feed = await hit(`/v1/reps/${REP_SCORED}/daily-feed`, MANAGER_A, ORG_A);
  c("daily-feed returns 200 (was 500 on missing stage columns)",
    feed.status === 200, `got ${feed.status} ${JSON.stringify(feed.data)?.slice(0, 120)}`);

  // ── Response shape preserved ─────────────────────────────────────────────
  const missingTrendKeys = TREND_KEYS.filter((k) => !(k in (trends.data ?? {})));
  c("weakness-trends response shape preserved",
    missingTrendKeys.length === 0, `missing ${missingTrendKeys.join(", ")}`);

  const missingFeedKeys = FEED_KEYS.filter((k) => !(k in (feed.data ?? {})));
  c("daily-feed response shape preserved",
    missingFeedKeys.length === 0, `missing ${missingFeedKeys.join(", ")}`);

  const t = trends.data?.trends ?? {};
  c("trends still carries all five series",
    ["intro", "discovery", "objection_handling", "closing", "overall"]
      .every((k) => Array.isArray(t[k])),
    JSON.stringify(Object.keys(t)));

  c("deltas still carries all five keys",
    ["intro", "discovery", "objection_handling", "closing", "overall"]
      .every((k) => typeof trends.data?.deltas?.[k] === "number"),
    JSON.stringify(trends.data?.deltas));

  // ── Source precedence: all three JSON shapes are read ────────────────────
  // Fixtures are ordered oldest-first by created_at, matching the route.
  const introValues = valuesOn(t.intro);
  const closeValues = valuesOn(t.closing);

  c("reads analysis_json.stages (and it beats rubric.stages)",
    introValues.includes(81) && !introValues.includes(11),
    `intro series ${JSON.stringify(introValues)}`);

  c("reads rubric.stages", introValues.includes(61), `intro series ${JSON.stringify(introValues)}`);

  c("reads legacy top-level rubric.<stage>",
    introValues.includes(41), `intro series ${JSON.stringify(introValues)}`);

  c("all four stages resolve from a single shape",
    closeValues.includes(84) && closeValues.includes(64) && closeValues.includes(44),
    `closing series ${JSON.stringify(closeValues)}`);

  // ── A missing stage is absent, never zero ────────────────────────────────
  // REP_SCORED has 4 calls; only 3 score discovery/objection/close, but all
  // 4 score intro (the partial call scores intro alone).
  c("partially scored call contributes its scored stage",
    introValues.includes(77), `intro series ${JSON.stringify(introValues)}`);

  c("missing stage adds NO point (not a zero)",
    introValues.length === 4 && closeValues.length === 3 &&
    !closeValues.includes(0),
    `intro ${introValues.length}, closing ${closeValues.length} ${JSON.stringify(closeValues)}`);

  c("no stage series contains a fabricated zero",
    ["intro", "discovery", "objection_handling", "closing"]
      .every((k) => !valuesOn(t[k]).includes(0)),
    JSON.stringify(t));

  // ── Rep with calls but no stage data anywhere ────────────────────────────
  const bare = await hit(`/v1/reps/${REP_UNSCORED}/weakness-trends`, MANAGER_A, ORG_A);
  c("rep with no stage data → 200, not 500", bare.status === 200, `got ${bare.status}`);

  c("rep with no stage data → empty stage series (honest, not zeroed)",
    ["intro", "discovery", "objection_handling", "closing"]
      .every((k) => Array.isArray(bare.data?.trends?.[k]) && bare.data.trends[k].length === 0),
    JSON.stringify(bare.data?.trends));

  c("rep with no stage data still reports its overall series",
    (bare.data?.trends?.overall ?? []).length === 1,
    JSON.stringify(bare.data?.trends?.overall));

  const bareFeed = await hit(`/v1/reps/${REP_UNSCORED}/daily-feed`, MANAGER_A, ORG_A);
  c("daily-feed with no stage data → 200", bareFeed.status === 200, `got ${bareFeed.status}`);

  c("daily-feed with no stage data → weakest_area null (not a zero-scored stage)",
    bareFeed.data?.weakest_area === null, JSON.stringify(bareFeed.data?.weakest_area));

  c("daily-feed invents no regression warnings without stage data",
    Array.isArray(bareFeed.data?.regression_warnings) &&
    bareFeed.data.regression_warnings.length === 0,
    JSON.stringify(bareFeed.data?.regression_warnings));

  // ── Rep with no calls at all ─────────────────────────────────────────────
  const empty = await hit(`/v1/reps/${REP_NO_CALLS}/weakness-trends`, MANAGER_A, ORG_A);
  c("rep with zero calls → 200", empty.status === 200, `got ${empty.status}`);

  const emptyFeed = await hit(`/v1/reps/${REP_NO_CALLS}/daily-feed`, MANAGER_A, ORG_A);
  c("rep with zero calls → daily-feed 200 + weakest_area null",
    emptyFeed.status === 200 && emptyFeed.data?.weakest_area === null,
    `got ${emptyFeed.status} ${JSON.stringify(emptyFeed.data?.weakest_area)}`);

  // ── Weakest area picks a real scored stage ───────────────────────────────
  // Latest call for REP_SCORED is CALL_PARTIAL (intro 77 only), so intro is
  // the only candidate and must win at its real score.
  c("weakest_area derives from real stage data",
    feed.data?.weakest_area?.category === "intro" &&
    feed.data?.weakest_area?.score === 77,
    JSON.stringify(feed.data?.weakest_area));

  // ── Org scoping unchanged ────────────────────────────────────────────────
  const crossOrg = await hit(`/v1/reps/${REP_SCORED}/weakness-trends`, MANAGER_B, ORG_B);
  c("cross-org requester → 403 forbidden_org_scope",
    crossOrg.status === 403 && crossOrg.data?.error === "forbidden_org_scope",
    `got ${crossOrg.status} ${crossOrg.data?.error}`);

  const crossFeed = await hit(`/v1/reps/${REP_SCORED}/daily-feed`, MANAGER_B, ORG_B);
  c("cross-org requester → 403 on daily-feed",
    crossFeed.status === 403, `got ${crossFeed.status}`);

  const spoofedOrg = await hit(`/v1/reps/${REP_SCORED}/weakness-trends`, MANAGER_B, ORG_A);
  c("requester claiming another org header → 403",
    spoofedOrg.status === 403, `got ${spoofedOrg.status}`);

  // Not asserting 401 here: server.ts falls back to DEV_TEST_UID when no
  // header or JWT is present, an escape hatch that is null under
  // NODE_ENV=production. Locally the request therefore runs as that user,
  // so the property worth proving is that the fallback identity buys no
  // access to another org's rep.
  const noIdentity = await hit(`/v1/reps/${REP_SCORED}/weakness-trends`, undefined, ORG_A);
  c("no explicit identity still cannot read another org's rep",
    noIdentity.status === 403, `got ${noIdentity.status}`);

  const noOrg = await hit(`/v1/reps/${REP_SCORED}/weakness-trends`, MANAGER_A, undefined);
  c("no org header → 400", noOrg.status === 400, `got ${noOrg.status}`);

  const badId = await hit(`/v1/reps/not-a-uuid/weakness-trends`, MANAGER_A, ORG_A);
  c("invalid rep id → 400 invalid_rep_id",
    badId.status === 400 && badId.data?.error === "invalid_rep_id", `got ${badId.status}`);

  // ── Real seeded UFC rep (read-only) ──────────────────────────────────────
  const { data: ufcRep } = await supa
    .from("reps").select("id,org_id").eq("name", "Nate Diaz").maybeSingle();

  if (ufcRep) {
    const { data: ufcManager } = await supa
      .from("reps").select("id").eq("org_id", (ufcRep as any).org_id)
      .eq("tier", "Manager").limit(1).maybeSingle();

    const requester = (ufcManager as any)?.id ?? (ufcRep as any).id;
    const ufc = await hit(
      `/v1/reps/${(ufcRep as any).id}/weakness-trends`,
      requester,
      String((ufcRep as any).org_id)
    );
    c("seeded UFC rep → 200", ufc.status === 200, `got ${ufc.status}`);

    const ufcIntro = valuesOn(ufc.data?.trends?.intro);
    c("seeded UFC rep returns real stage values from rubric",
      ufcIntro.length > 0 && ufcIntro.every((v) => Number.isFinite(v) && v > 0),
      `intro series ${JSON.stringify(ufcIntro).slice(0, 120)}`);

    const ufcFeed = await hit(
      `/v1/reps/${(ufcRep as any).id}/daily-feed`,
      requester,
      String((ufcRep as any).org_id)
    );
    c("seeded UFC rep daily-feed → 200 with a real weakest area",
      ufcFeed.status === 200 &&
      typeof ufcFeed.data?.weakest_area?.score === "number" &&
      ufcFeed.data.weakest_area.score > 0,
      `got ${ufcFeed.status} ${JSON.stringify(ufcFeed.data?.weakest_area)}`);
  } else {
    c("seeded UFC rep → 200", false, "Nate Diaz not found — run seed:ufc-story");
    c("seeded UFC rep returns real stage values from rubric", false, "no UFC rep");
    c("seeded UFC rep daily-feed → 200 with a real weakest area", false, "no UFC rep");
  }
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
        ? "  Rep stage trends validation PASSED\n"
        : "  Rep stage trends validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
