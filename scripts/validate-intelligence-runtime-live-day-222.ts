/**
 * validate-intelligence-runtime-live-day-222.ts
 *
 * Day 222 — LIVE Intelligence runtime proof on the real UFC demo company.
 *
 * Day 221 proved the resolver/prompt/cache/meta units against synthetic
 * fixture companies. This validator proves the same chain END-TO-END on real
 * demo data, through the real manager HTTP routes and the real scoring entry
 * point (scoreWithLLM), rather than by calling the resolvers directly:
 *
 *   ✓ Dana (real demo manager) publishes a real UFC company context over
 *     PUT /v1/intelligence/context + POST /v1/intelligence/context/publish
 *   ✓ Dana creates + activates a real UFC company-default scorecard over
 *     POST /v1/intelligence/scorecards → PUT .../versions/:id → POST .../activate
 *     using ONLY the fixed four stages (intro/discovery/objection/close)
 *   ✓ a controlled Day 222 proof call (price objection + weak close) is scored
 *     through scoreWithLLM and its persisted calls.rubric._meta names exactly
 *     the context version and scorecard version the manager just published
 *   ✓ the prompt blocks the runtime would send carry the real UFC content
 *   ✓ cache keys: custom ≠ default, context bump changes the key, scorecard
 *     bump changes the key, default key shape unchanged
 *   ✓ isolation: another company resolves none of UFC's assets; the surviving
 *     draft context is never scored; archived scorecards + superseded versions
 *     are never selected
 *
 * SCORING METHOD (honest statement — no LLM call is made):
 *   OPENAI_API_KEY is deliberately unset inside this process, so a cache MISS
 *   could never reach a paid model. A sentinel result is seeded into
 *   score_cache under the key derived from the context/scorecard versions the
 *   manager published. scoreWithLLM then runs for real: it resolves the
 *   company's assets ITSELF, builds the key ITSELF, and only hits that
 *   sentinel if its own resolution matched. The cache HIT is therefore the
 *   proof of correct live resolution, and the rubric it writes to calls.rubric
 *   is real runtime output. The LLM request path itself is unexercised — its
 *   two injection points (context + scorecard prompt blocks) are asserted
 *   directly against the live resolved assets instead.
 *
 * SELF-CLEANING — nothing is left behind. The published context and activated
 * scorecard are removed at the end, because validate:intelligence-context and
 * validate:intelligence-scorecards both assert the UFC company starts with no
 * context and no scorecards. Seeding these as permanent demo assets is a
 * separate decision (see Day 223 recommendation in the blueprint).
 *
 * Requirements: sql/20260714_company_context.sql +
 * sql/20260714b_scorecard_studio.sql applied, UFC demo seed present, and the
 * API server running (default http://localhost:4000 — override with API_BASE).
 * Usage: npm run validate:intelligence-runtime-live
 */

import "dotenv/config";

import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

import {
  GRAVIX_DEFAULT_SCORECARD_KEY,
  buildContextPromptBlock,
  buildScorecardPromptBlock,
  resolveActiveScorecard,
  resolvePublishedContext,
} from "../src/lib/intelligenceRuntime";
import { cleanTranscript } from "../src/lib/transcript";

// Env rails, applied before lib/scoring is loaded. ESM imports are hoisted and
// evaluated before any top-level statement, and lib/scoring reads
// SKIP_SCORING_SIDE_EFFECTS once at module init — so scoring MUST be pulled in
// with a dynamic import below (see loadScoring), never a static one, or the
// flag is captured as false and real Slack/email/rep-memory side effects fire
// against demo identities.
//   • OPENAI_API_KEY unset — getOpenAI() reads it per call, so a cache MISS can
//     only ever throw and degrade to the heuristic fallback. Never a paid call.
//   • SKIP_SCORING_SIDE_EFFECTS=1 — no Slack post, no rep email, no rep memory.
delete process.env.OPENAI_API_KEY;
process.env.SKIP_SCORING_SIDE_EFFECTS = "1";

type ScoringModule = typeof import("../src/lib/scoring");
async function loadScoring(): Promise<ScoringModule> {
  return import("../src/lib/scoring");
}

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const UFC_COMPANY_ID = process.env.DEMO_COMPANY_ID || "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const DANA_EMAIL = "dana.white@ufcelite.demo";
const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY222::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

// Deterministic Day 222 fixture ids — all removed by cleanup().
const PROOF_CALL_ID = uid("proof-call");
const XCOMPANY_ID = uid("cross-company");
const XMANAGER_ID = uid("cross-manager");
const SC_ARCHIVED = uid("sc-archived");
const SCV_ARCHIVED = uid("scv-archived");
const SCV_SUPERSEDED = uid("scv-superseded");

const SCORECARD_NAME = "UFC Elite — Company Default (Day222 proof)";
const SENTINEL = "DAY222 PROOF SENTINEL — served from the seeded score cache.";

async function hit(method: string, path: string, userId: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(20_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

// ---------------------------------------------------------------------------
// Real UFC demo context — bounded, from the Day 208 Context Engine field spec.
// Describes the actual product being demoed (an elite sales coaching system).
// ---------------------------------------------------------------------------
const UFC_CONTEXT = {
  profile: {
    about:
      "Gravix sells an elite sales training and coaching system to revenue teams: AI call review, sparring practice and manager-led coaching assignments in one platform.",
    sales_motion:
      "Managers review recorded calls, assign coaching, and reps practise objections in sparring before the next live call.",
    icp: "Sales managers, SDR leaders and revenue leaders running teams of 5–50 reps.",
  },
  offering: {
    products_services: [
      { name: "AI call review", description: "Every call scored against the company scorecard with stage-level evidence." },
      { name: "Sparring", description: "Reps rehearse objections against realistic AI scenarios and are scored on the same rubric." },
      { name: "Coaching assignments", description: "Managers assign drills from a weak call and track completion." },
    ],
    pricing_positioning: {
      pricing_notes: "Premium annual platform licence, priced per active seat.",
      positioning_notes:
        "A premium operational coaching platform, not a recording archive — bought for repeatable rep improvement, never sold on price.",
    },
  },
  objections: [
    {
      objection: "It's too expensive",
      approved_response:
        "Isolate the concern, then reframe on cost per rep against the revenue of one recovered deal.",
      weak_response: "Discounting immediately or apologising for the price.",
    },
    {
      objection: "I need to think about it",
      approved_response: "Find the actual hesitation, then agree a dated next step before the call ends.",
      weak_response: "Agreeing to 'circle back' with no date.",
    },
    {
      objection: "Just send me some info",
      approved_response: "Offer to walk one real call through the platform live rather than sending a deck.",
      weak_response: "Sending a brochure and waiting.",
    },
  ],
  compliance: {
    no_go_language: ["Guaranteed revenue increase", "You will definitely close more deals", "Risk-free"],
    required_disclosures: ["Call recording and AI scoring must be disclosed to the customer's reps."],
  },
  tone: {
    playbook_guidance:
      "Direct and evidence-led. Lead with what the manager will see in the platform, not adjectives.",
    tone_notes: "Manager-first, concrete, never pressure-heavy and never over-promising.",
  },
};

// Fixed four stages only — weights total 100 as required for activation.
const SCORECARD_STAGE_PAYLOAD = {
  intro: {
    weight: 20,
    guidance: "Judge whether the rep earned the right to run the call.",
    criteria: [
      {
        label: "Set agenda and establish credibility",
        description: "States the purpose of the call and why Gravix is credible for this manager's problem.",
        scoring_guidance: "Full marks only when both an agenda and a credibility anchor land before discovery.",
        emphasis: "standard",
      },
    ],
  },
  discovery: {
    weight: 30,
    guidance: "Judge the depth of the problem the rep uncovered.",
    criteria: [
      {
        label: "Uncover pain, current process and decision process",
        description: "Draws out how calls are reviewed today, what it costs, and who signs off.",
        scoring_guidance: "Full marks only when all three land before any pitch.",
        emphasis: "major",
      },
    ],
  },
  objection: {
    weight: 30,
    guidance: "Judge how a price objection was handled against the approved response.",
    criteria: [
      {
        label: "Isolate the price concern and reframe on value",
        description: "Isolates price as the real objection, then reframes on cost per rep against recovered revenue.",
        scoring_guidance: "Immediate discounting scores 0 for this criterion.",
        emphasis: "major",
      },
    ],
  },
  close: {
    weight: 20,
    guidance: "Judge whether a real commitment was secured.",
    criteria: [
      {
        label: "Secure a clear next step and commitment",
        description: "Locks a dated next step with the named decision maker.",
        scoring_guidance: "'I'll follow up' with no date scores 0 for this criterion.",
        emphasis: "major",
      },
    ],
  },
};

// Controlled proof transcript — clearly exercises a price objection + weak close.
const PROOF_TRANSCRIPT = [
  "Rep: Thanks for making the time, Dana. I'd like to spend fifteen minutes on how your team reviews calls today.",
  "Prospect: Fine, but I'll be honest, we've looked at tools like this before.",
  "Rep: Understood. How do your managers review calls at the moment?",
  "Prospect: Spot checks, when someone has a spare hour. Which is never.",
  "Rep: So most calls go unreviewed. Gravix scores every call and turns the weak ones into coaching assignments.",
  "Prospect: Look, this is going to be too expensive for us.",
  "Rep: I hear you. I can probably get you a discount if that helps.",
  "Prospect: Maybe. I think I need to think about it and talk to the team.",
  "Rep: No problem at all, I'll follow up at some point and see where you landed.",
  "Prospect: Sure, send me some info.",
].join("\n");

const PROOF_SHA256 = crypto.createHash("sha256").update(PROOF_TRANSCRIPT).digest("hex");

function sentinelResult(scoringModelVersion: string) {
  return {
    overall: 45,
    summary: SENTINEL,
    stages: {
      intro: { score: 62, notes: "Agenda set, credibility thin." },
      discovery: { score: 48, notes: "Current process surfaced; no decision process, no impact." },
      objection: { score: 30, notes: "Discounted immediately instead of isolating and reframing price." },
      close: { score: 40, notes: "No dated next step — 'follow up at some point'." },
    },
    moments: [],
    suggestions: [],
    model: scoringModelVersion,
  };
}

async function danaId(): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", DANA_EMAIL).maybeSingle();
  return (data as any)?.id ?? null;
}

async function orgIdForUfc(): Promise<string | null> {
  const { data } = await supa.from("reps").select("org_id").eq("email", DANA_EMAIL).maybeSingle();
  return (data as any)?.org_id ?? null;
}

async function cleanup() {
  // Proof call + everything the scoring runtime may have written for it.
  await supa.from("crm_activities").delete().eq("call_id", PROOF_CALL_ID);
  await supa.from("coach_assignments").delete().eq("call_id", PROOF_CALL_ID);
  await supa.from("assignments").delete().eq("target_id", PROOF_CALL_ID);
  await supa.from("call_scores").delete().eq("call_id", PROOF_CALL_ID);
  await supa.from("calls").delete().eq("id", PROOF_CALL_ID);
  await supa.from("score_cache").delete().eq("call_sha256", PROOF_SHA256);

  // Isolation fixtures.
  await supa.from("scorecard_versions").delete().in("id", [SCV_ARCHIVED, SCV_SUPERSEDED]);
  await supa.from("scorecards").delete().eq("id", SC_ARCHIVED);
  await supa.from("reps").delete().eq("id", XMANAGER_ID);
  await supa.from("company_context").delete().eq("company_id", XCOMPANY_ID);
  await supa.from("companies").delete().eq("id", XCOMPANY_ID);

  // Day 222 UFC assets — removed so the Day 218 / 219B validators keep their
  // "UFC starts clean" precondition.
  const { data: cards } = await supa
    .from("scorecards").select("id").eq("company_id", UFC_COMPANY_ID).eq("name", SCORECARD_NAME);
  const cardIds = ((cards ?? []) as any[]).map((r) => String(r.id));
  if (cardIds.length) {
    const { data: vers } = await supa
      .from("scorecard_versions").select("id").in("scorecard_id", cardIds);
    const verIds = ((vers ?? []) as any[]).map((r) => String(r.id));
    if (verIds.length) {
      await supa.from("scorecard_criteria").delete().in("scorecard_version_id", verIds);
      await supa.from("scorecard_stage_weights").delete().in("scorecard_version_id", verIds);
      await supa.from("scorecard_versions").delete().in("id", verIds);
    }
    await supa.from("scorecards").delete().in("id", cardIds);
  }
  await supa.from("company_context").delete().eq("company_id", UFC_COMPANY_ID);

  console.log("\n  Cleanup: removed the Day222 proof call, cache entry, isolation fixtures,");
  console.log("           and the Day222 UFC context + scorecard (UFC left as found).");
}

async function main() {
  console.log("\n  Day 222 — LIVE Intelligence runtime proof (UFC demo company)\n");

  const { RUBRIC_VERSION, SCORING_MODEL_VERSION, buildDeterministicPromptKey, scoreWithLLM } =
    await loadScoring();

  const probeCtx = await supa.from("company_context").select("id").limit(1);
  const probeSc = await supa.from("scorecard_versions").select("id").limit(1);
  if (probeCtx.error || probeSc.error) {
    console.error("  ✗ intelligence migrations not applied — run Day 218/219B SQL first.");
    process.exit(1);
  }

  const dana = await danaId();
  const orgId = await orgIdForUfc();
  if (!dana || !orgId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo && npm run seed:ufc-story).");
    process.exit(1);
  }

  const health = await fetch(`${API_BASE}/health`, { signal: AbortSignal.timeout(5_000) })
    .then((r) => r.status).catch(() => 0);
  if (health !== 200) {
    console.error(`  ✗ API server not reachable at ${API_BASE} — start it, or set API_BASE.`);
    process.exit(1);
  }

  // A pre-existing UFC context/scorecard would make this a different test.
  const { data: preCtx } = await supa
    .from("company_context").select("id").eq("company_id", UFC_COMPANY_ID);
  const { data: preSc } = await supa
    .from("scorecards").select("id").eq("company_id", UFC_COMPANY_ID);
  if ((preCtx ?? []).length || (preSc ?? []).length) {
    console.error("  ✗ UFC already has context/scorecard rows — resolve manually before proving.");
    process.exit(1);
  }

  try {
    // ---- 1. Manager publishes real UFC context (live HTTP) ----------------
    console.log("— Manager publishes UFC context (live HTTP as Dana)");

    const put = await hit("PUT", "/v1/intelligence/context", dana, { context: UFC_CONTEXT });
    c("Dana saves the UFC context draft", put.status === 200 && put.data?.draft?.status === "draft", `got ${put.status}`);
    // The route serialises without company_id and scopes server-side from
    // Dana's identity, so verify the scope on the stored row itself.
    const { data: draftScope } = await supa
      .from("company_context").select("company_id").eq("id", String(put.data?.draft?.id ?? "")).maybeSingle();
    c("draft lands in the UFC company scope", (draftScope as any)?.company_id === UFC_COMPANY_ID,
      JSON.stringify((draftScope as any)?.company_id));

    const pub = await hit("POST", "/v1/intelligence/context/publish", dana);
    c("Dana publishes the context → v1", pub.status === 200 && pub.data?.published?.version === 1, `got ${pub.status} v${pub.data?.published?.version}`);
    c("published row is stamped published_at/published_by=Dana",
      Boolean(pub.data?.published?.published_at) && pub.data?.published?.published_by === dana);

    const publishedCompiled = String(pub.data?.published?.compiled_context ?? "");
    c("compiled block carries the real UFC positioning",
      publishedCompiled.includes("premium operational coaching platform"));
    c("compiled block carries the real UFC objections",
      publishedCompiled.includes("It's too expensive") && publishedCompiled.includes("Just send me some info"));
    c("compiled block carries the no-go compliance language",
      publishedCompiled.includes("Guaranteed revenue increase"));

    // ---- 2. Manager creates + activates a UFC scorecard (live HTTP) -------
    console.log("— Manager creates + activates the UFC scorecard (live HTTP as Dana)");

    const create = await hit("POST", "/v1/intelligence/scorecards", dana, {
      name: SCORECARD_NAME,
      description: "Day 222 live proof — UFC company default over the fixed four stages.",
      is_company_default: true,
    });
    c("Dana creates the UFC scorecard", create.status === 201 && create.data?.ok === true, `got ${create.status} ${JSON.stringify(create.data)?.slice(0, 120)}`);
    const cardId = String(create.data?.scorecard?.id ?? "");
    const draftVersionId = String(create.data?.draft_version?.id ?? "");
    c("scorecard is created as a draft with a draft version", create.data?.scorecard?.status === "draft" && Boolean(draftVersionId));

    const saveVersion = await hit("PUT", `/v1/intelligence/scorecards/${cardId}/versions/${draftVersionId}`, dana, {
      call_types: [],
      stages: SCORECARD_STAGE_PAYLOAD,
    });
    c("Dana saves stage weights 20/30/30/20 + one criterion per stage",
      saveVersion.status === 200 && saveVersion.data?.ok === true,
      `got ${saveVersion.status} ${JSON.stringify(saveVersion.data)?.slice(0, 160)}`);

    const activate = await hit("POST", `/v1/intelligence/scorecards/${cardId}/activate`, dana, {});
    c("Dana activates the scorecard", activate.status === 200 && activate.data?.ok === true,
      `got ${activate.status} ${JSON.stringify(activate.data)?.slice(0, 160)}`);

    const { data: activeVer } = await supa
      .from("scorecard_versions").select("id, version, status, snapshot")
      .eq("scorecard_id", cardId).eq("status", "active").maybeSingle();
    const activeVersionId = String((activeVer as any)?.id ?? "");
    c("an active version now exists with an immutable snapshot",
      Boolean(activeVersionId) && Boolean((activeVer as any)?.snapshot));
    const snapStages = ((activeVer as any)?.snapshot?.stages ?? []) as any[];
    c("snapshot uses only the fixed four stages",
      JSON.stringify(snapStages.map((s) => s.stage)) === JSON.stringify(["intro", "discovery", "objection", "close"]),
      JSON.stringify(snapStages.map((s) => s.stage)));
    c("snapshot weights total 100 (20/30/30/20)",
      snapStages.reduce((t, s) => t + (Number(s.weight) || 0), 0) === 100 &&
      JSON.stringify(snapStages.map((s) => Number(s.weight))) === JSON.stringify([20, 30, 30, 20]));

    // ---- 3. The runtime resolves what the manager published ---------------
    console.log("— Runtime resolution against the live UFC assets");

    const liveCtx = await resolvePublishedContext(supa, UFC_COMPANY_ID);
    c("runtime resolves the published UFC context (v1)", liveCtx?.context_version === 1);
    c("resolved context is the published snapshot, not a recompile",
      liveCtx?.compiled_context === publishedCompiled);

    const liveSc = await resolveActiveScorecard(supa, UFC_COMPANY_ID, null);
    c("runtime resolves the activated UFC scorecard", liveSc.scorecard_version_id === activeVersionId);
    c("resolved scorecard source is company_default", liveSc.source === "company_default");
    c("resolved scorecard carries the manager's name", liveSc.scorecard_name === SCORECARD_NAME);
    c("resolved scorecard cache key is the active version id", liveSc.cache_key === activeVersionId);

    // ---- 4. Prompt injection carries the real UFC content -----------------
    console.log("— Prompt blocks (the runtime's only two prompt injection points)");

    const ctxBlock = buildContextPromptBlock(liveCtx);
    c("context prompt block is labelled with the published version",
      ctxBlock.includes("published v1"));
    c("context prompt block carries the real UFC objections + no-go language",
      ctxBlock.includes("It's too expensive") && ctxBlock.includes("Guaranteed revenue increase"));

    const scBlock = buildScorecardPromptBlock(liveSc);
    c("scorecard prompt block renders all four fixed stages with weights",
      ["intro (weight 20", "discovery (weight 30", "objection (weight 30", "close (weight 20"]
        .every((s) => scBlock.includes(`Stage ${s}`)));
    c("scorecard prompt block carries the manager's criteria",
      scBlock.includes("Isolate the price concern and reframe on value") &&
      scBlock.includes("Secure a clear next step and commitment"));
    c("scorecard prompt block pins the fixed JSON schema (no new stages)",
      scBlock.includes("fixed JSON schema"));

    // ---- 5. Score a controlled proof call through scoreWithLLM ------------
    console.log("— Live scoring of the Day222 proof call (real scoreWithLLM entry point)");

    const { error: callErr } = await supa.from("calls").insert({
      id: PROOF_CALL_ID,
      user_id: dana,
      org_id: orgId,
      company_id: UFC_COMPANY_ID,
      filename: "day222-proof-call.mp3",
      title: "Day222 proof call — price objection (validator fixture)",
      status: "ready",
      kind: "audio",
      mime_type: "audio/mpeg",
      // storage_path/audio_path are NOT NULL on calls. This fixture is
      // transcript-only — no audio object is ever uploaded or read.
      storage_path: `day222-proof/${PROOF_CALL_ID}.mp3`,
      audio_path: `day222-proof/${PROOF_CALL_ID}.mp3`,
      size_bytes: 0,
      filesize_bytes: 0,
      sample_rate_hz: 0,
      channels: 1,
      sha256: PROOF_SHA256,
      transcript: PROOF_TRANSCRIPT,
      duration_sec: 420,
      duration_ms: 420_000,
    });
    c("Day222 proof call fixture created in the UFC company", !callErr, callErr?.message);

    // Seed the cache under the key derived from the assets the manager just
    // published. scoreWithLLM resolves + keys independently; a HIT proves its
    // resolution matched, a MISS would fall back (never a paid LLM call).
    // scoreWithLLM keys on cleanTranscript(calls.transcript), not the raw
    // column — mirror that exactly or the key misses for the wrong reason.
    const expectedKey = buildDeterministicPromptKey({
      callId: PROOF_CALL_ID,
      filename: "day222-proof-call.mp3",
      sha256: PROOF_SHA256,
      transcript: cleanTranscript(PROOF_TRANSCRIPT),
      contextVersion: liveCtx?.context_version ?? null,
      scorecardCacheKey: liveSc.cache_key,
    });
    const { error: cacheErr } = await supa.from("score_cache").upsert({
      cache_key: expectedKey.key,
      call_sha256: PROOF_SHA256,
      transcript_hash: expectedKey.transcriptHash,
      rubric_version: RUBRIC_VERSION,
      prompt_version: "v1",
      model_version: SCORING_MODEL_VERSION,
      result_json: sentinelResult(SCORING_MODEL_VERSION),
      updated_at: new Date().toISOString(),
    }, { onConflict: "cache_key" });
    c("sentinel result seeded under the context+scorecard-keyed cache key", !cacheErr, cacheErr?.message);

    const scored = await scoreWithLLM({ supabase: supa, callId: PROOF_CALL_ID });
    c("scoreWithLLM resolved the UFC assets itself and hit the seeded key (no LLM call)",
      scored.summary === SENTINEL,
      `summary was "${String(scored.summary).slice(0, 80)}" — a miss means resolution disagreed`);
    c("scored result carries the seeded stage scores", scored.overall === 45 && scored.stages.close.score === 40);

    // ---- 6. rubric._meta persisted on the call ---------------------------
    console.log("— rubric._meta provenance persisted on the call");

    const { data: scoredCall } = await supa
      .from("calls").select("rubric, score_overall").eq("id", PROOF_CALL_ID).single();
    const meta = (scoredCall as any)?.rubric?._meta ?? {};

    c("call row was updated with the scored rubric", (scoredCall as any)?.score_overall === 45);
    c("_meta.scoring_model_version", meta.scoring_model_version === SCORING_MODEL_VERSION, JSON.stringify(meta.scoring_model_version));
    c("_meta.rubric_version", meta.rubric_version === RUBRIC_VERSION, JSON.stringify(meta.rubric_version));
    c("_meta.scorecard_source = company_default", meta.scorecard_source === "company_default", JSON.stringify(meta.scorecard_source));
    c("_meta.scorecard_name = the manager's scorecard", meta.scorecard_name === SCORECARD_NAME, JSON.stringify(meta.scorecard_name));
    c("_meta.scorecard_id = the activated scorecard", meta.scorecard_id === cardId, JSON.stringify(meta.scorecard_id));
    c("_meta.scorecard_version_id = the active version", meta.scorecard_version_id === activeVersionId, JSON.stringify(meta.scorecard_version_id));
    c("_meta.scorecard_version = 1", meta.scorecard_version === 1, JSON.stringify(meta.scorecard_version));
    c("_meta.context_version = the published version", meta.context_version === 1, JSON.stringify(meta.context_version));
    c("_meta.context_published_at = the publish timestamp",
      meta.context_published_at === pub.data?.published?.published_at, JSON.stringify(meta.context_published_at));
    c("persisted rubric keeps the fixed four-stage shape",
      JSON.stringify(Object.keys((scoredCall as any)?.rubric ?? {}).sort()) ===
      JSON.stringify(["_meta", "close", "discovery", "intro", "objection"]));

    // ---- 7. Cache key behaviour on live values ---------------------------
    console.log("— Cache key behaviour (live UFC values)");

    const KEY_ARGS = {
      callId: PROOF_CALL_ID,
      filename: "day222-proof-call.mp3",
      sha256: PROOF_SHA256,
      transcript: cleanTranscript(PROOF_TRANSCRIPT),
    };
    const defaultKey = buildDeterministicPromptKey(KEY_ARGS);
    const defaultResolvedKey = buildDeterministicPromptKey({
      ...KEY_ARGS, contextVersion: null, scorecardCacheKey: GRAVIX_DEFAULT_SCORECARD_KEY,
    });
    c("default path key shape is unchanged (no context/scorecard segments)",
      defaultResolvedKey.key === defaultKey.key &&
      !defaultKey.key.includes("|context=") && !defaultKey.key.includes("|scorecard="));
    c("live UFC key differs from the default key", expectedKey.key !== defaultKey.key);
    c("live UFC key names the published context version", expectedKey.key.includes("|context=1"));
    c("live UFC key names the active scorecard version id", expectedKey.key.includes(`|scorecard=${activeVersionId}`));

    const ctxBumpKey = buildDeterministicPromptKey({
      ...KEY_ARGS, contextVersion: 2, scorecardCacheKey: liveSc.cache_key,
    });
    c("publishing a new context version changes the key", ctxBumpKey.key !== expectedKey.key);

    const scBumpKey = buildDeterministicPromptKey({
      ...KEY_ARGS, contextVersion: liveCtx?.context_version ?? null, scorecardCacheKey: uid("other-version"),
    });
    c("activating a new scorecard version changes the key", scBumpKey.key !== expectedKey.key);
    c("all four key variants are distinct",
      new Set([defaultKey.key, expectedKey.key, ctxBumpKey.key, scBumpKey.key]).size === 4);

    // ---- 8. Isolation ----------------------------------------------------
    console.log("— Isolation");

    const { error: xCompErr } = await supa.from("companies").upsert(
      {
        id: XCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day222 Cross Company (validator)",
        slug: "day222-cross-company-validator",
      },
      { onConflict: "id" }
    );
    if (xCompErr) throw new Error(`cross-company fixture failed: ${xCompErr.message}`);
    const { error: xMgrErr } = await supa.from("reps").upsert(
      [{ id: XMANAGER_ID, name: "Day222 X Manager", tier: "Manager", org_id: orgId, company_id: XCOMPANY_ID }],
      { onConflict: "id" }
    );
    if (xMgrErr) throw new Error(`cross-manager fixture failed: ${xMgrErr.message}`);

    const xCtx = await resolvePublishedContext(supa, XCOMPANY_ID);
    c("another company resolves no UFC context", xCtx === null);
    const xSc = await resolveActiveScorecard(supa, XCOMPANY_ID, null);
    c("another company resolves the Gravix default, never UFC's scorecard",
      xSc.source === "gravix_default" && xSc.scorecard_version_id !== activeVersionId);

    const xGet = await hit("GET", "/v1/intelligence/context", XMANAGER_ID);
    c("another company's manager sees its own empty scope over HTTP",
      xGet.status === 200 && xGet.data?.company_id === XCOMPANY_ID && xGet.data?.published === null,
      `got ${xGet.status} ${JSON.stringify(xGet.data)?.slice(0, 120)}`);
    const xList = await hit("GET", "/v1/intelligence/scorecards", XMANAGER_ID);
    c("another company's manager lists none of UFC's scorecards",
      xList.status === 200 && !JSON.stringify(xList.data ?? {}).includes(SCORECARD_NAME));

    // Draft context survives publish — it must never reach scoring.
    const { data: draftRow } = await supa
      .from("company_context").select("id, status").eq("company_id", UFC_COMPANY_ID).eq("status", "draft").maybeSingle();
    c("the UFC draft context row still exists after publish", Boolean(draftRow));
    const draftEdit = await hit("PUT", "/v1/intelligence/context", dana, {
      context: { profile: { about: "DAY222 DRAFT EDIT — must never reach scoring." } },
    });
    c("Dana can edit the draft after publishing", draftEdit.status === 200);
    const ctxAfterDraftEdit = await resolvePublishedContext(supa, UFC_COMPANY_ID);
    c("draft edits never change what the runtime scores with",
      JSON.stringify(ctxAfterDraftEdit) === JSON.stringify(liveCtx));
    c("the runtime's context never contains draft-only text",
      !String(ctxAfterDraftEdit?.compiled_context).includes("DAY222 DRAFT EDIT"));

    // Archived scorecard with an active version + a superseded version.
    const now = new Date().toISOString();
    await supa.from("scorecards").upsert(
      { id: SC_ARCHIVED, company_id: UFC_COMPANY_ID, name: "Day222 Archived Card (validator)", status: "archived", is_company_default: false, archived_at: now },
      { onConflict: "id" }
    );
    await supa.from("scorecard_versions").upsert(
      [
        { id: SCV_ARCHIVED, scorecard_id: SC_ARCHIVED, company_id: UFC_COMPANY_ID, version: 1, status: "active", call_types: ["discovery"], snapshot: { stages: [] }, activated_at: now },
        { id: SCV_SUPERSEDED, scorecard_id: SC_ARCHIVED, company_id: UFC_COMPANY_ID, version: 2, status: "superseded", call_types: ["discovery"], snapshot: { stages: [] }, activated_at: now },
      ],
      { onConflict: "id" }
    );

    const afterArchived = await resolveActiveScorecard(supa, UFC_COMPANY_ID, "discovery");
    c("an archived scorecard's active version is never selected",
      afterArchived.scorecard_version_id !== SCV_ARCHIVED);
    c("a superseded version is never selected",
      afterArchived.scorecard_version_id !== SCV_SUPERSEDED);
    c("resolution still returns the live UFC company default",
      afterArchived.scorecard_version_id === activeVersionId && afterArchived.source === "company_default");
  } finally {
    await cleanup();
  }

  const passed = checks.filter((x) => x.passed).length;
  console.log(`\n  ${passed}/${checks.length} checks passed`);
  if (passed !== checks.length) {
    console.log("  Day 222 live intelligence runtime proof FAILED");
    process.exit(1);
  }
  console.log("  Day 222 live intelligence runtime proof PASSED");
}

main().catch(async (e) => {
  console.error("validator crashed:", e?.message || e);
  try { await cleanup(); } catch { /* best effort */ }
  process.exit(1);
});
