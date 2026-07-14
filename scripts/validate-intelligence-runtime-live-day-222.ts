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
 *   ✓ the seeded UFC context is published at v1 and its compiled block carries
 *     the real UFC positioning, objections and no-go language
 *   ✓ the seeded "UFC Sales Scorecard" is active + company default, over ONLY
 *     the fixed four stages (intro/discovery/objection/close), weights 100
 *   ✓ Dana reads both over the real manager HTTP routes
 *   ✓ a controlled Day 222 proof call (price objection + weak close) is scored
 *     through scoreWithLLM and its persisted calls.rubric._meta names exactly
 *     the seeded context version and scorecard version
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
 *   score_cache under the key derived from the SEEDED context/scorecard
 *   versions. scoreWithLLM then runs for real: it resolves the company's
 *   assets ITSELF, builds the key ITSELF, and only hits that sentinel if its
 *   own resolution matched. The cache HIT is therefore the proof of correct
 *   live resolution, and the rubric it writes to calls.rubric is real runtime
 *   output. The LLM request path itself is unexercised — its two injection
 *   points (context + scorecard prompt blocks) are asserted directly against
 *   the live resolved assets instead.
 *
 * Day 224 — this validator no longer publishes or activates anything. The UFC
 * context + scorecard are PERSISTENT demo assets owned by
 * npm run seed:ufc-intelligence; this run proves the runtime consumes them and
 * must never delete them. Creating them here (the Day 222 shape) is what forced
 * UFC to start empty and made this validator fight the seed. The manager
 * create/publish/activate flows are covered by validate:intelligence-context
 * and validate:intelligence-scorecards, which now run against their own
 * throwaway fixture companies.
 *
 * Self-cleaning: only what this run owns — the proof call, its cache entry and
 * the isolation fixtures. The seeded draft context is edited to prove scoring
 * ignores it, then restored byte-for-byte.
 *
 * Requirements: sql/20260714_company_context.sql +
 * sql/20260714b_scorecard_studio.sql applied, npm run seed:ufc-intelligence
 * applied, and the API server running (default http://localhost:4000 —
 * override with API_BASE).
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

// Day 224 — the persistent scorecard seeded by npm run seed:ufc-intelligence.
const SCORECARD_NAME = "UFC Sales Scorecard";
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

  // Day 224 — the UFC context + scorecard are PERSISTENT demo assets seeded by
  // npm run seed:ufc-intelligence. This validator proves them; it does not own
  // them and must never delete them.

  console.log("\n  Cleanup: removed the Day222 proof call, cache entry and isolation");
  console.log("           fixtures. The seeded UFC context + scorecard are left in place.");
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

  // Day 224 — the UFC Intelligence assets are now seeded and PERSISTENT, so
  // this validator proves the runtime against the real demo assets instead of
  // publishing its own and tearing them down.
  const { data: seededCtx } = await supa
    .from("company_context").select("id, version").eq("company_id", UFC_COMPANY_ID).eq("status", "published").maybeSingle();
  const { data: seededCard } = await supa
    .from("scorecards").select("id, name, is_company_default")
    .eq("company_id", UFC_COMPANY_ID).eq("name", SCORECARD_NAME).eq("status", "active").maybeSingle();
  if (!seededCtx || !seededCard) {
    console.error("  ✗ UFC Intelligence assets missing — run npm run seed:ufc-intelligence first.");
    process.exit(1);
  }
  const cardId = String((seededCard as any).id);

  try {
    // ---- 1. The seeded UFC context (published by seed:ufc-intelligence) ---
    console.log("— Seeded UFC context");

    const pubRow = seededCtx as any;
    c("UFC has exactly one published context", Boolean(pubRow?.id));
    c("published context is v1", Number(pubRow.version) === 1, `v${pubRow.version}`);

    const { data: ctxFull } = await supa
      .from("company_context").select("compiled_context, published_at, published_by")
      .eq("id", pubRow.id).single();
    const publishedCompiled = String((ctxFull as any)?.compiled_context ?? "");
    c("published row is stamped published_at/published_by",
      Boolean((ctxFull as any)?.published_at) && Boolean((ctxFull as any)?.published_by));
    c("compiled block carries the real UFC positioning",
      publishedCompiled.includes("premium, evidence-led sales coaching platform"));
    c("compiled block carries the real UFC objections",
      publishedCompiled.includes("It's too expensive") && publishedCompiled.includes("Just send me some info"));
    c("compiled block carries the no-go compliance language",
      publishedCompiled.includes("Guaranteed revenue increase"));

    // Dana can still read her own company's published context over HTTP.
    const danaGet = await hit("GET", "/v1/intelligence/context", dana);
    c("Dana reads the published UFC context over HTTP",
      danaGet.status === 200 && danaGet.data?.company_id === UFC_COMPANY_ID &&
      danaGet.data?.published?.version === 1,
      `got ${danaGet.status}`);

    // ---- 2. The seeded UFC scorecard --------------------------------------
    console.log("— Seeded UFC scorecard");

    c("UFC Sales Scorecard is active and the company default",
      (seededCard as any).is_company_default === true);

    const { data: activeVer } = await supa
      .from("scorecard_versions").select("id, version, status, snapshot")
      .eq("scorecard_id", cardId).eq("status", "active").maybeSingle();
    const activeVersionId = String((activeVer as any)?.id ?? "");
    c("an active version exists with an immutable snapshot",
      Boolean(activeVersionId) && Boolean((activeVer as any)?.snapshot));
    const snapStages = ((activeVer as any)?.snapshot?.stages ?? []) as any[];
    c("snapshot uses only the fixed four stages",
      JSON.stringify(snapStages.map((s) => s.stage)) === JSON.stringify(["intro", "discovery", "objection", "close"]),
      JSON.stringify(snapStages.map((s) => s.stage)));
    c("snapshot weights total 100 (20/30/30/20)",
      snapStages.reduce((t, s) => t + (Number(s.weight) || 0), 0) === 100 &&
      JSON.stringify(snapStages.map((s) => Number(s.weight))) === JSON.stringify([20, 30, 30, 20]));

    const danaList = await hit("GET", "/v1/intelligence/scorecards", dana);
    c("Dana sees the scorecard in her Studio list over HTTP",
      danaList.status === 200 && JSON.stringify(danaList.data ?? {}).includes(SCORECARD_NAME),
      `got ${danaList.status}`);

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
    c("scorecard prompt block carries the seeded criteria",
      scBlock.includes("Isolate price concern and reframe value") &&
      scBlock.includes("Secure clear next step and commitment"));
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
    c("_meta.context_published_at = the seeded publish timestamp",
      meta.context_published_at === (ctxFull as any)?.published_at, JSON.stringify(meta.context_published_at));
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
    // Edit the seeded draft, prove scoring ignores it, then restore it exactly
    // — the draft is demo data now, not this validator's to leave mangled.
    const { data: draftBefore } = await supa
      .from("company_context").select("context").eq("id", (draftRow as any)?.id).single();
    const draftEdit = await hit("PUT", "/v1/intelligence/context", dana, {
      context: { profile: { about: "DAY222 DRAFT EDIT — must never reach scoring." } },
    });
    c("Dana can edit the draft after publishing", draftEdit.status === 200);
    const ctxAfterDraftEdit = await resolvePublishedContext(supa, UFC_COMPANY_ID);
    c("draft edits never change what the runtime scores with",
      JSON.stringify(ctxAfterDraftEdit) === JSON.stringify(liveCtx));
    c("the runtime's context never contains draft-only text",
      !String(ctxAfterDraftEdit?.compiled_context).includes("DAY222 DRAFT EDIT"));

    const { error: restoreErr } = await supa
      .from("company_context").update({ context: (draftBefore as any)?.context })
      .eq("id", (draftRow as any)?.id);
    c("seeded draft restored exactly after the isolation probe", !restoreErr, restoreErr?.message);

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
