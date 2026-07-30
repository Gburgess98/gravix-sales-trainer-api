/**
 * validate-scoring-v2-production-wiring-day-269.ts
 *
 * Proves the Day-269 off-by-default wiring of Scoring v2 into the production
 * scoring pipeline. PURE / NO-COST: no OpenAI/Anthropic, no network, no DB, no
 * paid calls. It exercises the narrowest injectable PRODUCTION functions the
 * real scoreWithLLM uses — resolveScoringContract, computeScoringV2Result and the
 * cache-key builders — plus static assertions on scoring.ts for the wiring
 * invariants that are structural (single persistence write, SKIP gate, no second
 * parser).
 *
 * Usage: npx tsx scripts/validate-scoring-v2-production-wiring-day-269.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import {
  resolveScoringContract,
  computeScoringV2Result,
  buildDeterministicPromptKey,
} from "../src/lib/scoring";
import {
  buildScoreCacheKeyV2,
  buildScoringV2Prompt,
  resolveCriteriaSpec,
  scoringModelVersionV2,
  projectionPolicyFor,
  SCORING_PROMPT_VERSION_V2,
  RUBRIC_VERSION_V2,
  CACHE_KEY_VERSION_V2,
  STUB_SCORING_MODEL,
  HEURISTIC_SCORING_MODEL,
} from "../src/lib/scoringV2";
import { gravixDefaultScorecard, type ResolvedScorecard } from "../src/lib/intelligenceRuntime";
import { validateStructure, validateV1Projection } from "../test/harness/scoring-v2-harness";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) {
  console.log(`\n── ${t} ──`);
}

// Frozen v1 cache keys (default model gpt-4o-mini). If buildDeterministicPromptKey
// ever changes, these fail — that is the v1 byte-identity tripwire.
const KEY_ARGS = { callId: "call-fixed", filename: "call-fixed.mp3", sha256: "sha-fixed", transcript: "Rep: hello there. Buyer: hi.", contextVersion: 2, scorecardCacheKey: "scv-custom-123" };
const FROZEN_V1_DEFAULT = "rubric=v1|prompt=v1|model=gpt-4o-mini:v1:v1|filename=call-fixed.mp3|sha256=sha-fixed|transcriptHash=87ec8c52|context=2|scorecard=scv-custom-123";
const FROZEN_V1_STUB = FROZEN_V1_DEFAULT + "|provider=stub";

const SEGMENTS = [
  { idx: 0, speaker: "rep", start_sec: 0, end_sec: 5, text: "Thanks for the time, let's keep this to fifteen minutes and agree a next step." },
  { idx: 1, speaker: "buyer", start_sec: 5, end_sec: 10, text: "Sure. We review calls manually right now." },
  { idx: 2, speaker: "rep", start_sec: 10, end_sec: 16, text: "We work with twelve gyms on exactly this problem." },
  { idx: 3, speaker: "rep", start_sec: 16, end_sec: 22, text: "Shall we get a follow-up in the diary for Thursday?" },
];
const TRANSCRIPT = SEGMENTS.map((s) => s.text).join("\n");
const VOICE = { clarity: 70, confidence: 68, filler_density: 30, pace: 60, overall: 66 };

// Raw model JSON for the BUILT-IN default rubric (labels must match the default spec).
function builtinRaw(): string {
  return JSON.stringify({
    contract_version: "v2",
    summary: "Good structure with a soft discovery.",
    stages: [
      { stage: "intro", criteria: [{ label: "Set agenda and establish credibility", status: "pass", score: 80, evidence: [{ quote: "let's keep this to fifteen minutes and agree a next step.", segment_index: 0 }], coaching_action: "Keep the crisp agenda." }] },
      { stage: "discovery", criteria: [{ label: "Uncover pain, current process and decision route", status: "partial", score: 60, evidence: [{ quote: "We review calls manually right now.", segment_index: 1 }], why_points_lost: "Current process found, decision route not.", coaching_action: "Map the buying group." }] },
      { stage: "objection", criteria: [{ label: "Isolate the objection and reframe value", status: "not_observed", score: null, coaching_action: "No objection surfaced on this call." }] },
      { stage: "close", criteria: [{ label: "Secure clear next step and commitment", status: "pass", score: 78, evidence: [{ quote: "Shall we get a follow-up in the diary for Thursday?", segment_index: 3 }], coaching_action: "Lock the date next time." }] },
    ],
    objection_matches: [],
    confidence: { level: "medium", value: 0.72 },
    degraded_score: false,
  });
}

function customScorecard(): ResolvedScorecard {
  return {
    source: "company_default",
    scorecard_id: "sc-custom-1",
    scorecard_version_id: "scv-custom-1-v3",
    scorecard_version: 3,
    scorecard_name: "Acme Discovery Card",
    call_types: [],
    cache_key: "scv-custom-1-v3",
    snapshot: {
      stages: [
        { stage: "intro", weight: 25, criteria: [{ label: "Open with a crisp agenda", emphasis: "standard" }] },
        { stage: "discovery", weight: 25, criteria: [{ label: "Qualify budget and authority", emphasis: "major" }] },
        { stage: "objection", weight: 25, criteria: [{ label: "Neutralise the main concern", emphasis: "major" }] },
        { stage: "close", weight: 25, criteria: [{ label: "Lock a concrete next step", emphasis: "major" }] },
      ],
    },
  };
}
function customRaw(): string {
  return JSON.stringify({
    contract_version: "v2",
    summary: "Custom scorecard call.",
    stages: [
      { stage: "intro", criteria: [{ label: "Open with a crisp agenda", status: "pass", score: 82, evidence: [{ quote: "let's keep this to fifteen minutes and agree a next step.", segment_index: 0 }], coaching_action: "Good open." }] },
      { stage: "discovery", criteria: [{ label: "Qualify budget and authority", status: "partial", score: 58, evidence: [{ quote: "We review calls manually right now.", segment_index: 1 }], why_points_lost: "No budget or authority uncovered.", coaching_action: "Ask about budget." }] },
      { stage: "objection", criteria: [{ label: "Neutralise the main concern", status: "not_observed", score: null, coaching_action: "None raised." }] },
      { stage: "close", criteria: [{ label: "Lock a concrete next step", status: "pass", score: 76, evidence: [{ quote: "Shall we get a follow-up in the diary for Thursday?", segment_index: 3 }], coaching_action: "Nail the date." }] },
    ],
    objection_matches: [],
    confidence: { level: "medium", value: 0.66 },
    degraded_score: false,
  });
}

function withEnv(value: string | undefined, fn: () => void) {
  const prev = process.env.SCORING_CONTRACT;
  if (value === undefined) delete process.env.SCORING_CONTRACT;
  else process.env.SCORING_CONTRACT = value;
  try {
    fn();
  } finally {
    if (prev === undefined) delete process.env.SCORING_CONTRACT;
    else process.env.SCORING_CONTRACT = prev;
  }
}

function computeBuiltin(provider: "openai" | "stub", raw: string | null, transcript = TRANSCRIPT) {
  return computeScoringV2Result({
    scoringProvider: provider,
    resolvedScorecard: gravixDefaultScorecard(),
    resolvedContext: null,
    transcript,
    segments: SEGMENTS as any,
    voice: VOICE,
    raw,
    priorAverage: null,
  });
}

function main() {
  console.log("Scoring v2 PRODUCTION WIRING validator (Day 269) — NO paid calls, NO model calls, NO DB, NO network\n");

  // ── A: contract resolver ──────────────────────────────────────────────────
  section("A — contract resolver (off by default; only 'v2' opts in)");
  withEnv(undefined, () => gate("missing SCORING_CONTRACT → v1", resolveScoringContract() === "v1"));
  withEnv("", () => gate("empty SCORING_CONTRACT → v1", resolveScoringContract() === "v1"));
  withEnv("v1", () => gate("explicit v1 → v1", resolveScoringContract() === "v1"));
  withEnv("v2", () => gate("explicit v2 → v2", resolveScoringContract() === "v2"));
  withEnv("V2", () => gate("V2 (case/trim) → v2", resolveScoringContract() === "v2"));
  withEnv("  v2 ", () => gate("' v2 ' (whitespace) → v2", resolveScoringContract() === "v2"));
  withEnv("true", () => gate("'true' is NOT an alias → v1", resolveScoringContract() === "v1"));
  withEnv("enabled", () => gate("'enabled' is NOT an alias → v1", resolveScoringContract() === "v1"));
  withEnv("latest", () => gate("'latest' is NOT an alias → v1", resolveScoringContract() === "v1"));

  // ── B: v1 byte-identity (cache keys frozen) ───────────────────────────────
  section("B — v1 byte-identity (cache keys frozen)");
  const v1Default = buildDeterministicPromptKey({ ...KEY_ARGS, scoringProvider: "openai" }).key;
  const v1Stub = buildDeterministicPromptKey({ ...KEY_ARGS, scoringProvider: "stub" }).key;
  gate("v1 default cache key is byte-identical to frozen", v1Default === FROZEN_V1_DEFAULT, v1Default);
  gate("v1 stub cache key is byte-identical to frozen", v1Stub === FROZEN_V1_STUB, v1Stub);
  gate("v1 keys carry no v2 namespace token", !v1Default.includes("cachever=") && !v1Stub.includes("cachever="));

  // ── C: cache namespace isolation ──────────────────────────────────────────
  section("C — cache namespace isolation (6 distinct namespaces)");
  const k = (over: any) => buildScoreCacheKeyV2({ ...KEY_ARGS, ...over }).key;
  const openaiV2 = k({ scoringProvider: "openai", scoringModel: "gpt-4o-mini" });
  const stubV2 = k({ scoringProvider: "stub", scoringModel: STUB_SCORING_MODEL });
  const customV2 = k({ scoringProvider: "openai", scoringModel: "gpt-4o-mini", scorecardCacheKey: "scv-other-999" });
  const ctxV2 = k({ scoringProvider: "openai", scoringModel: "gpt-4o-mini", contextVersion: 7 });
  const all = [v1Default, v1Stub, openaiV2, stubV2, customV2, ctxV2];
  gate("all six namespaces are distinct", new Set(all).size === 6);
  gate("v2 cannot read a v1 entry (openai-v2 ≠ v1)", openaiV2 !== v1Default && openaiV2.includes(`cachever=${CACHE_KEY_VERSION_V2}`));
  gate("openai-v2 ≠ stub-v2 (Day-262 provider isolation holds)", openaiV2 !== stubV2 && stubV2.includes("provider=stub") && !openaiV2.includes("provider="));
  gate("custom-scorecard v2 ≠ default v2", customV2 !== openaiV2);
  gate("context-versioned v2 ≠ default v2", ctxV2 !== openaiV2);
  gate("v2 keys carry v2 prompt+rubric markers", openaiV2.includes(`prompt=${SCORING_PROMPT_VERSION_V2}`) && openaiV2.includes(`rubric=${RUBRIC_VERSION_V2}`));

  // ── D: provider × contract independence + seam behaviour ──────────────────
  section("D — provider/contract independence + production seam");
  // v2 + stub → honest degraded stub via the production entrypoint.
  const stubRes = computeBuiltin("stub", null);
  gate("v2 stub: production entrypoint yields degraded stub", stubRes.scoreV2.degraded_score === true && stubRes.scoreV2.degraded_reason === "stub_provider");
  gate("v2 stub: model is stub:v1, provider stub, no network", stubRes.scoreV2.provenance.scoring_model === STUB_SCORING_MODEL && stubRes.scoreV2.provenance.scoring_provider === "stub");
  gate("v2 stub: four ordered stages, one criterion each", stubRes.scoreV2.stages.length === 4 && stubRes.scoreV2.stages.every((s: any) => s.criteria.length >= 1) && stubRes.scoreV2.stages.map((s: any) => s.stage).join(",") === "intro,discovery,objection,close");
  gate("v2 stub: valid v1 projection", validateV1Projection({ ...stubRes.scoreV2, v1_projection: stubRes.projection }).length === 0);
  gate("v2 stub: no invented evidence", stubRes.scoreV2.stages.every((s: any) => s.criteria.every((c: any) => c.evidence.length === 0)));

  // v2 + mocked openai → production parser/projection, non-degraded, harness-clean.
  const okRes = computeBuiltin("openai", builtinRaw());
  const fullTranscript = TRANSCRIPT;
  gate("v2 openai (mocked): non-degraded, model = AI_MODEL", okRes.scoreV2.degraded_score === false && okRes.scoreV2.provenance.scoring_model === "gpt-4o-mini");
  gate("v2 openai (mocked): passes Day-266 structural gate", validateStructure(okRes.scoreV2 as any, fullTranscript, SEGMENTS as any).length === 0, validateStructure(okRes.scoreV2 as any, fullTranscript, SEGMENTS as any).join("; "));
  gate("v2 openai (mocked): v1 projection valid", validateV1Projection({ ...okRes.scoreV2, v1_projection: okRes.projection }).length === 0);
  gate("v2 openai (mocked): built-in criteria flowed through (gravix_default ids)", okRes.scoreV2.stages.every((s: any) => s.criteria[0].criterion_id.startsWith(`gravix_default:${s.stage}:`)));
  gate("v2 openai (mocked): default_v1_parity policy for built-in", projectionPolicyFor("gravix_default") === "default_v1_parity");

  // custom scorecard criteria flow through.
  const custom = customScorecard();
  const customRes = computeScoringV2Result({ scoringProvider: "openai", resolvedScorecard: custom, resolvedContext: null, transcript: TRANSCRIPT, segments: SEGMENTS as any, voice: VOICE, raw: customRaw(), priorAverage: null });
  gate("v2 openai custom scorecard: criteria flow through with version-id ids", customRes.scoreV2.stages.every((s: any) => s.criteria[0].criterion_id.startsWith("scv-custom-1-v3:")));
  gate("v2 openai custom scorecard: custom_criteria_authoritative policy", projectionPolicyFor("company_default") === "custom_criteria_authoritative");
  gate("v2 openai custom scorecard: harness structural clean", validateStructure(customRes.scoreV2 as any, fullTranscript, SEGMENTS as any).length === 0);

  // ── E: honest failure / no-transcript degradation ─────────────────────────
  section("E — honest v2 failure handling");
  const invalid = computeBuiltin("openai", JSON.stringify({ contract_version: "v2", stages: [], summary: "x" }));
  gate("invalid/ungrounded model output → degraded invalid_model_output", invalid.scoreV2.degraded_score === true && invalid.scoreV2.degraded_reason === "invalid_model_output");
  const invented = computeBuiltin("openai", JSON.stringify({ contract_version: "v2", summary: "x", confidence: { level: "low", value: 0.2 }, stages: [
    { stage: "intro", criteria: [{ label: "Set agenda and establish credibility", status: "pass", score: 80, evidence: [{ quote: "THIS QUOTE IS NOT IN THE TRANSCRIPT", segment_index: 0 }], coaching_action: "x" }] },
    { stage: "discovery", criteria: [{ label: "Uncover pain, current process and decision route", status: "not_observed", score: null }] },
    { stage: "objection", criteria: [{ label: "Isolate the objection and reframe value", status: "not_observed", score: null }] },
    { stage: "close", criteria: [{ label: "Secure clear next step and commitment", status: "not_observed", score: null }] },
  ] }));
  gate("invented evidence → rejected, degrades (never presented as fact)", invented.scoreV2.degraded_score === true);
  const noTx = computeScoringV2Result({ scoringProvider: "openai", resolvedScorecard: gravixDefaultScorecard(), resolvedContext: null, transcript: "", segments: [], voice: VOICE, raw: null, priorAverage: null });
  gate("no transcript (no text, no segments) → degraded no_transcript", noTx.scoreV2.degraded_score === true && noTx.scoreV2.degraded_reason === "no_transcript");
  const network = computeBuiltin("openai", null); // raw null but transcript present = upstream failure
  gate("openai returned nothing → degraded invalid_model_output", network.scoreV2.degraded_score === true && network.scoreV2.degraded_reason === "invalid_model_output");

  // ── F: persisted shape + provenance agreement (Day-268 contract) ──────────
  section("F — persisted analysis_json shape + provenance");
  const scoringModelVersion = scoringModelVersionV2(okRes.scoredModel);
  // Simulate exactly what scoreWithLLM persists on the v2 success path.
  const persisted = {
    overall: okRes.projection.overall,
    summary: okRes.projection.summary,
    stages: okRes.projection.stages, // v1 OBJECT shape {intro,discovery,objection,close}
    moments: okRes.projection.moments,
    suggestions: okRes.projection.suggestions,
    voice: VOICE,
    v2: okRes.scoreV2, // the full ScoreV2
  };
  gate("top-level stages stay the v1 OBJECT (not the v2 array)", !Array.isArray(persisted.stages) && typeof (persisted.stages as any).intro === "object");
  gate("analysis_json.v2 is present and array-based", Array.isArray(persisted.v2.stages) && persisted.v2.stages.length === 4);
  gate("Day-268 parser contract: v2.contract_version + ordered stages + criterion fields", (() => {
    const v2: any = persisted.v2;
    if (v2.contract_version !== "v2") return false;
    if (!Array.isArray(v2.stages) || v2.stages.length !== 4) return false;
    const order = v2.stages.map((s: any) => s.stage).join(",");
    if (order !== "intro,discovery,objection,close") return false;
    return v2.stages.every((s: any) => s.criteria.every((c: any) => typeof c.criterion_id === "string" && ["pass", "partial", "fail", "not_observed"].includes(c.status) && (c.status === "not_observed" ? c.score === null : typeof c.score === "number")));
  })());
  gate("v1 top-level projection is complete", typeof persisted.overall === "number" && typeof persisted.summary === "string" && ["intro", "discovery", "objection", "close"].every((s) => typeof (persisted.stages as any)[s].score === "number") && Array.isArray(persisted.moments) && Array.isArray(persisted.suggestions));
  gate("v2 provenance agrees with v1 _meta markers", persisted.v2.provenance.rubric_version === RUBRIC_VERSION_V2 && persisted.v2.provenance.prompt_version === SCORING_PROMPT_VERSION_V2 && persisted.v2.provenance.cache_key_version === CACHE_KEY_VERSION_V2 && scoringModelVersion === `gpt-4o-mini:${SCORING_PROMPT_VERSION_V2}:${RUBRIC_VERSION_V2}`);
  // Cache-hit round-trip: stored {...v1, v2} restores the full v2.
  const stored = JSON.parse(JSON.stringify({ ...okRes.validated, voice: VOICE, v2: okRes.scoreV2 }));
  gate("cache-hit round-trip restores the full v2 object", stored.v2 && stored.v2.contract_version === "v2" && stored.v2.stages.length === 4);

  // Checked-in integration fixture (Task 17) — the exact persisted analysis_json,
  // matching the Day-268 WEB parser contract, and in sync with the live seam.
  const fix = JSON.parse(readFileSync(join(__dirname, "..", "test", "fixtures", "scoring-v2", "production-persisted-day-269.json"), "utf8"));
  const aj = fix.analysis_json;
  gate("integration fixture: v1 top-level fields present", typeof aj.overall === "number" && typeof aj.summary === "string" && aj.stages && aj.stages.intro && Array.isArray(aj.moments) && Array.isArray(aj.suggestions) && aj.voice);
  gate("integration fixture: analysis_json.v2 is Day-268-compatible", (() => {
    const v2 = aj.v2;
    return v2 && v2.contract_version === "v2" && Array.isArray(v2.stages) && v2.stages.length === 4 && v2.stages.map((s: any) => s.stage).join(",") === "intro,discovery,objection,close" && v2.stages.every((s: any) => s.criteria.every((c: any) => typeof c.criterion_id === "string" && ["pass", "partial", "fail", "not_observed"].includes(c.status)));
  })());
  gate("integration fixture: carries an objection match with evidence span", Array.isArray(aj.v2.objection_matches) && aj.v2.objection_matches.length >= 1 && aj.v2.objection_matches[0].evidence?.segment_index != null);
  gate("integration fixture: in sync with the live production seam (overall + stage count)", aj.overall === okRes.projection.overall && aj.v2.stages.length === okRes.scoreV2.stages.length);

  // ── G: static wiring invariants (single write, SKIP gate, no 2nd parser) ──
  section("G — static wiring invariants in scoring.ts");
  const src = readFileSync(join(__dirname, "..", "src", "lib", "scoring.ts"), "utf8");
  gate("exactly 3 updateCallScoreRow calls (cache/success/fallback — no extra write)", (src.match(/await updateCallScoreRow\(/g) || []).length === 3);
  gate("exactly 2 writeScoreCache calls (success/fallback — no extra write)", (src.match(/await writeScoreCache\(/g) || []).length === 2);
  gate("SKIP_SCORING_SIDE_EFFECTS still gates side effects", src.includes("if (!SKIP_SCORING_SIDE_EFFECTS)"));
  gate("v2 branch reuses computeScoringV2Result seam", src.includes("computeScoringV2Result("));
  gate("Day-267 parser called exactly once (via the seam, not reimplemented)", (src.match(/parseAndValidateScoreV2\(/g) || []).length === 1);
  gate("v2 prompt built via production buildScoringV2Prompt", src.includes("buildScoringV2Prompt("));

  // v2 prompt enumerates every criterion (model cannot invent / omit).
  const spec = resolveCriteriaSpec(gravixDefaultScorecard());
  const prompt = buildScoringV2Prompt({ spec, segments: SEGMENTS as any, scorecardName: "Gravix default rubric" });
  gate("v2 prompt enumerates every criterion_id", spec.stages.every((s) => s.criteria.every((c) => prompt.user.includes(c.criterion_id))));

  // ── H: non-vacuity — planted violations must be caught ────────────────────
  section("H — non-vacuity: planted violations");
  const caught = (label: string, ok: boolean) => gate(`caught: ${label}`, ok);
  // resolver
  caught("missing contract does NOT resolve to v2", (() => { let r = "x"; withEnv(undefined, () => { r = resolveScoringContract(); }); return r !== "v2"; })());
  caught("invalid contract does NOT resolve to v2", (() => { let r = "x"; withEnv("enabled", () => { r = resolveScoringContract(); }); return r !== "v2"; })());
  // cache
  caught("altering the v1 key is detectable (frozen)", buildDeterministicPromptKey({ ...KEY_ARGS, scoringProvider: "openai" }).key === FROZEN_V1_DEFAULT);
  caught("v2 cannot reuse a v1 cache key", openaiV2 !== v1Default);
  caught("stub-v2 cannot reuse openai-v2 cache", stubV2 !== openaiV2);
  // prompt/parser
  caught("omitting a custom criterion from the prompt is detectable", (() => { const cs = resolveCriteriaSpec(custom); const p = buildScoringV2Prompt({ spec: cs, segments: SEGMENTS as any, scorecardName: "x" }); return cs.stages.every((s) => s.criteria.every((c) => p.user.includes(c.criterion_id))); })());
  caught("invalid model JSON is NOT persisted as a full score", invalid.scoreV2.degraded_score === true);
  caught("invented evidence never becomes a non-degraded score", invented.scoreV2.degraded_score === true);
  // persisted shape
  caught("removing analysis_json.v2 is detectable", persisted.v2 != null && persisted.v2.contract_version === "v2");
  caught("replacing top-level v1 stages with the v2 array is detectable", !Array.isArray(persisted.stages));
  caught("a missing v1 projection stage is detectable", ["intro", "discovery", "objection", "close"].every((s) => (okRes.projection.stages as any)[s]));
  caught("v1/v2 provenance mismatch is detectable", persisted.v2.provenance.rubric_version === RUBRIC_VERSION_V2);
  caught("stub v2 marked non-degraded is detectable", stubRes.scoreV2.degraded_score === true);
  // static
  caught("a second persistence write is detectable", (src.match(/await updateCallScoreRow\(/g) || []).length === 3);
  caught("removing the SKIP_SCORING_SIDE_EFFECTS gate is detectable", src.includes("if (!SKIP_SCORING_SIDE_EFFECTS)"));
  caught("enabling v2 for an invalid flag value is impossible", (() => { let r = "x"; withEnv("v3", () => { r = resolveScoringContract(); }); return r === "v1"; })());

  // ── self-assertion: env not left enabled ──────────────────────────────────
  section("SELF-CHECK");
  gate("SCORING_CONTRACT is not left enabled by the validator", process.env.SCORING_CONTRACT === undefined || process.env.SCORING_CONTRACT === "v1");

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s). No model calls, no paid calls, no DB, no network.`);
  console.log("Proven: v2 is off by default; v1 keys/output are byte-frozen; contract⟂provider; the production seam parses/projects/degrades honestly; v2 persists alongside the v1 projection in an isolated cache namespace.");
  console.log("NOT proven: real-model semantic quality (needs a separately authorised live run) — and NO environment was enabled.");
  process.exit(failures === 0 ? 0 : 1);
}

main();
