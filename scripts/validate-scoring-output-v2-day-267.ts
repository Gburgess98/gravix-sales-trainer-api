/**
 * validate-scoring-output-v2-day-267.ts
 *
 * Proves the Day-267 criteria-level Scoring v2 RUNTIME (src/lib/scoringV2.ts).
 * PURE / NO-COST: no OpenAI/Anthropic, no network, no DB, no paid calls, no env
 * mutation. Every lane feeds data through the SAME production functions a live
 * run would use.
 *
 * The key Day-267 proof (Lane A):
 *   mocked provider JSON  ->  parseAndValidateScoreV2 (production)
 *                         ->  validated ScoreV2
 *                         ->  projectScoreV2ToV1 (production)
 *                         ->  Day-266 harness gates (unchanged)
 *
 * Lanes:
 *   A  mocked-provider runtime proof through the Day-266 harness
 *   B  honest deterministic stub + heuristic fallback (structural)
 *   C  cache v2 isolation (v1 vs openai-v2 vs stub-v2; Day 262 intact)
 *   D  deterministic guarantees (criterion IDs, weights, roll-ups, provenance, prompt)
 *   E  non-vacuity — every planted violation is caught by a named check
 *   F  v1 regression — a v1-only score still serialises in its existing shape
 *
 * Usage: npx tsx scripts/validate-scoring-output-v2-day-267.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

import {
  parseAndValidateScoreV2,
  projectScoreV2ToV1,
  withV1Projection,
  resolveCriteriaSpec,
  resolveCriterionId,
  buildScoreCacheKeyV2,
  buildScoringV2Prompt,
  buildStubScoreV2,
  buildFallbackScoreV2,
  rollUpOverall,
  distributeWeights,
  InvalidModelOutputError,
  STAGES_V2,
  SCORING_PROMPT_VERSION_V2,
  RUBRIC_VERSION_V2,
  CACHE_KEY_VERSION_V2,
  STUB_SCORING_MODEL,
  type ScoreV2,
  type ScoringV2Context,
  type ResolvedCriteriaSpec,
} from "../src/lib/scoringV2";
import type { ResolvedScorecard } from "../src/lib/intelligenceRuntime";
// Production v1 cache-key builder + v1 fallback — imported to prove v2 cannot
// reuse a v1 cache entry and that a v1-only score still serialises (Lane F).
import { buildDeterministicPromptKey, heuristicScoreFallback } from "../src/lib/scoring";
// The Day-266 harness gates — reused unchanged (NOT weakened).
import {
  runHarness,
  validateStructure,
  validateV1Projection,
  POLICY,
} from "../test/harness/scoring-v2-harness";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(title: string) {
  console.log(`\n── ${title} ──`);
}
function load(rel: string): any {
  return JSON.parse(readFileSync(join(__dirname, "..", rel), "utf8"));
}
function clone<T>(x: T): T {
  return JSON.parse(JSON.stringify(x));
}

const FIXED_VOICE = { clarity: 72, confidence: 70, filler_density: 30, pace: 65, overall: 70 };
const BAND_REP: Record<string, number> = { excellent: 90, strong: 78, mixed: 60, weak: 40, poor: 22 };

// ── Build the UFC scorecard the golden set was authored from ──────────────────
// One criterion per stage; labels read from the golden dataset itself so the
// slug-match is guaranteed. Stage weights 20/30/30/20 (four sum to 100), the
// same config the Day-266 authored candidates use.
function deriveUfcScorecard(golden: any): ResolvedScorecard {
  const STAGE_WEIGHT: Record<string, number> = { intro: 20, discovery: 30, objection: 30, close: 20 };
  const STAGE_EMPHASIS: Record<string, string> = { intro: "standard", discovery: "major", objection: "major", close: "major" };
  const labelByStage = new Map<string, string>();
  for (const call of golden.calls) {
    for (const c of call.expected_criteria) {
      if (!labelByStage.has(c.stage)) labelByStage.set(c.stage, c.label);
      else if (labelByStage.get(c.stage) !== c.label) {
        throw new Error(`golden has >1 criterion label for stage ${c.stage}; validator assumes one`);
      }
    }
  }
  const stages = STAGES_V2.map((stage) => ({
    stage,
    weight: STAGE_WEIGHT[stage],
    criteria: [{ label: labelByStage.get(stage)!, emphasis: STAGE_EMPHASIS[stage], pass_fail: false, critical: false }],
  }));
  return {
    source: "company_default",
    scorecard_id: "sc-ufc-001",
    scorecard_version_id: "scv-ufc-001-v1",
    scorecard_version: 1,
    scorecard_name: "UFC Sales Scorecard",
    call_types: [],
    snapshot: { stages },
    cache_key: "scv-ufc-001-v1",
  };
}

// ── Build MOCKED provider JSON (what OpenAI would return, pre-normalisation) ───
// Deterministically derived from the golden expectations. Deliberately carries a
// BOGUS overall_score and points_lost so the proof shows the RUNTIME recomputes
// them (never trusting the model). Evidence quotes are copied verbatim from the
// golden transcript. NOT an LLM — no network.
function buildMockedModelOutput(call: any): any {
  const stages = STAGES_V2.map((stage) => {
    const gc = call.expected_criteria.find((c: any) => c.stage === stage);
    const notObserved = gc.status === "not_observed";
    return {
      stage,
      criteria: [
        {
          label: gc.label,
          status: gc.status,
          score: notObserved ? null : BAND_REP[gc.expected_score_band],
          evidence: notObserved
            ? []
            : (gc.evidence || []).map((e: any) => ({ quote: e.quote, segment_index: e.segment_index, speaker: e.speaker ?? null })),
          why_points_lost: gc.why_points_lost ?? null,
          points_lost: 999, // BOGUS — runtime must recompute
          coaching_action: gc.coaching_action ?? "Maintain strong execution on this criterion.",
          suggested_drill: gc.suggested_drill ?? null,
        },
      ],
    };
  });
  const objection_matches = (call.expected_objection_matches || []).map((o: any) => ({
    detected_text: o.detected_text,
    objection_item_key: o.objection_item_key,
    objection_label: o.objection_label,
    category: o.category,
    handled: o.handled,
    evidence: o.evidence
      ? { quote: o.evidence.quote, segment_index: o.evidence.segment_index, speaker: o.evidence.speaker ?? null }
      : null,
  }));
  return {
    contract_version: "v2",
    overall_score: 12345, // BOGUS — runtime must recompute
    summary: call.expected_summary,
    stages,
    objection_matches,
    confidence: { level: "medium", value: 0.7 },
    degraded_score: false,
    degraded_reason: null,
  };
}

function ctxFor(spec: ResolvedCriteriaSpec, scorecard: ResolvedScorecard, call: any): ScoringV2Context {
  const segments = call.transcript || [];
  return {
    spec,
    resolvedScorecard: scorecard,
    resolvedContext: null,
    segments,
    fullTranscript: segments.map((s: any) => String(s?.text || "")).join("\n"),
    voice: FIXED_VOICE,
    scoringProvider: "openai",
    scoringModel: "gpt-4o-mini",
    priorAverage: null,
  };
}

function main() {
  console.log("Scoring v2 RUNTIME validator (Day 267) — NO paid calls, NO model calls, NO DB, NO network");
  console.log(`Policy: harness stage-score projection = ${POLICY.STAGE_SCORE_PROJECTION} (custom-scorecard lane); runtime custom path = custom_criteria_authoritative\n`);

  const golden = load("test/fixtures/scoring-v2/golden-calls.json");
  const scorecard = deriveUfcScorecard(golden);
  const spec = resolveCriteriaSpec(scorecard);

  // ── Lane A: mocked-provider runtime proof through the Day-266 harness ───────
  section("LANE A — mocked provider JSON → runtime parse → ScoreV2 → v1 projection → Day-266 harness");
  const byId: Record<string, any> = {};
  const v2ById: Record<string, ScoreV2> = {};
  for (const call of golden.calls) {
    const raw = JSON.stringify(buildMockedModelOutput(call)); // string, as a real provider returns
    const ctx = ctxFor(spec, scorecard, call);
    const v2 = parseAndValidateScoreV2(raw, ctx);
    v2ById[call.id] = v2;
    byId[call.id] = { call_id: call.id, v2: withV1Projection(v2, { policy: "custom_criteria_authoritative" }) };
  }
  const report = runHarness(golden, byId);
  console.log(`  calls passed:         ${report.calls_passed}/${report.calls_total}`);
  console.log(`  criteria passed:      ${report.criteria_passed}/${report.criteria_total}`);
  console.log(`  status accuracy:      ${report.status_accuracy}%`);
  console.log(`  score-band accuracy:  ${report.band_accuracy}%`);
  console.log(`  evidence-grounding:   ${report.evidence_grounding_accuracy}%`);
  console.log(`  evidence-overlap:     ${report.evidence_overlap_accuracy}%`);
  console.log(`  objection-match:      ${report.objection_calls_matched}/${report.objection_calls_total}`);
  console.log(`  v1 projection pass:   ${report.v1_projection_pass}/${report.calls_total}`);
  console.log(`  structural pass:      ${report.structural_pass}/${report.calls_total}`);
  for (const c of report.per_call) {
    if (c.passed) continue;
    console.log(`  ✗ ${c.call_id}`);
    for (const s of c.structural_issues) console.log(`      [structural] ${s}`);
    for (const v of c.v1_issues) console.log(`      [v1] ${v}`);
    if (!c.overall_band_match) console.log(`      [overall] band mismatch`);
    for (const cr of c.criteria) for (const r of cr.reasons) console.log(`      [${cr.stage}] ${r}`);
    for (const r of c.objection.reasons) console.log(`      [objection] ${r}`);
  }
  gate("all golden calls pass the harness via runtime output", report.calls_passed === report.calls_total, `${report.calls_passed}/${report.calls_total}`);
  gate("all criteria pass", report.criteria_passed === report.criteria_total);
  gate("status/band/evidence accuracy all 100%",
    report.status_accuracy === 100 && report.band_accuracy === 100 && report.evidence_grounding_accuracy === 100 && report.evidence_overlap_accuracy === 100);
  gate("objection matches on every call", report.objection_calls_matched === report.objection_calls_total);
  gate("v1 projection valid on every call", report.v1_projection_pass === report.calls_total);
  gate("structural valid on every call", report.structural_pass === report.calls_total);
  // Runtime authority over model-authored roll-up / points.
  gate("runtime recomputed overall (ignored bogus model overall_score=12345)",
    Object.values(v2ById).every((v) => v.overall_score !== 12345 && v.overall_score === rollUpOverall(v.stages)));
  gate("runtime recomputed points_lost (ignored bogus model points_lost=999)",
    Object.values(v2ById).every((v) => v.stages.every((s) => s.criteria.every((c) =>
      (c.status === "partial" || c.status === "fail") ? (typeof c.points_lost === "number" && c.points_lost !== 999) : c.points_lost === null))));

  // ── Lane B: honest stub + heuristic fallback (structural) ──────────────────
  section("LANE B — honest deterministic stub + heuristic fallback (structural only)");
  const anyCall = golden.calls[0];
  const stubCtx = { spec, resolvedScorecard: scorecard, resolvedContext: null, voice: FIXED_VOICE };
  const stub = buildStubScoreV2(stubCtx);
  const stubFull = anyCall.transcript.map((s: any) => String(s?.text || "")).join("\n");
  const stubIssues = validateStructure(stub, stubFull, anyCall.transcript);
  const stubProj = validateV1Projection(withV1Projection(stub, { policy: "default_v1_parity" }));
  gate("stub is structurally valid", stubIssues.length === 0, stubIssues.join("; "));
  gate("stub is honestly degraded (stub_provider)", stub.degraded_score === true && stub.degraded_reason === "stub_provider");
  gate("stub provider/model honest", stub.provenance.scoring_provider === "stub" && stub.provenance.scoring_model === STUB_SCORING_MODEL);
  gate("stub v1 projection valid", stubProj.length === 0, stubProj.join("; "));

  const fb = buildFallbackScoreV2(stubCtx, "no_transcript");
  const fbIssues = validateStructure(fb, "", []);
  gate("heuristic fallback is structurally valid", fbIssues.length === 0, fbIssues.join("; "));
  gate("fallback is honestly degraded with a specific reason", fb.degraded_score === true && fb.degraded_reason === "no_transcript" && fb.provenance.scoring_model === "heuristic:v1");

  // ── Lane C: cache v2 isolation (Day 262 intact) ────────────────────────────
  section("LANE C — cache key v2 isolation (v1 vs openai-v2 vs stub-v2)");
  const keyArgs = { callId: "call-x", filename: "x.mp3", sha256: "abc", transcript: anyCall.transcript.map((s: any) => s.text).join(" "), contextVersion: 1, scorecardCacheKey: "scv-ufc-001-v1" };
  const v1Key = buildDeterministicPromptKey(keyArgs).key;
  const v2Openai = buildScoreCacheKeyV2({ ...keyArgs, scoringProvider: "openai", scoringModel: "gpt-4o-mini" }).key;
  const v2Stub = buildScoreCacheKeyV2({ ...keyArgs, scoringProvider: "stub", scoringModel: STUB_SCORING_MODEL }).key;
  gate("v2 (openai) key differs from v1 key", v1Key !== v2Openai);
  gate("v2 openai cannot reuse a v1 entry (no shared namespace token)", !v1Key.includes(`cachever=${CACHE_KEY_VERSION_V2}`) && v2Openai.includes(`cachever=${CACHE_KEY_VERSION_V2}`));
  gate("v2 key carries v2 prompt+rubric markers", v2Openai.includes(`prompt=${SCORING_PROMPT_VERSION_V2}`) && v2Openai.includes(`rubric=${RUBRIC_VERSION_V2}`));
  gate("stub-v2 key differs from openai-v2 key (Day 262 provider isolation)", v2Openai !== v2Stub);
  gate("openai-v2 has no provider segment; stub-v2 does", !v2Openai.includes("provider=") && v2Stub.includes("provider=stub"));

  // ── Lane D: deterministic guarantees ───────────────────────────────────────
  section("LANE D — deterministic IDs, weights, roll-ups, provenance, prompt");
  // Criterion IDs stable + match the documented derivation, and are re-derivable.
  const introSpec = spec.stages.find((s) => s.stage === "intro")!;
  const expectedIntroId = resolveCriterionId({ scorecardVersionId: "scv-ufc-001-v1", source: "company_default", stage: "intro", label: introSpec.criteria[0].label });
  gate("criterion_id matches <version>:<stage>:<slug> derivation", introSpec.criteria[0].criterion_id === expectedIntroId, `${introSpec.criteria[0].criterion_id} != ${expectedIntroId}`);
  const defaultSpec = resolveCriteriaSpec(null);
  gate("built-in rubric uses gravix_default:<stage>:<slug>", defaultSpec.stages.every((s) => s.criteria.every((c) => c.criterion_id.startsWith(`gravix_default:${s.stage}:`))));
  // Re-parse identical input → identical IDs + overall (determinism, no randomness).
  const c0 = golden.calls[0];
  const a = parseAndValidateScoreV2(JSON.stringify(buildMockedModelOutput(c0)), ctxFor(spec, scorecard, c0));
  const b = parseAndValidateScoreV2(JSON.stringify(buildMockedModelOutput(c0)), ctxFor(spec, scorecard, c0));
  gate("re-scoring the same call is byte-identical (no UUIDs, deterministic)", JSON.stringify(a) === JSON.stringify(b));
  // Weights.
  gate("distributeWeights sums to exactly 100", [1, 2, 3, 4, 5, 7].every((n) => distributeWeights(n).reduce((x, y) => x + y, 0) === 100));
  gate("every stage: criterion weights sum to 100", Object.values(v2ById).every((v) => v.stages.every((s) => s.criteria.reduce((x, c) => x + c.weight, 0) === 100)));
  gate("stage weights sum to 100", Object.values(v2ById).every((v) => v.stages.reduce((x, s) => x + s.weight, 0) === 100));
  // Provenance completeness.
  const provKeys = ["scoring_provider", "scoring_model", "scorecard_source", "scorecard_id", "scorecard_version_id", "scorecard_version", "context_version", "prompt_version", "rubric_version", "cache_key_version", "criteria_version"];
  gate("provenance carries every required key", Object.values(v2ById).every((v) => provKeys.every((k) => (v.provenance as any)[k] !== undefined)));
  // Prompt contract enumerates criteria + demands verbatim evidence, no SDK.
  const prompt = buildScoringV2Prompt({ spec, segments: c0.transcript, scorecardName: "UFC Sales Scorecard" });
  gate("v2 prompt enumerates each criterion_id (model cannot invent)", spec.stages.every((s) => s.criteria.every((c) => prompt.user.includes(c.criterion_id))));
  gate("v2 prompt demands verbatim evidence + fixed stages + contract v2", /VERBATIM/.test(prompt.system) && /intro, discovery, objection, close/.test(prompt.system) && prompt.promptVersion === SCORING_PROMPT_VERSION_V2);

  // ── Lane E: non-vacuity (every planted violation must be caught) ────────────
  section("LANE E — non-vacuity: planted violations must be caught");
  const good = clone(byId[golden.calls[2].id]); // golden-003: all observed, has partial/fail + objections
  const goodCall = golden.calls[2];
  const goodSegs = goodCall.transcript;
  const goodFull = goodSegs.map((s: any) => String(s?.text || "")).join("\n");
  const structFails = (v2: any) => validateStructure(v2, goodFull, goodSegs).length > 0;
  const v1Fails = (cand: any) => validateV1Projection(cand.v2).length > 0;

  // sanity: the un-tampered candidate is clean
  gate("baseline candidate is clean (structure + v1)", !structFails(good.v2) && !v1Fails(good), "tampering baseline is dirty");

  function violation(name: string, run: () => boolean) {
    gate(`caught: ${name}`, run());
  }

  // Parser-layer plants (raw model input) — parser must reject.
  const rawFor = () => buildMockedModelOutput(goodCall);
  const parseThrows = (mutate: (raw: any) => void, match?: RegExp) => {
    const raw = rawFor();
    mutate(raw);
    try {
      parseAndValidateScoreV2(JSON.stringify(raw), ctxFor(spec, scorecard, goodCall));
      return false;
    } catch (e: any) {
      return e instanceof InvalidModelOutputError && (!match || match.test(e.message));
    }
  };
  violation("invent a criterion (parser rejects)", () => parseThrows((r) => r.stages[0].criteria.push({ label: "made up", status: "pass", score: 80, evidence: r.stages[0].criteria[0].evidence, coaching_action: "x" }), /invented_criterion/));
  violation("invent an evidence quote (parser rejects ungrounded)", () => parseThrows((r) => { r.stages[0].criteria[0].evidence = [{ quote: "this text is not in the transcript at all", segment_index: 0 }]; }, /ungrounded_evidence/));
  violation("remove why_points_lost from a fail (parser rejects)", () => parseThrows((r) => { r.stages[2].criteria[0].why_points_lost = ""; }, /missing_why_points_lost/));
  violation("remove score from an observed pass (parser rejects)", () => parseThrows((r) => { r.stages[0].criteria[0].score = null; }, /observed_missing_score/));
  violation("put a score on not_observed (parser rejects)", () => {
    // use golden-001 which has a not_observed objection stage
    const c1 = golden.calls[0];
    const raw = buildMockedModelOutput(c1);
    const objStage = raw.stages.find((s: any) => s.stage === "objection");
    objStage.criteria[0].score = 50;
    try { parseAndValidateScoreV2(JSON.stringify(raw), ctxFor(spec, scorecard, c1)); return false; }
    catch (e: any) { return e instanceof InvalidModelOutputError && /not_observed_with_score/.test(e.message); }
  });
  violation("missing stage (parser rejects)", () => parseThrows((r) => { r.stages.splice(1, 1); }, /missing_stage/));
  violation("duplicate stage (parser rejects)", () => parseThrows((r) => { r.stages[1] = clone(r.stages[0]); }, /duplicate_stage/));
  violation("wrong contract_version (parser rejects)", () => parseThrows((r) => { r.contract_version = "v1"; }, /contract_version/));

  // Structural-layer plants (final ScoreV2) — harness validateStructure must catch.
  violation("remove one stage (harness structural)", () => { const v = clone(good.v2); v.stages.splice(2, 1); return structFails(v); });
  violation("duplicate one stage (harness structural)", () => { const v = clone(good.v2); v.stages[1] = clone(v.stages[0]); return structFails(v); });
  violation("break criterion weights (harness structural)", () => { const v = clone(good.v2); v.stages[0].criteria[0].weight = 50; return structFails(v); });
  violation("break stage weights (harness structural)", () => { const v = clone(good.v2); v.stages[0].weight = 5; return structFails(v); });
  violation("remove evidence span (harness structural)", () => { const v = clone(good.v2); v.stages[0].criteria[0].evidence[0].segment_index = null; v.stages[0].criteria[0].evidence[0].start_sec = null; return structFails(v); });
  violation("tampered evidence quote (harness structural)", () => { const v = clone(good.v2); v.stages[0].criteria[0].evidence[0].quote = "definitely not in transcript"; return structFails(v); });
  violation("mark stub as non-degraded (harness structural)", () => { const v: any = clone(stub); v.degraded_score = false; return validateStructure(v, stubFull, anyCall.transcript).length > 0; });
  violation("remove degraded_reason from a degraded score (harness structural)", () => { const v: any = clone(stub); v.degraded_reason = null; return validateStructure(v, stubFull, anyCall.transcript).length > 0; });

  // Deterministic roll-up plant — runtime consistency check must catch.
  violation("alter deterministic overall roll-up", () => { const v = clone(good.v2); v.overall_score = v.overall_score + 20; return rollUpOverall(v.stages) !== v.overall_score; });

  // Criterion-id identity plant — identity check against spec must catch.
  const specIdSet = new Set(spec.stages.flatMap((s) => s.criteria.map((c) => c.criterion_id)));
  violation("change a criterion_id (identity vs spec)", () => { const v = clone(good.v2); v.stages[0].criteria[0].criterion_id = "tampered-id"; return !v.stages.every((s: any) => s.criteria.every((c: any) => specIdSet.has(c.criterion_id))); });

  // v1-projection plants — harness validateV1Projection must catch.
  violation("remove v1 close stage (v1 projection)", () => { const c = clone(good); delete c.v2.v1_projection.stages.close; return v1Fails(c); });
  violation("force v1/v2 stage-score mismatch (authoritative_v2 policy)", () => {
    const c = clone(good);
    const introV2 = c.v2.stages.find((s: any) => s.stage === "intro");
    c.v2.v1_projection.stages.intro.score = introV2.score + 15;
    return v1Fails(c);
  });
  violation("reuse a v1 cache key for v2 (must be a different namespace)", () => v1Key !== v2Openai);

  // ── Lane F: v1 regression — a v1-only score still serialises unchanged ──────
  section("LANE F — v1 regression: v1-only readers untouched");
  const v1Only = heuristicScoreFallback(); // classic v1 shape, NO analysis_json.v2
  gate("v1-only score has four numeric stages + notes",
    ["intro", "discovery", "objection", "close"].every((s) => typeof (v1Only.stages as any)[s].score === "number" && typeof (v1Only.stages as any)[s].notes === "string"));
  gate("v1-only moments & suggestions are arrays", Array.isArray(v1Only.moments) && Array.isArray(v1Only.suggestions));
  gate("v1-only score needs no v2 field to read", !("v2" in (v1Only as any)) && !("contract_version" in (v1Only as any)));
  // Our v2 projection's _meta is a superset of the v1 keys a reader needs.
  const projMeta = projectScoreV2ToV1(v2ById[golden.calls[0].id]).rubric._meta;
  gate("v2 projection _meta preserves the v1 keys a reader needs",
    ["rubric_version", "prompt_version", "model_version", "scoring_model_version", "scorecard_source"].every((k) => projMeta[k] != null));
  gate("v2 projection moments & suggestions are arrays",
    Array.isArray(projectScoreV2ToV1(v2ById[golden.calls[0].id]).moments) && Array.isArray(projectScoreV2ToV1(v2ById[golden.calls[0].id]).suggestions));

  // ── No-cost / no-provider self-assertion ───────────────────────────────────
  section("SELF-CHECK — no network / DB / provider-SDK call in the v2 runtime");
  const runtimeSrc = readFileSync(join(__dirname, "..", "src", "lib", "scoringV2.ts"), "utf8");
  gate("scoringV2 makes no provider SDK / LLM call", !/chat\.completions|messages\.create|getOpenAI\(|new OpenAI/.test(runtimeSrc));
  gate("scoringV2 opens no DB / network handle", !/supabase|createClient|fetch\(|axios/.test(runtimeSrc));

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s). No model calls, no paid calls, no DB, no network.`);
  console.log("Proven: mocked-provider JSON flows through the production v2 parser + projection and passes the Day-266 golden gates; stub/fallback are honestly degraded; cache v2 is isolated; every planted violation is caught; v1 readers are untouched.");
  console.log("NOT proven: that a real model semantically understands these calls — that needs a separately authorised live run.");
  process.exit(failures === 0 ? 0 : 1);
}

main();
