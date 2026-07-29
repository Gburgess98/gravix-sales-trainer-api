/**
 * validate-scoring-v2-harness-day-266.ts
 *
 * Runs the deterministic Scoring v2 harness (test/harness/scoring-v2-harness.ts)
 * over the Day-265 golden dataset and the Day-266 authored candidate results.
 * PURE / NO-COST: no OpenAI/Anthropic, no network, no DB, no paid calls.
 *
 * Two lanes:
 *   1. FIXTURE lane — authored "correct" candidates vs golden. Full structural +
 *      golden + v1-projection comparison. This is the meaningful gate.
 *   2. STUB lane — a deterministic no-cost v2 stub (buildStubV2Candidate) is run
 *      through structural validation + degraded-honesty checks. STRUCTURAL ONLY —
 *      it is NOT proof of scoring quality, just that the harness handles the
 *      honest degraded shape a stub provider would emit.
 *
 * Exits non-zero if any gate fails. Usage:
 *   npx tsx scripts/validate-scoring-v2-harness-day-266.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import {
  runHarness,
  validateStructure,
  buildStubV2Candidate,
  POLICY,
} from "../test/harness/scoring-v2-harness";

let gateFailures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) gateFailures += 1;
}

function load(rel: string): any {
  return JSON.parse(readFileSync(join(__dirname, "..", rel), "utf8"));
}

function main() {
  const golden = load("test/fixtures/scoring-v2/golden-calls.json");
  const candidates = load("test/fixtures/scoring-v2/candidate-results.json");

  console.log("Scoring v2 deterministic harness (Day 266) — NO paid calls, NO model calls");
  console.log(`Policy: stage-score projection = ${POLICY.STAGE_SCORE_PROJECTION}; evidence overlap = ${POLICY.EVIDENCE_OVERLAP}\n`);

  const byId: Record<string, any> = {};
  for (const c of candidates.candidates) byId[c.call_id] = c;

  // ── Lane 1: fixture lane ───────────────────────────────────────────────────
  const report = runHarness(golden, byId);

  console.log("── FIXTURE LANE (authored candidates vs golden) ──");
  console.log(`  calls passed:            ${report.calls_passed}/${report.calls_total}`);
  console.log(`  criteria passed:         ${report.criteria_passed}/${report.criteria_total}`);
  console.log(`  status accuracy:         ${report.status_accuracy}%`);
  console.log(`  score-band accuracy:     ${report.band_accuracy}%`);
  console.log(`  evidence-grounding:      ${report.evidence_grounding_accuracy}%`);
  console.log(`  evidence-overlap:        ${report.evidence_overlap_accuracy}%`);
  console.log(`  objection-match calls:   ${report.objection_calls_matched}/${report.objection_calls_total}`);
  console.log(`  v1 projection pass:      ${report.v1_projection_pass}/${report.calls_total}`);
  console.log(`  structural pass:         ${report.structural_pass}/${report.calls_total}`);

  for (const c of report.per_call) {
    if (c.passed) continue;
    console.log(`\n  ✗ ${c.call_id} FAILED`);
    for (const s of c.structural_issues) console.log(`      [structural] ${s}`);
    for (const v of c.v1_issues) console.log(`      [v1] ${v}`);
    if (!c.overall_band_match) console.log(`      [overall] band mismatch`);
    for (const cr of c.criteria) for (const r of cr.reasons) console.log(`      [${cr.stage}] ${r}`);
    for (const r of c.objection.reasons) console.log(`      [objection] ${r}`);
  }
  console.log("");

  gate("all golden calls pass the harness", report.calls_passed === report.calls_total, `${report.calls_passed}/${report.calls_total}`);
  gate("all criteria pass", report.criteria_passed === report.criteria_total, `${report.criteria_passed}/${report.criteria_total}`);
  gate("status accuracy is 100%", report.status_accuracy === 100);
  gate("score-band accuracy is 100%", report.band_accuracy === 100);
  gate("evidence-grounding is 100%", report.evidence_grounding_accuracy === 100);
  gate("evidence-overlap is 100%", report.evidence_overlap_accuracy === 100);
  gate("objection matches on every call", report.objection_calls_matched === report.objection_calls_total);
  gate("v1 projection valid on every call", report.v1_projection_pass === report.calls_total);
  gate("structural valid on every call", report.structural_pass === report.calls_total);

  // ── Lane 2: stub lane (structural-only, honest degrade) ────────────────────
  console.log("\n── STUB LANE (deterministic no-cost v2 stub — STRUCTURAL ONLY, not scoring quality) ──");
  let stubStructOk = 0, stubDegradedOk = 0;
  for (const call of golden.calls) {
    const segments = call.transcript || [];
    const full = segments.map((s: any) => String(s?.text || "")).join("\n");
    const stub = buildStubV2Candidate(call);
    const issues = validateStructure(stub, full, segments);
    if (issues.length === 0) stubStructOk += 1;
    else console.log(`  ✗ ${call.id} stub structural issues: ${issues.join("; ")}`);
    if (stub.degraded_score === true && stub.degraded_reason === "stub_provider") stubDegradedOk += 1;
  }
  console.log(`  stub structural pass:    ${stubStructOk}/${golden.calls.length}`);
  console.log(`  stub honestly degraded:  ${stubDegradedOk}/${golden.calls.length}`);
  gate("stub candidates are structurally valid", stubStructOk === golden.calls.length);
  gate("stub candidates are honestly marked degraded", stubDegradedOk === golden.calls.length);

  // ── No-cost / no-provider self-assertion ───────────────────────────────────
  const harnessSrc = readFileSync(join(__dirname, "..", "test", "harness", "scoring-v2-harness.ts"), "utf8");
  gate("harness has no provider SDK / LLM imports", !/from ["']openai["']|@anthropic-ai|chat\.completions|messages\.create/.test(harnessSrc));

  console.log(`\n${gateFailures === 0 ? "PASS" : "FAIL"} — ${gateFailures} gate failure(s). No model calls, no paid calls.`);
  console.log("Proven: structural conformance + agreement with the human golden set + v1 back-compat projection.");
  console.log("NOT proven: that any real scorer understands these calls — that needs the Day 267 v2 scorer.");
  process.exit(gateFailures === 0 ? 0 : 1);
}

main();
