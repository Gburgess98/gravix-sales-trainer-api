// test/harness/build-candidate-results.ts — Day 266
//
// ONE-TIME deterministic builder: reads the Day-265 golden dataset and emits
// test/fixtures/scoring-v2/candidate-results.json — a set of "matching" v2
// candidate outputs (what a correct v2 scorer *would* return for these calls).
//
// NOT an LLM. Pure, deterministic transform. These candidates are checked in as
// authored fixtures so the harness compares two independent files; they exist to
// prove the harness accepts good input and (via planted violations) rejects bad
// input. They are NOT a claim that any real scorer understands these transcripts.
//
// Usage: npx tsx test/harness/build-candidate-results.ts

import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

const BAND_REP: Record<string, number> = { excellent: 90, strong: 78, mixed: 60, weak: 40, poor: 22 };
const STAGE_WEIGHT: Record<string, number> = { intro: 20, discovery: 30, objection: 30, close: 20 };
// Authoring emphasis → contract enum (major → high).
const STAGE_EMPHASIS: Record<string, string> = { intro: "standard", discovery: "high", objection: "high", close: "high" };
const DRILL_KEY_TO_SEVERITY: Record<string, string> = {};

function bandScore(band: string | null): number | null {
  if (band === null) return null;
  return BAND_REP[band] ?? 50;
}

function momentType(stage: string, status: string): string {
  if (stage === "close") return "closing_attempt";
  if (status === "fail" || status === "partial") return "mistake";
  return "highlight";
}
function severityFor(status: string): string {
  return status === "fail" ? "high" : status === "partial" ? "medium" : "low";
}

function main() {
  const goldenPath = join(__dirname, "..", "fixtures", "scoring-v2", "golden-calls.json");
  const golden = JSON.parse(readFileSync(goldenPath, "utf8"));

  const candidates = golden.calls.map((call: any) => {
    // Build v2 stages from expected criteria (one criterion per stage here).
    const v2stages = ["intro", "discovery", "objection", "close"].map((stage) => {
      const gc = call.expected_criteria.find((c: any) => c.stage === stage);
      const score = bandScore(gc.expected_score_band);
      const pointsLost =
        gc.status === "partial" || gc.status === "fail"
          ? Math.round((STAGE_WEIGHT[stage] * (100 - (score ?? 0))) / 100)
          : null;
      const criterion = {
        criterion_id: gc.criterion_id,
        label: gc.label,
        stage,
        score,
        status: gc.status,
        weight: 100,
        emphasis: STAGE_EMPHASIS[stage],
        pass_fail: false,
        evidence: gc.evidence.map((e: any) => ({
          quote: e.quote,
          start_sec: null,
          end_sec: null,
          segment_index: e.segment_index,
          speaker: e.speaker ?? null,
        })),
        why_points_lost: gc.why_points_lost ?? null,
        points_lost: pointsLost,
        coaching_action: gc.coaching_action ?? null,
        suggested_drill: gc.suggested_drill ?? null,
      };
      return {
        stage,
        score,
        weight: STAGE_WEIGHT[stage],
        status: gc.status,
        notes:
          gc.status === "partial" || gc.status === "fail"
            ? String(gc.why_points_lost)
            : String(gc.coaching_action ?? ""),
        criteria: [criterion],
      };
    });

    // overall = weighted average over OBSERVED stages, re-normalised.
    let wsum = 0, acc = 0;
    for (const s of v2stages) {
      if (s.status === "not_observed" || s.score == null) continue;
      wsum += s.weight; acc += s.weight * s.score;
    }
    const overall = wsum ? Math.round(acc / wsum) : 0;

    const objection_matches = (call.expected_objection_matches || []).map((o: any) => ({
      detected_text: o.detected_text,
      objection_item_id: null,
      objection_item_key: o.objection_item_key,
      objection_label: o.objection_label,
      category: o.category,
      handled: o.handled,
      evidence: o.evidence
        ? { quote: o.evidence.quote, start_sec: null, end_sec: null, segment_index: o.evidence.segment_index, speaker: o.evidence.speaker ?? null }
        : null,
    }));

    // v1 projection
    const v1stages: any = {};
    for (const s of v2stages) {
      v1stages[s.stage] =
        s.status === "not_observed"
          ? { score: overall, notes: "Not assessed on this call." }
          : { score: s.score, notes: s.notes };
    }
    const moments: any[] = [];
    for (const s of v2stages) {
      if (s.status === "not_observed") continue;
      for (const ev of s.criteria[0].evidence) {
        moments.push({ timestamp: null, type: momentType(s.stage, s.status), text: String(ev.quote).slice(0, 280), severity: severityFor(s.status) });
      }
    }
    for (const o of objection_matches) {
      moments.push({ timestamp: null, type: "objection", text: String(o.detected_text).slice(0, 280), severity: "medium" });
    }
    const suggestions = Array.from(new Set(v2stages.map((s) => s.criteria[0].coaching_action).filter(Boolean).map((t: any) => String(t).slice(0, 220)))).slice(0, 6);

    const confLevel = overall >= 70 ? "high" : overall >= 50 ? "medium" : "medium";
    const confValue = overall >= 70 ? 0.9 : 0.7;

    const meta = {
      rubric_version: "v2",
      prompt_version: "scoring-prompt-v2",
      model_version: "gpt-4o-mini",
      scoring_model_version: "gpt-4o-mini:v2:v2",
      scorecard_name: "UFC Sales Scorecard",
      scorecard_source: "company_default",
      context_version: 1,
      transcript_present: true,
      scoring_provider: "openai",
      contract_version: "v2",
      confidence: { level: confLevel, value: confValue },
      degraded_score: false,
    };

    return {
      call_id: call.id,
      note: "authored candidate (deterministic build from golden; no LLM)",
      v2: {
        contract_version: "v2",
        overall_score: overall,
        summary: call.expected_summary,
        stages: v2stages,
        objection_matches,
        confidence: { level: confLevel, value: confValue },
        degraded_score: false,
        degraded_reason: null,
        voice: { clarity: 72, confidence: 70, filler_density: 30, pace: 65, overall: 70 },
        provenance: {
          scoring_provider: "openai",
          scoring_model: "gpt-4o-mini",
          scorecard_source: "company_default",
          scorecard_id: "sc-ufc-001",
          scorecard_version_id: "scv-ufc-001-v1",
          scorecard_version: 1,
          context_version: 1,
          prompt_version: "scoring-prompt-v2",
          rubric_version: "v2",
          cache_key_version: "v2",
        },
        trend_delta: overall >= 70 ? 5 : overall >= 50 ? -1 : -6,
        v1_projection: {
          overall,
          summary: call.expected_summary,
          stages: v1stages,
          moments,
          suggestions,
          voice: { clarity: 72, confidence: 70, filler_density: 30, pace: 65, overall: 70 },
          rubric: { _meta: meta },
        },
      },
    };
  });

  const out = {
    contract: "SCORING_OUTPUT_CONTRACT_V2",
    dataset_version: "candidate-v1",
    generated_by: "test/harness/build-candidate-results.ts (deterministic; no LLM)",
    stage_score_projection_policy: "authoritative_v2",
    candidates,
  };

  const outPath = join(__dirname, "..", "fixtures", "scoring-v2", "candidate-results.json");
  writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
  console.log(`Wrote ${candidates.length} candidate results → ${outPath}`);
}

main();
