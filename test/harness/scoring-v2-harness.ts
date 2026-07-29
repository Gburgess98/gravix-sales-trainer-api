// test/harness/scoring-v2-harness.ts — Day 266
//
// Deterministic, no-cost comparison harness for Scoring Output Contract v2.
// PURE: no env mutation, no DB, no network, no provider SDK, no LLM. Every
// function here takes data in and returns findings out.
//
// It answers one question: does a candidate v2 scoring result (a) obey the v2
// contract structurally, (b) agree with the Day-265 human golden dataset, and
// (c) project cleanly down to the v1 shape the app reads today. It does NOT and
// cannot judge whether a real scorer *understood* a call — that needs a real v2
// scorer (Day 267). This is the safety rail, not the scorer.

/* eslint-disable @typescript-eslint/no-explicit-any */

// ── Policy knobs (documented; change on Day 267 if needed) ───────────────────
export const POLICY = {
  // How the v1 projection's stage score relates to the v2 stage score.
  //   "authoritative_v2" — v1 stage score == v2 stage score (criteria roll-up
  //                        is authoritative). Used by the Day-266 fixtures.
  //   "v1_parity"        — v1 stage score is taken from a separate v1 value
  //                        (the Gravix-default path may want this on Day 267).
  STAGE_SCORE_PROJECTION: "authoritative_v2" as "authoritative_v2" | "v1_parity",
  // Evidence overlap between candidate and golden: same transcript segment AND
  // one quote contains the other (exact match is the trivial case).
  EVIDENCE_OVERLAP: "same_segment_containment" as const,
  // Criterion weights within a stage must sum to this.
  CRITERION_WEIGHT_SUM: 100,
  // Stage weights across the four stages must sum to this.
  STAGE_WEIGHT_SUM: 100,
};

export const STAGES = ["intro", "discovery", "objection", "close"] as const;
export const STATUSES = ["pass", "partial", "fail", "not_observed"] as const;
export type Band = "excellent" | "strong" | "mixed" | "weak" | "poor";
export type BandMap = Record<Band, [number, number]>;

const REQUIRED_V1_META_KEYS = [
  "rubric_version",
  "prompt_version",
  "model_version",
  "scoring_model_version",
  "scorecard_source",
];

// ── helpers ──────────────────────────────────────────────────────────────────
export function scoreInBand(score: number, band: Band | null, bands: BandMap): boolean {
  if (band === null) return false;
  const range = bands[band];
  if (!range) return false;
  return score >= range[0] && score <= range[1];
}

function containment(a: string, b: string): boolean {
  const x = String(a || "").trim();
  const y = String(b || "").trim();
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}

function segText(segments: any[], idx: number | null | undefined): string | null {
  if (idx == null) return null;
  const seg = segments.find((s) => Number(s?.idx) === Number(idx));
  return seg ? String(seg.text || "") : null;
}

/** Is a quote verbatim: substring of the full transcript AND of its cited segment. */
export function quoteIsVerbatim(quote: string, segIdx: number | null | undefined, fullTranscript: string, segments: any[]): boolean {
  const q = String(quote || "");
  if (!q || !fullTranscript.includes(q)) return false;
  if (segIdx != null) {
    const st = segText(segments, segIdx);
    if (st == null || !st.includes(q)) return false;
  }
  return true;
}

/** Overlap: at least one candidate evidence item shares a golden item's segment with containment. */
export function evidenceOverlaps(goldenEvidence: any[], candidateEvidence: any[]): boolean {
  if (!Array.isArray(goldenEvidence) || goldenEvidence.length === 0) return true; // nothing required
  if (!Array.isArray(candidateEvidence) || candidateEvidence.length === 0) return false;
  return goldenEvidence.some((g) =>
    candidateEvidence.some((c) =>
      Number(c?.segment_index) === Number(g?.segment_index) && containment(String(c?.quote), String(g?.quote))
    )
  );
}

// ── Structural validation (contract-shape) ──────────────────────────────────
export function validateStructure(v2: any, fullTranscript: string, segments: any[]): string[] {
  const issues: string[] = [];
  const push = (m: string) => issues.push(m);

  if (v2?.contract_version !== "v2") push(`contract_version is not "v2" (got ${JSON.stringify(v2?.contract_version)})`);
  if (typeof v2?.overall_score !== "number") push("overall_score is missing/not a number");

  const stages: any[] = Array.isArray(v2?.stages) ? v2.stages : [];
  const order = stages.map((s) => s?.stage);
  if (order.length !== 4 || STAGES.some((s, i) => order[i] !== s)) {
    push(`stages must be exactly [${STAGES.join(", ")}] in order (got [${order.join(", ")}])`);
  }
  if (new Set(order).size !== order.length) push("duplicate stage entries");

  let stageWeightSum = 0;
  for (const st of stages) {
    const stName = st?.stage;
    stageWeightSum += Number(st?.weight) || 0;
    const criteria: any[] = Array.isArray(st?.criteria) ? st.criteria : [];
    if (criteria.length === 0) push(`stage ${stName} has no criteria`);

    let critWeightSum = 0;
    for (const c of criteria) {
      const tag = `${stName}/${c?.label ?? "?"}`;
      if (!c?.criterion_id || typeof c.criterion_id !== "string") push(`${tag}: criterion_id missing/unstable`);
      if (!STATUSES.includes(c?.status)) push(`${tag}: invalid status ${JSON.stringify(c?.status)}`);
      critWeightSum += Number(c?.weight) || 0;

      if (c?.status === "not_observed") {
        if (c?.score !== null && c?.score !== undefined) push(`${tag}: not_observed must have null score`);
      } else {
        if (typeof c?.score !== "number") push(`${tag}: observed criterion missing numeric score`);
      }
      if ((c?.status === "partial" || c?.status === "fail") && !(typeof c?.why_points_lost === "string" && c.why_points_lost.trim())) {
        push(`${tag}: ${c?.status} lacks why_points_lost`);
      }
      if ((c?.status === "partial" || c?.status === "fail") && typeof c?.points_lost !== "number") {
        push(`${tag}: ${c?.status} lacks numeric points_lost`);
      }
      const evidence: any[] = Array.isArray(c?.evidence) ? c.evidence : [];
      if (c?.status !== "not_observed" && evidence.length === 0) push(`${tag}: observed criterion has no evidence`);
      for (const ev of evidence) {
        const hasSpan = ev?.segment_index != null || (typeof ev?.start_sec === "number");
        if (!hasSpan) push(`${tag}: evidence lacks a usable span/segment_index`);
        if (!quoteIsVerbatim(String(ev?.quote), ev?.segment_index, fullTranscript, segments)) {
          push(`${tag}: evidence quote is not a verbatim transcript substring`);
        }
      }
    }
    if (criteria.length > 0 && critWeightSum !== POLICY.CRITERION_WEIGHT_SUM) {
      push(`stage ${stName}: criterion weights sum to ${critWeightSum}, expected ${POLICY.CRITERION_WEIGHT_SUM}`);
    }
  }
  if (stages.length === 4 && stageWeightSum !== POLICY.STAGE_WEIGHT_SUM) {
    push(`stage weights sum to ${stageWeightSum}, expected ${POLICY.STAGE_WEIGHT_SUM}`);
  }

  // Degraded honesty
  const model = String(v2?.provenance?.scoring_model || "");
  const provider = String(v2?.provenance?.scoring_provider || "");
  const looksStub = provider === "stub" || /stub|heuristic/.test(model);
  if (v2?.degraded_score === true && !(typeof v2?.degraded_reason === "string" && v2.degraded_reason.trim())) {
    push("degraded_score=true but degraded_reason is missing");
  }
  if (looksStub && v2?.degraded_score !== true) {
    push(`stub/heuristic result (provider=${provider}, model=${model}) must set degraded_score=true`);
  }

  // Provenance
  for (const k of ["scoring_provider", "scoring_model", "scorecard_source", "prompt_version", "rubric_version"]) {
    if (!v2?.provenance || v2.provenance[k] == null) push(`provenance.${k} is missing`);
  }
  if (!v2?.confidence || typeof v2.confidence.value !== "number") push("confidence.value is missing/not a number");

  return issues;
}

// ── v1 projection validation ─────────────────────────────────────────────────
export function validateV1Projection(v2: any): string[] {
  const issues: string[] = [];
  const p = v2?.v1_projection;
  if (!p || typeof p !== "object") return ["v1_projection is missing"];

  if (typeof p.overall !== "number") issues.push("v1.overall missing/not a number");
  if (typeof p.summary !== "string" || !p.summary.trim()) issues.push("v1.summary missing");
  if (!Array.isArray(p.moments)) issues.push("v1.moments is not an array");
  if (!Array.isArray(p.suggestions)) issues.push("v1.suggestions is not an array");
  if (!p.voice || typeof p.voice !== "object") issues.push("v1.voice missing");

  const stages = p.stages || {};
  for (const s of STAGES) {
    const st = stages[s];
    if (!st || typeof st !== "object") { issues.push(`v1.stages.${s} missing`); continue; }
    if (typeof st.score !== "number") issues.push(`v1.stages.${s}.score not a number`);
    if (typeof st.notes !== "string") issues.push(`v1.stages.${s}.notes not a string`);
  }

  const meta = p.rubric?._meta;
  if (!meta || typeof meta !== "object") {
    issues.push("v1.rubric._meta missing");
  } else {
    for (const k of REQUIRED_V1_META_KEYS) {
      if (meta[k] == null) issues.push(`v1.rubric._meta.${k} missing (v1 reader would break)`);
    }
  }

  // Stage-score projection policy
  if (POLICY.STAGE_SCORE_PROJECTION === "authoritative_v2") {
    const v2stages: any[] = Array.isArray(v2?.stages) ? v2.stages : [];
    for (const vs of v2stages) {
      if (vs?.status === "not_observed") continue; // not asserted for not_observed stages
      const proj = stages[vs?.stage];
      if (proj && typeof proj.score === "number" && typeof vs?.score === "number" && proj.score !== vs.score) {
        issues.push(`v1.stages.${vs.stage}.score (${proj.score}) != authoritative v2 stage score (${vs.score})`);
      }
    }
  }
  return issues;
}

// ── Golden comparison ────────────────────────────────────────────────────────
export type CriterionCompare = {
  stage: string;
  label: string;
  status_match: boolean;
  band_match: boolean;
  evidence_grounded: boolean; // candidate evidence verbatim
  evidence_overlap: boolean;  // overlaps golden evidence
  fields_present: boolean;    // why_points_lost / coaching_action / drill as expected
  reasons: string[];
};

export type CallReport = {
  call_id: string;
  structural_issues: string[];
  v1_issues: string[];
  overall_band_match: boolean;
  criteria: CriterionCompare[];
  objection: { match: boolean; reasons: string[] };
  passed: boolean;
};

function findV2Criterion(v2: any, stage: string): any {
  const st = (v2?.stages || []).find((s: any) => s?.stage === stage);
  return (st?.criteria || [])[0] ?? null; // one criterion per stage in this dataset
}

export function compareToGolden(golden: any, v2: any, bands: BandMap, fullTranscript: string, segments: any[]): CallReport {
  const structural_issues = validateStructure(v2, fullTranscript, segments);
  const v1_issues = validateV1Projection(v2);

  const overall_band_match = scoreInBand(Number(v2?.overall_score), golden?.expected_overall_band ?? null, bands);

  const criteria: CriterionCompare[] = [];
  for (const gc of golden?.expected_criteria || []) {
    const reasons: string[] = [];
    const cc = findV2Criterion(v2, gc.stage);
    if (!cc) {
      criteria.push({ stage: gc.stage, label: gc.label, status_match: false, band_match: false, evidence_grounded: false, evidence_overlap: false, fields_present: false, reasons: ["no candidate criterion for stage"] });
      continue;
    }
    const status_match = cc.status === gc.status;
    if (!status_match) reasons.push(`status ${cc.status} != expected ${gc.status}`);

    let band_match: boolean;
    if (gc.status === "not_observed") {
      band_match = cc.score === null || cc.score === undefined;
      if (!band_match) reasons.push("not_observed expected null score");
    } else {
      band_match = typeof cc.score === "number" && scoreInBand(cc.score, gc.expected_score_band, bands);
      if (!band_match) reasons.push(`score ${cc.score} not in expected band ${gc.expected_score_band}`);
    }

    const candEvidence: any[] = Array.isArray(cc.evidence) ? cc.evidence : [];
    const evidence_grounded = candEvidence.every((ev) => quoteIsVerbatim(String(ev?.quote), ev?.segment_index, fullTranscript, segments));
    if (!evidence_grounded) reasons.push("candidate evidence not all verbatim");
    const evidence_overlap = evidenceOverlaps(gc.evidence || [], candEvidence);
    if (!evidence_overlap) reasons.push("candidate evidence does not overlap golden evidence");

    let fields_present = true;
    if ((gc.status === "partial" || gc.status === "fail") && !(typeof cc.why_points_lost === "string" && cc.why_points_lost.trim())) {
      fields_present = false; reasons.push("missing why_points_lost");
    }
    if (!(typeof cc.coaching_action === "string" && cc.coaching_action.trim())) {
      fields_present = false; reasons.push("missing coaching_action");
    }
    if (gc.suggested_drill && !(cc.suggested_drill && cc.suggested_drill.key)) {
      fields_present = false; reasons.push("expected suggested_drill missing");
    }

    criteria.push({ stage: gc.stage, label: gc.label, status_match, band_match, evidence_grounded, evidence_overlap, fields_present, reasons });
  }

  // objection matches
  const objReasons: string[] = [];
  const goldenObj: any[] = golden?.expected_objection_matches || [];
  const candObj: any[] = Array.isArray(v2?.objection_matches) ? v2.objection_matches : [];
  const candKeys = new Set(candObj.map((o) => o?.objection_item_key));
  for (const go of goldenObj) {
    const co = candObj.find((o) => o?.objection_item_key === go.objection_item_key);
    if (!co) { objReasons.push(`missing expected objection ${go.objection_item_key}`); continue; }
    if (co.objection_label !== go.objection_label) objReasons.push(`label mismatch for ${go.objection_item_key}`);
    if (co.handled !== go.handled) objReasons.push(`handled ${co.handled} != expected ${go.handled} for ${go.objection_item_key}`);
    if (!quoteIsVerbatim(String(co.detected_text), co?.evidence?.segment_index, fullTranscript, segments)) {
      objReasons.push(`detected_text not verbatim for ${go.objection_item_key}`);
    }
  }
  const goldenKeys = new Set(goldenObj.map((o) => o.objection_item_key));
  for (const k of candKeys) {
    if (!goldenKeys.has(k)) objReasons.push(`invented objection match not in golden: ${k}`);
  }
  const objection = { match: objReasons.length === 0, reasons: objReasons };

  const criteriaPass = criteria.every((c) => c.status_match && c.band_match && c.evidence_grounded && c.evidence_overlap && c.fields_present);
  const passed = structural_issues.length === 0 && v1_issues.length === 0 && overall_band_match && criteriaPass && objection.match;

  return { call_id: golden?.id, structural_issues, v1_issues, overall_band_match, criteria, objection, passed };
}

// ── Deterministic no-cost v2 STUB (structural lane; not a real scorer) ────────
// Mirrors how SCORING_PROVIDER=stub would shape a v2 result: honest, degraded,
// one synthetic not_observed criterion per stage, no evidence, no paid call.
export function buildStubV2Candidate(golden: any): any {
  const stages = STAGES.map((stage) => ({
    stage,
    score: null,
    weight: stage === "discovery" || stage === "objection" ? 30 : 20,
    status: "not_observed",
    notes: "Not assessed — deterministic stub provider.",
    criteria: [{
      criterion_id: `stub:${golden.id}:${stage}`,
      label: `stub-${stage}`,
      stage,
      score: null,
      status: "not_observed",
      weight: 100,
      emphasis: "standard",
      pass_fail: false,
      evidence: [],
      why_points_lost: null,
      points_lost: null,
      coaching_action: "Run with a real scorer for criterion-level feedback.",
      suggested_drill: null,
    }],
  }));
  return {
    contract_version: "v2",
    overall_score: 0,
    summary: "Deterministic stub score (no model call).",
    stages,
    objection_matches: [],
    confidence: { level: "low", value: 0 },
    degraded_score: true,
    degraded_reason: "stub_provider",
    voice: { clarity: 0, confidence: 0, filler_density: 0, pace: 0, overall: 0 },
    provenance: {
      scoring_provider: "stub",
      scoring_model: "stub:v1",
      scorecard_source: "gravix_default",
      scorecard_id: null,
      scorecard_version_id: null,
      scorecard_version: null,
      context_version: null,
      prompt_version: "scoring-prompt-v2",
      rubric_version: "v2",
      cache_key_version: "v2",
    },
    trend_delta: 0,
    v1_projection: {
      overall: 0,
      summary: "Deterministic stub score (no model call).",
      stages: {
        intro: { score: 0, notes: "Not assessed — stub." },
        discovery: { score: 0, notes: "Not assessed — stub." },
        objection: { score: 0, notes: "Not assessed — stub." },
        close: { score: 0, notes: "Not assessed — stub." },
      },
      moments: [],
      suggestions: [],
      voice: { clarity: 0, confidence: 0, filler_density: 0, pace: 0, overall: 0 },
      rubric: {
        _meta: {
          rubric_version: "v2",
          prompt_version: "scoring-prompt-v2",
          model_version: "stub:v1",
          scoring_model_version: "stub:v1:v2:v2",
          scorecard_source: "gravix_default",
          contract_version: "v2",
          degraded_score: true,
        },
      },
    },
  };
}

// ── Aggregate runner ─────────────────────────────────────────────────────────
export type HarnessReport = {
  calls_total: number;
  calls_passed: number;
  criteria_total: number;
  criteria_passed: number;
  status_accuracy: number;
  band_accuracy: number;
  evidence_grounding_accuracy: number;
  evidence_overlap_accuracy: number;
  objection_calls_matched: number;
  objection_calls_total: number;
  v1_projection_pass: number;
  structural_pass: number;
  per_call: CallReport[];
};

export function runHarness(dataset: any, candidatesById: Record<string, any>): HarnessReport {
  const bands: BandMap = dataset.bands;
  const per_call: CallReport[] = [];
  let criteria_total = 0, criteria_passed = 0;
  let status_ok = 0, band_ok = 0, ground_ok = 0, overlap_ok = 0;
  let obj_matched = 0, v1_pass = 0, struct_pass = 0;

  for (const golden of dataset.calls) {
    const cand = candidatesById[golden.id];
    const segments = golden.transcript || [];
    const fullTranscript = segments.map((s: any) => String(s?.text || "")).join("\n");
    if (!cand) {
      per_call.push({ call_id: golden.id, structural_issues: ["no candidate result"], v1_issues: [], overall_band_match: false, criteria: [], objection: { match: false, reasons: ["no candidate"] }, passed: false });
      continue;
    }
    const rep = compareToGolden(golden, cand.v2, bands, fullTranscript, segments);
    per_call.push(rep);
    if (rep.structural_issues.length === 0) struct_pass += 1;
    if (rep.v1_issues.length === 0) v1_pass += 1;
    if (rep.objection.match) obj_matched += 1;
    for (const c of rep.criteria) {
      criteria_total += 1;
      if (c.status_match) status_ok += 1;
      if (c.band_match) band_ok += 1;
      if (c.evidence_grounded) ground_ok += 1;
      if (c.evidence_overlap) overlap_ok += 1;
      if (c.status_match && c.band_match && c.evidence_grounded && c.evidence_overlap && c.fields_present) criteria_passed += 1;
    }
  }

  const pct = (n: number) => (criteria_total ? Math.round((n / criteria_total) * 1000) / 10 : 0);
  return {
    calls_total: dataset.calls.length,
    calls_passed: per_call.filter((c) => c.passed).length,
    criteria_total,
    criteria_passed,
    status_accuracy: pct(status_ok),
    band_accuracy: pct(band_ok),
    evidence_grounding_accuracy: pct(ground_ok),
    evidence_overlap_accuracy: pct(overlap_ok),
    objection_calls_matched: obj_matched,
    objection_calls_total: dataset.calls.length,
    v1_projection_pass: v1_pass,
    structural_pass: struct_pass,
    per_call,
  };
}
