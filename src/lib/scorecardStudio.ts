// Intelligence Layer — Day 219B: Scorecard Studio deterministic helpers.
//
// Pure validation/build functions for the scorecard data layer — no AI, no
// network, no randomness. The scoring runtime does not import this yet;
// integration (resolveScorecard + prompt rendering + score_cache keying) is
// a later, explicit step per SCORECARD_STUDIO_SPEC.md.

export const SCORECARD_STAGES = ["intro", "discovery", "objection", "close"] as const;
export type ScorecardStage = (typeof SCORECARD_STAGES)[number];

// Fixed call-type keys (SCORECARD_STUDIO_FIELD_SPEC.md §1 — supersedes the
// Day 207 shorthand enum).
export const SCORECARD_CALL_TYPES = [
  "outbound_cold",
  "inbound_enquiry",
  "discovery",
  "demo",
  "objection_heavy",
  "renewal_upsell",
] as const;

export const CRITERION_EMPHASIS = ["minor", "standard", "major"] as const;

export const MAX_CRITERIA_PER_STAGE = 12; // field spec §3: suggested ≤8, cap 12
const MAX_TEXT_CHARS = 2_000;
const MAX_NAME_CHARS = 120;

// The Gravix default rubric is code, not a database row — it appears in the
// Studio list as a read-only card so managers see the whole picture.
export const GRAVIX_DEFAULT_RUBRIC = {
  id: "gravix-default",
  name: "Gravix Default",
  version: "v1", // matches RUBRIC_VERSION in lib/scoring.ts
  read_only: true,
  stages: SCORECARD_STAGES.map((stage) => ({ stage, weight: 25 })),
} as const;

export function defaultStageWeights(): { stage: ScorecardStage; weight: number; guidance: null }[] {
  return SCORECARD_STAGES.map((stage) => ({ stage, weight: 25, guidance: null }));
}

export type CriterionInput = {
  label: string;
  description?: string | null;
  scoring_guidance?: string | null;
  good_example?: string | null;
  weak_example?: string | null;
  coaching_prompt?: string | null;
  pass_fail?: boolean;
  critical?: boolean;
  emphasis?: string;
};

export type StageInput = {
  weight: number;
  guidance?: string | null;
  criteria?: CriterionInput[];
};

export type VersionPayload = {
  call_types?: string[];
  stages: Record<string, StageInput>;
};

export type ValidationError = { error: string; detail?: string };

function cleanText(v: unknown, field: string, errors: ValidationError[]): string | null {
  if (v == null || v === "") return null;
  if (typeof v !== "string") {
    errors.push({ error: "invalid_field_type", detail: field });
    return null;
  }
  const s = v.trim();
  if (s.length > MAX_TEXT_CHARS) {
    errors.push({ error: "field_too_long", detail: field });
    return null;
  }
  return s || null;
}

export type NormalisedCriterion = {
  stage: ScorecardStage;
  label: string;
  description: string | null;
  scoring_guidance: string | null;
  good_example: string | null;
  weak_example: string | null;
  coaching_prompt: string | null;
  pass_fail: boolean;
  critical: boolean;
  emphasis: (typeof CRITERION_EMPHASIS)[number];
  sort_order: number;
};

export type NormalisedVersion = {
  call_types: string[];
  weights: { stage: ScorecardStage; weight: number; guidance: string | null }[];
  criteria: NormalisedCriterion[];
};

/**
 * Validate + normalise a draft-version payload. Structural rules only —
 * the weights-total-100 rule is an ACTIVATION rule (validateForActivation),
 * never a save blocker (field spec §2: editing is never blocked).
 * Returns { errors } on failure, { value } on success. Deterministic:
 * criteria keep their submitted order, re-numbered 0..n per stage.
 */
export function normaliseVersionPayload(payload: unknown): { value?: NormalisedVersion; errors?: ValidationError[] } {
  const errors: ValidationError[] = [];
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return { errors: [{ error: "payload_must_be_object" }] };
  }
  const p = payload as Record<string, any>;

  const callTypes: string[] = [];
  if (p.call_types != null) {
    if (!Array.isArray(p.call_types)) {
      errors.push({ error: "invalid_call_types" });
    } else {
      for (const t of p.call_types) {
        if (!SCORECARD_CALL_TYPES.includes(t)) errors.push({ error: "invalid_call_type", detail: String(t) });
        else if (!callTypes.includes(t)) callTypes.push(t);
      }
    }
  }

  if (!p.stages || typeof p.stages !== "object" || Array.isArray(p.stages)) {
    errors.push({ error: "stages_must_be_object" });
    return { errors };
  }
  for (const key of Object.keys(p.stages)) {
    if (!SCORECARD_STAGES.includes(key as ScorecardStage)) {
      errors.push({ error: "invalid_stage", detail: key });
    }
  }
  for (const stage of SCORECARD_STAGES) {
    if (!p.stages[stage] || typeof p.stages[stage] !== "object") {
      errors.push({ error: "missing_stage", detail: stage });
    }
  }
  if (errors.length) return { errors };

  const weights: NormalisedVersion["weights"] = [];
  const criteria: NormalisedCriterion[] = [];

  for (const stage of SCORECARD_STAGES) {
    const s = p.stages[stage] as StageInput;
    const weight = Number(s.weight);
    if (!Number.isInteger(weight) || weight < 0 || weight > 100) {
      errors.push({ error: "invalid_stage_weight", detail: `${stage}: ${s.weight}` });
    }
    weights.push({ stage, weight, guidance: cleanText(s.guidance, `${stage}.guidance`, errors) });

    const list = s.criteria ?? [];
    if (!Array.isArray(list)) {
      errors.push({ error: "invalid_criteria_list", detail: stage });
      continue;
    }
    if (list.length > MAX_CRITERIA_PER_STAGE) {
      errors.push({ error: "too_many_criteria", detail: `${stage}: ${list.length} > ${MAX_CRITERIA_PER_STAGE}` });
      continue;
    }
    list.forEach((c, i) => {
      if (!c || typeof c !== "object") {
        errors.push({ error: "invalid_criterion", detail: `${stage}[${i}]` });
        return;
      }
      const label = typeof c.label === "string" ? c.label.trim() : "";
      if (!label) errors.push({ error: "criterion_label_required", detail: `${stage}[${i}]` });
      if (label.length > MAX_NAME_CHARS) errors.push({ error: "field_too_long", detail: `${stage}[${i}].label` });

      const emphasis = String(c.emphasis ?? "standard");
      if (!(CRITERION_EMPHASIS as readonly string[]).includes(emphasis)) {
        errors.push({ error: "invalid_emphasis", detail: `${stage}[${i}]: ${String(c.emphasis)}` });
      }
      const passFail = Boolean(c.pass_fail);
      const critical = Boolean(c.critical);
      if (critical && !passFail) {
        errors.push({ error: "critical_requires_pass_fail", detail: `${stage}[${i}]` });
      }

      criteria.push({
        stage,
        label,
        description: cleanText(c.description, `${stage}[${i}].description`, errors),
        scoring_guidance: cleanText(c.scoring_guidance, `${stage}[${i}].scoring_guidance`, errors),
        good_example: cleanText(c.good_example, `${stage}[${i}].good_example`, errors),
        weak_example: cleanText(c.weak_example, `${stage}[${i}].weak_example`, errors),
        coaching_prompt: cleanText(c.coaching_prompt, `${stage}[${i}].coaching_prompt`, errors),
        pass_fail: passFail,
        critical,
        emphasis: ((CRITERION_EMPHASIS as readonly string[]).includes(emphasis)
          ? emphasis
          : "standard") as NormalisedCriterion["emphasis"],
        sort_order: i,
      });
    });
  }

  if (errors.length) return { errors };
  return { value: { call_types: callTypes, weights, criteria } };
}

/**
 * Activation rules (field spec §5) — structural save rules are assumed to
 * already hold on the stored draft rows.
 */
export function validateForActivation(input: {
  weights: { stage: string; weight: number }[];
  criteriaCount: number;
  callTypes: string[];
  isCompanyDefault: boolean;
}): ValidationError[] {
  const errors: ValidationError[] = [];

  const byStage = new Map(input.weights.map((w) => [w.stage, w.weight]));
  const missing = SCORECARD_STAGES.filter((s) => !byStage.has(s));
  if (missing.length) errors.push({ error: "missing_stage_weights", detail: missing.join(", ") });

  const total = input.weights.reduce((sum, w) => sum + (Number(w.weight) || 0), 0);
  if (!missing.length && total !== 100) {
    errors.push({ error: "weights_must_total_100", detail: `total ${total}` });
  }
  if (input.criteriaCount < 1) errors.push({ error: "at_least_one_criterion_required" });
  if (!input.isCompanyDefault && input.callTypes.length === 0) {
    errors.push({ error: "call_type_or_company_default_required" });
  }
  return errors;
}

/**
 * Deterministic immutable snapshot for an activated version — fixed stage
 * order, criteria in sort order, empty optional fields omitted. This is the
 * structure the scoring runtime will consume when integration lands.
 */
export function buildVersionSnapshot(input: {
  weights: { stage: string; weight: number; guidance?: string | null }[];
  criteria: (NormalisedCriterion | Record<string, any>)[];
}): Record<string, unknown> {
  const weightByStage = new Map(input.weights.map((w) => [w.stage, w]));
  return {
    stages: SCORECARD_STAGES.map((stage) => {
      const w = weightByStage.get(stage);
      const stageCriteria = input.criteria
        .filter((c) => c.stage === stage)
        .sort((a, b) => (Number(a.sort_order) || 0) - (Number(b.sort_order) || 0))
        .map((c) => {
          const out: Record<string, unknown> = {
            label: c.label,
            emphasis: c.emphasis,
            pass_fail: Boolean(c.pass_fail),
            critical: Boolean(c.critical),
          };
          for (const k of ["description", "scoring_guidance", "good_example", "weak_example", "coaching_prompt"]) {
            if ((c as any)[k]) out[k] = (c as any)[k];
          }
          return out;
        });
      const out: Record<string, unknown> = { stage, weight: Number(w?.weight) || 0, criteria: stageCriteria };
      if (w?.guidance) out.guidance = w.guidance;
      return out;
    }),
  };
}
