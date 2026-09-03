// src/lib/scoringV2.ts — Day 267
//
// Criteria-level Scoring v2 runtime (Scoring Output Contract v2, Day 264).
//
// This module is the ADDITIVE v2 runtime. The production v1 path
// (`scoreWithLLM` in src/lib/scoring.ts) is UNTOUCHED and remains the default;
// nothing here changes production behaviour, the provider default, the DB
// schema or the v1 cache namespace. v2 exists so a future day can wire it in,
// and so the Day-266 deterministic harness can prove the full pipeline
//
//     mocked provider JSON -> parse -> ScoreV2 -> deterministic v1 projection
//                          -> Day-266 harness gates
//
// with ZERO cost. Every function here is PURE + deterministic: no DB, no
// network, no provider SDK call, no env mutation. It takes data in and returns
// data out. `AI_MODEL` is imported only as a version string (the OpenAI client
// is lazy in ./openai and is never constructed here).
//
// UK spelling throughout.

import { AI_MODEL } from "./openai";
import type { VoiceAnalysis, CallMoment } from "./types/call-analysis";
import type { ResolvedScorecard, ResolvedContext } from "./intelligenceRuntime";

// ── Version markers (v2 lane only — v1 markers in scoring.ts are untouched) ───
// These are NEW constants, not a reassignment of the v1 exports. The v1
// production path keeps RUBRIC_VERSION="v1"/SCORING_PROMPT_VERSION="v1"; the v2
// path uses the markers below so its prompt/rubric/cache land in a fresh
// namespace that can never read a v1 cache entry (Day 264 §6).
export const SCORING_PROMPT_VERSION_V2 = "scoring-prompt-v2";
export const RUBRIC_VERSION_V2 = "v2";
export const CACHE_KEY_VERSION_V2 = "v2";
export const GRAVIX_DEFAULT_CRITERIA_VERSION = "gravix-default-criteria-v1";
export const STUB_SCORING_MODEL = "stub:v1";
export const HEURISTIC_SCORING_MODEL = "heuristic:v1";

export const CONTRACT_VERSION_V2 = "v2" as const;
export const STAGES_V2 = ["intro", "discovery", "objection", "close"] as const;
export const STATUSES_V2 = ["pass", "partial", "fail", "not_observed"] as const;
export const OBJECTION_HANDLED_VALUES = ["handled", "partially", "missed"] as const;

// ── Types (Scoring Output Contract v2 §3) ─────────────────────────────────────
export type ContractVersion = typeof CONTRACT_VERSION_V2;
export type StageV2 = (typeof STAGES_V2)[number];
export type CriterionStatus = (typeof STATUSES_V2)[number];
export type ObjectionHandled = (typeof OBJECTION_HANDLED_VALUES)[number];
export type ScoringProviderName = "openai" | "stub";
export type ConfidenceLevel = "low" | "medium" | "high";
export type CriterionEmphasis = "low" | "standard" | "high" | "critical";

export type Confidence = { level: ConfidenceLevel; value: number };

export interface EvidenceQuote {
  quote: string; // VERBATIM transcript text
  start_sec: number | null;
  end_sec: number | null;
  segment_index: number | null;
  speaker: string | null;
}

export interface SuggestedDrill {
  key: string | null;
  title: string | null;
}

export interface CriterionResult {
  criterion_id: string;
  label: string;
  stage: StageV2;
  score: number | null; // 0–100; null iff not_observed
  status: CriterionStatus;
  weight: number; // criterion weight WITHIN its stage (sums to 100 per stage)
  emphasis: CriterionEmphasis;
  pass_fail: boolean;
  critical: boolean;
  evidence: EvidenceQuote[]; // >=1 unless not_observed
  why_points_lost: string | null; // required when partial|fail
  points_lost: number | null;
  coaching_action: string | null;
  suggested_drill: SuggestedDrill | null;
}

export interface ObjectionMatch {
  detected_text: string;
  objection_item_id: string | null;
  objection_item_key: string | null;
  objection_label: string | null;
  category: string | null;
  handled: ObjectionHandled | null;
  evidence: EvidenceQuote | null;
}

export interface StageResultV2 {
  stage: StageV2;
  score: number | null; // null iff every criterion not_observed
  weight: number; // stage weight (four stages sum to 100)
  status: CriterionStatus; // stage-level roll-up verdict
  notes: string;
  criteria: CriterionResult[];
}

export interface ScoreV2Provenance {
  scoring_provider: ScoringProviderName;
  scoring_model: string;
  scorecard_source: "custom" | "company_default" | "gravix_default";
  scorecard_id: string | null;
  scorecard_version_id: string | null;
  scorecard_version: number | null;
  scorecard_name: string | null;
  context_version: number | null;
  prompt_version: string;
  rubric_version: string;
  cache_key_version: string;
  criteria_version: string;
}

export interface ScoreV2 {
  contract_version: ContractVersion;
  overall_score: number;
  summary: string;
  stages: StageResultV2[]; // exactly the four fixed stages, in order
  objection_matches: ObjectionMatch[];
  confidence: Confidence;
  degraded_score: boolean;
  degraded_reason: string | null;
  voice: VoiceAnalysis;
  provenance: ScoreV2Provenance;
  trend_delta: number; // computed by runtime, never model-authored
}

export interface ScoreV1StageProjection {
  score: number;
  notes: string;
}

export interface ScoreV1Projection {
  overall: number;
  summary: string;
  stages: Record<StageV2, ScoreV1StageProjection>;
  moments: CallMoment[];
  suggestions: string[];
  voice: VoiceAnalysis;
  rubric: {
    intro: ScoreV1StageProjection;
    discovery: ScoreV1StageProjection;
    objection: ScoreV1StageProjection;
    close: ScoreV1StageProjection;
    _meta: Record<string, any>;
  };
}

// A ScoreV2 with its v1 projection attached — the on-disk / harness shape.
export interface ScoreV2WithProjection extends ScoreV2 {
  v1_projection: ScoreV1Projection;
}

// ── Transcript segments ───────────────────────────────────────────────────────
// Golden fixtures use `idx`; the runtime `buildSegments` uses `start_sec/end_sec`.
// We accept both and never mutate the caller's segments.
export interface TranscriptSegment {
  idx?: number;
  index?: number;
  segment_index?: number;
  speaker?: string | null;
  start_sec?: number | null;
  end_sec?: number | null;
  text?: string;
}

// ── Criteria spec (resolved authored / default rubric) ────────────────────────
export interface CriterionSpec {
  criterion_id: string;
  label: string;
  stage: StageV2;
  description: string | null;
  emphasis: CriterionEmphasis;
  pass_fail: boolean;
  critical: boolean;
  weight: number; // within-stage weight (sums to 100 per stage)
}

export interface StageSpec {
  stage: StageV2;
  weight: number; // stage weight (four sum to 100)
  criteria: CriterionSpec[];
}

export interface ResolvedCriteriaSpec {
  source: "custom" | "company_default" | "gravix_default";
  criteria_version: string;
  stages: StageSpec[];
}

// ── slug / helpers ────────────────────────────────────────────────────────────
export function slug(label: string): string {
  return String(label || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function clampScore(n: unknown): number {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(100, Math.round(v)));
}

function clampUnit(n: unknown): number {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, v));
}

function clampText(input: unknown, max: number): string {
  return String(input ?? "").trim().slice(0, max);
}

/**
 * Deterministic criterion_id (Day 264 §4.1):
 *   - prefer an authored persistent id if one is ever provided;
 *   - custom / company_default: "<scorecard_version_id>:<stage>:<slug(label)>";
 *   - built-in rubric:          "gravix_default:<stage>:<slug(label)>".
 * No random UUIDs; stable across re-scores of the same scorecard version.
 */
export function resolveCriterionId(args: {
  authoredId?: string | null;
  scorecardVersionId: string | null;
  source: ResolvedCriteriaSpec["source"];
  stage: StageV2;
  label: string;
}): string {
  const authored = String(args.authoredId ?? "").trim();
  if (authored) return authored;
  const prefix = args.source === "gravix_default" ? "gravix_default" : String(args.scorecardVersionId ?? "gravix_default");
  return `${prefix}:${args.stage}:${slug(args.label)}`;
}

/**
 * Deterministic even weight distribution that sums to EXACTLY 100.
 * base = floor(100/n); the first (100 - base*n) criteria get +1. n=1 → [100].
 */
export function distributeWeights(n: number): number[] {
  if (n <= 0) return [];
  const base = Math.floor(100 / n);
  const remainder = 100 - base * n;
  return Array.from({ length: n }, (_, i) => base + (i < remainder ? 1 : 0));
}

// ── Gravix built-in default criteria set (Day 264 §4.3, Day 267 §5) ───────────
// MVP: ONE criterion per stage, using the established product terminology from
// the UFC scorecard + Day-265 golden dataset (no invented taxonomy). The
// built-in rubric keeps the existing GRAVIX_DEFAULT_RUBRIC 25/25/25/25 stage
// weights (scorecardStudio.GRAVIX_DEFAULT_RUBRIC). Versioned by
// GRAVIX_DEFAULT_CRITERIA_VERSION.
interface DefaultCriterionSeed {
  stage: StageV2;
  label: string;
  description: string;
  emphasis: CriterionEmphasis;
  pass_fail: boolean;
  critical: boolean;
}

export const GRAVIX_DEFAULT_STAGE_WEIGHTS: Record<StageV2, number> = {
  intro: 25,
  discovery: 25,
  objection: 25,
  close: 25,
};

export const GRAVIX_DEFAULT_CRITERIA: DefaultCriterionSeed[] = [
  {
    stage: "intro",
    label: "Set agenda and establish credibility",
    description: "Time-box the call, state a clear reason for the conversation and anchor credibility before discovery.",
    emphasis: "standard",
    pass_fail: false,
    critical: false,
  },
  {
    stage: "discovery",
    label: "Uncover pain, current process and decision route",
    description: "Ask layered questions that surface pain and impact, the current process, and the buyer's budget/timeline/authority.",
    emphasis: "high",
    pass_fail: false,
    critical: false,
  },
  {
    stage: "objection",
    label: "Isolate the objection and reframe value",
    description: "Acknowledge the true objection, isolate it, reframe against value and test commitment.",
    emphasis: "high",
    pass_fail: false,
    critical: false,
  },
  {
    stage: "close",
    label: "Secure clear next step and commitment",
    description: "Make a clear, specific ask and lock a dated next step with mutual commitment.",
    emphasis: "high",
    pass_fail: false,
    critical: false,
  },
];

/**
 * Resolve the criteria spec for a scoring run. For the built-in rubric this is
 * the fixed Gravix default set; for a custom / company_default scorecard it is
 * read from the immutable activation snapshot (`snapshot.stages[]`, the same
 * shape intelligenceRuntime.buildScorecardPromptBlock reads). Criterion weights
 * are distributed evenly within a stage to sum to 100 (Day 264 §4.2); authored
 * per-criterion weights are used when present. Stage weights come from the
 * snapshot (custom) or the built-in map. Deterministic; no DB, no throw.
 */
export function resolveCriteriaSpec(resolved: ResolvedScorecard | null): ResolvedCriteriaSpec {
  const source = resolved?.source ?? "gravix_default";

  if (source === "gravix_default" || !resolved?.snapshot) {
    const stages: StageSpec[] = STAGES_V2.map((stage) => {
      const seeds = GRAVIX_DEFAULT_CRITERIA.filter((c) => c.stage === stage);
      const weights = distributeWeights(seeds.length);
      const criteria: CriterionSpec[] = seeds.map((seed, i) => ({
        criterion_id: resolveCriterionId({
          scorecardVersionId: null,
          source: "gravix_default",
          stage,
          label: seed.label,
        }),
        label: seed.label,
        stage,
        description: seed.description,
        emphasis: seed.emphasis,
        pass_fail: seed.pass_fail,
        critical: seed.critical,
        weight: weights[i],
      }));
      return { stage, weight: GRAVIX_DEFAULT_STAGE_WEIGHTS[stage], criteria };
    });
    return { source: "gravix_default", criteria_version: GRAVIX_DEFAULT_CRITERIA_VERSION, stages };
  }

  // Custom / company_default: read the activation snapshot.
  const snapStages: any[] = Array.isArray((resolved.snapshot as any).stages)
    ? ((resolved.snapshot as any).stages as any[])
    : [];
  const byStage = new Map(snapStages.map((s) => [String(s?.stage), s]));
  const versionId = resolved.scorecard_version_id ?? null;

  const stages: StageSpec[] = STAGES_V2.map((stage) => {
    const s = byStage.get(stage);
    const stageWeight = Number(s?.weight);
    const rawCriteria: any[] = Array.isArray(s?.criteria) ? s.criteria : [];
    // Authored per-criterion weights if every criterion carries one; else even.
    const hasAuthoredWeights = rawCriteria.length > 0 && rawCriteria.every((c) => Number.isFinite(Number(c?.weight)));
    const evenWeights = distributeWeights(rawCriteria.length);
    const criteria: CriterionSpec[] = rawCriteria.map((c, i) => ({
      criterion_id: resolveCriterionId({
        authoredId: c?.id ?? c?.criterion_id ?? null,
        scorecardVersionId: versionId,
        source: source as ResolvedCriteriaSpec["source"],
        stage,
        label: String(c?.label ?? ""),
      }),
      label: String(c?.label ?? ""),
      stage,
      description: c?.description != null ? String(c.description) : null,
      emphasis: mapEmphasis(c?.emphasis, Boolean(c?.critical)),
      pass_fail: Boolean(c?.pass_fail),
      critical: Boolean(c?.critical),
      weight: hasAuthoredWeights ? Number(c.weight) : evenWeights[i],
    }));
    return { stage, weight: Number.isFinite(stageWeight) ? stageWeight : 0, criteria };
  });

  return {
    source: source as ResolvedCriteriaSpec["source"],
    criteria_version: `scorecard:${versionId ?? "unknown"}`,
    stages,
  };
}

// Authoring emphasis (minor|standard|major + critical flag) → contract enum.
function mapEmphasis(raw: unknown, critical: boolean): CriterionEmphasis {
  if (critical) return "critical";
  const v = String(raw ?? "standard").toLowerCase();
  if (v === "minor" || v === "low") return "low";
  if (v === "major" || v === "high") return "high";
  if (v === "critical") return "critical";
  return "standard";
}

// ── Transcript normalisation + evidence resolution (Day 267 §11) ──────────────
// Normalisation used ONLY for the verbatim substring test, never to rewrite the
// stored quote (the stored quote stays byte-verbatim so downstream readers and
// the harness see the exact transcript text). We collapse runs of whitespace and
// CRLF so a quote that differs from the transcript only by whitespace still
// grounds — but the quote text itself is preserved as supplied.
export function normaliseForMatch(s: string): string {
  return String(s || "").replace(/\r\n/g, "\n").replace(/\s+/g, " ").trim();
}

export function buildFullTranscript(segments: TranscriptSegment[]): string {
  return (segments || []).map((s) => String(s?.text || "")).join("\n");
}

function segmentIndexOf(seg: TranscriptSegment, fallback: number): number {
  const cands = [seg?.idx, seg?.index, seg?.segment_index];
  for (const c of cands) if (Number.isFinite(Number(c))) return Number(c);
  return fallback;
}

function findSegment(segments: TranscriptSegment[], segIndex: number | null | undefined): { seg: TranscriptSegment; idx: number } | null {
  if (segIndex == null) return null;
  for (let i = 0; i < (segments || []).length; i += 1) {
    if (segmentIndexOf(segments[i], i) === Number(segIndex)) return { seg: segments[i], idx: Number(segIndex) };
  }
  return null;
}

/** A quote is grounded iff it is a (whitespace-normalised) substring of the full
 * transcript AND, when a segment is cited, of that segment's text. */
export function quoteIsGrounded(quote: string, segIndex: number | null | undefined, fullTranscript: string, segments: TranscriptSegment[]): boolean {
  const q = normaliseForMatch(quote);
  if (!q) return false;
  if (!normaliseForMatch(fullTranscript).includes(q)) return false;
  if (segIndex != null) {
    const hit = findSegment(segments, segIndex);
    if (!hit) return false;
    if (!normaliseForMatch(String(hit.seg.text || "")).includes(q)) return false;
  }
  return true;
}

/**
 * Resolve + verify one evidence item. Keeps the quote byte-verbatim; fills
 * span/speaker from the cited segment where the model omitted them; drops the
 * item entirely if it cannot be grounded (invented evidence is never presented
 * as fact — Day 267 §10). Returns null for a dropped item.
 */
export function resolveEvidenceItem(raw: any, fullTranscript: string, segments: TranscriptSegment[]): EvidenceQuote | null {
  const quote = String(raw?.quote ?? "");
  const segIndex = raw?.segment_index != null ? Number(raw.segment_index) : null;
  if (!quoteIsGrounded(quote, segIndex, fullTranscript, segments)) return null;
  const hit = segIndex != null ? findSegment(segments, segIndex) : null;
  const seg = hit?.seg;
  return {
    quote, // byte-verbatim, unmodified
    segment_index: segIndex,
    start_sec: raw?.start_sec != null ? Number(raw.start_sec) : (seg?.start_sec != null ? Number(seg.start_sec) : null),
    end_sec: raw?.end_sec != null ? Number(raw.end_sec) : (seg?.end_sec != null ? Number(seg.end_sec) : null),
    speaker: raw?.speaker != null ? String(raw.speaker) : (seg?.speaker != null ? String(seg.speaker) : null),
  };
}

// ── Deterministic roll-ups (Day 267 §12) ──────────────────────────────────────
/** Stage score = weighted average of OBSERVED criteria (weights sum to 100).
 * All-not_observed → null. Rounded. */
export function rollUpStageScore(criteria: CriterionResult[]): number | null {
  let wsum = 0;
  let acc = 0;
  for (const c of criteria) {
    if (c.status === "not_observed" || c.score == null) continue;
    wsum += c.weight;
    acc += c.weight * c.score;
  }
  if (wsum <= 0) return null;
  return Math.round(acc / wsum);
}

/** Deterministic worst-wins stage status from its criteria. Critical fail
 * dominates. All-not_observed → not_observed. */
export function rollUpStageStatus(criteria: CriterionResult[]): CriterionStatus {
  if (criteria.length === 0) return "not_observed";
  const observed = criteria.filter((c) => c.status !== "not_observed");
  if (observed.length === 0) return "not_observed";
  if (observed.some((c) => c.status === "fail")) return "fail";
  if (observed.some((c) => c.status === "partial")) return "partial";
  return "pass";
}

/** Overall = weighted average over OBSERVED stages, re-normalised by their
 * stage-weight sum. No observed stages → 0. Rounded. */
export function rollUpOverall(stages: StageResultV2[]): number {
  let wsum = 0;
  let acc = 0;
  for (const s of stages) {
    if (s.status === "not_observed" || s.score == null) continue;
    wsum += s.weight;
    acc += s.weight * s.score;
  }
  if (wsum <= 0) return 0;
  return Math.round(acc / wsum);
}

/** points_lost attributable to a criterion, deterministic:
 *   stageWeight * (100 - score)/100 * (criterionWeight/100). */
export function computePointsLost(stageWeight: number, criterionWeight: number, score: number): number {
  return Math.round((stageWeight * (100 - score) * criterionWeight) / 10000);
}

// ── Confidence + trend ─────────────────────────────────────────────────────────
export function normaliseConfidence(raw: any): Confidence {
  const value = clampUnit(raw?.value);
  let level = String(raw?.level ?? "").toLowerCase() as ConfidenceLevel;
  if (level !== "low" && level !== "medium" && level !== "high") {
    level = value >= 0.75 ? "high" : value >= 0.5 ? "medium" : "low";
  }
  return { level, value };
}

/** trend_delta = overall − rep rolling average before this call. Computed by the
 * runtime (never model-authored, Day 264 §3.2). Unknown prior → 0. */
export function computeTrendDelta(overall: number, priorAverage: number | null | undefined): number {
  if (priorAverage == null || !Number.isFinite(Number(priorAverage))) return 0;
  return Math.round(overall - Number(priorAverage));
}

// ── Provenance ─────────────────────────────────────────────────────────────────
export interface ScoringV2Context {
  spec: ResolvedCriteriaSpec;
  resolvedScorecard: ResolvedScorecard;
  resolvedContext?: ResolvedContext | null;
  segments: TranscriptSegment[];
  fullTranscript: string;
  voice: VoiceAnalysis;
  scoringProvider: ScoringProviderName;
  scoringModel: string; // AI_MODEL | STUB_SCORING_MODEL | HEURISTIC_SCORING_MODEL
  priorAverage?: number | null;
}

export function buildProvenance(ctx: ScoringV2Context): ScoreV2Provenance {
  const sc = ctx.resolvedScorecard;
  return {
    scoring_provider: ctx.scoringProvider,
    scoring_model: ctx.scoringModel,
    scorecard_source: sc.source,
    scorecard_id: sc.scorecard_id,
    scorecard_version_id: sc.scorecard_version_id,
    scorecard_version: sc.scorecard_version,
    scorecard_name: sc.scorecard_name ?? null,
    context_version: ctx.resolvedContext?.context_version ?? null,
    prompt_version: SCORING_PROMPT_VERSION_V2,
    rubric_version: RUBRIC_VERSION_V2,
    cache_key_version: CACHE_KEY_VERSION_V2,
    criteria_version: ctx.spec.criteria_version,
  };
}

export function scoringModelVersionV2(scoringModel: string): string {
  return `${scoringModel}:${SCORING_PROMPT_VERSION_V2}:${RUBRIC_VERSION_V2}`;
}

// ── Cache key (Day 267 §9) ────────────────────────────────────────────────────
// A v2 key can NEVER collide with a v1 key (different cachever/rubric/prompt/
// model tokens) and stub-v2 can never collide with openai-v2 (the provider
// segment, Day 262). Mirrors scoring.buildDeterministicPromptKey's join style so
// the two are directly comparable.
function stableHash(input: string): string {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export function buildScoreCacheKeyV2(params: {
  callId: string;
  filename?: string | null;
  sha256?: string | null;
  transcript?: string | null;
  contextVersion?: number | null;
  scorecardCacheKey?: string | null;
  scoringProvider?: ScoringProviderName | null;
  scoringModel?: string | null;
}): { transcript: string; transcriptHash: string; key: string } {
  const transcript = normaliseForMatch(params.transcript || "");
  const transcriptHash = stableHash(transcript || params.sha256 || params.callId);
  const model = scoringModelVersionV2(params.scoringModel || AI_MODEL);
  const parts = [
    `cachever=${CACHE_KEY_VERSION_V2}`,
    `rubric=${RUBRIC_VERSION_V2}`,
    `prompt=${SCORING_PROMPT_VERSION_V2}`,
    `model=${model}`,
    `filename=${params.filename || params.callId}`,
    `sha256=${params.sha256 || "missing"}`,
    `transcriptHash=${transcriptHash}`,
  ];
  if (params.contextVersion != null) parts.push(`context=${params.contextVersion}`);
  if (params.scorecardCacheKey && params.scorecardCacheKey !== "gravix_default_v1") {
    parts.push(`scorecard=${params.scorecardCacheKey}`);
  }
  if (params.scoringProvider && params.scoringProvider !== "openai") {
    parts.push(`provider=${params.scoringProvider}`);
  }
  return { transcript, transcriptHash, key: parts.join("|") };
}

// ── v2 prompt contract (Day 267 §8) ───────────────────────────────────────────
// Versioned, criteria-level prompt. Enumerates EXACTLY the criteria the model may
// score (by stage + label + criterion_id) so it cannot invent criteria or
// stages, and demands verbatim evidence + a span. MVP-conscious (bounded).
export function buildScoringV2Prompt(ctx: {
  spec: ResolvedCriteriaSpec;
  segments: TranscriptSegment[];
  scorecardName: string | null;
  contextBlock?: string;
}): { system: string; user: string; promptVersion: string } {
  const criteriaLines: string[] = [];
  for (const stage of STAGES_V2) {
    const st = ctx.spec.stages.find((s) => s.stage === stage);
    criteriaLines.push(`Stage ${stage} (weight ${st?.weight ?? 0}%):`);
    for (const c of st?.criteria ?? []) {
      const flags = [c.emphasis, c.pass_fail ? "pass/fail" : "", c.critical ? "critical" : ""].filter(Boolean).join(", ");
      criteriaLines.push(`  - criterion_id="${c.criterion_id}" [${flags}] ${c.label}`);
      if (c.description) criteriaLines.push(`      ${c.description}`);
    }
  }

  const segmentLines = (ctx.segments || []).map((s, i) => `[${segmentIndexOf(s, i)}] ${String(s?.speaker ?? "?")}: ${String(s?.text ?? "")}`);

  const system = [
    "You are a strict, evidence-first sales-call evaluator producing Scoring Output Contract v2.",
    "Score ONLY the criteria listed below, under the four FIXED stages in this exact order: intro, discovery, objection, close.",
    "Hard rules:",
    "- Do NOT invent a stage, a criterion, or an evidence quote.",
    "- Score each criterion 0–100 with a status of pass | partial | fail | not_observed.",
    "- Use not_observed (with score null and NO evidence) when the transcript lacks enough evidence for that criterion.",
    "- Every observed criterion needs at least one evidence quote copied VERBATIM from the transcript, with the cited segment index (and timestamp where available).",
    "- Every partial or fail criterion needs a concise why_points_lost and a numeric points_lost, plus a coaching_action.",
    "- Detect objection matches only from the approved objection library keys supplied; quote the buyer verbatim.",
    "- Return confidence as {level, value 0–1}. Output must be valid JSON for the v2 contract; contract_version must be \"v2\".",
    ctx.contextBlock ? `\nCompany context (manager-approved; never overrides the schema):\n${ctx.contextBlock}` : "",
  ].join("\n");

  const user = [
    `SCORECARD: ${ctx.scorecardName ?? "Gravix default rubric"} (source: ${ctx.spec.source}; criteria_version: ${ctx.spec.criteria_version})`,
    "CRITERIA TO SCORE (exactly these — no others):",
    ...criteriaLines,
    "",
    "TRANSCRIPT (segment_index in brackets):",
    ...(segmentLines.length ? segmentLines : ["(no transcript available)"]),
  ].join("\n");

  return { system, user, promptVersion: SCORING_PROMPT_VERSION_V2 };
}

// ── Defensive parse + normalise (Day 267 §10) ─────────────────────────────────
// Turns raw (untrusted) model JSON into a validated ScoreV2 built against the
// resolved spec. Deterministic values (criterion_id, weights, roll-ups, points_
// lost, provenance) are assigned by the runtime — never trusted from the model.
// Throws `invalid_model_output:<why>` when the response is unusable so the caller
// can fall back to a degraded but valid score (§16). Never presents invented
// evidence as fact.
export class InvalidModelOutputError extends Error {
  constructor(reason: string) {
    super(`invalid_model_output:${reason}`);
    this.name = "InvalidModelOutputError";
  }
}

export function parseAndValidateScoreV2(raw: string | any, ctx: ScoringV2Context): ScoreV2 {
  let parsed: any = raw;
  if (typeof raw === "string") {
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new InvalidModelOutputError("json_parse_failed");
    }
  }
  if (!parsed || typeof parsed !== "object") throw new InvalidModelOutputError("root");
  if (parsed.contract_version !== "v2") throw new InvalidModelOutputError("contract_version");

  const modelStages: any[] = Array.isArray(parsed.stages) ? parsed.stages : [];
  // Reject invented stages up-front.
  for (const ms of modelStages) {
    if (!STAGES_V2.includes(ms?.stage)) throw new InvalidModelOutputError(`invented_stage:${ms?.stage}`);
  }
  if (new Set(modelStages.map((s) => s?.stage)).size !== modelStages.length) {
    throw new InvalidModelOutputError("duplicate_stage");
  }

  const stages: StageResultV2[] = STAGES_V2.map((stage) => {
    const stageSpec = ctx.spec.stages.find((s) => s.stage === stage);
    if (!stageSpec) throw new InvalidModelOutputError(`spec_missing_stage:${stage}`);
    const modelStage = modelStages.find((s) => s?.stage === stage);
    if (!modelStage) throw new InvalidModelOutputError(`missing_stage:${stage}`);

    const modelCriteria: any[] = Array.isArray(modelStage.criteria) ? modelStage.criteria : [];
    const specLabels = new Set(stageSpec.criteria.map((c) => slug(c.label)));
    // Reject invented criteria the spec does not contain.
    for (const mc of modelCriteria) {
      if (!specLabels.has(slug(String(mc?.label ?? "")))) {
        throw new InvalidModelOutputError(`invented_criterion:${stage}/${mc?.label}`);
      }
    }

    const criteria: CriterionResult[] = stageSpec.criteria.map((cs) => {
      const mc = modelCriteria.find((m) => slug(String(m?.label ?? "")) === slug(cs.label));
      if (!mc) throw new InvalidModelOutputError(`missing_criterion:${stage}/${cs.label}`);

      const status = String(mc?.status ?? "");
      if (!STATUSES_V2.includes(status as CriterionStatus)) {
        throw new InvalidModelOutputError(`invalid_status:${stage}/${cs.label}=${status}`);
      }

      if (status === "not_observed") {
        if (mc.score != null) throw new InvalidModelOutputError(`not_observed_with_score:${stage}/${cs.label}`);
        return {
          criterion_id: cs.criterion_id,
          label: cs.label,
          stage,
          score: null,
          status: "not_observed",
          weight: cs.weight,
          emphasis: cs.emphasis,
          pass_fail: cs.pass_fail,
          critical: cs.critical,
          evidence: [],
          why_points_lost: null,
          points_lost: null,
          coaching_action: mc.coaching_action != null ? clampText(mc.coaching_action, 220) : null,
          suggested_drill: normaliseDrill(mc.suggested_drill),
        };
      }

      // Observed criterion.
      if (mc.score == null || !Number.isFinite(Number(mc.score))) {
        throw new InvalidModelOutputError(`observed_missing_score:${stage}/${cs.label}`);
      }
      const score = clampScore(mc.score);

      // Ground evidence — drop invented quotes; an observed criterion with no
      // grounded evidence makes the whole response unusable (documented §10).
      const rawEvidence: any[] = Array.isArray(mc.evidence) ? mc.evidence : [];
      const evidence = rawEvidence
        .map((e) => resolveEvidenceItem(e, ctx.fullTranscript, ctx.segments))
        .filter((e): e is EvidenceQuote => e != null);
      if (evidence.length === 0) throw new InvalidModelOutputError(`ungrounded_evidence:${stage}/${cs.label}`);

      const isPartialFail = status === "partial" || status === "fail";
      const why = isPartialFail ? clampText(mc.why_points_lost, 300) : null;
      if (isPartialFail && !why) throw new InvalidModelOutputError(`missing_why_points_lost:${stage}/${cs.label}`);
      const pointsLost = isPartialFail ? computePointsLost(stageSpec.weight, cs.weight, score) : null;

      return {
        criterion_id: cs.criterion_id,
        label: cs.label,
        stage,
        score,
        status: status as CriterionStatus,
        weight: cs.weight,
        emphasis: cs.emphasis,
        pass_fail: cs.pass_fail,
        critical: cs.critical,
        evidence,
        why_points_lost: why,
        points_lost: pointsLost,
        coaching_action: mc.coaching_action != null ? clampText(mc.coaching_action, 220) : null,
        suggested_drill: normaliseDrill(mc.suggested_drill),
      };
    });

    const stageScore = rollUpStageScore(criteria);
    const stageStatus = rollUpStageStatus(criteria);
    const notes = buildStageNotes(criteria);
    return { stage, score: stageScore, weight: stageSpec.weight, status: stageStatus, notes, criteria };
  });

  // Stage-weight invariant (defensive — spec should already sum to 100).
  const stageWeightSum = stages.reduce((s, st) => s + st.weight, 0);
  if (stageWeightSum !== 100) throw new InvalidModelOutputError(`stage_weights_sum:${stageWeightSum}`);

  const overall = rollUpOverall(stages);
  const objection_matches = normaliseObjectionMatches(parsed.objection_matches, ctx);
  const confidence = normaliseConfidence(parsed.confidence);
  const summary = clampText(parsed.summary, 220);
  if (!summary) throw new InvalidModelOutputError("summary");

  const degraded_score = Boolean(parsed.degraded_score);
  const degraded_reason = degraded_score ? clampText(parsed.degraded_reason, 120) || "unspecified" : null;

  return {
    contract_version: "v2",
    overall_score: overall,
    summary,
    stages,
    objection_matches,
    confidence,
    degraded_score,
    degraded_reason,
    voice: ctx.voice,
    provenance: buildProvenance(ctx),
    trend_delta: computeTrendDelta(overall, ctx.priorAverage),
  };
}

function normaliseDrill(raw: any): SuggestedDrill | null {
  if (!raw || typeof raw !== "object") return null;
  const key = raw.key != null ? String(raw.key) : null;
  const title = raw.title != null ? String(raw.title) : null;
  if (!key && !title) return null;
  return { key, title };
}

function buildStageNotes(criteria: CriterionResult[]): string {
  const lost = criteria.filter((c) => (c.status === "partial" || c.status === "fail") && c.why_points_lost);
  if (lost.length > 0) return clampText(lost.map((c) => c.why_points_lost).join(" "), 300);
  const coaching = criteria.map((c) => c.coaching_action).filter(Boolean);
  if (coaching.length > 0) return clampText(coaching.join(" "), 300);
  return "";
}

function normaliseObjectionMatches(raw: any, ctx: ScoringV2Context): ObjectionMatch[] {
  if (!Array.isArray(raw)) return [];
  const out: ObjectionMatch[] = [];
  for (const o of raw) {
    const detected = String(o?.detected_text ?? "");
    if (!detected) continue;
    const handled = OBJECTION_HANDLED_VALUES.includes(o?.handled) ? (o.handled as ObjectionHandled) : null;
    let evidence: EvidenceQuote | null = null;
    if (o?.evidence) {
      // detected_text must itself be grounded; evidence (if present) too.
      evidence = resolveEvidenceItem(o.evidence, ctx.fullTranscript, ctx.segments);
    }
    if (!quoteIsGrounded(detected, o?.evidence?.segment_index ?? null, ctx.fullTranscript, ctx.segments)) {
      // invented objection text is dropped, never presented as fact.
      continue;
    }
    out.push({
      detected_text: detected,
      objection_item_id: o?.objection_item_id != null ? String(o.objection_item_id) : null,
      objection_item_key: o?.objection_item_key != null ? String(o.objection_item_key) : null,
      objection_label: o?.objection_label != null ? String(o.objection_label) : null,
      category: o?.category != null ? String(o.category) : null,
      handled,
      evidence,
    });
  }
  return out;
}

// ── v2 → v1 projection (Day 267 §13, §14) ─────────────────────────────────────
export type StageProjectionPolicy = "custom_criteria_authoritative" | "default_v1_parity";

/** Hybrid MVP policy (Day 267 §13): custom scorecards → criteria-weighted v2
 * stage score is authoritative; the built-in Gravix default → keep the v1 stage
 * score authoritative (criteria are descriptive). */
export function projectionPolicyFor(source: ResolvedCriteriaSpec["source"]): StageProjectionPolicy {
  return source === "gravix_default" ? "default_v1_parity" : "custom_criteria_authoritative";
}

function momentTypeFor(stage: StageV2, status: CriterionStatus): CallMoment["type"] {
  if (stage === "close") return "closing_attempt";
  if (status === "fail" || status === "partial") return "mistake";
  return "highlight";
}
function severityFor(status: CriterionStatus, emphasis: CriterionEmphasis): CallMoment["severity"] {
  if (status === "fail" || emphasis === "critical") return "high";
  if (status === "partial") return "medium";
  return "low";
}

/**
 * Pure v2 → v1 projection. Produces every current v1 field (no key removed or
 * retyped). `existingV1StageScores` supplies authoritative v1 stage scores for
 * the default_v1_parity policy (the built-in default path); it is ignored for
 * custom_criteria_authoritative.
 */
export function projectScoreV2ToV1(
  v2: ScoreV2,
  opts?: {
    policy?: StageProjectionPolicy;
    existingV1StageScores?: Partial<Record<StageV2, number>>;
    extraMeta?: Record<string, any>;
  }
): ScoreV1Projection {
  const policy = opts?.policy ?? projectionPolicyFor(v2.provenance.scorecard_source);
  const overall = v2.overall_score;

  const stageObj = {} as Record<StageV2, ScoreV1StageProjection>;
  for (const st of v2.stages) {
    let score: number;
    if (st.status === "not_observed" || st.score == null) {
      // Not assessed on this call — carry the overall so v1 readers see a number.
      score = overall;
    } else if (policy === "default_v1_parity" && opts?.existingV1StageScores?.[st.stage] != null) {
      score = clampScore(opts.existingV1StageScores[st.stage]);
    } else {
      // custom_criteria_authoritative (and default fallback when no v1 score):
      // the criteria-weighted v2 stage score is authoritative.
      score = st.score;
    }
    const notes = st.status === "not_observed" ? "Not assessed on this call." : clampText(st.notes || "", 300);
    stageObj[st.stage] = { score, notes };
  }

  // moments: grounded criterion evidence + objection matches → v1 moments.
  const moments: CallMoment[] = [];
  for (const st of v2.stages) {
    if (st.status === "not_observed") continue;
    for (const c of st.criteria) {
      for (const ev of c.evidence) {
        moments.push({
          timestamp: ev.start_sec != null ? ev.start_sec : undefined,
          type: momentTypeFor(st.stage, c.status),
          text: clampText(ev.quote, 280),
          severity: severityFor(c.status, c.emphasis),
        });
      }
    }
  }
  for (const o of v2.objection_matches) {
    moments.push({
      timestamp: o.evidence?.start_sec != null ? o.evidence.start_sec : undefined,
      type: "objection",
      text: clampText(o.detected_text, 280),
      severity: o.handled === "missed" ? "high" : "medium",
    });
  }

  // suggestions: de-duplicated coaching actions (≤6, ≤220 each).
  const suggestions = Array.from(
    new Set(
      v2.stages
        .flatMap((st) => st.criteria.map((c) => c.coaching_action))
        .filter((x): x is string => Boolean(x))
        .map((t) => clampText(t, 220))
    )
  ).slice(0, 6);

  const meta: Record<string, any> = {
    // Existing v1 _meta keys (identical meaning) — v1 readers keep working.
    rubric_version: v2.provenance.rubric_version,
    prompt_version: v2.provenance.prompt_version,
    model_version: v2.provenance.scoring_model,
    scoring_model_version: scoringModelVersionV2(v2.provenance.scoring_model),
    scorecard_name: v2.provenance.scorecard_name,
    scorecard_source: v2.provenance.scorecard_source,
    scoring_provider: v2.provenance.scoring_provider,
    transcript_present: v2.stages.some((s) => s.status !== "not_observed"),
    voice: v2.voice,
    // Additive v2 keys (WEB getScoringProvenance only reads keys it knows).
    contract_version: v2.contract_version,
    confidence: v2.confidence,
    degraded_score: v2.degraded_score,
    degraded_reason: v2.degraded_reason,
    criteria_count: v2.stages.reduce((n, s) => n + s.criteria.length, 0),
    cache_key_version: v2.provenance.cache_key_version,
    criteria_version: v2.provenance.criteria_version,
    stage_score_projection: policy,
    ...(opts?.extraMeta ?? {}),
  };
  if (v2.provenance.context_version != null) meta.context_version = v2.provenance.context_version;
  if (v2.provenance.scorecard_source !== "gravix_default") {
    meta.scorecard_id = v2.provenance.scorecard_id;
    meta.scorecard_version_id = v2.provenance.scorecard_version_id;
    meta.scorecard_version = v2.provenance.scorecard_version;
  }

  return {
    overall,
    summary: v2.summary,
    stages: stageObj,
    moments,
    suggestions,
    voice: v2.voice,
    rubric: {
      intro: stageObj.intro,
      discovery: stageObj.discovery,
      objection: stageObj.objection,
      close: stageObj.close,
      _meta: meta,
    },
  };
}

/** Attach the v1 projection to a ScoreV2 to produce the on-disk / harness shape. */
export function withV1Projection(v2: ScoreV2, opts?: Parameters<typeof projectScoreV2ToV1>[1]): ScoreV2WithProjection {
  return { ...v2, v1_projection: projectScoreV2ToV1(v2, opts) };
}

// ── Honest degraded scores: stub + heuristic/fallback (Day 267 §15, §16) ──────
function degradedStages(spec: ResolvedCriteriaSpec, coaching: string): StageResultV2[] {
  return STAGES_V2.map((stage) => {
    const stageSpec = spec.stages.find((s) => s.stage === stage);
    const criteria: CriterionResult[] = (stageSpec?.criteria ?? []).map((cs) => ({
      criterion_id: cs.criterion_id,
      label: cs.label,
      stage,
      score: null,
      status: "not_observed",
      weight: cs.weight,
      emphasis: cs.emphasis,
      pass_fail: cs.pass_fail,
      critical: cs.critical,
      evidence: [],
      why_points_lost: null,
      points_lost: null,
      coaching_action: coaching,
      suggested_drill: null,
    }));
    return { stage, score: null, weight: stageSpec?.weight ?? 0, status: "not_observed", notes: coaching, criteria };
  });
}

const ZERO_VOICE: VoiceAnalysis = { clarity: 0, confidence: 0, filler_density: 0, pace: 0, overall: 0 };

/** Deterministic no-cost v2 stub (SCORING_PROVIDER=stub). Structurally valid,
 * honestly degraded, one not_observed criterion per stage, NO invented evidence,
 * no network. Do not make it look semantically intelligent. */
export function buildStubScoreV2(ctx: Pick<ScoringV2Context, "spec" | "resolvedScorecard" | "resolvedContext" | "voice">): ScoreV2 {
  const stages = degradedStages(ctx.spec, "Run with a real scorer for criterion-level feedback.");
  return {
    contract_version: "v2",
    overall_score: 0,
    summary: "Deterministic stub score (no model call).",
    stages,
    objection_matches: [],
    confidence: { level: "low", value: 0 },
    degraded_score: true,
    degraded_reason: "stub_provider",
    voice: ctx.voice ?? ZERO_VOICE,
    provenance: buildProvenance({
      ...(ctx as any),
      scoringProvider: "stub",
      scoringModel: STUB_SCORING_MODEL,
    }),
    trend_delta: 0,
  };
}

export type DegradedReason = "heuristic_fallback" | "no_transcript" | "invalid_model_output" | "insufficient_evidence";

/** Honest heuristic / no-transcript fallback (§16). Structurally valid, degraded,
 * no invented evidence, never masquerades as a live model result. */
export function buildFallbackScoreV2(
  ctx: Pick<ScoringV2Context, "spec" | "resolvedScorecard" | "resolvedContext" | "voice">,
  reason: DegradedReason
): ScoreV2 {
  const stages = degradedStages(ctx.spec, "A fuller transcript and a live model are needed for reliable coaching.");
  return {
    contract_version: "v2",
    overall_score: 0,
    summary: "Provisional score — full transcript / live model needed.",
    stages,
    objection_matches: [],
    confidence: { level: "low", value: 0 },
    degraded_score: true,
    degraded_reason: reason,
    voice: ctx.voice ?? ZERO_VOICE,
    provenance: buildProvenance({
      ...(ctx as any),
      scoringProvider: "openai",
      scoringModel: HEURISTIC_SCORING_MODEL,
    }),
    trend_delta: 0,
  };
}
