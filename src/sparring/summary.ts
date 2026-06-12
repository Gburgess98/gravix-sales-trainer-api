// src/sparring/summary.ts
// TIER 2A — Day 104: deterministic sparring session summary.
//
// Aggregates the Day 103 structured turn scores (meta.turn_scores) into a
// rep/manager-friendly coaching summary. No LLM calls — the batched LLM
// polish slots in later behind the same shape.

import type { StructuredTurnScore, TurnFlag } from "./scoring";

export type SummaryDimension =
  | "clarity"
  | "confidence"
  | "objectionHandling"
  | "progression";

export type StoredTurnScore = {
  turnId?: string | null;
  repMessage?: string;
  score: StructuredTurnScore;
  createdAt?: string;
};

export type WeakMomentEntry = {
  turnId: string | null;
  message: string;
  weakMoment: string;
  overall: number;
  recommendedNextMove: string;
};

export type SparringDrillType =
  | "discovery"
  | "objection_handling"
  | "closing"
  | "confidence"
  | "clarity"
  | "general";

export type SparringSessionSummary = {
  overall: number;
  dimensionAverages: Record<SummaryDimension, number>;
  turnCount: number;
  strongestDimension: SummaryDimension;
  weakestDimension: SummaryDimension;
  topFlags: Array<{ flag: TurnFlag; count: number }>;
  weakMoments: WeakMomentEntry[];
  recommendedDrill: {
    type: SparringDrillType;
    title: string;
    reason: string;
  };
  summaryText: string;
  nextBestAction: string;
};

const DIMENSIONS: SummaryDimension[] = [
  "clarity",
  "confidence",
  "objectionHandling",
  "progression",
];

const DIMENSION_LABELS: Record<SummaryDimension, string> = {
  clarity: "clarity",
  confidence: "confidence",
  objectionHandling: "objection handling",
  progression: "progression",
};

const clamp = (n: number) =>
  Math.max(0, Math.min(100, Math.round(Number.isFinite(n) ? n : 0)));

// ── Aggregation ──────────────────────────────────────────────────────────────

export function aggregateTurnScores(turnScores: StoredTurnScore[]): {
  overall: number;
  dimensionAverages: Record<SummaryDimension, number>;
  turnCount: number;
  flagCounts: Map<TurnFlag, number>;
} {
  const valid = (turnScores || []).filter(
    (t) => t && t.score && typeof t.score.overall === "number"
  );
  const turnCount = valid.length;

  const sums: Record<SummaryDimension, number> = {
    clarity: 0,
    confidence: 0,
    objectionHandling: 0,
    progression: 0,
  };
  let overallSum = 0;
  const flagCounts = new Map<TurnFlag, number>();

  for (const t of valid) {
    overallSum += t.score.overall;
    for (const d of DIMENSIONS) sums[d] += Number(t.score[d]) || 0;
    for (const flag of t.score.flags || []) {
      flagCounts.set(flag, (flagCounts.get(flag) || 0) + 1);
    }
  }

  const dimensionAverages = {
    clarity: turnCount ? clamp(sums.clarity / turnCount) : 0,
    confidence: turnCount ? clamp(sums.confidence / turnCount) : 0,
    objectionHandling: turnCount ? clamp(sums.objectionHandling / turnCount) : 0,
    progression: turnCount ? clamp(sums.progression / turnCount) : 0,
  };

  return {
    overall: turnCount ? clamp(overallSum / turnCount) : 0,
    dimensionAverages,
    turnCount,
    flagCounts,
  };
}

// ── Weak moments ─────────────────────────────────────────────────────────────

export function selectWeakMoments(
  turnScores: StoredTurnScore[],
  failedMoments?: any[] | null
): WeakMomentEntry[] {
  const withWeakMoment = (turnScores || [])
    .filter((t) => t?.score?.weakMoment)
    .sort((a, b) => (a.score.overall ?? 101) - (b.score.overall ?? 101))
    .slice(0, 2)
    .map((t) => ({
      turnId: t.turnId ?? null,
      message: String(t.repMessage || "").slice(0, 200),
      weakMoment: String(t.score.weakMoment),
      overall: clamp(t.score.overall),
      recommendedNextMove: String(t.score.recommendedNextMove || ""),
    }));

  if (withWeakMoment.length > 0) return withWeakMoment;

  // Fallback: legacy failed_moments entries (Day 66 replay engine)
  return (failedMoments || [])
    .slice(0, 2)
    .map((m: any) => ({
      turnId: null,
      message: String(m?.rep_text || m?.repText || "").slice(0, 200),
      weakMoment: String(m?.reason || "Weak moment flagged"),
      overall: clamp(Number(m?.score ?? 40)),
      recommendedNextMove: "Retry this moment in replay mode.",
    }))
    .filter((m) => m.message);
}

// ── Drill recommendation ─────────────────────────────────────────────────────

const DRILL_BY_DIMENSION: Record<
  SummaryDimension,
  { type: SparringDrillType; title: string }
> = {
  objectionHandling: { type: "objection_handling", title: "Objection Handling Drill" },
  progression: { type: "closing", title: "Closing / Next Step Drill" },
  clarity: { type: "clarity", title: "Clarity Drill" },
  confidence: { type: "confidence", title: "Confidence Drill" },
};

// "No clear weakness" = weakest dimension is decent and close to the strongest.
const NO_CLEAR_WEAKNESS_FLOOR = 65;
const NO_CLEAR_WEAKNESS_SPREAD = 10;

export function recommendSparringDrill(agg: {
  dimensionAverages: Record<SummaryDimension, number>;
  weakestDimension: SummaryDimension;
  strongestDimension: SummaryDimension;
  turnCount: number;
}): SparringSessionSummary["recommendedDrill"] {
  const weakScore = agg.dimensionAverages[agg.weakestDimension];
  const strongScore = agg.dimensionAverages[agg.strongestDimension];

  if (
    agg.turnCount === 0 ||
    (weakScore >= NO_CLEAR_WEAKNESS_FLOOR &&
      strongScore - weakScore <= NO_CLEAR_WEAKNESS_SPREAD)
  ) {
    return {
      type: "general",
      title: "General Call Control Drill",
      reason:
        agg.turnCount === 0
          ? "No scored turns in this session — run a full drill to establish a baseline."
          : "No clear weakness — keep all four skills sharp with a general drill.",
    };
  }

  const drill = DRILL_BY_DIMENSION[agg.weakestDimension];
  return {
    ...drill,
    reason: `${DIMENSION_LABELS[agg.weakestDimension]} averaged ${weakScore}/100 — the weakest of the four skills this session.`,
  };
}

// ── Text builders ────────────────────────────────────────────────────────────

export function buildSummaryText(s: {
  overall: number;
  turnCount: number;
  strongestDimension: SummaryDimension;
  weakestDimension: SummaryDimension;
  dimensionAverages: Record<SummaryDimension, number>;
}): string {
  if (s.turnCount === 0) {
    return "No scored turns in this session yet — complete a few exchanges to get a coaching summary.";
  }
  const band =
    s.overall >= 70 ? "Strong session" : s.overall >= 50 ? "Solid session with gaps" : "Tough session";
  return (
    `${band} — ${s.overall}/100 across ${s.turnCount} turn${s.turnCount === 1 ? "" : "s"}. ` +
    `Best skill: ${DIMENSION_LABELS[s.strongestDimension]} (${s.dimensionAverages[s.strongestDimension]}). ` +
    `Focus area: ${DIMENSION_LABELS[s.weakestDimension]} (${s.dimensionAverages[s.weakestDimension]}).`
  );
}

export function buildNextBestAction(s: {
  turnCount: number;
  weakestDimension: SummaryDimension;
  dimensionAverages: Record<SummaryDimension, number>;
  recommendedDrill: SparringSessionSummary["recommendedDrill"];
}): string {
  if (s.turnCount === 0) return "Run a sparring session with at least three exchanges.";
  if (s.recommendedDrill.type === "general")
    return "Book your next sparring session and push for a harder difficulty.";
  return `Run the ${s.recommendedDrill.title} and retry your weakest moments in replay mode.`;
}

// ── Main entry ───────────────────────────────────────────────────────────────

export function buildSparringSessionSummary(input: {
  turnScores: StoredTurnScore[] | null | undefined;
  failedMoments?: any[] | null;
}): SparringSessionSummary {
  const agg = aggregateTurnScores(input.turnScores || []);

  // Strongest/weakest (stable tie-break: dimension order)
  let strongestDimension: SummaryDimension = DIMENSIONS[0];
  let weakestDimension: SummaryDimension = DIMENSIONS[0];
  for (const d of DIMENSIONS) {
    if (agg.dimensionAverages[d] > agg.dimensionAverages[strongestDimension])
      strongestDimension = d;
    if (agg.dimensionAverages[d] < agg.dimensionAverages[weakestDimension])
      weakestDimension = d;
  }

  const topFlags = Array.from(agg.flagCounts.entries())
    .map(([flag, count]) => ({ flag, count }))
    .sort((a, b) => b.count - a.count || a.flag.localeCompare(b.flag));

  const recommendedDrill = recommendSparringDrill({
    dimensionAverages: agg.dimensionAverages,
    weakestDimension,
    strongestDimension,
    turnCount: agg.turnCount,
  });

  const base = {
    overall: agg.overall,
    dimensionAverages: agg.dimensionAverages,
    turnCount: agg.turnCount,
    strongestDimension,
    weakestDimension,
  };

  return {
    ...base,
    topFlags,
    weakMoments: selectWeakMoments(input.turnScores || [], input.failedMoments),
    recommendedDrill,
    summaryText: buildSummaryText(base),
    nextBestAction: buildNextBestAction({ ...base, recommendedDrill }),
  };
}
