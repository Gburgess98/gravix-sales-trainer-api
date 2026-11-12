// ONE definition of a "scored call" and of the reporting window, shared by every Insights figure
// (score trend, command-centre average, scored-call KPI, needs-review count, skills).
//
// A scored call is a call with a valid, finite score_overall. A status value is NOT part of the definition (it can lag behind the
// score). A genuine score of 0 is a score; null / undefined / "" / NaN / non-numeric are not. Unscored calls are never counted
// as 0 and never averaged.

/** A real score: a finite number (or numeric string). null / undefined / "" / NaN / booleans are NOT scores. */
export function realScore(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export const isScoredCall = (row: { score_overall?: unknown } | null | undefined): boolean => realScore(row?.score_overall) !== null;

/** Applied at the database AND re-checked in memory (isScoredCall) so the two can never disagree. */
export function onlyScoredCalls<T extends { not: (col: string, op: string, v: unknown) => T }>(query: T): T {
  return query.not("score_overall", "is", null);
}

/** Same row cap for every reader of the scored set, so one cannot see more calls than another. */
export const SCORED_CALL_ROW_LIMIT = 2000;

/** Start of an N-day reporting window: exactly N × 24h before `nowMs`. Every Insights reader uses this. */
export function windowStartIso(days: number, nowMs: number = Date.now()): string {
  return new Date(nowMs - days * 86_400_000).toISOString();
}

export type ScoredSummary = { count: number; sum: number; average: number | null };

/** Count / sum / rounded mean over the scored calls only. No scored calls → average null (never 0). */
export function summariseScored(rows: Array<{ score_overall?: unknown }>): ScoredSummary {
  let count = 0;
  let sum = 0;
  for (const r of rows) {
    const s = realScore(r.score_overall);
    if (s === null) continue;
    count += 1;
    sum += s;
  }
  return { count, sum, average: count > 0 ? Math.round(sum / count) : null };
}

/**
 * A count query result → a number only when the query really succeeded. A failed query, or a missing count, is "unavailable"
 * (null) — never 0. A genuine zero stays 0. Raw database errors are never part of the outcome.
 */
export function countOutcome(res: { count?: number | null; error?: unknown } | null | undefined): { value: number | null; available: boolean } {
  if (!res || res.error || typeof res.count !== "number" || !Number.isFinite(res.count) || res.count < 0) return { value: null, available: false };
  return { value: res.count, available: true };
}
