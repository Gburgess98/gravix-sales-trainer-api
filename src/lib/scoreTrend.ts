// Daily score trend for GET /v1/crm/analytics/score-trend (pure, network-free).
//
// Before: `Number(score_overall || 0)` turned an UNSCORED call into a score of 0 and counted it, so unscored calls dragged the
// day's average toward 0. Now only calls with a real, finite score contribute; a day with no scored calls has no point.
// Each point also carries `count` (scored calls behind the average) — additive, existing consumers ignore it.

import { realScore } from "./scoredCalls";

export type TrendRow = { score_overall?: unknown; created_at?: unknown };
export type TrendPoint = { date: string; avg_score: number; count: number };

export { realScore };

export function buildScoreTrend(rows: TrendRow[]): TrendPoint[] {
  const buckets = new Map<string, { total: number; count: number }>();
  for (const r of rows) {
    const score = realScore(r.score_overall);
    if (score === null) continue;
    const date = typeof r.created_at === "string" ? r.created_at.slice(0, 10) : "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;
    const b = buckets.get(date) ?? { total: 0, count: 0 };
    b.total += score;
    b.count += 1;
    buckets.set(date, b);
  }
  return Array.from(buckets.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([date, v]) => ({ date, avg_score: Math.round(v.total / v.count), count: v.count }));
}
