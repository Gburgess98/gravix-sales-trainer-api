/**
 * validate-sparring-summary.ts — Tier 2A Day 104
 * Deterministic assertions for the sparring session summary builder.
 * Usage: npx tsx scripts/validate-sparring-summary.ts
 */

import {
  buildSparringSessionSummary,
  aggregateTurnScores,
  selectWeakMoments,
  recommendSparringDrill,
  type StoredTurnScore,
} from "../src/sparring/summary";
import type { StructuredTurnScore } from "../src/sparring/scoring";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const mkScore = (over: Partial<StructuredTurnScore>): StructuredTurnScore => ({
  overall: 50,
  clarity: 50,
  confidence: 50,
  objectionHandling: 50,
  progression: 50,
  feedback: "x",
  weakMoment: null,
  recommendedNextMove: "x",
  flags: [],
  ...over,
});

const turns: StoredTurnScore[] = [
  {
    turnId: "t1",
    repMessage: "um maybe",
    score: mkScore({ overall: 30, clarity: 20, confidence: 25, objectionHandling: 40, progression: 35, weakMoment: "um maybe", flags: ["vague", "filler"], recommendedNextMove: "Be specific." }),
    createdAt: "2026-06-12T10:00:00Z",
  },
  {
    turnId: "t2",
    repMessage: "Fair point — the ROI pays for itself in a month.",
    score: mkScore({ overall: 70, clarity: 70, confidence: 60, objectionHandling: 80, progression: 70, flags: ["strong_reframe"] }),
    createdAt: "2026-06-12T10:01:00Z",
  },
  {
    turnId: "t3",
    repMessage: "Shall we book Thursday?",
    score: mkScore({ overall: 80, clarity: 75, confidence: 80, objectionHandling: 60, progression: 90, flags: ["clear_next_step", "vague"] }),
    createdAt: "2026-06-12T10:02:00Z",
  },
];

// ── Averages ──
const agg = aggregateTurnScores(turns);
check("overall average correct (60)", agg.overall === 60);
check("clarity average correct (55)", agg.dimensionAverages.clarity === 55);
check("confidence average correct (55)", agg.dimensionAverages.confidence === 55);
check("objectionHandling average correct (60)", agg.dimensionAverages.objectionHandling === 60);
check("progression average correct (65)", agg.dimensionAverages.progression === 65);
check("turnCount correct", agg.turnCount === 3);

// ── Full summary ──
const summary = buildSparringSessionSummary({ turnScores: turns });
check("strongest dimension is progression", summary.strongestDimension === "progression");
check("weakest dimension is clarity or confidence", ["clarity", "confidence"].includes(summary.weakestDimension));
check("top flag is vague with count 2", summary.topFlags[0]?.flag === "vague" && summary.topFlags[0]?.count === 2);
check("weak moments selected (lowest overall first)", summary.weakMoments.length === 1 && summary.weakMoments[0].turnId === "t1");
check("weak moment carries recommendation", summary.weakMoments[0].recommendedNextMove.length > 0);
check("summaryText present", summary.summaryText.includes("/100"));
check("nextBestAction present", summary.nextBestAction.length > 0);

// ── Drill mapping ──
const drillFor = (dims: Record<string, number>) =>
  recommendSparringDrill({
    dimensionAverages: dims as any,
    weakestDimension: (Object.entries(dims).sort((a, b) => a[1] - b[1])[0][0]) as any,
    strongestDimension: (Object.entries(dims).sort((a, b) => b[1] - a[1])[0][0]) as any,
    turnCount: 3,
  });
check("weak objectionHandling → Objection Handling Drill", drillFor({ clarity: 70, confidence: 70, objectionHandling: 40, progression: 70 }).type === "objection_handling");
check("weak progression → Closing drill", drillFor({ clarity: 70, confidence: 70, objectionHandling: 70, progression: 40 }).type === "closing");
check("weak clarity → Clarity Drill", drillFor({ clarity: 40, confidence: 70, objectionHandling: 70, progression: 70 }).type === "clarity");
check("weak confidence → Confidence Drill", drillFor({ clarity: 70, confidence: 40, objectionHandling: 70, progression: 70 }).type === "confidence");
check("no clear weakness → General drill", drillFor({ clarity: 70, confidence: 68, objectionHandling: 72, progression: 70 }).type === "general");

// ── Weak-moment fallback to failed_moments ──
const noWeak = turns.map((t) => ({ ...t, score: mkScore({ ...t.score, weakMoment: null }) }));
const fallback = selectWeakMoments(noWeak, [{ rep_text: "bad answer", reason: "weak close", score: 35 }]);
check("falls back to failed_moments", fallback.length === 1 && fallback[0].message === "bad answer");

// ── Empty session ──
const empty = buildSparringSessionSummary({ turnScores: [] });
check("no-turn summary does not crash", empty.turnCount === 0 && empty.overall === 0);
check("no-turn summary recommends general drill", empty.recommendedDrill.type === "general");
check("no-turn summaryText is safe", empty.summaryText.toLowerCase().includes("no scored turns"));

// ── Determinism ──
const again = buildSparringSessionSummary({ turnScores: turns });
check("repeated input gives identical output", JSON.stringify(again) === JSON.stringify(summary));

console.log(failures === 0 ? "\nSparring summary validation PASSED" : `\nSparring summary validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
