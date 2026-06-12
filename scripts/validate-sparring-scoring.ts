/**
 * validate-sparring-scoring.ts — Tier 2A Day 103
 * Deterministic assertions for structured turn scoring.
 * Usage: npx tsx scripts/validate-sparring-scoring.ts
 */

import { createInitialSparringState, updateSparringState } from "../src/sparring/state";
import {
  scoreSparringTurn,
  mergeTurnScoreIntoState,
  type TurnScoreInput,
} from "../src/sparring/scoring";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const baseState = createInitialSparringState({ personaId: "price_sensitive", difficulty: "standard" });
const score = (repMessage: string, overrides: Partial<TurnScoreInput> = {}) =>
  scoreSparringTurn({ repMessage, currentState: baseState, ...overrides });

// ── Discovery vs vague ──
const strongDiscovery = score("How does your team currently handle onboarding, and what does a delay cost you each month?");
const vague = score("um yeah maybe");
check("strong discovery question outscores vague reply", strongDiscovery.overall > vague.overall);
check("vague reply has low clarity", vague.clarity < 45);
check("vague reply has low progression", vague.progression < 45);
check("vague flags include vague", vague.flags.includes("vague"));
check("strong discovery flags strong_question", strongDiscovery.flags.includes("strong_question"));

// ── Objection handling: direct vs avoidance ──
const objectionState = updateSparringState(baseState, {
  repText: "x",
  turnScore: 50,
  newObjectionText: "this is too expensive for us",
});
const handled = score(
  "That's a fair concern. Compared to the 10 hours a week you'd save, the ROI means it pays for itself within a month.",
  { currentState: objectionState }
);
const avoided = score("Anyway, let me tell you about our other features.", { currentState: objectionState });
check("direct price handling outscores avoidance", handled.objectionHandling > avoided.objectionHandling);
check("avoidance flags missed_objection", avoided.flags.includes("missed_objection"));
check("direct handling earns strong_reframe", handled.flags.includes("strong_reframe"));
check("objection weighting lifts overall for handler", handled.overall > avoided.overall);

// ── Close / next step ──
const closeState = { ...baseState, stage: "close" as const };
const goodClose = score("Shall we book a demo on Thursday so we can set up a trial for your team?", { currentState: closeState });
const badClose = score("Okay well, think it over and let me know sometime.", { currentState: closeState });
check("clear next step scores progression high", goodClose.progression > 70);
check("good close flags clear_next_step", goodClose.flags.includes("clear_next_step"));
check("no next step at close stage flags weak_close", badClose.flags.includes("weak_close"));
check("weak close caps progression", badClose.progression <= 30);

// ── Filler ──
const filler = score("um, you know, basically it's kind of like a tool that, i mean, does stuff");
check("filler-heavy reply flags filler", filler.flags.includes("filler"));
check("filler-heavy reply drops confidence", filler.confidence < 50);

// ── Clamping ──
const all = [strongDiscovery, vague, handled, avoided, goodClose, badClose, filler];
check(
  "all dimensions clamped 0–100",
  all.every((t) =>
    [t.overall, t.clarity, t.confidence, t.objectionHandling, t.progression].every((n) => n >= 0 && n <= 100)
  )
);

// ── Feedback / recommendation / weak moment ──
check("feedback text present", all.every((t) => t.feedback.length > 0));
check("recommendedNextMove present", all.every((t) => t.recommendedNextMove.length > 0));
check("weak turn captures weakMoment", vague.weakMoment !== null);
check("strong turn has no weakMoment", strongDiscovery.weakMoment === null || strongDiscovery.overall < 50);
check("missed objection advice mentions objection", avoided.recommendedNextMove.toLowerCase().includes("objection"));

// ── State merge ──
const merged = mergeTurnScoreIntoState(baseState, strongDiscovery);
check("mergeTurnScoreIntoState moves repPerformance", merged.repPerformance.clarity !== baseState.repPerformance.clarity);
check(
  "merge moves towards the turn score",
  Math.abs(merged.repPerformance.clarity - strongDiscovery.clarity) <
    Math.abs(baseState.repPerformance.clarity - strongDiscovery.clarity)
);

// ── Determinism ──
const r1 = score("How does your team currently handle onboarding, and what does a delay cost you each month?");
check("repeated input gives identical output", JSON.stringify(r1) === JSON.stringify(strongDiscovery));

console.log(failures === 0 ? "\nSparring scoring validation PASSED" : `\nSparring scoring validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
