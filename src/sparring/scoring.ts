// src/sparring/scoring.ts
// TIER 2A — Day 103: structured turn scoring for sparring turns.
//
// Deterministic and heuristic — no LLM calls (the batched session-end LLM tier
// is a later day). Produces the four coachable dimensions plus feedback,
// flags and a recommended next move. The existing micro-score heuristic in
// sparring.ts keeps running unchanged for backward compatibility.

import {
  type SparringState,
  type ObjectionType,
  clampState,
} from "./state";

export type TurnFlag =
  | "filler"
  | "vague"
  | "missed_objection"
  | "strong_question"
  | "clear_next_step"
  | "weak_close"
  | "strong_reframe";

export type StructuredTurnScore = {
  overall: number;
  clarity: number;
  confidence: number;
  objectionHandling: number;
  progression: number;
  feedback: string;
  weakMoment: string | null;
  recommendedNextMove: string;
  flags: TurnFlag[];
};

export type TurnScoreInput = {
  repMessage: string;
  buyerMessage?: string | null;
  currentState: SparringState;
  previousState?: SparringState | null;
  difficulty?: string | null;
  personaId?: string | null;
};

const clamp = (n: number) =>
  Math.max(0, Math.min(100, Math.round(Number.isFinite(n) ? n : 0)));

// ── Deterministic text signals ───────────────────────────────────────────────

const FILLER_PHRASES = [
  "um", "uh", "like,", "you know", "i mean", "basically", "kind of",
  "sort of", "i guess", "to be honest", "honestly,",
];
const HEDGING = /\b(maybe|i think|probably|not sure|i guess|possibly|might be able)\b/i;
const DONT_KNOW = /\b(i don'?t know|no idea|not certain|can'?t say)\b/i;
const DISCOVERY_STARTERS = /\b(how|what|why|when|who|tell me|walk me through|help me understand)\b/i;
const NEXT_STEP = /\b(next step|book|schedule|calendar|demo|trial|follow.?up|send (you|over)|set up a|shall we|let'?s agree)\b/i;
const ACKNOWLEDGE = /\b(understand|fair|i hear you|good question|valid|appreciate|makes sense)\b/i;
const PROOF = /\b(\d+%|\d+ (hours?|minutes?|days?|weeks?)|case study|customers?|saved|results?|roi|payback)\b/i;

const OBJECTION_COUNTERS: Record<ObjectionType, RegExp> = {
  price: /\b(roi|payback|saves?|cost of (not|doing nothing)|value|per (month|user|seat)|investment|pays for itself)\b/i,
  timing: /\b(start small|pilot|quick win|takes (only|just)|by (next|the end)|cost of waiting|sooner)\b/i,
  authority: /\b(decision maker|loop in|invite|your (boss|team|manager)|stakeholders?|together|who else)\b/i,
  trust: /\b(case study|references?|guarantee|proof|customers? like you|track record|testimonial|pilot)\b/i,
  competitor: /\b(differen(t|ce|tiator)|unlike|compared to|switch|migration|side.by.side|gap)\b/i,
  unknown: /\b(understand|specifically|tell me more|what.s behind|dig into|concern)\b/i,
};

type Signals = {
  words: number;
  fillerCount: number;
  hedging: boolean;
  dontKnow: boolean;
  vague: boolean;
  question: boolean;
  discoveryQuestion: boolean;
  nextStep: boolean;
  acknowledges: boolean;
  proof: boolean;
  countersObjection: boolean;
  objectionActive: boolean;
  objectionType: ObjectionType | null;
  stage: SparringState["stage"];
  stageAdvanced: boolean;
};

function readSignals(input: TurnScoreInput): Signals {
  const text = String(input.repMessage || "");
  const lower = text.toLowerCase();
  const words = lower.split(/\s+/).filter(Boolean).length;
  const state = input.currentState;

  const objectionActive = state.objectionState.active;
  const objectionType = state.objectionState.type;

  const countersObjection =
    objectionActive && objectionType
      ? OBJECTION_COUNTERS[objectionType].test(text)
      : false;

  return {
    words,
    fillerCount: FILLER_PHRASES.filter((p) => lower.includes(p)).length,
    hedging: HEDGING.test(text),
    dontKnow: DONT_KNOW.test(text),
    vague: words < 6,
    question: text.includes("?"),
    discoveryQuestion: text.includes("?") && DISCOVERY_STARTERS.test(text),
    nextStep: NEXT_STEP.test(text),
    acknowledges: ACKNOWLEDGE.test(text),
    proof: PROOF.test(text),
    countersObjection,
    objectionActive,
    objectionType,
    stage: state.stage,
    stageAdvanced:
      !!input.previousState && input.previousState.stage !== state.stage,
  };
}

// ── Dimension scorers (each 0–100, deterministic) ───────────────────────────

export function scoreClarity(input: TurnScoreInput): number {
  const s = readSignals(input);
  let score = 55;
  if (s.vague) score -= 25;
  if (s.words > 80) score -= 10; // rambling
  if (s.discoveryQuestion) score += 15;
  if (s.proof) score += 10;
  score -= Math.min(20, s.fillerCount * 5);
  if (s.dontKnow) score -= 15;
  return clamp(score);
}

export function scoreConfidence(input: TurnScoreInput): number {
  const s = readSignals(input);
  let score = 55;
  if (s.hedging) score -= 15;
  if (s.dontKnow) score -= 20;
  if (s.vague) score -= 10;
  if (s.proof) score += 10;
  if (s.nextStep) score += 10;
  score -= Math.min(15, s.fillerCount * 5);
  return clamp(score);
}

export function scoreObjectionHandling(input: TurnScoreInput): number {
  const s = readSignals(input);
  if (!s.objectionActive) {
    // No live objection: neutral baseline, small credit for pre-emptive value framing.
    return clamp(55 + (s.proof ? 10 : 0));
  }
  let score = 40;
  if (s.acknowledges) score += 15;
  if (s.countersObjection) score += 25;
  if (s.proof) score += 10;
  if (!s.acknowledges && !s.countersObjection) score = 25; // ignored it
  if (s.dontKnow) score -= 10;
  return clamp(score);
}

export function scoreProgression(input: TurnScoreInput): number {
  const s = readSignals(input);
  let score = 50;
  if (s.nextStep) score += 25;
  if (s.discoveryQuestion) score += 10;
  if (s.stageAdvanced) score += 10;
  if (s.vague) score -= 15;
  if (s.stage === "close" && !s.nextStep) score = Math.min(score, 30); // weak close
  return clamp(score);
}

// ── Flags ────────────────────────────────────────────────────────────────────

export function detectTurnFlags(input: TurnScoreInput): TurnFlag[] {
  const s = readSignals(input);
  const flags: TurnFlag[] = [];
  if (s.fillerCount >= 2) flags.push("filler");
  if (s.vague || s.dontKnow) flags.push("vague");
  if (s.objectionActive && !s.acknowledges && !s.countersObjection)
    flags.push("missed_objection");
  if (s.discoveryQuestion && !s.vague) flags.push("strong_question");
  if (s.nextStep) flags.push("clear_next_step");
  if (s.stage === "close" && !s.nextStep) flags.push("weak_close");
  if (s.objectionActive && s.countersObjection && s.acknowledges && s.proof)
    flags.push("strong_reframe");
  return flags;
}

// ── Feedback + recommendation ────────────────────────────────────────────────

const NEXT_MOVE_BY_FLAG: Array<[TurnFlag, string]> = [
  ["missed_objection", "Address the buyer's objection directly before moving on — acknowledge it, then counter with value."],
  ["weak_close", "Ask for a specific next step — a booked time beats a vague follow-up."],
  ["vague", "Be specific: one clear point, a number or proof, then a question."],
  ["filler", "Tighten the delivery — drop the filler and lead with your strongest point."],
];

function recommendNextMove(score: StructuredTurnScore, s: Signals): string {
  for (const [flag, advice] of NEXT_MOVE_BY_FLAG) {
    if (score.flags.includes(flag)) return advice;
  }
  if (s.objectionActive && score.objectionHandling >= 65)
    return "Good handling — confirm the objection is settled, then move towards the next step.";
  if (score.progression >= 70)
    return "Momentum is good — lock in the next step while the buyer is engaged.";
  if (s.stage === "discovery")
    return "Keep digging — one more discovery question before you pitch.";
  return "Build on this: tie your point to the buyer's situation and ask a question.";
}

export function buildTurnFeedback(score: StructuredTurnScore): string {
  const dims: Array<[string, number]> = [
    ["clarity", score.clarity],
    ["confidence", score.confidence],
    ["objection handling", score.objectionHandling],
    ["progression", score.progression],
  ];
  dims.sort((a, b) => a[1] - b[1]);
  const weakest = dims[0];
  const strongest = dims[dims.length - 1];

  if (score.overall >= 70)
    return `Strong turn — ${strongest[0]} stood out. Keep the momentum.`;
  if (score.overall >= 50)
    return `Decent turn, but ${weakest[0]} needs work (${weakest[1]}/100).`;
  return `Weak turn — ${weakest[0]} let you down (${weakest[1]}/100). ${
    score.flags.includes("missed_objection") ? "The buyer's objection went unanswered." : ""
  }`.trim();
}

// ── Main entry ───────────────────────────────────────────────────────────────

export function scoreSparringTurn(input: TurnScoreInput): StructuredTurnScore {
  const s = readSignals(input);

  const clarity = scoreClarity(input);
  const confidence = scoreConfidence(input);
  const objectionHandling = scoreObjectionHandling(input);
  const progression = scoreProgression(input);

  // Weighted blend: 25/20/30/25 baseline; objection active → objection 40%;
  // close stage → progression 40%. Weights renormalised to 1.
  let w = { clarity: 0.25, confidence: 0.2, objectionHandling: 0.3, progression: 0.25 };
  if (s.objectionActive) w = { clarity: 0.2, confidence: 0.15, objectionHandling: 0.4, progression: 0.25 };
  else if (s.stage === "close") w = { clarity: 0.2, confidence: 0.15, objectionHandling: 0.25, progression: 0.4 };

  const overall = clamp(
    clarity * w.clarity +
      confidence * w.confidence +
      objectionHandling * w.objectionHandling +
      progression * w.progression
  );

  const partial: StructuredTurnScore = {
    overall,
    clarity,
    confidence,
    objectionHandling,
    progression,
    feedback: "",
    weakMoment: null,
    recommendedNextMove: "",
    flags: detectTurnFlags(input),
  };

  partial.feedback = buildTurnFeedback(partial);
  partial.recommendedNextMove = recommendNextMove(partial, s);
  partial.weakMoment =
    overall < 50 ? String(input.repMessage || "").trim().slice(0, 120) || null : null;

  return partial;
}

// ── State feedback loop ──────────────────────────────────────────────────────
// EMA merge of the structured dimensions into state.repPerformance (recent
// turns weigh more, matching the state manager's smoothing).

const EMA_ALPHA = 0.4;
const ema = (prev: number, next: number) => clamp(prev + EMA_ALPHA * (next - prev));

export function mergeTurnScoreIntoState(
  state: SparringState,
  score: StructuredTurnScore
): SparringState {
  return clampState({
    ...state,
    repPerformance: {
      clarity: ema(state.repPerformance.clarity, score.clarity),
      confidence: ema(state.repPerformance.confidence, score.confidence),
      objectionHandling: ema(state.repPerformance.objectionHandling, score.objectionHandling),
      progression: ema(state.repPerformance.progression, score.progression),
    },
  });
}
