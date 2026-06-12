import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import { PERSONAS } from "../personas";
import { getScoringConfig } from "../services/scoringConfig";
import { completeAssignmentsForTarget } from "../lib/assignmentsComplete";
import {
  getPersonaConfig,
  buildPersonaBehaviourSummary,
  DifficultyLevel,
} from "../personas";
// TIER 2A Day 101 — Sparring Brain: pure state manager + provider router
import {
  createInitialSparringState,
  updateSparringState,
  coerceState,
  generateBuyerReply,
  withStateDirectives,
  scoreSparringTurn,
  mergeTurnScoreIntoState,
  buildSparringSessionSummary,
  type SparringState,
  type StructuredTurnScore,
} from "../sparring";

// Create a service-role Supabase client (write access)
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY!,
});

// -------------------------
// Scoring + XP helpers
// -------------------------


function clampScore(n: number): number {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}


// -------------------- XP + STREAK HELPERS --------------------

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

// ✅ Default to a 75+ standard for "good" selling.
// We still use a calibrated streak score (Option A) because our heuristic micro-score
// is currently conservative. You can switch back instantly by setting:
//   SPAR_STREAK_USE_CALIBRATION=0
// or by lowering SPAR_STREAK_MIN_SCORE.
const STREAK_MIN_SCORE = Number.isFinite(Number(process.env.SPAR_STREAK_MIN_SCORE))
  ? clamp(Number(process.env.SPAR_STREAK_MIN_SCORE), 0, 100)
  : 75;

const STREAK_USE_CALIBRATION =
  String(process.env.SPAR_STREAK_USE_CALIBRATION || "1").trim() !== "0";

// XP multiplier can be based on the **current** streak or the **best** streak.
// Default: current (more competitive; drops hurt immediately).
// Flip back instantly by setting: SPAR_XP_MULTIPLIER_MODE=best
const XP_MULTIPLIER_MODE = String(process.env.SPAR_XP_MULTIPLIER_MODE || "current")
  .trim()
  .toLowerCase();

// Calibrate the heuristic micro-score up a touch so streaks are achievable while
// we iterate the micro-scoring rubric.
// - If calibration is OFF: uses raw score.
// - If calibration is ON: shifts score up modestly (capped 0..100).
function getStreakScore(rawTurnScore: number): number {
  const raw = clamp100(rawTurnScore);
  if (!STREAK_USE_CALIBRATION) return raw;

  // modest lift: +10 baseline and +15% scale, then cap
  const calibrated = raw * 1.15 + 10;
  return clamp100(calibrated);
}

function streakMultiplier(streak: number, streakThreshold = 3) {
  const t = Number.isFinite(Number(streakThreshold)) ? Math.max(1, Math.floor(Number(streakThreshold))) : 3;

  // If you haven't hit the threshold yet, no multiplier.
  if (streak < t) return 1.0;

  // At threshold and beyond, step up gently.
  // Example with t=3: 3->1.1, 4->1.2, 5+->1.3
  if (streak === t) return 1.1;
  if (streak === t + 1) return 1.2;
  return 1.3;
}

function isGoodTurn(rawTurnScore: number) {
  // "Good" is judged on the calibrated streak score (Option A)
  return getStreakScore(rawTurnScore) >= STREAK_MIN_SCORE;
}

type StreakMeta = {
  streak?: number;
  best_streak?: number;
  xp_multiplier?: number;
  last_turn_score?: number; // score used for streak logic (calibrated if enabled)
  last_turn_score_raw?: number; // raw heuristic micro-score
  comeback_pending?: boolean;
  xp_bonus_pending?: number;
};

function updateStreakMeta(
  prev: any,
  rawTurnScore: number,
  opts?: { streakThreshold?: number; comebackBonus?: number }
): StreakMeta {
  const meta: StreakMeta = { ...(prev || {}) };

  // Hard defaults so meta fields are never null/undefined in DB/UI
  if (typeof meta.streak !== "number") meta.streak = 0;
  if (typeof meta.best_streak !== "number") meta.best_streak = 0;
  if (typeof meta.xp_multiplier !== "number") meta.xp_multiplier = streakMultiplier(Number(meta.streak || 0), opts?.streakThreshold ?? 3);
  if (typeof meta.last_turn_score !== "number") meta.last_turn_score = 0;
  if (typeof meta.last_turn_score_raw !== "number") meta.last_turn_score_raw = 0;
  if (typeof meta.comeback_pending !== "boolean") meta.comeback_pending = false;
  if (typeof meta.xp_bonus_pending !== "number") meta.xp_bonus_pending = 0;

  const prevStreak = Number(meta.streak || 0);
  const prevBest = Number(meta.best_streak || 0);

  const streakScore = getStreakScore(rawTurnScore);

  let streak = prevStreak;
  let best_streak = prevBest;

  if (isGoodTurn(rawTurnScore)) {
    streak = prevStreak + 1;
    best_streak = Math.max(best_streak, streak);

    if (meta.comeback_pending) {
      const cb = Number.isFinite(Number(opts?.comebackBonus)) ? Math.max(0, Math.floor(Number(opts?.comebackBonus))) : 5;
      meta.xp_bonus_pending = Number(meta.xp_bonus_pending || 0) + cb;
      meta.comeback_pending = false;
    }
  } else {
    if (prevStreak >= 2) meta.comeback_pending = true;
    streak = 0;
  }

  meta.streak = streak;
  meta.best_streak = best_streak;
  // Multiplier is based on current streak by default (more competitive).
  // Can be switched back to best streak via SPAR_XP_MULTIPLIER_MODE=best.
  const basis = XP_MULTIPLIER_MODE === "best" ? best_streak : streak;
  meta.xp_multiplier = streakMultiplier(basis, opts?.streakThreshold ?? 3);
  meta.last_turn_score_raw = clamp100(rawTurnScore);
  meta.last_turn_score = streakScore;

  return meta;
}

// -------------------------
// Micro-scoring (per rep turn)
// -------------------------

function clamp100(n: number) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function isUuid(v: string) {
  const s = String(v || "").trim();
  // basic UUID v4-ish check (good enough for guardrails)
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s);
}

function containsAny(text: string, needles: string[]) {
  const t = String(text || "").toLowerCase();
  return needles.some((n) => t.includes(n));
}

type FailedMoment = {
  turn: number;
  reason: string;
  buyer_message: string;
  rep_response: string;
  score: number;
  created_at: string;
};

type ReplayState = {
  source_session_id: string;
  source_turn: number;
  source_reason: string;
  replay_started_at: string;
  replay_attempt: number;
};

type MicroScore = {
  turn_score: number;
  micro_breakdown: {
    opener: number;
    discovery: number;
    pitch: number;
    objections: number;
    close: number;
  };
  coach_note: string;
  flags: string[];
};

function scoreRepTurnHeuristic(repText: string, buyerText: string): MicroScore {
  const rep = (repText || "").trim();
  const buyer = (buyerText || "").trim();

  const flags: string[] = [];

  // Signals
  const askedQuestion = rep.includes("?") || containsAny(rep, ["what", "how", "why", "when", "where"]);
  const hasNumbers = /\d/.test(rep);
  const mentionsPrice = containsAny(rep, ["price", "cost", "expensive", "budget", "roi", "payback", "investment"]);
  const valueLanguage = containsAny(rep, ["save", "increase", "reduce", "roi", "payback", "time", "revenue", "profit", "conversion"]);
  const closeLanguage = containsAny(rep, ["next step", "book", "schedule", "slot", "calendar", "move forward", "go ahead", "start", "trial", "pilot"]);
  const empathy = containsAny(rep, ["totally fair", "makes sense", "i hear you", "understand", "fair point"]);

  // Buyer objection detection (simple)
  const buyerPriceObj = containsAny(buyer, ["expensive", "too high", "price", "cost", "budget", "roi"]);
  const buyerStall = containsAny(buyer, ["think about it", "send info", "email me", "later", "not now"]);

  // Breakdown scoring (0..100)
  let opener = 50;
  let discovery = askedQuestion ? 70 : 45;
  let pitch = valueLanguage ? 65 : 45;
  let objections = buyerPriceObj || buyerStall ? (empathy ? 70 : 50) : 55;
  let close = closeLanguage ? 70 : 40;

  // Penalties / flags
  if (rep.length < 10) {
    flags.push("too_short");
    discovery -= 10;
    pitch -= 10;
  }

  if (mentionsPrice && !valueLanguage) {
    flags.push("price_without_value");
    pitch -= 10;
  }

  if (buyerPriceObj && !mentionsPrice && !valueLanguage) {
    flags.push("missed_price_objection");
    objections -= 15;
  }

  if (buyerStall && !askedQuestion && !closeLanguage) {
    flags.push("stall_not_addressed");
    objections -= 10;
  }

  // Bonus
  if (hasNumbers) pitch += 5;

  opener = clamp100(opener);
  discovery = clamp100(discovery);
  pitch = clamp100(pitch);
  objections = clamp100(objections);
  close = clamp100(close);

  const total = clamp100(opener * 0.1 + discovery * 0.25 + pitch * 0.25 + objections * 0.25 + close * 0.15);

  let coach_note = "Keep it tight and ask a clear next question.";
  if (buyerPriceObj && valueLanguage) coach_note = "Good: validate price concern then anchor ROI/payback with a number.";
  else if (buyerPriceObj && !valueLanguage) coach_note = "Anchor ROI before discussing cost. Tie it to their world (time/money).";
  else if (!askedQuestion) coach_note = "Ask a sharper discovery question to regain control.";
  else if (closeLanguage) coach_note = "Nice: you moved toward a next step. Keep the close binary.";

  return {
    turn_score: total,
    micro_breakdown: {
      opener,
      discovery,
      pitch,
      objections,
      close,
    },
    coach_note,
    flags,
  };
}

function buildFailedMoment(opts: {
  turnNumber: number;
  repText: string;
  buyerText: string;
  micro: MicroScore;
}): FailedMoment | null {
  const { turnNumber, repText, buyerText, micro } = opts;

  if (micro.turn_score >= 55) {
    return null;
  }

  let reason = "weak_response";

  if (micro.flags.includes("missed_price_objection")) {
    reason = "missed_price_objection";
  } else if (micro.flags.includes("stall_not_addressed")) {
    reason = "stall_not_addressed";
  } else if (micro.flags.includes("price_without_value")) {
    reason = "price_without_value";
  } else if (micro.flags.includes("too_short")) {
    reason = "weak_confidence";
  }

  return {
    turn: turnNumber,
    reason,
    buyer_message: buyerText,
    rep_response: repText,
    score: micro.turn_score,
    created_at: new Date().toISOString(),
  };
}

function buildReplayState(opts: {
  sourceSessionId: string;
  failedMoment: FailedMoment;
  existingAttempts?: number;
}): ReplayState {
  return {
    source_session_id: opts.sourceSessionId,
    source_turn: opts.failedMoment.turn,
    source_reason: opts.failedMoment.reason,
    replay_started_at: new Date().toISOString(),
    replay_attempt: (opts.existingAttempts || 0) + 1,
  };
}

function getDifficultyFromRow(row: any): string {
  if (!row) return "normal";
  return (
    row.difficulty ||
    row.meta?.difficulty ||
    "normal"
  );
}

function getModeFromRow(row: any): string {
  if (!row) return "standard";
  return row.meta?.mode || "standard";
}

function computeOutcomeFromScore(total: number): "win" | "loss" | "neutral" {
  if (!Number.isFinite(total)) return "neutral";
  if (total >= 80) return "win";
  if (total <= 50) return "loss";
  return "neutral";
}


function computeXpForScore(
  total: number,
  difficulty: string,
  mode: string
): number {
  const difficultyBaseXp: Record<string, number> = {
    easy: 20,
    normal: 30,
    hard: 45,
    nightmare: 60,
  };

  const modeMultiplier: Record<string, number> = {
    standard: 1,
    time_trial: 1.1,
    close_in_2m: 1.2,
  };

  const baseXp = difficultyBaseXp[difficulty] ?? difficultyBaseXp["normal"];

  // Simple performance multiplier: 0.5x – 1.5x roughly
  const performanceMultiplier = 0.5 + (clampScore(total) / 100);

  const modeMult = modeMultiplier[mode] ?? 1;

  const raw = baseXp * performanceMultiplier * modeMult;
  return Math.max(5, Math.round(raw));
}

// -------------------------
// Emotional state helpers
// -------------------------
type EmotionalState = {
  anger: number;    // 0–100
  boredom: number;  // 0–100
  trust: number;    // 0–100
};

type PersonaMutationState = {
  volatility: number; // how quickly persona mood shifts
  resistance: number; // how hard they push back
  unpredictability: number; // chance to switch behaviour mid-call
};

function getPersonaMutationState(personaId: string, difficulty: string): PersonaMutationState {
  const base: PersonaMutationState = {
    volatility: 40,
    resistance: 40,
    unpredictability: 30,
  };

  // 🔥 Difficulty scaling
  const difficultyBoost =
    difficulty === "nightmare"
      ? 30
      : difficulty === "hard"
        ? 20
        : 0;

  switch (personaId) {
    case "angry":
      return {
        volatility: 70 + difficultyBoost,
        resistance: 80 + difficultyBoost,
        unpredictability: 50 + difficultyBoost,
      };

    case "silent":
      return {
        volatility: 30 + difficultyBoost,
        resistance: 60 + difficultyBoost,
        unpredictability: 65 + difficultyBoost,
      };

    case "price_sensitive":
      return {
        volatility: 50 + difficultyBoost,
        resistance: 70 + difficultyBoost,
        unpredictability: 45 + difficultyBoost,
      };

    case "cfo":
      return {
        volatility: 35 + difficultyBoost,
        resistance: 85 + difficultyBoost,
        unpredictability: 25 + difficultyBoost,
      };

    case "procurement":
      return {
        volatility: 45 + difficultyBoost,
        resistance: 75 + difficultyBoost,
        unpredictability: 40 + difficultyBoost,
      };

    default:
      return {
        volatility: base.volatility + difficultyBoost,
        resistance: base.resistance + difficultyBoost,
        unpredictability: base.unpredictability + difficultyBoost,
      };
  }
}

function clampEmotion(n: number): number {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function getInitialEmotionalState(personaId: string, difficulty: string): EmotionalState {
  const base: EmotionalState = { anger: 10, boredom: 10, trust: 30 };

  switch (personaId) {
    case "angry":
      return {
        anger: clampEmotion(difficulty === "nightmare" ? 55 : 45),
        boredom: 15,
        trust: 15,
      };
    case "silent":
      return {
        anger: 15,
        boredom: clampEmotion(difficulty === "nightmare" ? 55 : 45),
        trust: 20,
      };
    case "price_sensitive":
      return {
        anger: 25,
        boredom: 20,
        trust: 30,
      };
    case "cfo":
      return {
        anger: 20,
        boredom: 20,
        trust: 35,
      };
    case "procurement":
      return {
        anger: 20,
        boredom: 25,
        trust: 25,
      };
    default:
      return base;
  }
}

// Day 101 fix: this pure helper was nested inside getNextStackedObjection but
// called from applyEmotionalDelta, so every turn threw "evaluateRepWeakness is
// not defined" (pre-existing since Day 79.5). Hoisted to module scope unchanged.
function evaluateRepWeakness(text: string) {
  const t = (text || "").toLowerCase();

  let weakness = {
    vague: false,
    no_question: false,
    weak_close: false,
    low_confidence: false,
  };

  if (t.length < 20) weakness.vague = true;

  if (!t.includes("?")) weakness.no_question = true;

  if (!/\b(next step|book|schedule|move forward|go ahead)\b/i.test(t)) {
    weakness.weak_close = true;
  }

  if (/\b(maybe|kind of|sort of|might|possibly)\b/i.test(t)) {
    weakness.low_confidence = true;
  }

  return weakness;
}

function applyEmotionalDelta(
  prev: EmotionalState,
  opts: {
    personaId: string;
    difficulty: string;
    turnsSoFar: number;
    lastUserText: string;
    lastAiText: string;
  }
): EmotionalState {
  const { personaId, difficulty, turnsSoFar, lastUserText } = opts;
  const lower = (lastUserText || "").toLowerCase();
  let next: EmotionalState = { ...prev };

  // Base drift: longer drills slowly increase boredom
  next.boredom = clampEmotion(next.boredom + (turnsSoFar > 8 ? 2 : 1));

  // Persona-driven adjustments
  if (personaId === "angry") {
    // Angry buyers heat up faster, especially on harder difficulties
    const inc = difficulty === "nightmare" ? 4 : difficulty === "hard" ? 3 : 2;
    next.anger = clampEmotion(next.anger + inc);
  }

  if (personaId === "silent") {
    // Silent buyers get bored quickly
    next.boredom = clampEmotion(next.boredom + (turnsSoFar > 6 ? 3 : 2));
  }

  if (personaId === "price_sensitive") {
    // Talk of price/ROI can spike frustration
    if (/\b(price|cost|expensive|budget|roi)\b/i.test(lower)) {
      next.anger = clampEmotion(next.anger + 5);
    }
  }

  // Positive acknowledgement / buying signals → increase trust, soften anger
  if (/\b(thanks|thank you|that makes sense|makes sense|got it|sounds good|sounds great|okay that sounds|ok that sounds)\b/i.test(lower)) {
    next.trust = clampEmotion(next.trust + 4);
    next.anger = clampEmotion(next.anger - 2);
  }

  // Empathy / apology from the rep → reduce anger a bit
  if (/\b(sorry|i understand|i get that|i hear you|totally get)\b/i.test(lower)) {
    next.anger = clampEmotion(next.anger - 3);
  }

  // Classic stall / brush-off phrases → boredom up, trust down
  if (/\b(think about it|send me an email|send me an e-mail|circle back|touch base|maybe later|not a priority)\b/i.test(lower)) {
    next.boredom = clampEmotion(next.boredom + 4);
    next.trust = clampEmotion(next.trust - 2);
  }

  // Hard-close / pressure phrases → anger up, trust down
  if (/\b(sign today|sign right now|right now|last chance|today only|lock this in)\b/i.test(lower)) {
    next.anger = clampEmotion(next.anger + 4);
    next.trust = clampEmotion(next.trust - 3);
  }

  // Very rough trust nudge if rep is asking questions (helps engagement)
  if (lastUserText.includes("?")) {
    next.trust = clampEmotion(next.trust + 2);
    next.boredom = clampEmotion(next.boredom - 1);
  }

  // 🔥 escalate if rep keeps failing
  if (prev.trust < 30 && prev.anger > 50) {
    next.anger = clampEmotion(next.anger + 3);
    next.boredom = clampEmotion(next.boredom + 2);
  }

  // Generic "salesy" language → small trust drop + anger bump
  if (/\b(deal|sign up|discount|offer|promotion)\b/i.test(lower)) {
    next.trust = clampEmotion(next.trust - 2);
    next.anger = clampEmotion(next.anger + 2);
  }

  const weakness = evaluateRepWeakness(lastUserText);

  // 🔥 escalate aggressively if weak
  if (weakness.vague || weakness.low_confidence) {
    next.anger = clampEmotion(next.anger + 5);
    next.trust = clampEmotion(next.trust - 4);
  }

  if (weakness.no_question) {
    next.boredom = clampEmotion(next.boredom + 5);
  }

  if (weakness.weak_close && turnsSoFar > 6) {
    next.boredom = clampEmotion(next.boredom + 4);
    next.trust = clampEmotion(next.trust - 3);
  }

  return next;
}

function shouldAutoHangUp(opts: {
  personaId: string;
  difficulty: string;
  mode: string;
  turnsSoFar: number;
  emotionalState?: EmotionalState | null;

}): { endNow: boolean; reason: "bored" | "angry" | "timeout" | "closed" | null } {
  const { personaId, difficulty, mode, turnsSoFar, emotionalState } = opts;

  // Very basic heuristic for now:
  // - Count total messages in the thread (user + assistant)
  // - Thresholds vary a bit by persona / mode / difficulty
  const t = Math.max(0, turnsSoFar);

  const anger = emotionalState?.anger ?? 0;
  const boredom = emotionalState?.boredom ?? 0;
  const trust = emotionalState?.trust ?? 0;
  const weakness = {
    vague: false,
  };

  // Emotion-driven early exits
  // High anger → rage hang-up
  if (anger >= 85 && t >= 6) {
    return { endNow: true, reason: "angry" };
  }

  if (anger > 75 && weakness?.vague) {
    return { endNow: true, reason: "angry" };
  }

  // High boredom → bored hang-up
  if (boredom >= 85 && t >= 8) {
    return { endNow: true, reason: "bored" };
  }

  // High trust late in the call → "closed, but has to jump"
  if (trust >= 75 && t >= 10) {
    return { endNow: true, reason: "closed" };
  }

  // If they've somehow gone really long, always end the call
  if (t >= 24) {
    return { endNow: true, reason: "timeout" };
  }

  // Persona-specific basic rules
  switch (personaId) {
    case "angry": {
      // Angry buyers tend to hang up earlier, especially on harder modes
      const angerTilt = anger >= 70 ? -2 : 0;
      if (difficulty === "nightmare" && t >= 10 + angerTilt) {
        return { endNow: true, reason: "angry" };
      }
      if (t >= 14 + angerTilt) {
        return { endNow: true, reason: "angry" };
      }
      break;
    }
    case "silent": {
      // Silent buyers get bored quickly in drills
      const boredomTilt = boredom >= 70 ? -2 : 0;
      if (mode === "time_trial" || mode === "close_in_2m") {
        if (t >= 12 + boredomTilt) return { endNow: true, reason: "bored" };
      } else if (t >= 16 + boredomTilt) {
        return { endNow: true, reason: "bored" };
      }
      break;
    }
    case "price_sensitive": {
      // Price-sensitive will eventually call it if rep isn't landing the value
      if (mode === "close_in_2m" && t >= 14) {
        return { endNow: true, reason: "timeout" };
      }
      if (t >= 18) {
        return { endNow: true, reason: "timeout" };
      }
      break;
    }
    default: {
      // Generic catch-all
      if (mode === "close_in_2m" && t >= 14) {
        return { endNow: true, reason: "timeout" };
      }
      if (t >= 20) {
        return { endNow: true, reason: "bored" };
      }
      break;
    }
  }

  return { endNow: false, reason: null };
}

type ObjectionStackState = {
  active: string[];
  last_added_at_turn: number;
};

function getNextStackedObjection(opts: {
  personaId: string;
  difficulty: string;
  emotional: EmotionalState;
  turnsSoFar: number;
  failures?: number;
  currentStack: string[];
}): string | null {
  const { personaId, difficulty, emotional, turnsSoFar, failures = 0, currentStack } = opts;

  const pool: Record<string, string[]> = {
    price_sensitive: [
      "This feels too expensive.",
      "I’ve seen cheaper options.",
      "What’s the ROI here?"
    ],
    cfo: [
      "Show me the numbers.",
      "What’s the downside risk?",
      "This isn’t in budget."
    ],
    procurement: [
      "We already have a vendor.",
      "This needs approval.",
      "We can’t switch easily."
    ],
    angry: [
      "This is wasting my time.",
      "You’re not listening.",
      "This isn’t relevant."
    ],
    silent: [
      "Hmm.",
      "Not sure.",
      "Maybe."
    ]
  };

  const basePool = pool[personaId] || pool["price_sensitive"];

  // 🔥 CONDITIONS TO STACK
  const shouldStack =
    turnsSoFar > 4 &&
    (
      emotional.anger > 50 ||
      emotional.boredom > 50 ||
      failures >= 2 ||
      difficulty === "hard" ||
      difficulty === "nightmare"
    );

  if (!shouldStack) return null;

  // Avoid repeating same objection
  const remaining = basePool.filter(o => !currentStack.includes(o));
  if (remaining.length === 0) return null;

  return remaining[Math.floor(Math.random() * remaining.length)];
}

function getUserIdHeader(req: Request): string {
  const raw = (req.headers['x-user-id'] as string | undefined)?.toString().trim();
  if (!raw) {
    throw new Error('missing x-user-id header');
  }
  return raw;
}

function getOrgIdFromRequest(req: Request): string | null {
  const headerOrg = (req.headers['x-org-id'] as string | undefined)?.toString().trim();
  if (headerOrg) return headerOrg;

  const bodyOrg =
    typeof (req.body as any)?.orgId === 'string'
      ? String((req.body as any).orgId).trim()
      : '';
  if (bodyOrg) return bodyOrg;

  const fallbackOrg = String(process.env.DEFAULT_ORG_ID || '').trim();
  if (fallbackOrg) return fallbackOrg;

  return null;
}

const DEFAULT_TEAM_SETTINGS_SNAPSHOT = {
  streak_threshold: 3,
  xp_multiplier: 1,
  comeback_bonus: 0,
  xp_cap_daily: 500,
  voice_score_threshold: 60,
  weak_close_threshold: 60,
  filler_density_threshold: 0.08,
  coaching_trigger_thresholds: {
    voice_score_lt: 60,
    weak_close: true,
    inactive_days_gt: 3,
  },
};

async function loadTeamSettingsSnapshot(orgId: string | null) {
  if (!orgId) {
    return {
      org_id: null,
      source: 'default_no_org',
      ...DEFAULT_TEAM_SETTINGS_SNAPSHOT,
    };
  }

  const selectCandidates = [
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold, weak_close_threshold, filler_density_threshold, coaching_trigger_thresholds, updated_at, updated_by',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold, weak_close_threshold, filler_density_threshold, updated_at, updated_by',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold, weak_close_threshold, filler_density_threshold, updated_at',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold, weak_close_threshold, filler_density_threshold',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold, weak_close_threshold',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily, voice_score_threshold',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus, xp_cap_daily',
    'org_id, streak_threshold, xp_multiplier, comeback_bonus',
    'org_id, streak_threshold, xp_multiplier',
    'org_id, streak_threshold',
    'org_id',
  ];

  for (const sel of selectCandidates) {
    const r = await supa
      .from('team_settings')
      .select(sel)
      .eq('org_id', orgId)
      .limit(1)
      .maybeSingle();

    if (!r.error) {
      const row = (r.data ?? {}) as any;
      return {
        org_id: orgId,
        source: r.data ? 'team_settings' : 'default_missing_row',
        streak_threshold:
          typeof row?.streak_threshold === 'number'
            ? row.streak_threshold
            : Number.isFinite(Number(row?.streak_threshold))
              ? Number(row?.streak_threshold)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.streak_threshold,
        xp_multiplier:
          typeof row?.xp_multiplier === 'number'
            ? row.xp_multiplier
            : Number.isFinite(Number(row?.xp_multiplier))
              ? Number(row?.xp_multiplier)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.xp_multiplier,
        comeback_bonus:
          typeof row?.comeback_bonus === 'number'
            ? row.comeback_bonus
            : Number.isFinite(Number(row?.comeback_bonus))
              ? Number(row?.comeback_bonus)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.comeback_bonus,
        xp_cap_daily:
          typeof row?.xp_cap_daily === 'number'
            ? row.xp_cap_daily
            : Number.isFinite(Number(row?.xp_cap_daily))
              ? Number(row?.xp_cap_daily)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.xp_cap_daily,
        voice_score_threshold:
          typeof row?.voice_score_threshold === 'number'
            ? row.voice_score_threshold
            : Number.isFinite(Number(row?.voice_score_threshold))
              ? Number(row?.voice_score_threshold)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.voice_score_threshold,
        weak_close_threshold:
          typeof row?.weak_close_threshold === 'number'
            ? row.weak_close_threshold
            : Number.isFinite(Number(row?.weak_close_threshold))
              ? Number(row?.weak_close_threshold)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.weak_close_threshold,
        filler_density_threshold:
          typeof row?.filler_density_threshold === 'number'
            ? row.filler_density_threshold
            : Number.isFinite(Number(row?.filler_density_threshold))
              ? Number(row?.filler_density_threshold)
              : DEFAULT_TEAM_SETTINGS_SNAPSHOT.filler_density_threshold,
        coaching_trigger_thresholds:
          row?.coaching_trigger_thresholds && typeof row.coaching_trigger_thresholds === 'object'
            ? row.coaching_trigger_thresholds
            : DEFAULT_TEAM_SETTINGS_SNAPSHOT.coaching_trigger_thresholds,
        updated_at: row?.updated_at ?? null,
        updated_by: row?.updated_by ?? null,
      };
    }

    const msg = String((r.error as any)?.message ?? '').toLowerCase();
    if (
      (msg.includes('relation') && msg.includes('does not exist')) ||
      (msg.includes('could not find the table') && msg.includes('team_settings')) ||
      (msg.includes('schema cache') && msg.includes('team_settings'))
    ) {
      return {
        org_id: orgId,
        source: 'default_missing_table',
        ...DEFAULT_TEAM_SETTINGS_SNAPSHOT,
      };
    }

    if (msg.includes('column') && msg.includes('does not exist')) {
      continue;
    }

    throw new Error((r.error as any)?.message ?? 'team_settings_snapshot_failed');
  }

  return {
    org_id: orgId,
    source: 'default_fallback',
    ...DEFAULT_TEAM_SETTINGS_SNAPSHOT,
  };
}


// ==== COMPANY PERSONA PROFILE HELPERS ====
type CompanyPersonaProfile = {
  company_name?: string | null;
  industry?: string | null;
  buyer_style?: string | null;
  industry_preset?: string | null;
  objection_patterns?: string[];
  competitor_names?: string[];
  common_pushbacks?: string[];
  persona_memory?: string[];
  emotional_tuning?: {
    pressure_level?: number;
    trust_decay?: number;
    objection_aggression?: number;
  } | null;
};

async function loadCompanyPersonaProfile(opts: {
  companyId?: string | null;
  officeId?: string | null;
}) {
  const emptyProfile: CompanyPersonaProfile = {
    company_name: null,
    industry: null,
    buyer_style: null,
    industry_preset: null,
    objection_patterns: [],
    competitor_names: [],
    common_pushbacks: [],
    persona_memory: [],
    emotional_tuning: null,
  };

  try {
    if (!opts.companyId) {
      return emptyProfile;
    }

    const { data, error } = await supa
      .from("companies")
      .select(`
        id,
        name,
        industry,
        settings
      `)
      .eq("id", opts.companyId)
      .single();

    if (error || !data) {
      return emptyProfile;
    }

    const settings =
      data.settings && typeof data.settings === "object"
        ? data.settings
        : {};

    return {
      company_name: data.name || null,
      industry: data.industry || null,
      buyer_style:
        typeof settings.buyer_style === "string"
          ? settings.buyer_style
          : null,
      industry_preset:
        typeof settings.industry_preset === "string"
          ? settings.industry_preset
          : null,
      emotional_tuning:
        settings.emotional_tuning &&
          typeof settings.emotional_tuning === "object"
          ? settings.emotional_tuning
          : {
            pressure_level: 50,
            trust_decay: 50,
            objection_aggression: 50,
          },
      objection_patterns: Array.isArray(settings.objection_patterns)
        ? settings.objection_patterns
        : [],
      competitor_names: Array.isArray(settings.competitor_names)
        ? settings.competitor_names
        : [],
      common_pushbacks: Array.isArray(settings.common_pushbacks)
        ? settings.common_pushbacks
        : [],
      persona_memory: Array.isArray(settings.persona_memory)
        ? settings.persona_memory
        : [],
    };
  } catch (e) {
    console.warn("[persona_profile_load_failed]", e);
    return emptyProfile;
  }
}


function computeAdaptiveDifficulty(opts: {
  requestedDifficulty?: string | null;
  emotionalTuning?: {
    pressure_level?: number;
    trust_decay?: number;
    objection_aggression?: number;
  } | null;
}) {
  const requested = String(opts.requestedDifficulty || "normal").toLowerCase();

  const pressure = Number(opts.emotionalTuning?.pressure_level ?? 50);
  const aggression = Number(
    opts.emotionalTuning?.objection_aggression ?? 50
  );

  let baseline = requested;

  if (
    requested === "normal" &&
    (pressure >= 75 || aggression >= 75)
  ) {
    baseline = "hard";
  }

  if (
    requested === "hard" &&
    pressure >= 90 &&
    aggression >= 90
  ) {
    baseline = "nightmare";
  }

  return {
    requested,
    effective: baseline,
    pressure,
    aggression,
  };
}


// ==== BEHAVIOURAL ANALYTICS TYPES + HELPERS ====
type ReplayComparison = {
  original_session_id: string | null;
  replay_attempt: number;
  original_score: number | null;
  replay_score: number | null;
  score_delta: number | null;
  trust_recovery_delta: number | null;
  anger_reduction_delta: number | null;
  objection_improvement_delta: number | null;
  replay_improvement_score: number | null;
  confidence_recovery_detected: boolean;
};

type BehaviourDiagnostics = {
  trustCollapseDetected: boolean;
  angerSpikeDetected: boolean;
  disengagedDetected: boolean;
  escalationScore: number;
  strongestEmotion: "anger" | "boredom" | "trust" | "balanced";
  recoveryDetected: boolean;
};

function buildBehaviourDiagnostics(opts: {
  emotionalTimeline?: any[];
  objectionHistory?: any[];
}) : BehaviourDiagnostics {
  const timeline = Array.isArray(opts.emotionalTimeline)
    ? opts.emotionalTimeline
    : [];

  const objections = Array.isArray(opts.objectionHistory)
    ? opts.objectionHistory
    : [];

  const latest = timeline[timeline.length - 1] || {};

  const anger = Number(latest.anger || 0);
  const boredom = Number(latest.boredom || 0);
  const trust = Number(latest.trust || 0);

  let strongestEmotion: BehaviourDiagnostics["strongestEmotion"] = "balanced";

  if (anger > boredom && anger > trust) {
    strongestEmotion = "anger";
  } else if (boredom > anger && boredom > trust) {
    strongestEmotion = "boredom";
  } else if (trust > anger && trust > boredom) {
    strongestEmotion = "trust";
  }

  const escalationScore = Math.min(
    100,
    Math.round(
      objections.length * 12 +
      anger * 0.35 +
      boredom * 0.2
    )
  );

  const recoveryDetected = timeline.some((x: any) => {
    const t = Number(x?.trust || 0);
    return t >= 60;
  });

  return {
    trustCollapseDetected: trust <= 25,
    angerSpikeDetected: anger >= 75,
    disengagedDetected: boredom >= 75,
    escalationScore,
    strongestEmotion,
    recoveryDetected,
  };
}

function buildCoachingInsights(opts: {
  diagnostics: BehaviourDiagnostics;
  objectionHistory?: any[];
  failedMoments?: any[];
}) {
  const insights: string[] = [];

  const diagnostics = opts.diagnostics;

  if (diagnostics.trustCollapseDetected) {
    insights.push(
      "Buyer trust collapsed during the conversation. Rep likely failed to anchor value or build confidence."
    );
  }

  if (diagnostics.angerSpikeDetected) {
    insights.push(
      "Buyer frustration escalated aggressively. Rep may have pushed too hard or failed to handle objections calmly."
    );
  }

  if (diagnostics.disengagedDetected) {
    insights.push(
      "Buyer disengagement detected. Rep likely lost conversational control or failed discovery."
    );
  }

  if (diagnostics.recoveryDetected) {
    insights.push(
      "Rep successfully recovered buyer trust at certain points in the conversation."
    );
  }

  const objections = Array.isArray(opts.objectionHistory)
    ? opts.objectionHistory
    : [];

  if (objections.length >= 3) {
    insights.push(
      "Multiple objections stacked during the call. Rep struggled under sustained pressure."
    );
  }

  const failed = Array.isArray(opts.failedMoments)
    ? opts.failedMoments
    : [];

  if (failed.length >= 2) {
    insights.push(
      "Repeated weak moments detected. Recommend replay drills focused on objection handling and confidence control."
    );
  }

  return insights;
}

function buildReplayComparison(opts: {
  currentSession: any;
  replayState?: any;
  emotionalTimeline?: any[];
  objectionHistory?: any[];
  failedMoments?: any[];
}) : ReplayComparison {
  const replay = opts.replayState || null;

  const latestEmotion = Array.isArray(opts.emotionalTimeline)
    ? opts.emotionalTimeline[opts.emotionalTimeline.length - 1] || {}
    : {};

  const trust = Number(latestEmotion?.trust || 0);
  const anger = Number(latestEmotion?.anger || 0);

  const replayScore = Number(opts.currentSession?.total_score || 0);

  const originalScore = Number(
    replay?.original_score ||
    replay?.source_score ||
    0
  );

  const scoreDelta =
    replayScore && originalScore
      ? replayScore - originalScore
      : null;

  const trustRecoveryDelta = trust - 25;
  const angerReductionDelta = 75 - anger;

  const objectionImprovementDelta = Math.max(
    0,
    100 - ((opts.objectionHistory || []).length * 12)
  );

  const replayImprovementScore = Math.max(
    0,
    Math.min(
      100,
      Math.round(
        (scoreDelta || 0) * 1.4 +
        trustRecoveryDelta * 0.45 +
        angerReductionDelta * 0.25 +
        objectionImprovementDelta * 0.2
      )
    )
  );

  return {
    original_session_id:
      replay?.source_session_id || null,

    replay_attempt:
      Number(replay?.replay_attempt || 0),

    original_score:
      originalScore || null,

    replay_score:
      replayScore || null,

    score_delta:
      scoreDelta,

    trust_recovery_delta:
      trustRecoveryDelta,

    anger_reduction_delta:
      angerReductionDelta,

    objection_improvement_delta:
      objectionImprovementDelta,

    replay_improvement_score:
      replayImprovementScore,

    confidence_recovery_detected:
      trust >= 60 && anger <= 40,
  };
}

const router = express.Router();

function buildDynamicScenario(opts: {
  assignment?: any;
  callContext?: any;
}) {
  if (!opts.assignment) return null;

  const meta = opts.assignment.meta || {};

  const section = meta.flag_section || "unknown";
  const score = meta.score_before ?? null;
  const callId = meta.call_id || null;
  const failures = meta.failure_count ?? 1;
  const difficulty = meta.difficulty || "normal";

  let scenario = `This is a REALISTIC sales training scenario based on a real failure.\n`;

  // 🔥 CORE BEHAVIOUR CONTROL
  if (section === "objection") {
    scenario += `
The rep FAILED handling objections.

YOU MUST:
- Challenge price aggressively
- Interrupt weak answers
- Push "cheaper competitor" angle
- Reject vague ROI claims
`;
  }

  if (section === "close") {
    scenario += `
The rep FAILED to close.

YOU MUST:
- Stall repeatedly
- Say "I'll think about it"
- Avoid commitment
- Only move forward if properly closed
`;
  }

  if (section === "discovery") {
    scenario += `
The rep FAILED discovery.

YOU MUST:
- Be vague
- Withhold key info
- Only open up if asked STRONG questions
`;
  }

  if (section === "pitch") {
    scenario += `
The rep FAILED to communicate value.

YOU MUST:
- Act confused
- Ask "why should I care?"
- Challenge relevance
`;
  }

  // 🔥 ESCALATION LOGIC (VERY IMPORTANT)
  if (failures >= 3) {
    scenario += `
This rep has failed this multiple times.

YOU MUST:
- Be more difficult than normal
- Lose patience faster
- Push harder objections
`;
  }

  if (difficulty === "hard" || difficulty === "nightmare") {
    scenario += `
Difficulty is HIGH.

YOU MUST:
- Resist most attempts
- Only respond to strong, confident selling
`;
  }

  if (score !== null) {
    scenario += `\nPrevious score: ${score}/100\n`;
  }

  if (callId) {
    scenario += `Derived from real call: ${callId}\n`;
  }

  return scenario.trim();
}

function buildPersonaSystemPrompt(opts: {
  personaId: string | null;
  mode?: string | null;
  difficulty?: string | null;
  dynamicScenario?: string | null;
  stackedObjection?: string | null;
  replayContext?: any;
  companyProfile?: CompanyPersonaProfile | null;
  buyerStyle?: string | null;
  emotionalState?: EmotionalState | null;
  failures?: number;
  section?: string | null;
}) {
  const mode = opts.mode || "standard";
  const personaId = opts.personaId || "price_sensitive";

  const pressureBlock = `
=== PRESSURE ENGINE ===
You are NOT passive.

You must actively test and break the rep.

If the rep:
- is vague → say "That doesn't really tell me anything"
- doesn't ask questions → disengage and respond shorter
- shows low confidence → challenge them harder
- avoids closing → stall and resist

Ask trap questions:
- "What does that actually mean for me?"
- "Why would I choose you over someone cheaper?"
- "Give me a real example"

If the rep struggles:
- interrupt more
- stack objections faster
- reduce trust quickly

Your goal is to expose weakness.
`;

  // Use shared persona config from ../personas
  const persona = getPersonaConfig(personaId);
  const difficulty =
    opts.difficulty ||
    (persona as any)?.difficultyDefault ||
    (persona as any)?.difficulty_default ||
    "normal";

  const personaLabel = (persona as any)?.label || "Generic Buyer";
  const traits = Array.isArray((persona as any)?.traits)
    ? (persona as any).traits
    : [];
  const desc = (persona as any)?.description || "";

  // Try to build a richer behaviour summary from the shared persona helper
  let behaviourSummary = "";
  try {
    behaviourSummary = buildPersonaBehaviourSummary(
      {
        personaId,
        difficulty: difficulty as DifficultyLevel,
        mode,
      } as any
    );
  } catch (e) {
    console.warn(
      "[sparring] buildPersonaBehaviourSummary failed, falling back to basic traits",
      e
    );
  }

  const lines: string[] = [];
  if (behaviourSummary && typeof behaviourSummary === "string") {
    lines.push(behaviourSummary);
  }
  if (traits.length) {
    lines.push(`Key traits: ${traits.join(", ")}.`);
  }
  if (desc) {
    lines.push(`Description: ${desc}`);
  }

  const modeLine =
    mode === "time_trial"
      ? "This is a *time trial* drill. Keep answers tight and push the rep to move the conversation forward quickly."
      : mode === "close_in_2m"
        ? "This is a *close in 2 minutes* drill. You are open to buying, but only if they create urgency and a clear next step fast."
        : "This is a standard sparring drill. Challenge the rep realistically, but do not be cartoonish.";

  const diffLine =
    difficulty === "nightmare"
      ? "Difficulty: NIGHTMARE. You are highly resistant. Push back often, raise objections, and make the rep really earn progress."
      : difficulty === "hard"
        ? "Difficulty: HARD. You are sceptical and demanding. Raise serious objections and require strong justification."
        : "Difficulty: NORMAL. You behave like a typical, slightly cautious buyer.";

  const companyProfile = opts.companyProfile || null;

  const buyerStyleBlock = opts.buyerStyle
    ? `
=== BUYER STYLE ===
Buyer style: ${opts.buyerStyle}

You MUST embody this buyer style consistently.
`
    : "";

  const companyBlock = companyProfile
    ? `
=== COMPANY CONTEXT ===
Company: ${companyProfile.company_name || "Unknown"}
Industry: ${companyProfile.industry || "General"}

Common objections:
${(companyProfile.objection_patterns || []).map((x) => `- ${x}`).join("\n")}

Competitors:
${(companyProfile.competitor_names || []).map((x) => `- ${x}`).join("\n")}

Typical pushbacks:
${(companyProfile.common_pushbacks || []).map((x) => `- ${x}`).join("\n")}

Persona memory:
${(companyProfile.persona_memory || []).map((x) => `- ${x}`).join("\n")}
`
    : "";

  const emotionalPressureBlock = opts.emotionalState
    ? `
=== LIVE EMOTIONAL STATE ===
Anger: ${opts.emotionalState.anger}/100
Boredom: ${opts.emotionalState.boredom}/100
Trust: ${opts.emotionalState.trust}/100

If anger is high:
- interrupt more
- challenge harder
- lose patience faster

If boredom is high:
- shorten responses
- disengage
- become dismissive

If trust is high:
- slowly open up
- provide more information
- soften resistance slightly
`
    : "";

  const replayBlock = opts.replayContext
    ? `
=== FAILURE REPLAY MODE ===
This is a replay coaching scenario.

The rep PREVIOUSLY FAILED this exact moment.

Original buyer objection:
"${opts.replayContext.buyer_message}"

Previous failed rep response:
"${opts.replayContext.previous_rep_response}"

Failure reason:
${opts.replayContext.failure_reason}

Original score:
${opts.replayContext.original_score}/100

YOU MUST:
- challenge weak answers harder than before
- punish repeated weak behaviour
- compare current answers against the failed attempt
- only reward genuine improvement
- pressure the rep more aggressively if they repeat mistakes
- test confidence harder than normal
- escalate objections faster if the rep stays vague
`
    : "";

  const scenarioBlock = opts.dynamicScenario
    ? `
=== REAL FAILURE CONTEXT ===
${opts.dynamicScenario}

You MUST fully embody this behaviour.
Do NOT soften or assist the rep.
`
    : "";

  const stackedBlock = opts.stackedObjection
    ? `
=== NEW OBJECTION (STACK PRESSURE) ===
You MUST introduce this naturally into the conversation:

"${opts.stackedObjection}"

Do NOT wait. Bring it up even if the rep is mid-flow.
Stack it with previous objections.
`
    : "";

  const adaptivePressureBlock = opts.failures && opts.failures >= 2
    ? `
=== ADAPTIVE PRESSURE ===
The rep has repeatedly struggled.

You MUST:
- escalate objections faster
- stack multiple objections together
- reduce trust aggressively
- punish generic sales language
- become more emotionally difficult
`
    : "";

  // 🔥 PERSONA MUTATION (CRITICAL)
  const mutation = getPersonaMutationState(personaId, difficulty);
  const mutationBlock = `
=== BUYER BEHAVIOUR PROFILE ===
Volatility: ${mutation.volatility}/100 (how fast mood shifts)
Resistance: ${mutation.resistance}/100 (how hard they push back)
Unpredictability: ${mutation.unpredictability}/100 (chance to switch tone mid-call)

You MUST reflect this in behaviour:
- High volatility → sudden emotional swings
- High resistance → reject weak answers aggressively
- High unpredictability → change tone without warning
`;

  const personaBlock = lines.join("\n");

  return `
You are role-playing as a sales prospect in a training drill.

${companyBlock}
${buyerStyleBlock}
${emotionalPressureBlock}
${replayBlock}
${scenarioBlock}
${stackedBlock}
${adaptivePressureBlock}
${mutationBlock}
${pressureBlock}

Persona: ${personaLabel} (${personaId}).
${personaBlock}

${diffLine}
${modeLine}

Rules:
- Stay strictly in character
- NEVER act like a training bot
- NEVER help the rep
- NEVER explain reasoning
- React emotionally and realistically
- Interrupt weak responses
- Challenge vague claims
- Push back on poor answers
- If the rep is bad → make the conversation harder
- If the rep is strong → slowly open up

This must feel like a REAL buyer, not a simulation.
`.trim();
}

// === Behavioural Analytics Endpoint ===
router.get('/sessions/:id/analytics', async (req: Request, res: Response) => {
  try {
    const sessionId = String(req.params.id || '').trim();

    if (!sessionId) {
      return res.status(400).json({
        ok: false,
        error: 'invalid_session_id',
      });
    }

    const { data: session, error } = await supa
      .from('sparring_sessions')
      .select(`
        id,
        rep_id,
        persona_id,
        difficulty,
        total_score,
        created_at,
        meta
      `)
      .eq('id', sessionId)
      .single();

    if (error || !session) {
      return res.status(404).json({
        ok: false,
        error: 'session_not_found',
      });
    }

    const meta =
      session.meta && typeof session.meta === 'object'
        ? session.meta
        : {};

    const emotionalTimeline = Array.isArray(meta.emotional_timeline)
      ? meta.emotional_timeline
      : [];

    const objectionHistory = Array.isArray(meta.objection_history)
      ? meta.objection_history
      : [];

    const failedMoments = Array.isArray(meta.failed_moments)
      ? meta.failed_moments
      : [];

    const diagnostics = buildBehaviourDiagnostics({
      emotionalTimeline,
      objectionHistory,
    });

    const coachingInsights = buildCoachingInsights({
      diagnostics,
      objectionHistory,
      failedMoments,
    });

    const replayComparison = buildReplayComparison({
      currentSession: session,
      replayState: meta.replay_state || null,
      emotionalTimeline,
      objectionHistory,
      failedMoments,
    });

    return res.json({
      ok: true,

      analytics: {
        session: {
          id: session.id,
          rep_id: session.rep_id,
          persona_id: session.persona_id,
          difficulty: session.difficulty,
          total_score: session.total_score,
          created_at: session.created_at,
        },

        diagnostics,

        emotional_timeline: emotionalTimeline,

        objection_history: objectionHistory,

        failed_moments: failedMoments,

        coaching_insights: coachingInsights,

        replay_comparison: replayComparison,

        coaching_progression: {
          replay_attempt:
            replayComparison.replay_attempt,

          confidence_recovery_detected:
            replayComparison.confidence_recovery_detected,

          improvement_detected:
            (replayComparison.score_delta || 0) > 0,

          trust_recovery_delta:
            replayComparison.trust_recovery_delta,

          replay_improvement_score:
            replayComparison.replay_improvement_score,
        },

        analytics_state:
          meta.analytics_state || {},

        replay_state:
          meta.replay_state || null,

        company_profile:
          meta.company_persona_profile || null,
      },
    });
  } catch (e: any) {
    console.error('[sparring.analytics] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'analytics_failed',
    });
  }
});

// List available sparring personas (global presets for now)
router.get("/personas", async (_req, res) => {
  return res.json({
    ok: true,
    personas: [
      {
        id: "price_sensitive",
        label: "Price Sensitive",
        traits: ["ROI-focused", "Budget restricted"],
        description:
          "Pushes back on price early and often. Fixated on ROI and alternatives.",
        difficulty_default: "normal",
      },
      {
        id: "angry",
        label: "Angry Buyer",
        traits: ["Short fuse", "Interrupts"],
        description:
          "Easily irritated, talks over you, demanding strong justification.",
        difficulty_default: "hard",
      },
      {
        id: "silent",
        label: "Ultra Silent Mode",
        traits: ["1–3 word answers", "Low engagement"],
        description:
          "Barely gives anything. You must carry the conversation and create momentum.",
        difficulty_default: "nightmare",
      },
      {
        id: "cfo",
        label: "The CFO",
        traits: ["Analytical", "Sceptical", "Risk averse"],
        description:
          "Wants numbers, risk minimisation, and clear upside justification.",
        difficulty_default: "hard",
      },
      {
        id: "procurement",
        label: "Procurement Wall",
        traits: ["Policy-driven", "Process-focused"],
        description:
          "Everything must go through a committee or existing vendor. Loves process.",
        difficulty_default: "normal",
      },
    ],
  });
});

// --------------------------------------------
// GET /v1/sparring/leaderboard/:personaId
// Returns win/loss stats across all users
// --------------------------------------------
router.get("/leaderboard/:personaId", async (req, res) => {
  try {
    const personaId = String(req.params.personaId || "").trim();

    if (!personaId) {
      return res.status(400).json({
        ok: false,
        error: "invalid_persona_id",
        message: "Persona id is required.",
      });
    }

    type Row = {
      total_score: number | null;
      difficulty: string | null;
      meta: {
        total?: number | null;
        difficulty?: string | null;
        [key: string]: any;
      } | null;
    };

    const { data, error } = await supa
      .from("sparring_sessions")
      .select("total_score,difficulty,meta")
      .eq("persona_id", personaId)
      .not("total_score", "is", null);

    if (error) {
      console.error("[sparring] leaderboard error", error);
      return res
        .status(400)
        .json({ ok: false, error: error.message || "leaderboard_query_failed" });
    }

    const rows: Row[] = Array.isArray(data) ? (data as Row[]) : [];

    let wins = 0;
    let losses = 0;
    let total = 0;

    type DiffKey = "easy" | "normal" | "hard" | "nightmare" | "unknown";
    const difficultyStats: Record<
      DiffKey,
      { wins: number; losses: number; total: number; winRate: number | null }
    > = {
      easy: { wins: 0, losses: 0, total: 0, winRate: null },
      normal: { wins: 0, losses: 0, total: 0, winRate: null },
      hard: { wins: 0, losses: 0, total: 0, winRate: null },
      nightmare: { wins: 0, losses: 0, total: 0, winRate: null },
      unknown: { wins: 0, losses: 0, total: 0, winRate: null },
    };

    const normaliseDifficulty = (raw: string | null | undefined): DiffKey => {
      if (!raw) return "unknown";
      const d = String(raw).toLowerCase();
      if (d === "easy") return "easy";
      if (d === "normal") return "normal";
      if (d === "hard") return "hard";
      if (d === "nightmare") return "nightmare";
      return "unknown";
    };

    for (const row of rows) {
      const score =
        typeof row.total_score === "number"
          ? row.total_score
          : typeof row.meta?.total === "number"
            ? row.meta.total
            : null;

      if (typeof score !== "number" || Number.isNaN(score)) continue;

      const diffKey: DiffKey = normaliseDifficulty(
        row.difficulty || row.meta?.difficulty || null
      );

      total += 1;
      difficultyStats[diffKey].total += 1;

      if (score >= 80) {
        wins += 1;
        difficultyStats[diffKey].wins += 1;
      } else {
        losses += 1;
        difficultyStats[diffKey].losses += 1;
      }
    }

    const winRate = total > 0 ? (wins / total) * 100 : null;

    (Object.keys(difficultyStats) as DiffKey[]).forEach((key) => {
      const bucket = difficultyStats[key];
      if (bucket.total > 0) {
        bucket.winRate = (bucket.wins / bucket.total) * 100;
      } else {
        bucket.winRate = null;
      }
    });

    return res.json({
      ok: true,
      personaId,
      wins,
      losses,
      total,
      winRate,
      difficulty: difficultyStats,
    });
  } catch (e: any) {
    console.error("[sparring] leaderboard unexpected error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "leaderboard_failed" });
  }
});

/**
 * POST /v1/sparring/log
 * Body: { repId?: string, xp?: number, meta?: any, personaId?: string }
 * - Records a sparring session
 * - Adds an XP event for the rep (if provided)
 * - Attempts to increment the rep's total XP
 *
 * This endpoint is tolerant:
 * - If a table is missing, it returns ok:true with a warning.
 * - Writes are attempted independently, errors collected in warnings[].
 */
router.post('/log', express.json(), async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  const { repId, xp, meta, personaId } = req.body ?? {};
  const orgId = (req.header('x-org-id') || (req.body && req.body.orgId) || '').toString().trim();

  const award = Number.isFinite(xp) ? Number(xp) : 25;
  const rep_id: string | null = typeof repId === 'string' && repId.length ? repId : null;
  const persona_id: string = (typeof personaId === 'string' && personaId.trim().length) ? personaId.trim() : 'unknown';

  const session_id = uuidv4();
  const warnings: string[] = [];
  const nowIso = new Date().toISOString();

  // Ensure rep exists to satisfy FK on sparring_sessions.rep_id
  if (rep_id) {
    try {
      const { error: repUpsertErr } = await supa
        .from('reps')
        .upsert([{ id: rep_id, name: 'Rep' }], { onConflict: 'id', ignoreDuplicates: false });
      if (repUpsertErr) {
        // Non-fatal; continue but warn
        // (If table doesn't exist yet, this will be collected in warnings)
        // @ts-ignore
        warnings?.push?.(`reps upsert: ${repUpsertErr.message}`);
      }
    } catch (e: any) {
      // @ts-ignore
      warnings?.push?.(`reps upsert threw: ${e?.message ?? String(e)}`);
    }
  }

  // 1) Insert sparring session (if table exists)
  try {
    const { error } = await supa
      .from('sparring_sessions')
      .insert({
        id: session_id,
        rep_id,
        persona_id,       // <-- persona persisted
        xp_awarded: award,
        meta: meta ?? null,
        created_at: nowIso,
      } as any);
    if (error) {
      // Table may not exist yet — keep tolerant
      warnings.push(`sparring_sessions insert: ${error.message}`);
    }
  } catch (e: any) {
    warnings.push(`sparring_sessions insert threw: ${e?.message ?? String(e)}`);
  }

  // 2) Insert XP event (if table exists) — ties to rep
  if (rep_id) {
    try {
      const { error } = await supa
        .from('xp_events')
        .insert({
          id: uuidv4(),
          rep_id,                    // match actual schema column
          source: 'sparring',
          delta: award,
          amount: award,
          session_id,
          created_at: nowIso,
        } as any);
      if (error) warnings.push(`xp_events insert: ${error.message}`);
    } catch (e: any) {
      warnings.push(`xp_events insert threw: ${e?.message ?? String(e)}`);
    }

    // 3) Increment reps.xp (if column exists)
    try {
      const { error } = await supa.rpc('increment_rep_xp', { p_rep_id: rep_id, p_delta: award });
      if (error) {
        warnings.push(`increment_rep_xp rpc: ${error.message}`);
      }
    } catch (e: any) {
      warnings.push(`increment_rep_xp threw: ${e?.message ?? String(e)}`);
    }
  } else {
    warnings.push('No repId provided — recorded session without pinning XP to a rep.');
  }

  // 4) Activity log — only if explicitly enabled AND we have an org id.
  // Toggle with env var SPAR_ACTIVITY_LOG=1 to avoid type check constraint until schema includes 'sparring_session'.
  if (orgId && process.env.SPAR_ACTIVITY_LOG === '1') {
    try {
      const { error } = await supa
        .from('activities')
        .insert({
          id: uuidv4(),
          org_id: orgId,
          type: 'sparring_session', // add this to activities_type_check in DB before enabling
          actor_user_id: rep_id,
          call_id: null,
          payload: {
            session_id,
            xp_awarded: award,
            source: 'sparring',
          },
          created_at: nowIso,
        } as any);

      if (error) warnings.push(`activities insert: ${error.message}`);
    } catch (e: any) {
      warnings.push(`activities insert threw: ${e?.message ?? String(e)}`);
    }
  }

  return res.status(200).json({
    ok: true,
    session_id,
    rep_id,
    persona_id,
    xp_awarded: award,
    warnings,
  });
});


// --- Score endpoint (safe heuristic scorer) ---
// Supports two modes:
// 1) Persist mode: { sessionId, total?, meta?, durationMs?, turns? }
// 2) Mock mode: { transcript, personaId } → returns ephemeral scoring only
// --- Score endpoint (safe heuristic scorer) ---
// Supports two modes:
// 1) Persist mode: { sessionId, total?, meta?, durationMs?, turns? }
// 2) Mock mode: { transcript, personaId } → returns ephemeral scoring only
router.post("/score", express.json(), async (req, res) => {
  res.setHeader("Cache-Control", "no-store");

  try {
    const body = (req.body || {}) as any;
    const { sessionId, total, meta, durationMs, turns } = body;

    // -----------------------------
    // MODE 1 — REAL SESSION SCORING
    // -----------------------------
    if (typeof sessionId === "string" && sessionId.trim().length) {
      ; (global as any).__last_completed_count = 0;
      ; (global as any).__last_xp_awarded_total = 0;
      const { data: row, error: selErr } = await supa
        .from("sparring_sessions")
        .select(
          "id, meta, difficulty, total_score, xp_awarded, duration_ms, turns, summary, flags"
        )
        .eq("id", sessionId)
        .single();

      if (selErr || !row) {
        return res.status(400).json({
          ok: false,
          error: `session select failed: ${selErr?.message || "not_found"}`,
        });
      }

      const difficulty = getDifficultyFromRow(row);
      const mode = getModeFromRow(row);

      let numericTotal: number;
      if (typeof total === "number" && Number.isFinite(total)) {
        numericTotal = clampScore(total);
      } else if (typeof (row as any).total_score === "number") {
        numericTotal = clampScore((row as any).total_score);
      } else if (typeof row.meta?.total === "number") {
        numericTotal = clampScore(row.meta.total);
      } else {
        let base = 70 + Math.floor(Math.random() * 11);
        if (difficulty === "easy") base += 5;
        if (difficulty === "hard") base -= 5;
        if (difficulty === "nightmare") base -= 10;
        numericTotal = clampScore(base);
      }

      const baseBeforeEmotion = numericTotal;

      const emotional =
        (row.meta?.emotional_state as EmotionalState | undefined) ?? null;

      if (emotional) {
        if (emotional.trust >= 70) numericTotal += 5;
        if (emotional.anger >= 70) numericTotal -= 10;
        if (emotional.boredom >= 70) numericTotal -= 8;
      }

      const ended = (row.meta as any)?.ended || false;
      const endReason = (row.meta as any)?.end_reason || null;

      if (ended && endReason) {
        if (endReason === "closed") numericTotal += 8;
        else if (endReason === "angry") numericTotal -= 20;
        else if (endReason === "bored") numericTotal -= 15;
        else if (endReason === "timeout") numericTotal -= 10;
      }

      numericTotal = clampScore(numericTotal);

      const outcome = computeOutcomeFromScore(numericTotal);
      const xpAwarded = computeXpForScore(numericTotal, difficulty, mode);

      // ---- apply streak multiplier + comeback bonus ----
      // IMPORTANT: multiplier is derived from CURRENT streak (not cached xp_multiplier)
      // so you can switch modes later without breaking awards.
      const prevMeta = (row.meta || {}) as any;

      // Fetch config ONCE for both streak threshold and global multiplier
      let cfgForAward: any = null;
      try {
        cfgForAward = await getScoringConfig();
      } catch { }

      const streakNow = Number.isFinite(Number(prevMeta?.streak)) ? Number(prevMeta.streak) : 0;
      const mult = streakMultiplier(
        streakNow,
        Number.isFinite(Number(cfgForAward?.streakThreshold)) ? cfgForAward.streakThreshold : 3
      );

      const bonus = Number.isFinite(Number(prevMeta?.xp_bonus_pending)) ? Number(prevMeta.xp_bonus_pending) : 0;

      const baseXp = xpAwarded;
      const multXp = Math.round(baseXp * mult);

      // Apply GLOBAL admin XP multiplier (from /v1/admin/config)
      let globalMult = 1;
      try {
        if (Number.isFinite(Number(cfgForAward?.xpMultiplier))) globalMult = Number(cfgForAward.xpMultiplier);
      } catch { }

      const multXpGlobal = Math.round(multXp * globalMult);

      let finalXpAwarded = multXpGlobal + bonus;
      finalXpAwarded = clamp(finalXpAwarded, 0, 200);

      // Clear bonus once consumed (only if we actually applied it)
      if (bonus > 0) {
        try {
          await supa
            .from("sparring_sessions")
            .update({
              meta: { ...prevMeta, xp_bonus_pending: 0 },
            } as any)
            .eq("id", sessionId);
        } catch {
          // non-fatal
        }
      }

      const existingMeta =
        row.meta && typeof row.meta === "object" ? { ...row.meta } : {};
      const alreadyCommitted = Boolean((existingMeta as any)?.xp_committed);
      const mergedMeta = {
        ...existingMeta,
        ...(meta || {}),
        total: numericTotal,
        base_total: baseBeforeEmotion,
        difficulty,
        mode,
        // XP breakdown (helps UI + debugging)
        xp_base: baseXp,
        xp_multiplier_used: mult,
        xp_global_multiplier_used: globalMult,
        xp_multiplied: multXpGlobal,
        xp_bonus_used: bonus,
        xp_awarded: finalXpAwarded,
        outcome,

        // XP commit guard: we only grant rep XP once per session
        xp_committed: alreadyCommitted,
        xp_committed_amount: (existingMeta as any)?.xp_committed_amount ?? null,
        xp_committed_at: (existingMeta as any)?.xp_committed_at ?? null,
      };

      let finalDuration =
        typeof durationMs === "number" ? durationMs : row.duration_ms ?? null;

      let finalTurns =
        typeof turns === "number" ? turns : row.turns ?? null;

      let summary = row.summary ?? null;
      let flags = row.flags ?? null;

      if (finalDuration == null || finalTurns == null || !summary) {
        try {
          const { data: turnRows } = await supa
            .from("sparring_turns")
            .select("created_at, role, text")
            .eq("session_id", sessionId)
            .order("created_at", { ascending: true });

          if (Array.isArray(turnRows) && turnRows.length > 0) {
            if (finalTurns == null) finalTurns = turnRows.length;

            if (finalDuration == null && turnRows.length > 1) {
              const first = new Date(turnRows[0].created_at).getTime();
              const last = new Date(
                turnRows[turnRows.length - 1].created_at
              ).getTime();
              if (Number.isFinite(first) && Number.isFinite(last)) {
                finalDuration = Math.max(last - first, 0);
              }
            }

            if (!summary) {
              summary = `Call scored ${numericTotal}% — outcome: ${outcome}.`;
              // Keep existing flags shape from DB; do not overwrite here to avoid type mismatches.
            }
          }
        } catch (e) {
          console.warn("[sparring.score] derive duration/summary failed", e);
        }
      }

      const { error: updErr } = await supa
        .from("sparring_sessions")
        .update({
          total_score: numericTotal,
          xp_awarded: finalXpAwarded,
          duration_ms: finalDuration,
          turns: finalTurns,
          meta: mergedMeta,
          summary,
          flags,
        })
        .eq("id", sessionId);

      if (updErr) {
        console.error("[sparring.score] update failed", updErr);
        return res.status(500).json({
          ok: false,
          error: `score update failed: ${updErr?.message || "unknown"}`,
        });
      }


      const { data: updatedRow, error: fetchErr } = await supa
        .from("sparring_sessions")
        .select(
          "id, rep_id, persona_id, difficulty, xp_awarded, total_score, created_at, duration_ms, turns, summary, flags, meta"
        )
        .eq("id", sessionId)
        .single();

      if (fetchErr || !updatedRow) {
        console.error("[sparring.score] post-update select failed", fetchErr);
        return res.status(500).json({
          ok: false,
          error: `score select failed: ${fetchErr?.message || "unknown"}`,
        });
      }


      // -----------------------------
      // Assignment auto-complete (SAFE + LOGGED)
      // -----------------------------
      try {
        const repIdForAssign = String((updatedRow as any).rep_id || "").trim();
        const personaIdForAssign = String((updatedRow as any).persona_id || "").trim();

        // Accept assignmentId if explicitly provided AND looks valid
        const rawAssignmentId =
          typeof body?.assignmentId === "string" ? body.assignmentId.trim() : "";
        const safeAssignmentId = isUuid(rawAssignmentId) ? rawAssignmentId : "";

        // Safe guardrails: only attempt if we have a real rep id
        if (repIdForAssign && repIdForAssign.length > 10) {
          console.info("[assignments:lifecycle]", {
            event: "auto_complete_attempt",
            via: "sparring",
            rep_id: repIdForAssign,
            assignment_id: safeAssignmentId || null,
            target_id: personaIdForAssign || null,
            session_id: sessionId,
          });

          const result = await completeAssignmentsForTarget({
            repId: repIdForAssign,
            assignmentId: safeAssignmentId || null,
            type: "sparring",
            targetId: personaIdForAssign || null,
            completedVia: "sparring",
            xpAwarded: finalXpAwarded,
          });

          const completedCount =
            typeof (result as any)?.completedCount === "number"
              ? (result as any).completedCount
              : 0;
          const xpAwardedTotal =
            typeof (result as any)?.xpAwardedTotal === "number"
              ? (result as any).xpAwardedTotal
              : 0;
          ; (global as any).__last_completed_count = completedCount;
          ; (global as any).__last_xp_awarded_total = xpAwardedTotal;

          if (completedCount && completedCount > 0) {
            console.info("[assignments:lifecycle]", {
              event: "auto_completed",
              via: "sparring",
              rep_id: repIdForAssign,
              assignment_id: safeAssignmentId || null,
              target_id: personaIdForAssign || null,
              session_id: sessionId,
              completed_count: completedCount,
              xp_awarded_total: xpAwardedTotal,
            });
          } else {
            console.info("[assignments:lifecycle]", {
              event: "auto_complete_noop",
              via: "sparring",
              rep_id: repIdForAssign,
              assignment_id: safeAssignmentId || null,
              target_id: personaIdForAssign || null,
              session_id: sessionId,
              xp_awarded_total: xpAwardedTotal,
            });
          }
        }
      } catch (e: any) {
        console.warn("[assignments:lifecycle]", {
          event: "auto_complete_failed",
          via: "sparring",
          error: e?.message || e,
          session_id: sessionId,
        });
      }

      // -----------------------------
      // XP GRANT SOURCE OF TRUTH
      // XP is awarded via assignment completion (rep_xp_events) in completeAssignmentsForTarget.
      // Do NOT also insert into xp_events or increment reps.xp here (prevents double-award).
      // -----------------------------

      // If assignment completion ran, these are set inside the auto-complete try-block.
      // Default to 0 for safety.
      const completed_count_safe =
        typeof (global as any).__last_completed_count === "number"
          ? (global as any).__last_completed_count
          : 0;
      const xp_awarded_total_safe =
        typeof (global as any).__last_xp_awarded_total === "number"
          ? (global as any).__last_xp_awarded_total
          : 0;

      return res.json({
        ok: true,
        session: updatedRow,
        total: numericTotal,
        xp_base: baseXp,
        xp_multiplier_used: mult,
        xp_global_multiplier_used: globalMult,
        xp_bonus_used: bonus,
        // Top-level xp_awarded reflects COMMITTED rep XP (assignment completion), not session scoring intent.
        xp_awarded: xp_awarded_total_safe,
        xp_awarded_total: xp_awarded_total_safe,
        completed_count: completed_count_safe,
        // Keep outcome + session XP available via session.xp_awarded
        outcome,
      });
    }

    // -----------------------------
    // MODE 2 — MOCK (transcript only)
    // -----------------------------
    const { transcript, personaId } = body as any;
    if (!transcript || typeof transcript !== "string") {
      return res.status(400).json({
        ok: false,
        error:
          "transcript required (or provide { sessionId } to persist score)",
      });
    }

    const base = 60 + Math.floor(Math.random() * 31);
    const totalMock = clampScore(base);
    const xpMock = computeXpForScore(totalMock, "normal", "standard");

    return res.json({
      ok: true,
      personaId,
      total: totalMock,
      xp_awarded: xpMock,
      mock: true,
    });
  } catch (e: any) {
    console.error("[sparring.score] exception", e);
    return res.status(500).json({
      ok: false,
      error: e?.message || "score_failed",
    });
  }
});

// ---------------------------
// POST /v1/sparring/sessions
// ---------------------------
// Create a new sparring session for the current rep
router.post('/sessions', express.json(), async (req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');

  try {
    // 1) Work out who the rep is
    let repId: string | null = null;
    try {
      // Preferred: Supabase user id via proxy
      repId = getUserIdHeader(req);
    } catch {
      const bodyRep = (req.body as any)?.repId;
      if (typeof bodyRep === 'string' && bodyRep.trim().length) {
        repId = bodyRep.trim();
      }
    }

    const DEV_REP_ID =
      process.env.DEV_REP_ID || '11111111-1111-1111-8111-111111111111';

    // If we still don't have a repId (e.g. local curl), fall back to dev rep
    const effectiveRepId = repId || DEV_REP_ID;

    const {
      personaId,
      difficulty,
      mode,
      targetDurationSec,
      assignmentId,
      assignment
    } = req.body as {
      personaId?: string;
      difficulty?: string;
      mode?: string;
      targetDurationSec?: number;
      assignmentId?: string;
      assignment?: any;
    };

    const orgId = getOrgIdFromRequest(req);
    const teamSettingsSnapshot = await loadTeamSettingsSnapshot(orgId);

    let requesterUser: any = null;

    try {
      const { data } = await supa
        .from("users")
        .select("company_id, office_id")
        .eq("id", effectiveRepId)
        .maybeSingle();

      requesterUser = data || null;
    } catch (e) {
      console.warn("[sparring] failed loading requester user", e);
    }

    const companyProfile = await loadCompanyPersonaProfile({
      companyId: requesterUser?.company_id || null,
      officeId: requesterUser?.office_id || null,
    });

    const adaptiveDifficulty = computeAdaptiveDifficulty({
      requestedDifficulty: difficulty || "normal",
      emotionalTuning: companyProfile.emotional_tuning || null,
    });

    const sessionId = uuidv4();

    let dynamicScenario: string | null = null;

    try {
      if (assignment) {
        dynamicScenario = buildDynamicScenario({
          assignment,
          callContext: assignment.meta?.call_context || null,
        });
      }
    } catch (e) {
      console.warn("[scenario_builder_failed]", e);
    }

    // 2) Make sure the rep exists to satisfy FK (reps.id)
    try {
      const { error: repUpsertErr } = await supa
        .from('reps')
        .upsert(
          [{ id: effectiveRepId, name: 'Rep' }],
          { onConflict: 'id', ignoreDuplicates: false }
        );
      if (repUpsertErr) {
        console.warn('[sparring/sessions POST] reps upsert warning', repUpsertErr);
        // non-fatal – session insert may still succeed if FK not enforced
      }
    } catch (e: any) {
      console.warn('[sparring/sessions POST] reps upsert threw', e?.message || e);
    }

    // 3) Insert the new session
    const payload: any = {
      id: sessionId,
      rep_id: effectiveRepId,
      persona_id: personaId || 'price_sensitive',
      difficulty: adaptiveDifficulty.effective || difficulty || 'normal',
      meta: {
        personaId: personaId || "price_sensitive",
        difficulty: adaptiveDifficulty.effective || difficulty || "normal",

        requested_difficulty: difficulty || "normal",
        adaptive_difficulty: adaptiveDifficulty,

        company_id: requesterUser?.company_id || null,
        office_id: requesterUser?.office_id || null,

        buyer_style:
          companyProfile.buyer_style ||
          companyProfile.industry_preset ||
          "default",

        company_persona_profile: {
          company_name: companyProfile.company_name || null,
          industry: companyProfile.industry || null,
          industry_preset: companyProfile.industry_preset || null,
          buyer_style: companyProfile.buyer_style || null,
        },

        emotional_tuning:
          companyProfile.emotional_tuning || {
            pressure_level: 50,
            trust_decay: 50,
            objection_aggression: 50,
          },

        objection_library: {
          objection_patterns:
            companyProfile.objection_patterns || [],
          competitor_names:
            companyProfile.competitor_names || [],
          common_pushbacks:
            companyProfile.common_pushbacks || [],
        },

        persona_memory:
          companyProfile.persona_memory || [],

        persona_pack_version: "v1",

        dynamic_scenario: dynamicScenario,
        assignment_id: assignmentId || null,
        // game mode + target duration for time-trial / turns-based drills
        mode: mode || "standard",
        targetDurationSec:
          typeof targetDurationSec === "number" && Number.isFinite(targetDurationSec)
            ? targetDurationSec
            : null,

        // --- XP / streak meta defaults (never null) ---
        streak: 0,
        best_streak: 0,
        xp_multiplier: 1,
        comeback_pending: false,
        xp_bonus_pending: 0,
        last_turn_score: 0,
        last_turn_score_raw: 0,

        // Initial emotional state for this drill
        emotional_state: getInitialEmotionalState(
          personaId || "price_sensitive",
          adaptiveDifficulty.effective || difficulty || "normal"
        ),

        // Day 56 — snapshot manager/team scoring config at session start
        team_settings_snapshot: teamSettingsSnapshot,

        // Failure Replay Engine
        failed_moments: [],
        replay_enabled: true,

        // TIER 2A Day 101 — initial conversation state (Sparring Brain)
        state: createInitialSparringState({
          personaId: personaId || "price_sensitive",
          difficulty: adaptiveDifficulty.effective || difficulty || "normal",
          flagSection: (assignment as any)?.meta?.flag_section || null,
        }),
      },
    };

    const { data, error } = await supa
      .from('sparring_sessions')
      .insert(payload)
      .select(
        'id, rep_id, persona_id, difficulty, xp_awarded, total_score, created_at, duration_ms, turns, summary, flags, meta'
      )
      .single();

    if (error || !data) {
      console.error('[sparring/sessions POST] insert error', error);
      throw error || new Error('insert_failed');
    }

    return res.json({ ok: true, session: data });
  } catch (err: any) {
    console.error('[sparring/sessions POST] unexpected error', err);
    return res
      .status(400)
      .json({ ok: false, error: err?.message || 'bad_request' });
  }
});

// -----------------------------------------
// POST /v1/sparring/sessions/:id/turns
// -----------------------------------------
// Append a user turn and generate an AI reply
import { buildAIContext } from "../lib/contextBuilder";
router.post(
  '/sessions/:id/turns',
  express.json(),
  async (req: Request, res: Response) => {
    res.setHeader('Cache-Control', 'no-store');

    try {
      const id = String(req.params.id);
      const { text } = (req.body ?? {}) as { text?: string };

      if (!text || typeof text !== 'string' || !text.trim()) {
        return res
          .status(400)
          .json({ ok: false, error: 'text_required' });
      }

      let repId: string | null = null;
      let orgIdHeader: string | null = null;
      try {
        repId = getUserIdHeader(req);
      } catch {
        const bodyRep = (req.body as any)?.repId;
        if (typeof bodyRep === 'string' && bodyRep.trim().length) {
          repId = bodyRep.trim();
        }
      }
      orgIdHeader =
        (req.headers['x-org-id'] as string | undefined)?.toString().trim() ||
        null;

      // 1) Load session and basic access check
      const { data: session, error: sessErr } = await supa
        .from('sparring_sessions')
        .select('id, rep_id, persona_id, difficulty, meta')
        .eq('id', id)
        .single();

      if (sessErr || !session) {
        console.error('[sparring/turns] session not found', sessErr);
        return res.status(404).json({ ok: false, error: 'not_found' });
      }

      if (
        repId &&
        session.rep_id &&
        session.rep_id !== repId
      ) {
        return res.status(403).json({ ok: false, error: 'forbidden' });
      }

      // Prevent further turns once a session has been marked as ended
      const endedMeta =
        (session as any)?.meta && typeof (session as any).meta === "object"
          ? (session as any).meta
          : null;
      if (endedMeta && (endedMeta as any).ended) {
        return res.status(409).json({
          ok: false,
          error: "session_ended",
          reason: (endedMeta as any).end_reason || null,
        });
      }

      // 2) Fetch existing turns to build conversation context
      const { data: existingTurns, error: turnsErr } = await supa
        .from('sparring_turns')
        .select('role, text')
        .eq('session_id', id)
        .order('created_at', { ascending: true });

      if (turnsErr) {
        console.error('[sparring/turns] load turns error', turnsErr);
        throw turnsErr;
      }

      const history = (existingTurns ?? []).map((t: any) => ({
        role: t.role === 'assistant' ? 'assistant' : 'user',
        content: t.text,
      })) as { role: 'user' | 'assistant'; content: string }[];

      // 3) Append the new user turn into the prompt
      history.push({ role: 'user', content: text });

      // Compute auto hang-up intent before calling OpenAI
      const personaId =
        (session as any).persona_id ||
        (session as any)?.meta?.personaId ||
        "price_sensitive";

      const difficultyVal =
        (session as any).difficulty ||
        (session as any)?.meta?.difficulty ||
        "normal";

      const modeVal =
        (session as any)?.meta?.mode || "standard";

      // Turns so far = full conversation history including this new user turn
      const turnsSoFar = history.length;

      // --- Emotional state tracking (load previous, then apply delta for this turn) ---
      const previousMeta =
        (session as any)?.meta && typeof (session as any).meta === "object"
          ? ((session as any).meta as Record<string, any>)
          : {};

      const prevStack: ObjectionStackState =
        previousMeta.objection_stack || { active: [], last_added_at_turn: 0 };

      const prevEmotion: EmotionalState =
        (previousMeta.emotional_state as EmotionalState) ||
        getInitialEmotionalState(personaId, difficultyVal);

      const updatedEmotion = applyEmotionalDelta(prevEmotion, {
        personaId,
        difficulty: difficultyVal,
        turnsSoFar,
        lastUserText: text,
        lastAiText: "", // we haven't generated AI yet; this is mainly user-driven
      });

      const emotionalTimeline = Array.isArray(
        previousMeta.emotional_timeline
      )
        ? [...previousMeta.emotional_timeline]
        : [];

      emotionalTimeline.push({
        at_turn: turnsSoFar,
        anger: updatedEmotion.anger,
        boredom: updatedEmotion.boredom,
        trust: updatedEmotion.trust,
        created_at: new Date().toISOString(),
      });

      const newObjection = getNextStackedObjection({
        personaId,
        difficulty: difficultyVal,
        emotional: updatedEmotion,
        turnsSoFar,
        failures: previousMeta.failure_count || 0,
        currentStack: prevStack.active
      });

      const objectionHistory = Array.isArray(
        previousMeta.objection_history
      )
        ? [...previousMeta.objection_history]
        : [];

      if (newObjection) {
        objectionHistory.push({
          objection: newObjection,
          at_turn: turnsSoFar,
          anger: updatedEmotion.anger,
          boredom: updatedEmotion.boredom,
          trust: updatedEmotion.trust,
          created_at: new Date().toISOString(),
        });
      }

      const hangupDecision = shouldAutoHangUp({
        personaId,
        difficulty: difficultyVal,
        mode: modeVal,
        turnsSoFar,
        emotionalState: updatedEmotion,
      });

      // TIER 2A Day 101 — Sparring Brain state update (pure, pre-reply).
      // Heuristic score of the rep turn alone feeds pressure/performance;
      // the buyer-aware micro-score below remains the persisted score.
      const brainMicro = scoreRepTurnHeuristic(text, "");
      const prevBrainState = coerceState(previousMeta.state, {
        personaId,
        difficulty: difficultyVal,
        flagSection: previousMeta.flag_section || null,
        initialTrust: prevEmotion?.trust ?? null,
      });
      const brainState: SparringState = updateSparringState(prevBrainState, {
        repText: text,
        turnScore: brainMicro?.turn_score ?? null,
        emotional: updatedEmotion,
        newObjectionText: newObjection || null,
        endedThisTurn: hangupDecision.endNow,
      });

      // 4) Decide whether the buyer should "hang up" before calling OpenAI
      let aiText = "";
      let endedThisTurn = false;
      let endReason: "bored" | "angry" | "timeout" | "closed" | null = null;

      if (hangupDecision.endNow) {
        endedThisTurn = true;
        endReason = hangupDecision.reason;

        // Basic canned lines depending on reason/persona
        switch (hangupDecision.reason) {
          case "angry":
            aiText =
              "You know what, let's just leave it there. I'm going to stick with what we already have. Goodbye.";
            break;
          case "bored":
            aiText =
              "Look, I’ve got to jump to another call. Let’s leave this here for now.";
            break;
          case "closed":
            aiText =
              "Alright, that sounds good. Send me the details and we can move ahead, but I have to run now.";
            break;
          case "timeout":
          default:
            aiText =
              "I’m out of time on my side, I need to drop off here.";
            break;
        }
      } else {
        try {
          // 🔥 CONTEXT INJECTION (Day 66)
          let aiContext: any = null;
          try {
            aiContext = await buildAIContext({
              supa,
              repId: repId || session.rep_id,
            });
          } catch (e) {
            console.warn("[context_builder_failed]", e);
          }

          const systemPrompt = buildPersonaSystemPrompt({
            personaId,
            difficulty: difficultyVal,
            mode: modeVal,
            dynamicScenario: (session.meta as any)?.dynamic_scenario || null,
            replayContext: previousMeta.replay_context || null,
            companyProfile: {
              ...(previousMeta.company_persona_profile || {}),
              objection_patterns:
                previousMeta.objection_library?.objection_patterns || [],
              competitor_names:
                previousMeta.objection_library?.competitor_names || [],
              common_pushbacks:
                previousMeta.objection_library?.common_pushbacks || [],
              persona_memory:
                previousMeta.persona_memory || [],
            },

            buyerStyle:
              previousMeta.buyer_style || null,
            emotionalState: updatedEmotion,
            failures: previousMeta.failure_count || 1,
            section: previousMeta.flag_section || null,
            stackedObjection: newObjection,
          });

          // 🔥 AUGMENT PROMPT WITH CONTEXT (CRITICAL)
          const enrichedSystemPrompt = `
${systemPrompt}

=== REP CONTEXT ===
${aiContext?.rep?.topWeaknesses?.map((w: any) => `- ${w.section} (${w.count})`).join("\n") || "None"}

=== COMPANY CONTEXT ===
Top weakness: ${aiContext?.company?.topWeakness || "unknown"}

INSTRUCTIONS:
- Focus on the rep’s weaknesses
- Apply pressure where they fail most
- Be harder in those areas
`;

          // TIER 2A Day 101 — provider router (SPARRING_PROVIDER env; default
          // "openai" preserves the previous inline behaviour exactly). State
          // directives are appended to the persona prompt; the router falls
          // back to the deterministic stub and never throws.
          const reply = await generateBuyerReply({
            systemPrompt: withStateDirectives(enrichedSystemPrompt, brainState),
            history,
            state: brainState,
            personaId,
            difficulty: difficultyVal,
          });
          aiText = reply.text;
        } catch (llmErr: any) {
          console.error("[sparring/turns] buyer reply error", llmErr);
          aiText =
            "I'm still not sure about this. The price feels high compared to what I'm getting.";
        }
      }

      // 5) Persist both turns
      const { data: insertedTurns, error: insertErr } = await supa
        .from('sparring_turns')
        .insert([
          {
            session_id: id,
            role: 'user',
            text,
          },
          {
            session_id: id,
            role: 'assistant',
            text: aiText,
          },
        ])
        .select('id, session_id, role, text, created_at');

      if (insertErr) {
        console.error('[sparring/turns] insert error', insertErr);
        throw insertErr;
      }

      // Day 102 fix: the micro-score block and the emotional/state block both
      // built their meta from the stale `session.meta` and wrote twice — the
      // second write dropped micro_scores/failed_moments. The micro block now
      // accumulates its additions here and the final block writes ONCE.
      const pendingMetaPatch: Record<string, any> = {};

      // Day 103 — structured turn score feeds back into repPerformance.
      let structuredScore: StructuredTurnScore | null = null;
      let finalBrainState: SparringState = brainState;

      // -----------------------------
      // Micro-score the REP turn (best-effort)
      // -----------------------------
      try {
        const repTurn = (insertedTurns ?? []).find((t: any) => t.role === "user");
        const buyerTurn = (insertedTurns ?? []).find((t: any) => t.role === "assistant");

        if (repTurn) {
          const micro = scoreRepTurnHeuristic(text, buyerTurn?.text || aiText);

          // We may not have sparring_turns.meta in DB (depending on schema).
          // Always return micro to the web immediately.
          (repTurn as any).micro = micro;

          // Best-effort: persist into turn meta if the column exists.
          try {
            await supa
              .from("sparring_turns")
              .update({
                meta: {
                  micro_score: micro.turn_score,
                  micro_breakdown: micro.micro_breakdown,
                  coach_note: micro.coach_note,
                  flags: micro.flags,
                }
              } as any)
              .eq("id", repTurn.id);
          } catch (e: any) {
            const msg = String(e?.message || "");
            if (!msg.toLowerCase().includes("column") || !msg.toLowerCase().includes("meta")) {
              console.warn("[sparring/turns] micro-score persist failed", msg);
            }
          }

          // Also append to session meta (this is our guaranteed storage)
          try {
            const currentMeta =
              (session as any)?.meta && typeof (session as any).meta === "object"
                ? ((session as any).meta as Record<string, any>)
                : {};

            const existing = Array.isArray((currentMeta as any).micro_scores)
              ? (currentMeta as any).micro_scores
              : [];

            const entry = {
              at: new Date().toISOString(),
              turn_id: repTurn.id,
              turn_score: micro.turn_score,
              breakdown: micro.micro_breakdown,
              coach_note: micro.coach_note,
              flags: micro.flags,
            };

            const failedMoment = buildFailedMoment({
              turnNumber: turnsSoFar,
              repText: text,
              buyerText: buyerTurn?.text || aiText,
              micro,
            });

            const existingFailedMoments = Array.isArray(
              (currentMeta as any).failed_moments
            )
              ? (currentMeta as any).failed_moments
              : [];

            // Day 102: accumulate instead of writing — the final merged write
            // below persists these together with emotional state + brain state.
            pendingMetaPatch.micro_scores = [...existing, entry].slice(-200);
            pendingMetaPatch.failed_moments = failedMoment
              ? [...existingFailedMoments, failedMoment].slice(-50)
              : existingFailedMoments;
          } catch (e: any) {
            // non-fatal
          }
        }
      } catch (e: any) {
        console.warn("[sparring/turns] micro-score persist failed", e?.message || e);
      }

      // -----------------------------
      // Day 103 — structured turn score (deterministic, no LLM)
      // -----------------------------
      try {
        structuredScore = scoreSparringTurn({
          repMessage: text,
          buyerMessage: aiText,
          currentState: brainState,
          previousState: prevBrainState,
          difficulty: difficultyVal,
          personaId,
        });

        // Feed the structured dimensions back into repPerformance
        finalBrainState = mergeTurnScoreIntoState(brainState, structuredScore);

        const repTurnId =
          (insertedTurns ?? []).find((t: any) => t.role === "user")?.id || null;
        const existingTurnScores = Array.isArray(previousMeta.turn_scores)
          ? previousMeta.turn_scores
          : [];
        pendingMetaPatch.turn_scores = [
          ...existingTurnScores,
          {
            turnId: repTurnId,
            repMessage: String(text).slice(0, 500),
            score: structuredScore,
            createdAt: new Date().toISOString(),
          },
        ].slice(-100);
      } catch (e: any) {
        console.warn("[sparring/turns] structured score failed", e?.message || e);
      }

      // Persist updated emotional state (and hang-up info if relevant) on the session meta (best-effort)
      // Day 102: this is now the SINGLE meta write for the turn — it merges the
      // micro-score additions (pendingMetaPatch) so nothing is dropped.
      try {
        const currentMeta =
          (session as any)?.meta && typeof (session as any).meta === "object"
            ? ((session as any).meta as Record<string, any>)
            : {};

        const updatedStack = newObjection
          ? {
            active: [...(prevStack?.active || []), newObjection],
            last_added_at_turn: turnsSoFar
          }
          : prevStack;

        const mergedMeta: Record<string, any> = {
          ...currentMeta,
          ...pendingMetaPatch,
          emotional_state: updatedEmotion,
          emotional_timeline: emotionalTimeline,

          objection_history: objectionHistory,

          analytics_state: {
            latest_anger: updatedEmotion.anger,
            latest_boredom: updatedEmotion.boredom,
            latest_trust: updatedEmotion.trust,

            objection_count: objectionHistory.length,

            trust_drop_detected:
              updatedEmotion.trust < 25,

            anger_spike_detected:
              updatedEmotion.anger > 75,

            disengaged:
              updatedEmotion.boredom > 75,
          },
          objection_stack: updatedStack,

          // TIER 2A Day 101 — latest Sparring Brain state
          // (Day 103: includes repPerformance updated from the structured score)
          state: finalBrainState,

        };

        if (endedThisTurn) {
          mergedMeta.ended = true;
          mergedMeta.end_reason = endReason || "timeout";
        }

        await supa
          .from("sparring_sessions")
          .update({ meta: mergedMeta })
          .eq("id", id);
      } catch (e: any) {
        console.warn(
          "[sparring/turns] failed to persist emotional_state / end flags",
          e?.message || e
        );
      }

      return res.json({
        ok: true,
        turns: insertedTurns ?? [],
        ai: aiText,
        // TIER 2A Day 101 — expose conversation state (additive)
        state: finalBrainState,
        // TIER 2A Day 103 — structured turn score (additive)
        turnScore: structuredScore,
      });
    } catch (err: any) {
      console.error(
        'POST /v1/sparring/sessions/:id/turns unexpected error',
        err
      );
      return res
        .status(400)
        .json({ ok: false, error: err?.message || 'bad_request' });
    }
  }
);

// TIER 2A Day 101 — alias: POST /v1/sparring/sessions/:id/messages
// Same behaviour as /turns (the Tier 2A canonical name). Express re-dispatch
// keeps a single handler without restructuring the existing registration.
router.post(
  '/sessions/:id/messages',
  express.json(),
  (req: Request, res: Response, next) => {
    req.url = `/sessions/${req.params.id}/turns`;
    (router as any).handle(req, res, next);
  }
);

// -----------------------------------------
// TIER 2A Day 104 — POST /v1/sparring/sessions/:id/complete
// -----------------------------------------
// Aggregates the structured turn scores into a coaching summary, persists it
// (summary column = human text, meta.session_summary = full structure), marks
// the session ended, and completes any linked sparring assignment via the
// same completeAssignmentsForTarget path the legacy /score route uses.
// Note: sparring_sessions has no status/completed_at columns — completion is
// recorded in meta (ended/end_reason/completed_at) per the Day 100 data plan.
router.post(
  "/sessions/:id/complete",
  express.json(),
  async (req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-store");

    try {
      const id = String(req.params.id || "").trim();
      if (!isUuid(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

      let repId: string | null = null;
      try {
        repId = getUserIdHeader(req);
      } catch {
        const bodyRep = (req.body as any)?.repId;
        if (typeof bodyRep === "string" && bodyRep.trim().length) repId = bodyRep.trim();
      }

      const { data: session, error: sessErr } = await supa
        .from("sparring_sessions")
        .select("id, rep_id, persona_id, difficulty, meta, summary, total_score")
        .eq("id", id)
        .single();

      if (sessErr || !session) {
        return res.status(404).json({ ok: false, error: "not_found" });
      }
      if (repId && session.rep_id && session.rep_id !== repId) {
        return res.status(403).json({ ok: false, error: "forbidden" });
      }

      const meta =
        (session as any)?.meta && typeof (session as any).meta === "object"
          ? ((session as any).meta as Record<string, any>)
          : {};

      const summary = buildSparringSessionSummary({
        turnScores: Array.isArray(meta.turn_scores) ? meta.turn_scores : [],
        failedMoments: Array.isArray(meta.failed_moments) ? meta.failed_moments : [],
      });

      const nowIso = new Date().toISOString();
      const mergedMeta: Record<string, any> = {
        ...meta,
        session_summary: summary,
        ended: true,
        end_reason: meta.end_reason || "completed",
        completed_at: meta.completed_at || nowIso,
      };

      const { error: updErr } = await supa
        .from("sparring_sessions")
        .update({
          summary: summary.summaryText,
          total_score: summary.turnCount > 0 ? summary.overall : (session as any).total_score ?? null,
          meta: mergedMeta,
        } as any)
        .eq("id", id);

      if (updErr) {
        console.error("[sparring/complete] persist failed", updErr);
        return res.status(500).json({ ok: false, error: updErr.message });
      }

      // Assignment auto-completion (same guarded path as the legacy /score route)
      let assignmentCompleted: boolean | null = null;
      try {
        const repIdForAssign = String(session.rep_id || "").trim();
        const rawAssignmentId = String(meta.assignment_id || "").trim();
        const safeAssignmentId = isUuid(rawAssignmentId) ? rawAssignmentId : "";

        if (repIdForAssign && repIdForAssign.length > 10) {
          const xpAwarded =
            summary.overall >= 80 ? 35 : summary.overall >= 60 ? 25 : 15;

          const result = await completeAssignmentsForTarget({
            repId: repIdForAssign,
            assignmentId: safeAssignmentId || null,
            type: "sparring",
            targetId: String(session.persona_id || "") || null,
            completedVia: "sparring",
            xpAwarded,
          } as any);

          const completedCount =
            typeof (result as any)?.completedCount === "number"
              ? (result as any).completedCount
              : 0;
          assignmentCompleted = completedCount > 0;
        }
      } catch (e: any) {
        console.warn("[sparring/complete] assignment completion failed", e?.message || e);
        assignmentCompleted = null;
      }

      return res.json({ ok: true, summary, assignmentCompleted });
    } catch (err: any) {
      console.error("POST /v1/sparring/sessions/:id/complete error", err);
      return res.status(400).json({ ok: false, error: err?.message || "bad_request" });
    }
  }
);

// -----------------------------------------
// POST /v1/sparring/sessions/:id/micro-score
// -----------------------------------------
// Computes + persists a micro-score for the latest rep turn (or explicit pair)
// Body: { lastUserText?: string, lastBuyerText?: string }
// - If text is not provided, we infer the latest (rep=user) + (buyer=assistant) pair from sparring_turns.
// - Persists micro fields into the rep turn's meta, and appends summary into session.meta.micro_scores[]
router.post(
  "/sessions/:id/micro-score",
  express.json(),
  async (req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-store");

    try {
      const sessionId = String(req.params.id || "").trim();
      if (!sessionId) {
        return res.status(400).json({ ok: false, error: "session_id_required" });
      }

      // Load session (for persona/difficulty defaults + to update meta)
      const { data: session, error: sessErr } = await supa
        .from("sparring_sessions")
        .select("id, persona_id, difficulty, meta")
        .eq("id", sessionId)
        .single();

      if (sessErr || !session) {
        return res.status(404).json({ ok: false, error: "not_found" });
      }

      // Allow explicit scoring (from web) OR infer from DB
      let lastUserText: string | null =
        typeof (req.body as any)?.lastUserText === "string"
          ? String((req.body as any).lastUserText)
          : null;

      let lastBuyerText: string | null =
        typeof (req.body as any)?.lastBuyerText === "string"
          ? String((req.body as any).lastBuyerText)
          : null;

      let repTurnId: string | null = null;

      if (!lastUserText || !lastBuyerText) {
        // Infer latest rep+buyer pair from DB
        const { data: turns, error: turnsErr } = await supa
          .from("sparring_turns")
          .select("id, role, text, created_at")
          .eq("session_id", sessionId)
          .order("created_at", { ascending: true });

        if (turnsErr) {
          return res.status(500).json({ ok: false, error: "load_turns_failed" });
        }

        const rows = Array.isArray(turns) ? turns : [];
        // Find last assistant (buyer) turn and the immediately preceding user (rep) turn
        let lastAssistantIdx = -1;
        for (let i = rows.length - 1; i >= 0; i--) {
          if (rows[i]?.role === "assistant") {
            lastAssistantIdx = i;
            break;
          }
        }

        if (lastAssistantIdx <= 0) {
          return res.json({ ok: true, micro: null, message: "Not enough turns to score yet." });
        }

        const buyerRow = rows[lastAssistantIdx];
        const repRow = rows[lastAssistantIdx - 1];

        if (!repRow || repRow.role !== "user" || !buyerRow?.text) {
          return res.json({ ok: true, micro: null, message: "No valid rep+buyer turn pair found." });
        }

        repTurnId = repRow.id;
        lastUserText = String(repRow.text || "");
        lastBuyerText = String(buyerRow.text || "");
      }

      const micro = scoreRepTurnHeuristic(lastUserText || "", lastBuyerText || "");

      // Update streak/comeback meta AND append micro_scores[] in ONE write
      // (Avoids the second update overwriting streak fields with stale `session.meta`)
      try {
        const prevMeta =
          session.meta && typeof session.meta === "object"
            ? { ...(session.meta as any) }
            : {};

        let cfgForMicro: any = null;
        try {
          cfgForMicro = await getScoringConfig();
        } catch { }

        const streakMeta = updateStreakMeta(prevMeta, micro.turn_score, {
          streakThreshold: cfgForMicro?.streakThreshold,
          comebackBonus: cfgForMicro?.comebackBonus,
        });

        const existing = Array.isArray((prevMeta as any).micro_scores)
          ? (prevMeta as any).micro_scores
          : [];

        const entry = {
          at: new Date().toISOString(),
          turn_score: micro.turn_score,
          breakdown: micro.micro_breakdown,
          coach_note: micro.coach_note,
          flags: micro.flags,
        };

        const mergedMeta = {
          ...prevMeta,
          ...streakMeta,
          micro_scores: [...existing, entry].slice(-100), // cap to last 100
        };

        await supa
          .from("sparring_sessions")
          .update({ meta: mergedMeta } as any)
          .eq("id", sessionId);
      } catch (e: any) {
        console.warn("[micro-score] session meta update failed", e?.message || e);
      }

      return res.json({
        ok: true,
        sessionId,
        micro,
      });
    } catch (err: any) {
      console.error("[micro-score] unexpected error", err);
      return res.status(500).json({ ok: false, error: err?.message || "micro_score_failed" });
    }
  }
);

// -----------------------------------------
// POST /v1/sparring/sessions/:id/replay
// -----------------------------------------
router.post(
  "/sessions/:id/replay",
  express.json(),
  async (req: Request, res: Response) => {
    try {
      const sessionId = String(req.params.id || "").trim();
      const failedTurn = Number(req.body?.failed_turn || 0);

      if (!sessionId) {
        return res.status(400).json({
          ok: false,
          error: "session_id_required",
        });
      }

      if (!Number.isFinite(failedTurn) || failedTurn <= 0) {
        return res.status(400).json({
          ok: false,
          error: "invalid_failed_turn",
        });
      }

      const { data: sourceSession, error: sourceError } = await supa
        .from("sparring_sessions")
        .select("*")
        .eq("id", sessionId)
        .single();

      if (sourceError || !sourceSession) {
        return res.status(404).json({
          ok: false,
          error: "source_session_not_found",
        });
      }

      const failedMoments = Array.isArray(
        (sourceSession.meta as any)?.failed_moments
      )
        ? (sourceSession.meta as any).failed_moments
        : [];

      const targetMoment = failedMoments.find(
        (m: any) => Number(m?.turn || 0) === failedTurn
      );

      if (!targetMoment) {
        return res.status(404).json({
          ok: false,
          error: "failed_moment_not_found",
        });
      }

      const replayAttempts = Number(
        (sourceSession.meta as any)?.replay_attempts || 0
      );

      const replayState = buildReplayState({
        sourceSessionId: sourceSession.id,
        failedMoment: targetMoment,
        existingAttempts: replayAttempts,
      });

      const newSessionId = uuidv4();

      const newMeta = {
        ...(sourceSession.meta || {}),

        replay_mode: true,
        replay_state: replayState,
        replay_attempts: replayState.replay_attempt,

        replay_context: {
          buyer_message: targetMoment.buyer_message,
          previous_rep_response: targetMoment.rep_response,
          failure_reason: targetMoment.reason,
          original_score: targetMoment.score,
        },

        failed_moments: [],
        micro_scores: [],
        ended: false,
        end_reason: null,
      };

      const { data: replaySession, error: replayError } = await supa
        .from("sparring_sessions")
        .insert({
          id: newSessionId,
          rep_id: sourceSession.rep_id,
          persona_id: sourceSession.persona_id,
          difficulty: sourceSession.difficulty,
          meta: newMeta,
        })
        .select("*")
        .single();

      if (replayError || !replaySession) {
        return res.status(500).json({
          ok: false,
          error: "replay_session_create_failed",
        });
      }

      return res.json({
        ok: true,
        replay_session: replaySession,
        replay_state: replayState,
      });
    } catch (e: any) {
      console.error("[sparring/replay-create] failed", e);

      return res.status(500).json({
        ok: false,
        error: e?.message || "replay_create_failed",
      });
    }
  }
);

// -----------------------------------------
// GET /v1/sparring/sessions/:id/replay
// -----------------------------------------
router.get(
  "/sessions/:id/replay",
  async (req: Request, res: Response) => {
    try {
      const sessionId = String(req.params.id || "").trim();

      if (!sessionId) {
        return res.status(400).json({
          ok: false,
          error: "session_id_required",
        });
      }

      const { data, error } = await supa
        .from("sparring_sessions")
        .select("id, meta")
        .eq("id", sessionId)
        .single();

      if (error || !data) {
        return res.status(404).json({
          ok: false,
          error: "session_not_found",
        });
      }

      const failedMoments = Array.isArray(
        (data.meta as any)?.failed_moments
      )
        ? (data.meta as any).failed_moments
        : [];

      return res.json({
        ok: true,
        session_id: sessionId,
        replay_enabled:
          (data.meta as any)?.replay_enabled === true,
        failed_moments: failedMoments,
      });
    } catch (e: any) {
      console.error("[sparring/replay] failed", e);

      return res.status(500).json({
        ok: false,
        error: e?.message || "replay_failed",
      });
    }
  }
);

// GET /v1/sparring/sessions?repId=<uuid>&limit=5
router.get("/sessions", async (req: Request, res: Response) => {
  res.setHeader("Cache-Control", "no-store");

  try {
    // Prefer explicit ?repId= (manager views),
    // otherwise fall back to x-user-id header if present.
    let repId: string | null = null;

    if (typeof req.query.repId === "string" && req.query.repId.trim().length) {
      repId = req.query.repId.trim();
    } else {
      try {
        repId = getUserIdHeader(req);
      } catch {
        // no header – allowed for now (e.g. future manager/global view)
        repId = null;
      }
    }

    const limitRaw =
      typeof req.query.limit === "string"
        ? parseInt(req.query.limit as string, 10)
        : NaN;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 && limitRaw <= 50
        ? limitRaw
        : 20;

    let query = supa
      .from("sparring_sessions")
      .select(
        "id, rep_id, persona_id, difficulty, total_score, xp_awarded, created_at, duration_ms, turns, summary, flags, meta"
      )
      .order("created_at", { ascending: false })
      .limit(limit);

    // TEMP: manager/global view — return ALL sessions regardless of rep
    // If you want to scope per rep again later, re-enable this:
    // if (repId) {
    //   query = query.eq("rep_id", repId);
    // }

    const { data, error } = await query;

    if (error) {
      console.error("[sparring/sessions] Supabase error", error);
      return res
        .status(500)
        .json({ ok: false, error: "Failed to load sparring sessions" });
    }

    const sessions = (data || []).map((row: any) => {
      const personaId =
        row.persona_id ??
        row.meta?.personaId ??
        "unknown";

      const difficulty =
        row.difficulty ||
        row.meta?.difficulty ||
        "normal";

      const total =
        typeof row.total_score === "number"
          ? row.total_score
          : typeof row.meta?.total === "number"
            ? row.meta.total
            : null;

      const xp =
        typeof row.xp_awarded === "number"
          ? row.xp_awarded
          : row.meta?.xp ?? 0;

      const durationMs =
        typeof row.duration_ms === "number"
          ? row.duration_ms
          : row.meta?.duration_ms ?? null;

      const turns =
        typeof row.turns === "number"
          ? row.turns
          : Array.isArray(row.meta?.transcript)
            ? row.meta.transcript.length
            : null;

      const summary = row.summary ?? row.meta?.summary ?? null;
      const flags = row.flags ?? row.meta?.flags ?? null;

      return {
        id: row.id,
        rep_id: row.rep_id,
        persona_id: personaId,
        difficulty,
        total,
        xp_awarded: xp,
        created_at: row.created_at,
        duration_ms: durationMs,
        turns,
        summary,
        flags,
        meta: row.meta ?? null,
      };
    });

    return res.json({
      ok: true,
      sessions,
    });

  } catch (err: any) {
    console.error("[sparring/sessions] Unexpected error", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Unexpected error loading sparring sessions",
    });
  }
});

// GET /v1/sparring/sessions/:id
// Returns a single sparring session with metadata + full turn history
router.get('/sessions/:id', async (req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');

  try {
    const { id } = req.params;
    if (!id) {
      return res.status(400).json({ ok: false, error: 'id is required' });
    }

    const { data, error } = await supa
      .from('sparring_sessions')
      .select(
        'id, rep_id, persona_id, total_score, xp_awarded, created_at, duration_ms, turns, summary, flags, meta'
      )
      .eq('id', id)
      .single();

    if (error) {
      // PGRST116: ".single() returned 0 rows" — session doesn't exist, not a server fault.
      if (error.code === 'PGRST116') {
        return res.status(404).json({ ok: false, error: 'session_not_found' });
      }
      console.error('[sparring/sessions/:id] Supabase error', error);
      return res
        .status(500)
        .json({ ok: false, error: 'Failed to load sparring session' });
    }

    if (!data) {
      return res
        .status(404)
        .json({ ok: false, error: 'session_not_found' });
    }

    const personaId =
      (data as any).persona_id ??
      (data as any)?.meta?.personaId ??
      'unknown';

    const total =
      typeof (data as any).total_score === 'number'
        ? (data as any).total_score
        : typeof (data as any)?.meta?.total === 'number'
          ? (data as any).meta.total
          : null;

    const xp =
      typeof (data as any).xp_awarded === 'number'
        ? (data as any).xp_awarded
        : (data as any)?.meta?.xp ?? 0;

    const durationMs =
      typeof (data as any).duration_ms === 'number'
        ? (data as any).duration_ms
        : (data as any)?.meta?.duration_ms ?? null;

    const turnsCount =
      typeof (data as any).turns === 'number'
        ? (data as any).turns
        : Array.isArray((data as any)?.meta?.transcript)
          ? (data as any).meta.transcript.length
          : null;

    const summary =
      (data as any).summary ?? (data as any)?.meta?.summary ?? null;

    const flags =
      (data as any).flags ?? (data as any)?.meta?.flags ?? null;

    const session = {
      id: (data as any).id,
      rep_id: (data as any).rep_id,
      persona_id: personaId,
      difficulty: (data as any).difficulty || (data as any)?.meta?.difficulty || "normal",
      total,
      // Day 105: expose the persisted column under its real name too (additive;
      // Day 104 writes total_score on completion but it was omitted here)
      total_score: total,
      xp_awarded: xp,
      created_at: (data as any).created_at,
      duration_ms: durationMs,
      turns: turnsCount,
      summary,
      flags,
      meta: (data as any).meta ?? null,
    };

    // Fetch full turn history
    const { data: turns, error: turnsErr } = await supa
      .from('sparring_turns')
      .select('id, session_id, role, text, created_at')
      .eq('session_id', id)
      .order('created_at', { ascending: true });

    if (turnsErr) {
      console.error('[sparring/sessions/:id] load turns error', turnsErr);
      return res.status(500).json({
        ok: false,
        error: 'Failed to load sparring turns',
      });
    }

    // Backwards compat: keep `turns` at top-level, but also attach the full turn list to `session.turns`.
    const fullTurns = (turns ?? []) as any[];

    return res.json({
      ok: true,
      session: {
        ...(session as any),
        // Full conversation history (array of turn rows)
        turns: fullTurns,
      },
      turns: fullTurns,
    });
  } catch (err: any) {
    console.error('[sparring/sessions/:id] Unexpected error', err);
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Unexpected error loading sparring session',
    });
  }
});

router.post("/analyse-turn", async (req, res) => {
  const { text } = req.body || {};
  if (!text) return res.json({ ok: false, error: "text_required" });

  const prompt = `
Classify this buyer message into:
- sentiment: positive | neutral | negative
- intent: curious | buying | resisting | frustrated
- category: price | timing | authority | confusion | none
Text: "${text}"
Return JSON ONLY.
`;

  const r = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
    max_tokens: 80
  });

  return res.json({ ok: true, label: r.choices[0].message.content });
});

export default router;
