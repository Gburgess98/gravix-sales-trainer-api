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

function containsAny(text: string, needles: string[]) {
  const t = String(text || "").toLowerCase();
  return needles.some((n) => t.includes(n));
}

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

  // Generic "salesy" language → small trust drop + anger bump
  if (/\b(deal|sign up|discount|offer|promotion)\b/i.test(lower)) {
    next.trust = clampEmotion(next.trust - 2);
    next.anger = clampEmotion(next.anger + 2);
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

  // Emotion-driven early exits
  // High anger → rage hang-up
  if (anger >= 85 && t >= 6) {
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

function getUserIdHeader(req: Request): string {
  const raw = (req.headers['x-user-id'] as string | undefined)?.toString().trim();
  if (!raw) {
    throw new Error('missing x-user-id header');
  }
  return raw;
}

const router = express.Router();

function buildPersonaSystemPrompt(opts: {
  personaId: string | null;
  mode?: string | null;
  difficulty?: string | null;
}) {
  const mode = opts.mode || "standard";
  const personaId = opts.personaId || "price_sensitive";

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

  const personaBlock = lines.join("\n");

  return `
You are role-playing as a sales prospect in a training drill.

Persona: ${personaLabel} (${personaId}).
${personaBlock}

${diffLine}
${modeLine}

Rules:
- Stay strictly in character.
- Keep replies concise, like a real buyer on a call.
- React realistically to the rep's last message.
- Ask follow-up questions and raise objections based on your persona.
- Never reveal that you are an AI or that this is a drill.
`.trim();
}

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
      } catch {}

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
      } catch {}

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

      // --- Auto-complete sparring assignment (Day 15 wiring) ---
      // Priority:
      // 1) If the web passes `assignmentId`, complete THAT specific assignment.
      // 2) Otherwise, complete the latest assigned sparring assignment for this rep.
      //    Prefer matching `target_id` to persona_id when present.
      try {
        const repIdForAssign = String((updatedRow as any).rep_id || "").trim();
        const personaIdForAssign = String((updatedRow as any).persona_id || "").trim();
        const assignmentId = typeof body?.assignmentId === "string" ? body.assignmentId.trim() : "";

        if (repIdForAssign) {
          // 1) Exact assignment completion (strongest signal)
          if (assignmentId) {
            const { error: updAssignErr } = await supa
              .from("assignments")
              .update({
                status: "completed",
                completed_at: new Date().toISOString(),
                completed_by: "system",
              } as any)
              .eq("id", assignmentId)
              .eq("rep_id", repIdForAssign)
              .eq("status", "assigned");

            if (updAssignErr) {
              console.warn(
                "[sparring.score] complete exact assignment failed",
                updAssignErr.message
              );
            }
          } else {
            // 2) Fallback: latest assigned sparring assignment
            // Prefer target match when target_id exists.
            let q = supa
              .from("assignments")
              .select("id,target_id")
              .eq("rep_id", repIdForAssign)
              .eq("type", "sparring")
              .eq("status", "assigned")
              .order("created_at", { ascending: false })
              .limit(5);

            const { data: candidates, error: candErr } = await q;
            if (candErr) {
              console.warn(
                "[sparring.score] load sparring assignment candidates failed",
                candErr.message
              );
            } else {
              const rows = Array.isArray(candidates) ? candidates : [];
              const picked =
                (personaIdForAssign
                  ? rows.find((r: any) => String(r?.target_id || "").trim() === personaIdForAssign)
                  : null) ||
                rows[0] ||
                null;

              const pickedId = picked?.id ? String(picked.id) : "";
              if (pickedId) {
                const { error: updAssignErr } = await supa
                  .from("assignments")
                  .update({
                    status: "completed",
                    completed_at: new Date().toISOString(),
                    completed_by: "system",
                  } as any)
                  .eq("id", pickedId);

                if (updAssignErr) {
                  console.warn(
                    "[sparring.score] complete sparring assignment failed",
                    updAssignErr.message
                  );
                }
              }
            }
          }
        }
      } catch (e: any) {
        console.warn(
          "[sparring.score] assignment auto-complete failed",
          e?.message || e
        );
      }

      return res.json({
        ok: true,
        session: updatedRow,
        total: numericTotal,
        xp_base: baseXp,
        xp_multiplier_used: mult,
        xp_global_multiplier_used: globalMult,
        xp_bonus_used: bonus,
        xp_awarded: finalXpAwarded,
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

    const { personaId, difficulty, mode, targetDurationSec } = req.body as {
      personaId?: string;
      difficulty?: string;
      mode?: string;
      targetDurationSec?: number;
    };

    const sessionId = uuidv4();

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
      difficulty: difficulty || 'normal',
      meta: {
        personaId: personaId || "price_sensitive",
        difficulty: difficulty || "normal",
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
          difficulty || "normal"
        ),
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

      const hangupDecision = shouldAutoHangUp({
        personaId,
        difficulty: difficultyVal,
        mode: modeVal,
        turnsSoFar,
        emotionalState: updatedEmotion,
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
          const systemPrompt = buildPersonaSystemPrompt({
            personaId,
            difficulty: difficultyVal,
            mode: modeVal,
          });

          const completion = await openai.chat.completions.create({
            model: process.env.OPENAI_SPARRING_MODEL || "gpt-4o-mini",
            messages: [
              {
                role: "system",
                content: systemPrompt,
              },
              ...history,
            ],
            temperature: 0.7,
            max_tokens: 220,
          });

          aiText =
            completion.choices[0]?.message?.content?.trim() ||
            "I'm not convinced yet. Can you explain why this is worth the price?";
        } catch (llmErr: any) {
          console.error("[sparring/turns] OpenAI error", llmErr);
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

            const mergedMeta: Record<string, any> = {
              ...currentMeta,
              micro_scores: [...existing, entry].slice(-200),
            };

            // Preserve emotional_state / end flags which are also being written later
            await supa
              .from("sparring_sessions")
              .update({ meta: mergedMeta } as any)
              .eq("id", id);
          } catch (e: any) {
            // non-fatal
          }
        }
      } catch (e: any) {
        console.warn("[sparring/turns] micro-score persist failed", e?.message || e);
      }

      // Persist updated emotional state (and hang-up info if relevant) on the session meta (best-effort)
      try {
        const currentMeta =
          (session as any)?.meta && typeof (session as any).meta === "object"
            ? ((session as any).meta as Record<string, any>)
            : {};

        const mergedMeta: Record<string, any> = {
          ...currentMeta,
          emotional_state: updatedEmotion,
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
        } catch {}

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
      console.error('[sparring/sessions/:id] Supabase error', error);
      return res
        .status(500)
        .json({ ok: false, error: 'Failed to load sparring session' });
    }

    if (!data) {
      return res
        .status(404)
        .json({ ok: false, error: 'Sparring session not found' });
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
