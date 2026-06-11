// src/sparring/state.ts
// TIER 2A — Day 101: pure conversation state manager for the Sparring Brain.
//
// Deterministic, no DB/LLM dependencies. The LLM never owns this state — it
// receives a summary of it as a prompt directive and returns only the buyer's
// words. State persists in sparring_sessions.meta.state (no migration needed).

export type SparringStage =
  | "opening"
  | "discovery"
  | "pitch"
  | "objection"
  | "close"
  | "ended";

export type BuyerMood = "neutral" | "curious" | "sceptical" | "frustrated" | "warm";

export type ObjectionType =
  | "price"
  | "timing"
  | "authority"
  | "trust"
  | "competitor"
  | "unknown";

export type SparringDifficulty = "easy" | "standard" | "hard" | "nightmare";

export type NextBuyerMove =
  | "ask_question"
  | "raise_objection"
  | "request_info"
  | "soften"
  | "push_back"
  | "close_window";

export type SparringState = {
  stage: SparringStage;
  buyerMood: BuyerMood;
  trustLevel: number; // 0–100
  pressureLevel: number; // 0–100
  objectionState: {
    active: boolean;
    type: ObjectionType | null;
    resolved: boolean;
  };
  repPerformance: {
    clarity: number;
    confidence: number;
    objectionHandling: number;
    progression: number;
  };
  difficulty: SparringDifficulty;
  nextBuyerMove: NextBuyerMove;
};

// Legacy difficulty values ("normal") map onto the Tier 2A scale.
export function normaliseDifficulty(raw?: string | null): SparringDifficulty {
  const d = String(raw || "").toLowerCase();
  if (d === "easy") return "easy";
  if (d === "hard") return "hard";
  if (d === "nightmare") return "nightmare";
  return "standard"; // includes legacy "normal"
}

const clamp = (n: number, lo = 0, hi = 100) =>
  Math.max(lo, Math.min(hi, Math.round(Number.isFinite(n) ? n : lo)));

export function clampState(state: SparringState): SparringState {
  return {
    ...state,
    trustLevel: clamp(state.trustLevel),
    pressureLevel: clamp(state.pressureLevel),
    repPerformance: {
      clarity: clamp(state.repPerformance.clarity),
      confidence: clamp(state.repPerformance.confidence),
      objectionHandling: clamp(state.repPerformance.objectionHandling),
      progression: clamp(state.repPerformance.progression),
    },
  };
}

const BASE_PRESSURE: Record<SparringDifficulty, number> = {
  easy: 20,
  standard: 35,
  hard: 55,
  nightmare: 70,
};

// How strongly pressure reacts to weak/strong rep turns per difficulty.
const PRESSURE_SLOPE: Record<SparringDifficulty, number> = {
  easy: 0.6,
  standard: 1,
  hard: 1.4,
  nightmare: 1.8,
};

export type InitialStateInput = {
  personaId?: string | null;
  difficulty?: string | null;
  // Assignment context: weakest section seeds the objection focus
  flagSection?: string | null;
  // Existing emotional state (anger/boredom/trust) if available at creation
  initialTrust?: number | null;
};

export function createInitialSparringState(input: InitialStateInput): SparringState {
  const difficulty = normaliseDifficulty(input.difficulty);
  const personaId = String(input.personaId || "");

  const scepticalPersona =
    personaId.includes("sceptic") ||
    personaId.includes("price") ||
    personaId.includes("competitor");

  const seededObjection = sectionToObjectionType(input.flagSection);

  return clampState({
    stage: "opening",
    buyerMood: scepticalPersona ? "sceptical" : "neutral",
    trustLevel: typeof input.initialTrust === "number" ? input.initialTrust : 40,
    pressureLevel: BASE_PRESSURE[difficulty],
    objectionState: {
      active: false,
      type: seededObjection,
      resolved: false,
    },
    repPerformance: {
      clarity: 50,
      confidence: 50,
      objectionHandling: 50,
      progression: 50,
    },
    difficulty,
    nextBuyerMove: "ask_question",
  });
}

// ── Text inference helpers (deterministic keyword rules) ────────────────────

const STAGE_ORDER: SparringStage[] = [
  "opening",
  "discovery",
  "pitch",
  "objection",
  "close",
  "ended",
];

function stageIndex(s: SparringStage) {
  return STAGE_ORDER.indexOf(s);
}

const CLOSE_SIGNALS = [
  "sign", "contract", "move forward", "get started", "next steps",
  "onboard", "agreement", "deal", "send over the", "paperwork", "trial",
];
const PITCH_SIGNALS = [
  "our product", "our platform", "we offer", "what we do", "feature",
  "solution", "it works by", "benefit", "saves you", "helps you",
];
const DISCOVERY_SIGNALS = [
  "tell me about", "how do you currently", "what are you using",
  "your process", "your team", "how many", "what's your", "challenge",
  "pain point", "walk me through",
];

export function inferStageFromText(
  text: string,
  previousStage: SparringStage
): SparringStage {
  if (previousStage === "ended") return "ended";

  const t = String(text || "").toLowerCase();
  const has = (needles: string[]) => needles.some((n) => t.includes(n));

  // Candidate stage from the rep's words
  let candidate: SparringStage | null = null;
  if (has(CLOSE_SIGNALS)) candidate = "close";
  else if (has(PITCH_SIGNALS)) candidate = "pitch";
  else if (has(DISCOVERY_SIGNALS)) candidate = "discovery";

  if (!candidate) return previousStage === "opening" ? "discovery" : previousStage;

  // Stages only advance (objection interjections are handled via objectionState,
  // not by regressing the stage).
  return stageIndex(candidate) > stageIndex(previousStage) ? candidate : previousStage;
}

const OBJECTION_KEYWORDS: Array<{ type: ObjectionType; needles: string[] }> = [
  { type: "price", needles: ["price", "cost", "expensive", "budget", "discount", "cheaper", "afford"] },
  { type: "timing", needles: ["not the right time", "next quarter", "next year", "too busy", "later", "timing"] },
  { type: "authority", needles: ["my boss", "decision maker", "sign off", "approval", "not my call", "committee"] },
  { type: "trust", needles: ["don't trust", "never heard of", "proof", "guarantee", "case study", "references", "risk"] },
  { type: "competitor", needles: ["competitor", "already use", "current provider", "switching", "alternative"] },
];

export function inferObjectionType(text: string): ObjectionType | null {
  const t = String(text || "").toLowerCase();
  for (const { type, needles } of OBJECTION_KEYWORDS) {
    if (needles.some((n) => t.includes(n))) return type;
  }
  return null;
}

function sectionToObjectionType(section?: string | null): ObjectionType | null {
  const s = String(section || "").toLowerCase();
  if (s === "objection") return "unknown";
  if (s === "close") return "timing";
  if (s === "discovery") return null;
  return null;
}

// ── Turn update ──────────────────────────────────────────────────────────────

export type TurnInput = {
  repText: string;
  // Heuristic micro-score for the rep turn (existing scoreRepTurnHeuristic)
  turnScore?: number | null;
  // Existing emotional state after this turn (anger/boredom/trust 0–100)
  emotional?: { anger: number; boredom: number; trust: number } | null;
  // New stacked objection raised this turn (existing getNextStackedObjection)
  newObjectionText?: string | null;
  // Buyer hang-up decided this turn
  endedThisTurn?: boolean;
};

// Exponential moving average — recent turns weigh more.
const EMA_ALPHA = 0.4;
const ema = (prev: number, next: number) =>
  clamp(prev + EMA_ALPHA * (next - prev));

export function updateSparringState(
  previous: SparringState,
  turn: TurnInput
): SparringState {
  const prev = clampState(previous);
  const score = typeof turn.turnScore === "number" ? clamp(turn.turnScore) : null;
  const emotional = turn.emotional || null;

  // Stage
  let stage = inferStageFromText(turn.repText, prev.stage);
  if (turn.endedThisTurn) stage = "ended";

  // Trust follows the existing emotional engine when available
  const trustLevel = emotional ? clamp(emotional.trust) : prev.trustLevel;

  // Pressure: weak turns push it up, strong turns relieve it; slope by difficulty
  let pressureLevel = prev.pressureLevel;
  if (score !== null) {
    const slope = PRESSURE_SLOPE[prev.difficulty];
    if (score < 55) pressureLevel += Math.round(8 * slope);
    else if (score >= 70) pressureLevel -= Math.round(6 * slope);
  }
  pressureLevel = clamp(pressureLevel);

  // Objection lifecycle
  let objectionState = { ...prev.objectionState };
  if (turn.newObjectionText) {
    objectionState = {
      active: true,
      type: inferObjectionType(turn.newObjectionText) || "unknown",
      resolved: false,
    };
  } else if (objectionState.active && score !== null && score >= 70) {
    objectionState = { ...objectionState, active: false, resolved: true };
  }

  // Rep performance dimensions (EMA over turn signals).
  // Day 101 derives dimensions from the single heuristic turn score plus
  // simple text signals; the structured scoring tier replaces this later.
  const words = String(turn.repText || "").trim().split(/\s+/).filter(Boolean);
  const vague = words.length < 6;
  const hedging = /\b(maybe|i think|probably|not sure|i guess|sort of|kind of)\b/i.test(
    turn.repText || ""
  );

  const base = score ?? 50;
  const repPerformance = {
    clarity: ema(prev.repPerformance.clarity, vague ? Math.min(base, 40) : base),
    confidence: ema(prev.repPerformance.confidence, hedging ? Math.min(base, 45) : base),
    objectionHandling: ema(
      prev.repPerformance.objectionHandling,
      prev.objectionState.active ? base : prev.repPerformance.objectionHandling
    ),
    progression: ema(
      prev.repPerformance.progression,
      stage !== prev.stage && stage !== "ended" ? Math.max(base, 60) : base
    ),
  };

  // Buyer mood from emotional engine + trust
  let buyerMood: BuyerMood = prev.buyerMood;
  if (emotional) {
    if (emotional.anger > 65) buyerMood = "frustrated";
    else if (emotional.trust > 65) buyerMood = "warm";
    else if (emotional.boredom > 60 || emotional.trust < 30) buyerMood = "sceptical";
    else if (emotional.trust >= 45 && (score ?? 0) >= 60) buyerMood = "curious";
    else buyerMood = "neutral";
  }

  // Next buyer move (deterministic rule ladder)
  let nextBuyerMove: NextBuyerMove = "ask_question";
  if (stage === "ended") nextBuyerMove = "close_window";
  else if (emotional && emotional.boredom > 75) nextBuyerMove = "close_window";
  else if (objectionState.active && !objectionState.resolved) nextBuyerMove = "push_back";
  else if (pressureLevel > 70) nextBuyerMove = "raise_objection";
  else if (score !== null && score >= 70 && trustLevel >= 55) nextBuyerMove = "soften";
  else if (stage === "pitch" || stage === "close") nextBuyerMove = "request_info";
  else nextBuyerMove = "ask_question";

  return clampState({
    stage,
    buyerMood,
    trustLevel,
    pressureLevel,
    objectionState,
    repPerformance,
    difficulty: prev.difficulty,
    nextBuyerMove,
  });
}

// ── Prompt summary ───────────────────────────────────────────────────────────

export function summariseStateForPrompt(state: SparringState): string {
  const s = clampState(state);
  const objection = s.objectionState.active
    ? `an unresolved ${s.objectionState.type || "unknown"} objection is on the table — do not drop it until properly handled`
    : s.objectionState.resolved
      ? "your last objection was handled adequately"
      : "no objection is currently active";

  return [
    "=== CONVERSATION STATE (directives — do not mention this block) ===",
    `Call stage: ${s.stage}. Your mood: ${s.buyerMood}. Trust in the rep: ${s.trustLevel}/100. Buying pressure you apply: ${s.pressureLevel}/100.`,
    `Objection status: ${objection}.`,
    `Your next move this turn: ${s.nextBuyerMove.replace(/_/g, " ")}.`,
    `Difficulty: ${s.difficulty} — scale resistance accordingly.`,
  ].join("\n");
}

// Rebuild a usable state when meta.state is missing/corrupt (state is derivable).
export function coerceState(
  raw: unknown,
  fallbackInput: InitialStateInput
): SparringState {
  if (raw && typeof raw === "object" && (raw as any).stage && (raw as any).repPerformance) {
    try {
      return clampState(raw as SparringState);
    } catch {
      /* fall through */
    }
  }
  return createInitialSparringState(fallbackInput);
}
