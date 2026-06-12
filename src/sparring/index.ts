// src/sparring/index.ts — Sparring Brain public surface (Tier 2A).
export {
  type SparringState,
  type SparringStage,
  type BuyerMood,
  type ObjectionType,
  type SparringDifficulty,
  type NextBuyerMove,
  createInitialSparringState,
  updateSparringState,
  inferStageFromText,
  inferObjectionType,
  clampState,
  summariseStateForPrompt,
  coerceState,
  normaliseDifficulty,
} from "./state";

export {
  type TurnFlag,
  type StructuredTurnScore,
  type TurnScoreInput,
  scoreSparringTurn,
  scoreClarity,
  scoreConfidence,
  scoreObjectionHandling,
  scoreProgression,
  detectTurnFlags,
  buildTurnFeedback,
  mergeTurnScoreIntoState,
} from "./scoring";

export {
  type SparringSessionSummary,
  type StoredTurnScore,
  type WeakMomentEntry,
  type SparringDrillType,
  buildSparringSessionSummary,
  aggregateTurnScores,
  selectWeakMoments,
  recommendSparringDrill,
  buildSummaryText,
  buildNextBestAction,
} from "./summary";

export {
  type SparringProviderName,
  type BuyerReplyInput,
  type BuyerReplyResult,
  generateBuyerReply,
  withStateDirectives,
  resolveProviderName,
} from "./providers";
