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
  type SparringProviderName,
  type BuyerReplyInput,
  type BuyerReplyResult,
  generateBuyerReply,
  withStateDirectives,
  resolveProviderName,
} from "./providers";
