// src/sparring/index.ts — Sparring Brain public surface (Tier 2A).
export { createInitialSparringState, updateSparringState, inferStageFromText, inferObjectionType, clampState, summariseStateForPrompt, coerceState, normaliseDifficulty, } from "./state";
export { generateBuyerReply, withStateDirectives, resolveProviderName, } from "./providers";
