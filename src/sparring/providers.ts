// src/sparring/providers.ts
// Compatibility shim over the Gravix AI Core Prospect Brain.
//
// The buyer-reply provider logic was formalised on Day 258 into
// `src/lib/sparringBrain/` (the `Brain` interface from
// GRAVIX_AI_CORE_ARCHITECTURE.md). This module keeps the Tier 2A Day-101 public
// surface — `generateBuyerReply`, `resolveProviderName`, `BuyerReplyInput`,
// `BuyerReplyResult`, `withStateDirectives` — so the sparring route (and the
// Day-101 validator) are unchanged. New code should import the Brain directly
// from `src/lib/sparringBrain`.

import {
  generateProspectReply,
  resolveBrainProvider,
  type BrainProviderName,
  type ProspectBrainInput,
} from "../lib/sparringBrain";
import { type SparringState, summariseStateForPrompt } from "./state";

// ── Legacy type aliases (unchanged public surface) ──────────────────────────
export type SparringProviderName = BrainProviderName;

export type BuyerReplyInput = ProspectBrainInput;

export type BuyerReplyResult = {
  ok: boolean;
  text: string;
  provider: SparringProviderName;
  fallback: boolean; // true when the configured provider failed and another answered
  latencyMs: number;
};

/**
 * Legacy resolver name — delegates to the Brain router. Selection order:
 * SPARRING_BRAIN_PROVIDER → SPARRING_PROVIDER → "openai".
 */
export function resolveProviderName(): SparringProviderName {
  return resolveBrainProvider();
}

// Build a state-augmented system prompt (state block appended, never replaces
// the existing persona prompt). Prompt assembly stays route/engine-owned, not
// part of the Brain provider.
export function withStateDirectives(
  systemPrompt: string,
  state?: SparringState | null
): string {
  if (!state) return systemPrompt;
  return `${systemPrompt}\n\n${summariseStateForPrompt(state)}`;
}

/**
 * Generate a buyer reply via the configured Brain provider. Thin adapter that
 * maps the canonical `BrainResult` to the legacy `BuyerReplyResult` shape the
 * route consumes. Never throws — see the Brain router's fallback chain.
 */
export async function generateBuyerReply(
  input: BuyerReplyInput
): Promise<BuyerReplyResult> {
  const result = await generateProspectReply(input);
  return {
    ok: result.ok,
    text: result.text,
    provider: result.provider,
    fallback: result.fallbackUsed,
    latencyMs: result.latencyMs,
  };
}
