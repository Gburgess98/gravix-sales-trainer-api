// src/lib/sparringBrain/types.ts
// Gravix AI Core — Phase 1: the "Prospect Brain" provider interface.
//
// The Brain renders ONLY the buyer's next spoken words. The conversation loop,
// prompt assembly, state, scoring and persistence stay Gravix-owned in the
// route (see GRAVIX_AI_CORE_ARCHITECTURE.md §5). A provider is configuration
// behind this interface — swapping OpenAI ⇆ Claude ⇆ stub never changes the
// engine contracts above it.

import type { SparringState } from "../../sparring/state";

export type BrainProviderName = "openai" | "claude" | "stub";

/**
 * Input to the Prospect Brain. `systemPrompt` and `history` are already fully
 * assembled by the route (persona, company profile, emotional state, scenario,
 * rep context and state directives are baked in). The remaining fields are
 * context/options a provider may use but must not depend on.
 */
export type ProspectBrainInput = {
  /** Fully-built system prompt from the route. */
  systemPrompt: string;
  /** Ordered conversation history, oldest → newest, including this rep turn. */
  history: Array<{ role: "user" | "assistant"; content: string }>;
  /** Rules-engine state (Gravix-owned); a model may read it, never own it. */
  state?: SparringState | null;
  personaId?: string | null;
  difficulty?: string | null;
  /** Force a specific provider for this call, bypassing env resolution. */
  providerOverride?: BrainProviderName | null;
};

/** Token usage when the provider SDK exposes it (best-effort, never required). */
export type BrainUsage = {
  inputTokens?: number;
  outputTokens?: number;
};

/**
 * What a single provider returns. Providers report the model they used and any
 * usage; the orchestrator adds provider name, latency and fallback provenance.
 */
export type ProviderReply = {
  text: string;
  model: string;
  usage?: BrainUsage;
};

/**
 * The canonical Prospect Brain result. Provenance travels with the output
 * (architecture §5.6): the session can record which provider/model spoke.
 */
export type BrainResult = {
  ok: boolean;
  text: string;
  provider: BrainProviderName;
  model: string;
  latencyMs: number;
  /** true when the configured provider failed and another answered. */
  fallbackUsed: boolean;
  usage?: BrainUsage;
};

/** One provider behind the Brain interface. */
export interface BrainProvider {
  name: BrainProviderName;
  generateProspectReply(input: ProspectBrainInput): Promise<ProviderReply>;
}
