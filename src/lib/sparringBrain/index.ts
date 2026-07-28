// src/lib/sparringBrain/index.ts
// Gravix AI Core — Prospect Brain public surface + provider router.
//
// Selection is configuration (architecture §5.2): SPARRING_BRAIN_PROVIDER
// (canonical), falling back to the legacy SPARRING_PROVIDER, default "openai".
// The router owns the fallback chain and provenance; it NEVER throws — the worst
// case is a deterministic stub line, so a sparring turn cannot crash on provider
// availability (architecture §5.5).

import type {
  BrainProvider,
  BrainProviderName,
  BrainResult,
  ProspectBrainInput,
} from "./types";
import { openaiBrain } from "./openaiBrain";
import { claudeBrain } from "./claudeBrain";
import { stubBrain, STUB_MODEL } from "./stubBrain";

export type {
  BrainProvider,
  BrainProviderName,
  BrainResult,
  BrainUsage,
  ProspectBrainInput,
  ProviderReply,
} from "./types";

const PROVIDERS: Record<BrainProviderName, BrainProvider> = {
  openai: openaiBrain,
  claude: claudeBrain,
  stub: stubBrain,
};

const DEFAULT_PROVIDER: BrainProviderName = "openai";

function isBrainProviderName(v: string): v is BrainProviderName {
  return v === "openai" || v === "claude" || v === "stub";
}

/**
 * Resolve the configured Brain provider. Canonical env is
 * SPARRING_BRAIN_PROVIDER; the legacy SPARRING_PROVIDER is honoured for
 * backward compatibility. Anything unset or invalid resolves to "openai",
 * matching the existing safety convention (never crash on misconfiguration).
 */
export function resolveBrainProvider(): BrainProviderName {
  const raw = String(
    process.env.SPARRING_BRAIN_PROVIDER ||
      process.env.SPARRING_PROVIDER ||
      DEFAULT_PROVIDER
  )
    .trim()
    .toLowerCase();
  return isBrainProviderName(raw) ? raw : DEFAULT_PROVIDER;
}

/**
 * Generate the buyer/prospect reply via the configured provider, degrading down
 * the chain (configured → stub) on failure. Provenance (provider, model,
 * latency, fallback) travels with the result. Never throws.
 */
export async function generateProspectReply(
  input: ProspectBrainInput
): Promise<BrainResult> {
  const configured = input.providerOverride || resolveBrainProvider();
  const chain: BrainProviderName[] =
    configured === "stub" ? ["stub"] : [configured, "stub"];

  const started = Date.now();
  let fallbackUsed = false;

  for (const name of chain) {
    try {
      const reply = await PROVIDERS[name].generateProspectReply(input);
      return {
        ok: true,
        text: reply.text,
        provider: name,
        model: reply.model,
        latencyMs: Date.now() - started,
        fallbackUsed: fallbackUsed || name !== configured,
        usage: reply.usage,
      };
    } catch (e: any) {
      console.error(`[sparringBrain] ${name} failed:`, e?.message || e);
      fallbackUsed = true;
    }
  }

  // Unreachable (stub never throws), but keep the caller safe regardless.
  return {
    ok: false,
    text: "I'm still not sure about this. The price feels high compared to what I'm getting.",
    provider: "stub",
    model: STUB_MODEL,
    latencyMs: Date.now() - started,
    fallbackUsed: true,
  };
}
