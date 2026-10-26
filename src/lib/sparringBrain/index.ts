// src/lib/sparringBrain/index.ts
// Gravix AI Core — Prospect Brain public surface + provider router.
//
// Selection is configuration (architecture §5.2): SPARRING_BRAIN_PROVIDER
// (canonical), falling back to the legacy SPARRING_PROVIDER. Day 42: the contract lives in
// ../sparringProvider (non-production default "stub"; production must be explicit; a paid
// provider needs its key) — the router itself never defaults to a paid provider.
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
import { resolveSparringProvider } from "../sparringProvider";

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


/**
 * Resolve the configured Brain provider through the explicit Day-42 contract
 * (../sparringProvider). Boot already refuses an invalid/unset-in-production
 * configuration; this per-request call must still never throw or pick a paid
 * provider by accident, so any resolution error degrades to the no-egress "stub".
 */
export function resolveBrainProvider(): BrainProviderName {
  try {
    return resolveSparringProvider(process.env);
  } catch {
    return "stub";
  }
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
