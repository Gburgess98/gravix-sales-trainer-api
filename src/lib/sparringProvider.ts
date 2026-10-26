// Go Live Day 42 (F-41-2) — explicit, fail-closed Sparring provider contract.
//
// Before: the Prospect Brain resolved SPARRING_BRAIN_PROVIDER → SPARRING_PROVIDER → "openai",
// so an unset or mistyped selector silently chose a PAID provider, and nothing in
// /v1/version said which provider answered. This mirrors the Day-35 transcription contract.
//
// Contract (case-insensitive after trim; whitespace-only = unset). Selector order is
// SPARRING_BRAIN_PROVIDER (canonical) then SPARRING_PROVIDER (legacy):
//   valid values            "stub" | "openai" | "claude" — anything else is INVALID in every environment
//   non-production, unset   -> "stub"  (even when OPENAI_API_KEY / ANTHROPIC_API_KEY are present)
//   production, unset       -> boot REFUSED (never inferred from a key, never defaulted to a paid provider)
//   "openai"                -> requires a non-empty OPENAI_API_KEY, else boot REFUSED
//   "claude"                -> requires a non-empty ANTHROPIC_API_KEY, else boot REFUSED
//   "stub"                  -> always allowed
// "Production" = NODE_ENV === "production" AND APP_ENV !== "staging" (same rule as Day 35).
//
// Errors are stable codes with fixed text: the supplied value and any key are never included.

import { isProductionEnv, type EnvLike } from "./transcriptionProvider";

export type SparringProvider = "stub" | "openai" | "claude";

export class SparringProviderConfigError extends Error {
  constructor(public readonly code: "sparring_provider_invalid" | "sparring_provider_required_in_production" | "sparring_provider_openai_requires_key" | "sparring_provider_claude_requires_key") {
    super(
      code === "sparring_provider_invalid"
        ? "FATAL: SPARRING_BRAIN_PROVIDER / SPARRING_PROVIDER is invalid — allowed values are 'stub', 'openai' or 'claude' (value not shown)."
        : code === "sparring_provider_required_in_production"
          ? "FATAL: SPARRING_BRAIN_PROVIDER must be set explicitly ('stub', 'openai' or 'claude') in production — refusing to boot."
          : code === "sparring_provider_openai_requires_key"
            ? "FATAL: the Sparring provider 'openai' requires OPENAI_API_KEY — refusing to boot."
            : "FATAL: the Sparring provider 'claude' requires ANTHROPIC_API_KEY — refusing to boot."
    );
    this.name = "SparringProviderConfigError";
  }
}

const rawSelector = (env: EnvLike): string => {
  for (const k of ["SPARRING_BRAIN_PROVIDER", "SPARRING_PROVIDER"]) {
    const v = String(env[k] ?? "").trim().toLowerCase();
    if (v) return v;
  }
  return "";
};

/** Resolve the provider or throw a SparringProviderConfigError. Does not check keys. */
export function resolveSparringProvider(env: EnvLike = process.env): SparringProvider {
  const raw = rawSelector(env);
  if (raw === "") {
    if (isProductionEnv(env)) throw new SparringProviderConfigError("sparring_provider_required_in_production");
    return "stub";
  }
  if (raw === "stub" || raw === "openai" || raw === "claude") return raw;
  throw new SparringProviderConfigError("sparring_provider_invalid");
}

/** Boot-time check (runs before listen): a valid, explicit provider; a paid one only with its key. */
export function assertSparringProviderConfig(env: EnvLike = process.env): SparringProvider {
  const provider = resolveSparringProvider(env);
  if (provider === "openai" && String(env.OPENAI_API_KEY ?? "").trim() === "") {
    throw new SparringProviderConfigError("sparring_provider_openai_requires_key");
  }
  if (provider === "claude" && String(env.ANTHROPIC_API_KEY ?? "").trim() === "") {
    throw new SparringProviderConfigError("sparring_provider_claude_requires_key");
  }
  return provider;
}

/** Non-throwing label for the non-secret /v1/version marker. */
export function describeSparringProvider(env: EnvLike = process.env): SparringProvider | "unresolved" {
  try { return resolveSparringProvider(env); } catch { return "unresolved"; }
}
