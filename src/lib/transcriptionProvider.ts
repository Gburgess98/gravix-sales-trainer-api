// Go Live Day 35 (F-34-1) — explicit, fail-closed transcription provider contract.
//
// Before: simulateTranscription chose OpenAI from the mere PRESENCE of OPENAI_API_KEY,
// so a staging deploy that reported "stub scoring, side effects disabled" still sent
// uploaded audio to a paid third party. SCORING_PROVIDER / SKIP_SCORING_SIDE_EFFECTS /
// TRANSCRIBE_MODEL never governed transcription and still do not (independent controls).
//
// Contract (TRANSCRIPTION_PROVIDER, case-insensitive after trim; whitespace-only = unset):
//   valid values            "stub" | "openai" — anything else is INVALID in every environment
//   non-production, unset   -> "stub"  (even when OPENAI_API_KEY is present)
//   production, unset       -> boot REFUSED (never inferred from the key)
//   "openai"                -> requires a non-empty OPENAI_API_KEY, else boot REFUSED
//   "stub"                  -> always allowed (incl. production, for controlled validation)
// "Production" = NODE_ENV === "production" AND APP_ENV !== "staging": the Railway
// staging service may run with NODE_ENV=production but is marked APP_ENV=staging, and
// for staging an unset provider safely resolves to the no-egress "stub".
//
// Errors are stable codes with fixed text: the supplied provider value and any key are
// never included in a message.

export type TranscriptionProvider = "stub" | "openai";
export type EnvLike = Record<string, string | undefined>;

export class TranscriptionProviderConfigError extends Error {
  constructor(public readonly code: "transcription_provider_invalid" | "transcription_provider_required_in_production" | "transcription_provider_openai_requires_key") {
    super(
      code === "transcription_provider_invalid"
        ? "FATAL: TRANSCRIPTION_PROVIDER is invalid — allowed values are 'stub' or 'openai' (value not shown)."
        : code === "transcription_provider_required_in_production"
          ? "FATAL: TRANSCRIPTION_PROVIDER must be set explicitly ('stub' or 'openai') in production — refusing to boot."
          : "FATAL: TRANSCRIPTION_PROVIDER=openai requires OPENAI_API_KEY — refusing to boot."
    );
    this.name = "TranscriptionProviderConfigError";
  }
}

export function isProductionEnv(env: EnvLike = process.env): boolean {
  return env.NODE_ENV === "production" && String(env.APP_ENV || "").trim().toLowerCase() !== "staging";
}

/** Resolve the provider or throw a TranscriptionProviderConfigError. */
export function resolveTranscriptionProvider(env: EnvLike = process.env): TranscriptionProvider {
  const raw = String(env.TRANSCRIPTION_PROVIDER ?? "").trim().toLowerCase();
  if (raw === "") {
    if (isProductionEnv(env)) throw new TranscriptionProviderConfigError("transcription_provider_required_in_production");
    return "stub";
  }
  if (raw === "stub" || raw === "openai") return raw;
  throw new TranscriptionProviderConfigError("transcription_provider_invalid");
}

/** Boot-time check (runs before listen): a valid provider, and OpenAI only with its key. */
export function assertTranscriptionProviderConfig(env: EnvLike = process.env): TranscriptionProvider {
  const provider = resolveTranscriptionProvider(env);
  if (provider === "openai" && String(env.OPENAI_API_KEY ?? "").trim() === "") {
    throw new TranscriptionProviderConfigError("transcription_provider_openai_requires_key");
  }
  return provider;
}

/** Non-throwing label for the non-secret /v1/version marker. */
export function describeTranscriptionProvider(env: EnvLike = process.env): TranscriptionProvider | "unresolved" {
  try { return resolveTranscriptionProvider(env); } catch { return "unresolved"; }
}
