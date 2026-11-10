// GET /v1/calls/:id/signed-audio — decision logic (pure + injectable, network-free for tests).
//
// Before: a call whose audio object was absent from storage returned 500 "sign_failed: Object not found" (the raw
// provider message leaked and a normal "no audio" condition looked like a server fault); a call with no audio_path
// returned 400; an unauthorised caller got 403, which confirmed that the call exists.
//
// Now (all authorisation is decided BEFORE anything about audio is revealed):
//   unknown call, or caller not allowed (other tenant, no access)  → 404 not_found     (identical body: no existence oracle)
//   allowed, call has no audio_path                                → 404 no_audio
//   allowed, referenced object missing from storage                → 404 no_audio
//   allowed, storage/provider failure (anything else)              → 500 audio_unavailable (generic; details only in logs)
//   allowed, signed                                                → 200 { ok, url, ttl }
// Storage paths, bucket names and provider messages never appear in a response body.

export type CallAudioRow = { user_id: string; org_id: string | null; audio_path: string | null };

export type SignResult =
  | { ok: true; url: string }
  | { ok: false; reason: "missing" | "failure" };

export type SignedAudioDeps = {
  /** null = no such call. Throws on a database error. */
  getCall: (id: string) => Promise<CallAudioRow | null>;
  /** Throws on a lookup error. */
  canAccess: (requester: string, callUserId: string, callOrgId: string | null) => Promise<boolean>;
  sign: (audioPath: string, ttlSec: number) => Promise<SignResult>;
  ttlSec: number;
  /** Server-side diagnostics only (never part of the response). */
  log?: (event: string, detail: Record<string, unknown>) => void;
};

export type Result = { status: number; body: Record<string, unknown> };

const NOT_FOUND: Result = { status: 404, body: { ok: false, error: "not_found" } };
const NO_AUDIO: Result = { status: 404, body: { ok: false, error: "no_audio" } };
const UNAVAILABLE: Result = { status: 500, body: { ok: false, error: "audio_unavailable" } };

/**
 * Classifies a Supabase Storage error. Only an explicit "object does not exist" signal is treated as missing; every
 * other error (auth, network, 5xx, bucket misconfiguration, unknown shape) is a genuine failure.
 */
export function classifyStorageError(err: unknown): "missing" | "failure" {
  const e = err as { status?: unknown; statusCode?: unknown; message?: unknown } | null;
  if (!e || typeof e !== "object") return "failure";
  const code = String(e.statusCode ?? "");
  const msg = typeof e.message === "string" ? e.message.toLowerCase() : "";
  // Supabase Storage reports a missing object as statusCode "404" (HTTP status 400) with message "Object not found".
  if (code === "404" || e.status === 404) return "missing";
  if (msg === "object not found") return "missing";
  return "failure";
}

export async function resolveSignedAudio(deps: SignedAudioDeps, requester: string, id: string): Promise<Result> {
  try {
    const call = await deps.getCall(id);
    if (!call) return NOT_FOUND;

    const allowed = await deps.canAccess(requester, call.user_id, call.org_id ?? null);
    if (!allowed) return NOT_FOUND; // same as "no such call": do not reveal that it exists, or whether it has audio

    if (!call.audio_path) return NO_AUDIO;

    const signed = await deps.sign(call.audio_path, deps.ttlSec);
    if (signed.ok) return { status: 200, body: { ok: true, url: signed.url, ttl: deps.ttlSec } };
    if (signed.reason === "missing") {
      deps.log?.("signed_audio_object_missing", { callId: id });
      return NO_AUDIO;
    }
    deps.log?.("signed_audio_storage_failure", { callId: id });
    return UNAVAILABLE;
  } catch (e) {
    deps.log?.("signed_audio_error", { callId: id, error: e instanceof Error ? e.name : "unknown" });
    return UNAVAILABLE;
  }
}
