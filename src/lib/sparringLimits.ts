// Go Live Day 42 (F-41-8) — deterministic, bounded Sparring abuse controls.
//
// Keyed ONLY by the verified caller id (req.userId, set by the global identity middleware from
// a verified Bearer token or the secret-checked proxy header) — never by IP or any browser-
// controlled header, so the limits hold behind the existing proxy without a `trust proxy` guess.
// In-memory fixed windows (single API instance); a multi-instance deployment multiplies the
// effective ceiling and needs a shared store — documented, not guessed.

export const SPARRING_LIMITS = {
  maxMessageChars: 2000,
  maxUserTurnsPerSession: 40,
  sessionsPerHour: 20,
  turnsPerMinute: 20,
  scoresPerMinute: 10,
} as const;

export type SparringRateScope = "sessions" | "turns" | "score";
const RULES: Record<SparringRateScope, { windowMs: number; max: number }> = {
  sessions: { windowMs: 60 * 60_000, max: SPARRING_LIMITS.sessionsPerHour },
  turns: { windowMs: 60_000, max: SPARRING_LIMITS.turnsPerMinute },
  score: { windowMs: 60_000, max: SPARRING_LIMITS.scoresPerMinute },
};

export type RateResult = { ok: true } | { ok: false; retryAfterSec: number };

export function createSparringLimiter(now: () => number = Date.now) {
  const buckets = new Map<string, { start: number; count: number }>();
  return {
    hit(userId: string, scope: SparringRateScope): RateResult {
      const uid = String(userId || "").trim();
      if (!uid) return { ok: false, retryAfterSec: 1 };
      const rule = RULES[scope], t = now(), key = `${scope}:${uid}`;
      if (buckets.size > 5000) for (const [k, b] of buckets) if (t - b.start >= 60 * 60_000) buckets.delete(k);
      const b = buckets.get(key);
      if (!b || t - b.start >= rule.windowMs) { buckets.set(key, { start: t, count: 1 }); return { ok: true }; }
      if (b.count >= rule.max) return { ok: false, retryAfterSec: Math.max(1, Math.ceil((b.start + rule.windowMs - t) / 1000)) };
      b.count += 1;
      return { ok: true };
    },
  };
}
export const sparringLimiter = createSparringLimiter();

export type TurnTextResult = { ok: true; text: string } | { ok: false; error: "text_required" | "message_too_long" };
/** Validate a rep message BEFORE any read, write or provider call. */
export function validateTurnText(text: unknown): TurnTextResult {
  if (typeof text !== "string" || !text.trim()) return { ok: false, error: "text_required" };
  if (text.length > SPARRING_LIMITS.maxMessageChars) return { ok: false, error: "message_too_long" };
  return { ok: true, text };
}
