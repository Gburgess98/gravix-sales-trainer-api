// Railway proxy trust (express `trust proxy`) — see RAILWAY_TRUST_PROXY.md.
//
// Topology (measured on the staging Railway service, 2026-11-06; production is the same platform): client → Railway edge node
// → Railway internal proxy → container. The app sees the socket peer 100.64.0.x (internal) and
//   X-Forwarded-For: "<real client>, <Railway edge node 152.233.29.x>"
// The edge OVERWRITES any client-supplied X-Forwarded-For (a forged "6.6.6.6" never reached the app) and its node address varies
// per request. So TWO hops are ours to trust: with trust proxy = 2 Express resolves req.ip to the real client; with 1 it would be
// the varying edge-node address (one client spread over several rate-limit buckets); with false it is the internal peer
// (callers share buckets, and express-rate-limit logs ERR_ERL_UNEXPECTED_X_FORWARDED_FOR); with `true` it is the LEFTMOST entry,
// which would be attacker-controlled if any hop ever appended instead of overwrote.
// The value is a small non-negative integer only: never `true`, a CIDR list, a function or "loopback". Override with
// TRUST_PROXY_HOPS (0–2) only if the platform topology changes; anything else refuses to boot.

export type EnvLike = Record<string, string | undefined>;
export const MAX_TRUSTED_HOPS = 2;
export const RAILWAY_HOPS = 2;

export function resolveTrustProxyHops(env: EnvLike = process.env): number {
  const raw = String(env.TRUST_PROXY_HOPS ?? "").trim();
  if (raw !== "") {
    if (!/^[0-9]+$/.test(raw) || Number(raw) > MAX_TRUSTED_HOPS) {
      throw new Error(`FATAL: TRUST_PROXY_HOPS must be an integer 0–${MAX_TRUSTED_HOPS} — refusing to boot.`);
    }
    return Number(raw);
  }
  // Production and the Railway staging service both run behind the same two Railway hops. Local dev has no proxy.
  return env.NODE_ENV === "production" ? RAILWAY_HOPS : 0;
}
