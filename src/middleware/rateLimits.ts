import rateLimit, { ipKeyGenerator } from "express-rate-limit";
import crypto from "node:crypto";

const jsonHandler = (message: string) => ({
  handler(_req: any, res: any) {
    res.status(429).json({ ok: false, error: message });
  },
});

// ── Who is being limited ───────────────────────────────────────────────────────────────────────────────────────
// Traffic from the web app arrives through the Vercel proxy, so its network address is Vercel's egress, shared by every user.
// Keying those requests on the address would let one user exhaust the allowance of all. Rules (see RAILWAY_TRUST_PROXY.md):
//   • A request counts as "from our proxy" ONLY when PROXY_SHARED_SECRET is configured and the caller presented the matching
//     x-proxy-secret (timing-safe). A missing or wrong secret is never trusted, in every environment.
//   • Such a request is limited per AUTHENTICATED USER, using the identity the server itself resolved (req.auth, set by the
//     identity middleware: the proxy-asserted id that the secret vouches for, cross-checked against any Bearer token) — never a
//     raw x-user-id/x-org-id/forwarded header. The key is a namespaced hash of the internal id: no email, token or secret.
//   • Everything else (direct callers, anonymous, bad/forged credentials, trusted traffic without an identity) is limited by the
//     real client address (req.ip, with the bounded trust-proxy hops).
function proxySecretValid(req: any): boolean {
  const expected = String(process.env.PROXY_SHARED_SECRET || "").trim();
  if (!expected) return false;
  const provided = String(req.headers?.["x-proxy-secret"] || "").trim();
  if (!provided || provided.length !== expected.length) return false;
  try { return crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(expected)); } catch { return false; }
}
const authedUserId = (req: any): string | null => {
  const a = req?.auth;
  return a && typeof a.userId === "string" && a.userId && (a.via === "header" || a.via === "jwt") ? a.userId : null;
};

export const isTrustedProxyRequest = proxySecretValid;

/** Rate-limit key. Pure given the request; never contains a raw id, email, token or secret. */
export function rateLimitKey(req: any): string {
  if (proxySecretValid(req)) {
    const uid = authedUserId(req);
    if (uid) return "u:" + crypto.createHash("sha256").update("gravix:rl:user:v1:" + uid).digest("hex").slice(0, 32);
  }
  return "ip:" + ipKeyGenerator(String(req.ip || ""));
}

const BASE = { standardHeaders: "draft-7", legacyHeaders: false } as const;

/** Early limiter, before authentication: by client address. Requests from our own proxy are handled by the per-user limiter below. */
export function createIpRateLimit(max = 300) {
  return rateLimit({
    windowMs: 60 * 1000,
    max,
    ...BASE,
    skip: (req) => proxySecretValid(req),
    keyGenerator: (req) => "ip:" + ipKeyGenerator(String(req.ip || "")),
    ...jsonHandler("rate_limit_exceeded"),
  });
}

/** After the identity middleware: per authenticated user for proxied traffic (skips everything else; the IP limiter covers that). */
export function createUserRateLimit(max = 300) {
  return rateLimit({
    windowMs: 60 * 1000,
    max,
    ...BASE,
    skip: (req) => !proxySecretValid(req),
    keyGenerator: (req) => rateLimitKey(req),
    ...jsonHandler("rate_limit_exceeded"),
  });
}

// Global (IP): 300 requests / minute per client address, for everything that is not our own proxy.
export const globalRateLimit = createIpRateLimit();

// Global (user): 300 requests / minute per authenticated user behind the trusted proxy.
export const userRateLimit = createUserRateLimit();

// Auth endpoints: 10 requests / 15 minutes per IP.
// Applied to /v1/auth/login and /v1/auth/reset-password to resist
// credential stuffing and account-enumeration attacks.
export const authRateLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max:      10,
  standardHeaders: "draft-7",
  legacyHeaders:   false,
  ...jsonHandler("auth_rate_limit_exceeded"),
});

// Upload endpoints: 30 requests / 10 minutes per IP.
// Each upload triggers transcription + scoring jobs; this prevents
// runaway costs from a compromised or misbehaving client.
// Mounted AFTER the identity middleware so web-app uploads are limited per user, not per shared Vercel egress address.
export const uploadRateLimit = rateLimit({
  windowMs: 10 * 60 * 1000,
  max:      30,
  standardHeaders: "draft-7",
  legacyHeaders:   false,
  keyGenerator: (req) => rateLimitKey(req),
  ...jsonHandler("upload_rate_limit_exceeded"),
});
