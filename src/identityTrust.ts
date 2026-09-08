// Day 316 — identity trust boundary primitives.
//
// Extracted verbatim from src/server.ts (the Day 175 / commit 3add39b hardening)
// so the trust contract can be exercised by a network-free regression test
// (scripts/validate-identity-boundary-day-316.ts). Behaviour is identical to the
// previous inline implementation — this is a pure refactor, not a change.
//
// Contract:
//   - When PROXY_SHARED_SECRET is set, x-user-id (and its aliases) are only
//     honoured with a matching x-proxy-secret; otherwise those headers are
//     STRIPPED before resolution. When the env is unset, behaviour is unchanged
//     (dev/local opt-in rollout).
//   - Identity priority: explicit header > jwt sub > dev-env (dev only).
//   - If a header uid and a jwt sub are both present and disagree, that is an
//     auth_mismatch (the caller must fail the request loudly).

import crypto from "crypto";

export type AuthVia = "header" | "jwt" | "env" | null;
export type AuthCtx = { userId: string | null; via: AuthVia };

/** Minimal shape shared by express.Request and test doubles. */
export interface IdentityRequestLike {
  header(name: string): string | undefined;
  headers: Record<string, unknown>;
}

export function tryDecodeJwtSub(token: string | null): string | null {
  if (!token) return null;
  try {
    const parts = token.split(".");
    if (parts.length < 2) return null;
    const b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "===".slice((b64.length + 3) % 4);
    const json = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    const sub = typeof json?.sub === "string" ? json.sub : null;
    return sub && sub.length > 10 ? sub : null;
  } catch {
    return null;
  }
}

export function getBearerToken(req: IdentityRequestLike): string | null {
  const raw = (req.header("authorization") || req.header("Authorization") || "").trim();
  if (!raw) return null;
  const m = raw.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

export function isUuid(v: string | null | undefined): boolean {
  if (!v) return false;
  return /^[0-9a-fA-F-]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(v);
}

// Day 175 — proxy trust boundary for identity headers.
// When PROXY_SHARED_SECRET is configured, x-user-id (and its aliases) are only
// honoured when the request carries the matching x-proxy-secret, i.e. it came
// from our web proxy rather than a direct caller. When the env is unset,
// behaviour is unchanged (dev/local).
export function identityHeadersTrusted(req: IdentityRequestLike): boolean {
  const expected = String(process.env.PROXY_SHARED_SECRET || "").trim();
  if (!expected) return true;
  const provided = String(req.header("x-proxy-secret") || "").trim();
  if (!provided || provided.length !== expected.length) return false;
  try {
    return crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(expected));
  } catch {
    return false;
  }
}

export const SPOOFABLE_IDENTITY_HEADERS = [
  "x-user-id",
  "x-gravix-user-id",
  "x-forwarded-user-id",
  "x-real-user-id",
] as const;

export type ResolvedIdentity = {
  userId: string | null;
  via: AuthVia;
  headerUid: string | null;
  jwtUid: string | null;
  mismatch: boolean;
};

/**
 * Resolve the caller identity, applying the Day-175 trust boundary.
 * Mutates `req.headers` (stripping spoofable identity headers) exactly as the
 * original inline middleware did, so downstream per-route header reads are also
 * protected. Returns the resolved context plus a `mismatch` flag; the caller
 * decides how to respond to a mismatch.
 */
export function resolveIdentity(req: IdentityRequestLike): ResolvedIdentity {
  if (!identityHeadersTrusted(req)) {
    for (const h of SPOOFABLE_IDENTITY_HEADERS) delete req.headers[h];
  }

  const headerUid = (req.header("x-user-id") || "").trim() || null;
  const token = getBearerToken(req);
  const jwtUid = tryDecodeJwtSub(token);

  // DEV escape hatch (local only — never honoured in production)
  const envUid = process.env.NODE_ENV === "production"
    ? null
    : (process.env.DEV_TEST_UID || "").trim() || null;

  let ctx: AuthCtx = { userId: null, via: null };

  // Priority: explicit header > jwt > env
  if (isUuid(headerUid)) ctx = { userId: headerUid, via: "header" };
  else if (isUuid(jwtUid)) ctx = { userId: jwtUid, via: "jwt" };
  else if (isUuid(envUid)) ctx = { userId: envUid, via: "env" };

  const mismatch = isUuid(headerUid) && isUuid(jwtUid) && headerUid !== jwtUid;

  return { userId: ctx.userId, via: ctx.via, headerUid, jwtUid, mismatch };
}
