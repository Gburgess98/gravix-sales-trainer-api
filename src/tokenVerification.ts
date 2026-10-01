// Go Live Day 26 (G1) — server-side Bearer JWT verification.
//
// Before Day 26 the API took the Bearer token's `sub` as the caller identity
// after a bare base64 decode: no signature, expiry, issuer or audience check,
// so anyone could mint a token for any user id. This module is now the ONLY
// way a Bearer token becomes an identity.
//
// Verification order (every step must pass, otherwise the token is ignored):
//   1. local pre-checks on the decoded header/payload — alg is an accepted
//      signing alg (never "none"), sub is a UUID, exp is in the future,
//      iss === `${SUPABASE_URL}/auth/v1`, aud contains "authenticated";
//   2. signature — Supabase auth-js `getClaims()`: asymmetric tokens are
//      verified against the project JWKS; symmetric (HS*) tokens fall back to
//      `auth.getUser(token)`, i.e. Supabase itself validates the token;
//   3. the claims returned by step 2 are re-checked with the same rules.
// Verified results are cached (by token hash) until min(exp, 5 min).
//
// Network-free tests inject their own `SignatureCheck` (see
// scripts/validate-identity-boundary-day-316.ts).

import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

export type VerifiedClaims = { sub: string; exp: number };
export type ClaimsVerifier = (token: string) => Promise<VerifiedClaims | null>;

/** Returns the verified claims (payload + header) or null if the signature is not valid. */
export type SignatureCheck = (
  token: string
) => Promise<{ claims: Record<string, unknown>; header: Record<string, unknown> } | null>;

export const EXPECTED_AUDIENCE = "authenticated";
// Only algorithms Supabase auth-js getClaims() can actually verify: ES256/RS256
// via the project JWKS, HS256 via the auth server (getUser fallback).
export const ACCEPTED_ALGS = new Set(["ES256", "RS256", "HS256"]);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function expectedIssuer(supabaseUrl = process.env.SUPABASE_URL): string | null {
  const base = String(supabaseUrl || "").trim().replace(/\/+$/, "");
  return base ? `${base}/auth/v1` : null;
}

function b64urlJson(part: string): Record<string, unknown> | null {
  try {
    const b64 = part.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "===".slice((b64.length + 3) % 4);
    const v = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    return v && typeof v === "object" ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

/** Decodes WITHOUT trusting anything; used only for the pre-checks. */
export function decodeUnverified(token: string) {
  const parts = String(token || "").split(".");
  if (parts.length !== 3 || !parts[2]) return null;
  const header = b64urlJson(parts[0]);
  const payload = b64urlJson(parts[1]);
  if (!header || !payload) return null;
  return { header, payload };
}

/** Returns null when the registered claims are acceptable, else a reason code. */
export function claimsRejectionReason(
  header: Record<string, unknown>,
  payload: Record<string, unknown>,
  issuer: string | null,
  nowSec = Math.floor(Date.now() / 1000)
): string | null {
  const alg = typeof header.alg === "string" ? header.alg : "";
  if (!ACCEPTED_ALGS.has(alg)) return "bad_alg";
  if (typeof payload.sub !== "string" || !UUID_RE.test(payload.sub)) return "bad_sub";
  if (typeof payload.exp !== "number" || payload.exp <= nowSec) return "expired";
  if (!issuer || payload.iss !== issuer) return "bad_iss";
  const aud = payload.aud;
  const audOk = Array.isArray(aud) ? aud.includes(EXPECTED_AUDIENCE) : aud === EXPECTED_AUDIENCE;
  if (!audOk) return "bad_aud";
  return null;
}

const CACHE_MAX = 2000;
const CACHE_TTL_MS = 5 * 60 * 1000;

export function createClaimsVerifier(opts: {
  signatureCheck: SignatureCheck;
  issuer?: () => string | null;
  now?: () => number;
}): ClaimsVerifier {
  const issuerOf = opts.issuer ?? (() => expectedIssuer());
  const nowMs = opts.now ?? (() => Date.now());
  const cache = new Map<string, { sub: string; exp: number; until: number }>();

  return async (token: string) => {
    const decoded = decodeUnverified(token);
    if (!decoded) return null;
    const issuer = issuerOf();
    const nowSec = Math.floor(nowMs() / 1000);
    if (claimsRejectionReason(decoded.header, decoded.payload, issuer, nowSec)) return null;

    const key = crypto.createHash("sha256").update(token).digest("hex");
    const hit = cache.get(key);
    if (hit && hit.until > nowMs() && hit.exp > nowSec) return { sub: hit.sub, exp: hit.exp };
    if (hit) cache.delete(key);    const verified = await opts.signatureCheck(token).catch(() => null);
    if (!verified) return null;
    if (claimsRejectionReason(verified.header, verified.claims, issuer, nowSec)) return null;
    // The signed claims must be the ones we pre-checked.
    if (verified.claims.sub !== decoded.payload.sub) return null;

    const sub = verified.claims.sub as string;
    const exp = verified.claims.exp as number;
    if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value as string);
    cache.set(key, { sub, exp, until: Math.min(exp * 1000, nowMs() + CACHE_TTL_MS) });
    return { sub, exp };
  };
}

let _supabaseCheck: SignatureCheck | null = null;

/** Production signature check backed by Supabase auth-js getClaims(). */
export function supabaseSignatureCheck(): SignatureCheck {
  if (_supabaseCheck) return _supabaseCheck;
  const url = String(process.env.SUPABASE_URL || "").trim();
  const key = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
  const client = url && key
    ? createClient(url, key, { auth: { persistSession: false, autoRefreshToken: false } })
    : null;

  _supabaseCheck = async (token: string) => {
    if (!client) return null;
    const { data, error } = await client.auth.getClaims(token);
    if (error || !data?.claims) return null;
    return {
      claims: data.claims as unknown as Record<string, unknown>,
      header: (data.header ?? {}) as unknown as Record<string, unknown>,
    };
  };
  return _supabaseCheck;
}

let _defaultVerifier: ClaimsVerifier | null = null;

export function defaultClaimsVerifier(): ClaimsVerifier {
  if (!_defaultVerifier) {
    _defaultVerifier = createClaimsVerifier({ signatureCheck: supabaseSignatureCheck() });
  }
  return _defaultVerifier;
}
