// Go Live Day 26 (G2 backstop + G4 contract) — default-deny route policy.
//
// Every request must carry a verified identity (req.userId, set by the
// identity middleware from a trusted proxy header or a verified Bearer token)
// unless its method+path is listed here. Routes on this list are either
// intentionally public (static status / credential exchange) or service
// routes guarded by their own secret. Per-route role/tenant checks still apply
// on top of this gate; the gate only guarantees "no identity => 401".
//
// `npm run validate:route-authz` enumerates the real Express router and fails
// if an entry here matches no route, or if any other route answers an
// anonymous or forged-token request with anything but 401.

import type { NextFunction, Request, Response } from "express";

export type PublicRoute = { method: string; path: string; reason: string };

export const PUBLIC_ROUTES: readonly PublicRoute[] = [
  { method: "GET", path: "/", reason: "static status page" },
  { method: "GET", path: "/index.html", reason: "static status page" },
  { method: "GET", path: "/__probe", reason: "static liveness probe" },
  { method: "GET", path: "/health", reason: "static health (Railway healthcheck)" },
  { method: "HEAD", path: "/health", reason: "static health" },
  { method: "GET", path: "/v1/health", reason: "static health" },
  { method: "HEAD", path: "/v1/health", reason: "static health" },
  { method: "GET", path: "/v1/version", reason: "build stamp, no tenant data" },
  { method: "GET", path: "/v1/coach/drills", reason: "static drill catalogue" },
  { method: "GET", path: "/v1/crm/health", reason: "static health" },
  { method: "GET", path: "/v1/debug/health", reason: "static health" },
  { method: "GET", path: "/v1/sparring/personas", reason: "static persona catalogue" },
  { method: "POST", path: "/v1/auth/login", reason: "credential exchange (rate-limited)" },
  { method: "POST", path: "/v1/auth/reset-password", reason: "credential recovery (rate-limited)" },
  { method: "POST", path: "/v1/auth/logout", reason: "signs out only the verified caller, if any" },
  { method: "GET", path: "/v1/auth/me", reason: "verifies its own Bearer token" },
  { method: "POST", path: "/v1/cron/crm/auto-assign", reason: "service route: requireCron (x-cron-secret)" },
];

function normalise(path: string): string {
  const p = String(path || "/").toLowerCase();
  return p.length > 1 ? p.replace(/\/+$/, "") : p;
}

const PUBLIC_KEYS = new Set(PUBLIC_ROUTES.map((r) => `${r.method} ${normalise(r.path)}`));

export function isPublicRoute(method: string, path: string): boolean {
  const m = String(method || "").toUpperCase();
  if (m === "OPTIONS") return true; // CORS preflight carries no credentials
  return PUBLIC_KEYS.has(`${m} ${normalise(path)}`);
}

/** Default-deny: no verified identity => 401, except PUBLIC_ROUTES. */
export function requireIdentityByDefault(req: Request, res: Response, next: NextFunction) {
  if (isPublicRoute(req.method, req.path)) return next();
  const uid = (req as any).userId;
  if (typeof uid === "string" && uid.trim()) return next();
  return res.status(401).json({ ok: false, error: "missing_user_identity" });
}
