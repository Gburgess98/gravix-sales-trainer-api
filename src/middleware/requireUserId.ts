import { Request, Response, NextFunction } from "express";

// Day 26 (G1): this middleware used to re-read identity headers and base64-decode
// the Bearer token itself (no signature check). It now trusts ONLY the identity
// resolved by the global middleware in server.ts: a proxy header that passed the
// Day-175 x-proxy-secret boundary, or a Bearer token verified by
// src/tokenVerification.ts. Untrusted identity headers are already stripped there.
export function requireUserId(req: Request, res: Response, next: NextFunction) {
  const uid = String((req as any).userId || "").trim();

  if (!uid) return res.status(401).json({ ok: false, error: "missing_user_identity" });

  // Single source of truth for downstream routes
  (req as any).user = { id: uid };
  (req as any).userId = uid; // keep back-compat until we finish the sweep

  return next();
}
