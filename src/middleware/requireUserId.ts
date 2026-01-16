import { Request, Response, NextFunction } from "express";

function getBearerToken(req: Request): string | null {
  const h = req.header("authorization") || req.header("Authorization");
  if (!h) return null;
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

function tryDecodeJwtSub(token: string | null): string | null {
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

export function requireUserId(req: Request, res: Response, next: NextFunction) {
  const headerUid =
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    null;

  const uid =
    (headerUid ? headerUid.trim() : null) ||
    tryDecodeJwtSub(getBearerToken(req));

  if (!uid) return res.status(401).json({ ok: false, error: "missing_user_identity" });

  // Single source of truth for downstream routes
  (req as any).user = { id: uid };
  (req as any).userId = uid; // keep back-compat until we finish the sweep

  return next();
}