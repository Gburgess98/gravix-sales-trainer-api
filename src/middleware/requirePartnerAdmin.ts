import type { Request, Response, NextFunction } from "express";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

const PARTNER_ADMIN_TIERS = new Set(["PartnerAdmin", "SuperAdmin"]);

let _supa: SupabaseClient | null = null;
function getSupa(): SupabaseClient {
  if (_supa) return _supa;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error("server_missing_supabase_env");
  _supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });
  return _supa;
}

// Read only from the explicit request header — never from server-injected
// req.userId (which DEV_TEST_UID can fill). Privileged middleware must require
// an explicit credential; a missing header must return 401, not fall through
// to the dev fallback identity.
function getUserId(req: Request): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") || ""
  ).trim();
}

/**
 * Allows PartnerAdmin and SuperAdmin.
 * Rejects SalesRep, Manager, Owner with 403.
 */
export async function requirePartnerAdmin(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const userId = getUserId(req);
    if (!userId) {
      res.status(401).json({ ok: false, error: "missing_user_identity" });
      return;
    }

    const { data: rep, error } = await getSupa()
      .from("reps")
      .select("id, tier")
      .eq("id", userId)
      .maybeSingle();

    if (error) {
      res.status(500).json({ ok: false, error: error.message });
      return;
    }

    const tier = String((rep as any)?.tier || "");

    if (!PARTNER_ADMIN_TIERS.has(tier)) {
      res.status(403).json({ ok: false, error: "forbidden_not_partner_admin" });
      return;
    }

    (req as any).repTier = tier;
    next();
  } catch (e: any) {
    const msg = e?.message || "require_partner_admin_failed";
    res.status(msg === "server_missing_supabase_env" ? 500 : 500).json({ ok: false, error: msg });
  }
}
