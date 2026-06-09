import type { Request, Response, NextFunction } from "express";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

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
// req.userId. A missing header must return 401, not fall through to the
// DEV_TEST_UID dev fallback.
function getUserId(req: Request): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") || ""
  ).trim();
}

/**
 * Allows SuperAdmin only.
 * Rejects every other tier — including PartnerAdmin — with 403.
 */
export async function requireSuperAdmin(
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

    if (tier !== "SuperAdmin") {
      res.status(403).json({ ok: false, error: "forbidden_not_super_admin" });
      return;
    }

    (req as any).repTier = tier;
    next();
  } catch (e: any) {
    const msg = e?.message || "require_super_admin_failed";
    res.status(500).json({ ok: false, error: msg });
  }
}
