import type { Request, Response, NextFunction } from "express";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

const MANAGER_TIERS = new Set(["Manager", "Owner"]);

let _supabaseAdmin: SupabaseClient | null = null;
function getSupabaseAdmin(): SupabaseClient {
  if (_supabaseAdmin) return _supabaseAdmin;

  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    // Throw once; caught by middleware and returned as 500.
    throw new Error("server_missing_supabase_env");
  }

  _supabaseAdmin = createClient(url, key, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  return _supabaseAdmin;
}

function getUserId(req: Request): string {
  // Prefer request context if requireUserId ran earlier
  const ctxId = (req as any)?.user?.id || (req as any)?.userId;
  if (typeof ctxId === "string" && ctxId.trim()) return ctxId.trim();

  // Fallback to headers (support aliases)
  const headerId =
    req.header("x-user-id") ||
    req.header("X-User-Id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    req.header("X-Gravix-User-Id") ||
    "";

  return String(headerId).trim();
}

export async function requireManager(req: Request, res: Response, next: NextFunction) {
  try {
    const userId = getUserId(req);

    if (!userId) {
      return res.status(401).json({ ok: false, error: "missing_user_identity" });
    }

    const supabase = getSupabaseAdmin();

    const { data: rep, error } = await supabase
      .from("reps")
      .select("id,tier")
      .eq("id", userId)
      .maybeSingle();

    if (error) {
      return res.status(500).json({ ok: false, error: error.message });
    }

    const tier = String((rep as any)?.tier || "");
    if (!MANAGER_TIERS.has(tier)) {
      return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
    }

    // Attach a simple flag for downstream handlers (optional but useful)
    (req as any).isManager = true;

    return next();
  } catch (e: any) {
    const msg = e?.message || "require_manager_failed";
    if (msg === "server_missing_supabase_env") {
      return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });
    }
    return res.status(500).json({ ok: false, error: msg });
  }
}