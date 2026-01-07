import type { Request, Response, NextFunction } from "express";
import { createClient } from "@supabase/supabase-js";

const MANAGER_TIERS = new Set(["Manager", "Owner"]);

export async function requireManager(req: Request, res: Response, next: NextFunction) {
  try {
    const userId =
      String(req.header("x-user-id") || req.header("X-User-Id") || "").trim();

    if (!userId) {
      return res.status(401).json({ ok: false, error: "missing_x_user_id" });
    }

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !key) {
      return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });
    }

    const supabase = createClient(url, key, {
      auth: { autoRefreshToken: false, persistSession: false },
    });

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

    return next();
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "require_manager_failed" });
  }
}