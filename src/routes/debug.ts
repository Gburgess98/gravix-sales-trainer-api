import { Router } from "express";
import { requireAuth } from "../middleware/requireAuth";

const router = Router();

async function tryGetSupabaseAdmin() {
  try {
    // Lazy import so missing file doesn't crash server boot
    const mod: any = await import("../lib/supabaseAdmin");
    return mod?.supabaseAdmin ?? mod?.default ?? null;
  } catch {
    return null;
  }
}

router.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "gravix-sales-trainer-api",
    time: new Date().toISOString(),
  });
});

router.get("/auth", requireAuth, (req, res) => {
  res.json({
    ok: true,
    user_id: req.user?.id ?? null,
    headers_seen: {
      authorization: Boolean(req.headers.authorization),
      x_user_id: Boolean(req.headers["x-user-id"]),
    },
  });
});

router.get("/db", requireAuth, async (_req, res) => {
  try {
    const supabaseAdmin = await tryGetSupabaseAdmin();

    if (!supabaseAdmin) {
      return res.status(503).json({
        ok: false,
        error: "supabase_admin_not_configured",
        hint: "Missing src/lib/supabaseAdmin.ts or debug.ts import path is wrong",
      });
    }

    const { data, error } = await supabaseAdmin
      .from("reps")
      .select("id")
      .limit(1);

    if (error) {
      return res.status(500).json({
        ok: false,
        error: error.message,
      });
    }

    return res.json({
      ok: true,
      db: "ok",
      sample_rows: Array.isArray(data) ? data.length : 0,
    });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message || "db_check_failed",
    });
  }
});

export default router;