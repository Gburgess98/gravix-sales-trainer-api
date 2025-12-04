import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import 'dotenv/config';

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

// ----- Users list (schema-agnostic) ----------------------------------------
const CANDIDATES: { select: string; searchCols: string[] }[] = [
  { select: "id, full_name", searchCols: ["full_name"] },
  { select: "id:user_id, full_name", searchCols: ["full_name"] },
  { select: "id, full_name:name", searchCols: ["name"] },
  { select: "id:user_id, full_name:name", searchCols: ["name"] },
  { select: "id, full_name, email", searchCols: ["full_name", "email"] },
  { select: "id:user_id, full_name, email", searchCols: ["full_name", "email"] },
  { select: "id, full_name:name, email", searchCols: ["name", "email"] },
  { select: "id:user_id, full_name:name, email", searchCols: ["name", "email"] },
  { select: "id", searchCols: [] },
  { select: "id:user_id", searchCols: [] },
];

async function trySelect(selectClause: string, searchCols: string[], q: string, limit: number) {
  let query = supa.from("profiles").select(selectClause, { count: "exact" }).limit(limit);
  if (q && searchCols.length) {
    const expr = searchCols.map((c) => `${c}.ilike.%${q}%`).join(",");
    query = query.or(expr);
  }
  return await query;
}

// GET /v1/team/users?q=alex&limit=100
router.get("/users", async (req, res) => {
  try {
    const q = (req.query.q as string | undefined)?.trim() || "";
    const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));

    let data: any[] | null = null;
    let lastErr: any = null;
    for (const cand of CANDIDATES) {
      const { data: d, error } = await trySelect(cand.select, cand.searchCols, q, limit);
      if (!error) { data = d || []; lastErr = null; break; }
      lastErr = error;
    }
    if (!data) return res.status(500).json({ ok: false, error: lastErr?.message || "select_failed" });

    const items = (data as any[]).map((u) => {
      const id = u.id;
      const email = typeof u.email === "string" ? u.email : null;
      const fullName = typeof u.full_name === "string" ? u.full_name : null;
      const name = fullName || email || id;
      return { id, name, email };
    }).sort((a, b) => (a.name || "").localeCompare(b.name || ""));

    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- Ensure profile exists (schema-agnostic upsert) ----------------------
router.post("/ensure-profile", async (req, res) => {
  try {
    const { id, name, email } = (req.body || {}) as { id?: string; name?: string; email?: string };
    if (!id || typeof id !== "string") return res.status(400).json({ ok: false, error: "missing id" });

    const table = supa.from("profiles");

    // 1) Minimal shape first: only user_id
    let r = await table.upsert({ user_id: id }, { onConflict: "user_id" }).select().single();

    // 2) If that fails, progressively try richer shapes
    if (r.error) {
      const candidates: any[] = [
        { user_id: id, name: name ?? null },
        { user_id: id, full_name: name ?? null },
        { user_id: id, name: name ?? null, email: email ?? null },
        { user_id: id, full_name: name ?? null, email: email ?? null },
      ];
      let success = false;
      for (const candidate of candidates) {
        r = await table.upsert(candidate, { onConflict: "user_id" }).select().single();
        if (!r.error) { success = true; break; }
      }
      if (!success && r.error) return res.status(500).json({ ok: false, error: r.error.message });
    }

    return res.json({ ok: true, item: r.data });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "ensure_failed" });
  }
});

export default router;