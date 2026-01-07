// src/routes/assignments.ts
import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import { createClient } from "@supabase/supabase-js";

// Reuse the same header convention we’ve used everywhere
function getUserId(req: Request) {
  return String(req.header("x-user-id") || "").trim();
}

// Minimal RBAC: allow Manager/Admin/Owner
const MANAGER_ROLES = new Set(["Manager", "Admin", "Owner"]);

async function requireManager(req: Request, res: Response, next: NextFunction) {
  try {
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const { data, error } = await supa
      .from("reps")
      .select("tier")
      .eq("id", userId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    const tier = String((data as any)?.tier || "");

    if (!MANAGER_ROLES.has(tier)) {
      return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
    }

    (req as any).authUserId = userId;
    next();
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "manager_gate_failed" });
  }
}

export function assignmentsRoutes() {
  const r = Router();

  const SELECT_FIELDS =
    "id,rep_id,manager_id,type,target_id,title,status,due_at,created_at,completed_at,completed_by";

  // Rep view: list my assignments (or manager can pass ?repId=)
  // GET /v1/assignments?repId=<uuid>
  r.get("/", async (req: Request, res: Response) => {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const userId = getUserId(req);
    const repId = String((req.query.repId as string) || userId || "").trim();
    if (!repId) return res.status(401).json({ ok: false, error: "missing_user" });

    // MVP: if you pass repId (manager view), we still allow for now.
    const { data, error } = await supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .eq("rep_id", repId)
      .order("created_at", { ascending: false });

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, repId, assignments: data || [] });
  });



  // Create assignment (manager only)
  // POST /v1/assignments
  // Body: { rep_id, type, target_id?, title?, due_at? }
  r.post("/", requireManager, async (req: Request, res: Response) => {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const managerId = String((req as any).authUserId || "");
    const { rep_id, type, target_id, title, due_at } = req.body ?? {};

    if (!rep_id || typeof rep_id !== "string") {
      return res.status(400).json({ ok: false, error: "rep_id_required" });
    }
    if (!type || typeof type !== "string" || !["call_review", "sparring", "custom"].includes(type)) {
      return res.status(400).json({ ok: false, error: "invalid_type" });
    }
    if (title !== undefined && typeof title !== "string") {
      return res.status(400).json({ ok: false, error: "title_must_be_string" });
    }

    const payload: any = {
      rep_id,
      manager_id: managerId,
      type,
      title: typeof title === "string" ? title : "",
      status: "assigned",
    };

    if (target_id !== undefined) payload.target_id = target_id;
    if (due_at !== undefined) payload.due_at = due_at;

    const { data, error } = await supa
      .from("assignments")
      .insert(payload)
      .select(SELECT_FIELDS)
      .single();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, assignment: data });
  });

  // PATCH /v1/assignments/:id/complete
  // Rep completes their own assignment (MVP manual done)
  // Later: system completion uses completed_by='system'
  r.patch("/:id/complete", async (req: Request, res: Response) => {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const userId = String(req.header("x-user-id") || "").trim();
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "missing_id" });

    // Only the assignee can complete
    const { data: row, error: findErr } = await supa
      .from("assignments")
      .select("id, rep_id, status")
      .eq("id", id)
      .maybeSingle();

    if (findErr) return res.status(500).json({ ok: false, error: findErr.message });
    if (!row) return res.status(404).json({ ok: false, error: "not_found" });
    if (String((row as any).rep_id) !== userId) {
      return res.status(403).json({ ok: false, error: "forbidden_not_owner" });
    }

    const { data, error } = await supa
      .from("assignments")
      .update({
        status: "completed",
        completed_at: new Date().toISOString(),
        completed_by: "rep",
      })
      .eq("id", id)
      .select(SELECT_FIELDS)
      .single();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, assignment: data });
  });

  // GET /v1/assignments/manager?rep_id=&status=
  // Manager views assignments they created (optional filters)
  r.get("/manager", requireManager, async (req: Request, res: Response) => {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const managerId = String((req as any).authUserId || "");
    const repId = String((req.query.rep_id as string) || "").trim();
    const status = String((req.query.status as string) || "").trim();

    let q = supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .eq("manager_id", managerId)
      .order("created_at", { ascending: false });

    if (repId) q = q.eq("rep_id", repId);
    if (status) q = q.eq("status", status);

    const { data, error } = await q;

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, managerId, assignments: data || [] });
  });

  // GET /v1/assignments/manager/signals
  // Lightweight manager signals: overdue, completion rate, stale reps
  r.get("/manager/signals", requireManager, async (req: Request, res: Response) => {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });

    const managerId = String((req as any).authUserId || "");
    const now = new Date();
    const nowIso = now.toISOString();
    const sevenDaysAgoIso = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();

    // Pull a lean set of rows for this manager (last 30d is enough for MVP signals)
    const thirtyDaysAgoIso = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString();

    const { data: rows, error } = await supa
      .from("assignments")
      .select("rep_id,status,due_at,created_at,completed_at")
      .eq("manager_id", managerId)
      .gte("created_at", thirtyDaysAgoIso)
      .order("created_at", { ascending: false });

    if (error) return res.status(500).json({ ok: false, error: error.message });

    const list = (rows || []) as any[];

    // Overdue = assigned and due_at in the past
    const overdue = list.filter((a) => {
      const st = String(a.status || "");
      if (st === "completed") return false;
      if (!a.due_at) return false;
      return String(a.due_at) < nowIso;
    }).length;

    // Completion rate (last 7d): completed / assigned created within 7d
    const created7d = list.filter((a) => String(a.created_at || "") >= sevenDaysAgoIso);
    const assigned_7d = created7d.length;
    const completed_7d = created7d.filter((a) => String(a.status || "") === "completed").length;
    const completion_rate_7d = assigned_7d > 0 ? Number((completed_7d / assigned_7d).toFixed(2)) : 0;

    // Stale reps = reps with at least 1 assignment in last 30d and 0 completions in last 7d
    const repsTouched = Array.from(new Set(list.map((a) => String(a.rep_id || "")).filter(Boolean)));

    const completedByRepLast7d = new Set(
      list
        .filter((a) => String(a.status || "") === "completed" && String(a.completed_at || "") >= sevenDaysAgoIso)
        .map((a) => String(a.rep_id || ""))
        .filter(Boolean)
    );

    const staleRepIds = repsTouched.filter((rid) => !completedByRepLast7d.has(rid));

    // Optional: enrich stale reps with names (best-effort)
    let stale_reps_7d: Array<{ rep_id: string; name: string | null; tier: string | null }> = staleRepIds.map((id) => ({
      rep_id: id,
      name: null,
      tier: null,
    }));

    if (staleRepIds.length > 0) {
      const { data: reps, error: repsErr } = await supa
        .from("reps")
        .select("id,name,tier")
        .in("id", staleRepIds);

      if (!repsErr && reps) {
        const byId = new Map<string, any>(reps.map((r: any) => [String(r.id), r]));
        stale_reps_7d = staleRepIds.map((id) => {
          const rr = byId.get(id);
          return {
            rep_id: id,
            name: rr ? String(rr.name || "") : null,
            tier: rr ? String(rr.tier || "") : null,
          };
        });
      }
    }

    return res.json({
      ok: true,
      managerId,
      signals: {
        overdue,
        assigned_7d,
        completed_7d,
        completion_rate_7d,
        stale_reps_7d,
      },
    });
  });

  return r;
}