// src/routes/assignments.ts
import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

let _supaAdmin: ReturnType<typeof createClient> | null = null;
function getSupaAdmin() {
  if (_supaAdmin) return _supaAdmin;
  _supaAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
  return _supaAdmin;
}

// Reuse the same header convention we’ve used everywhere
function getUserId(req: Request) {
  const anyReq = req as any;
  return (
    String(
      anyReq.user?.id ||
      anyReq.userId ||
      req.header("x-user-id") ||
      req.header("x-forwarded-user-id") ||
      req.header("x-gravix-user-id") ||
      ""
    ).trim()
  );
}

// Minimal RBAC: allow Manager/Admin/Owner

const MANAGER_ROLES = new Set(["Manager", "Admin", "Owner"]);

async function isManagerUser(userId: string) {
  const supa = getSupaAdmin();

  const { data, error } = await supa
    .from("reps")
    .select("tier")
    .eq("id", userId)
    .maybeSingle();

  if (error) throw error;

  const tier = String((data as any)?.tier || "");
  return MANAGER_ROLES.has(tier);
}

async function requireManager(req: Request, res: Response, next: NextFunction) {
  try {
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const supa = getSupaAdmin();

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
    const supa = getSupaAdmin();

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    // Default: rep can only see their own assignments.
    // If a different repId is requested, enforce manager RBAC.
    const repIdFromQuery = String((req.query.repId as string) || "").trim();
    const repId = repIdFromQuery || userId;

    if (repId !== userId) {
      // Require manager if attempting to view another rep
      try {
        const ok = await isManagerUser(userId);
        if (!ok) {
          return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
        }
        // Keep parity with manager flows that expect authUserId.
        (req as any).authUserId = userId;
      } catch (e: any) {
        return res.status(500).json({ ok: false, error: e?.message || "manager_gate_failed" });
      }
    }

    // If a manager is viewing another rep, only return assignments created by that manager.
    // This prevents managers from seeing other managers' assignments.
    let q = supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .eq("rep_id", repId)
      .order("created_at", { ascending: false });

    if (repId !== userId) {
      const managerId = String((req as any).authUserId || "").trim();
      if (managerId) {
        q = q.eq("manager_id", managerId);
      }
    }

    const { data, error } = await q;

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, repId, assignments: data || [] });
  });



  // Create assignment (manager only)
  // POST /v1/assignments
  // Body: { rep_id, type, target_id?, title?, due_at? }
  r.post("/", requireManager, async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const managerId = String((req as any).authUserId || "");
    if (!managerId) {
      return res.status(401).json({ ok: false, error: "missing_user" });
    }
    const { rep_id, type, target_id, title, due_at } = req.body ?? {};

    if (!rep_id || typeof rep_id !== "string") {
      return res.status(400).json({ ok: false, error: "rep_id_required" });
    }
    if (!type || typeof type !== "string" || !["call_review", "sparring", "custom"].includes(type)) {
      return res.status(400).json({ ok: false, error: "invalid_type" });
    }

    // Hard guardrail: title must be present + non-empty
    if (typeof title !== "string") {
      return res.status(400).json({ ok: false, error: "title_required" });
    }
    const cleanTitle = title.trim();
    if (cleanTitle.length < 3) {
      return res.status(400).json({ ok: false, error: "title_too_short" });
    }

    // Optional fields validation
    if (target_id !== undefined && target_id !== null && typeof target_id !== "string") {
      return res.status(400).json({ ok: false, error: "target_id_must_be_string" });
    }

    let cleanDueAt: string | null = null;
    if (due_at !== undefined && due_at !== null) {
      if (typeof due_at !== "string") {
        return res.status(400).json({ ok: false, error: "due_at_must_be_string" });
      }
      const parsed = new Date(due_at);
      if (Number.isNaN(parsed.getTime())) {
        return res.status(400).json({ ok: false, error: "invalid_due_at" });
      }
      cleanDueAt = parsed.toISOString();
    }

    const payload: any = {
      rep_id,
      manager_id: managerId,
      type,
      title: cleanTitle,
      status: "assigned",
    };

    if (target_id !== undefined) payload.target_id = target_id;
    if (cleanDueAt) payload.due_at = cleanDueAt;

    const { data, error } = await supa
      .from("assignments")
      .insert(payload)
      .select(SELECT_FIELDS)
      .single();

    if (error) return res.status(500).json({ ok: false, error: error.message });

    console.log("[assign.lifecycle]", {
      event: "created",
      assignmentId: (data as any)?.id,
      repId: (data as any)?.rep_id,
      managerId,
      type: (data as any)?.type,
      due_at: (data as any)?.due_at ?? null,
    });

    return res.json({ ok: true, assignment: data });
  });

  // PATCH /v1/assignments/:id/complete
  // Rep completes their own assignment (MVP manual done)
  // System auto-completion uses completed_by='system'
  r.patch("/:id/complete", async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "missing_id" });

    // Only the assignee can complete
    const { data: row, error: findErr } = await supa
      .from("assignments")
      .select("id, rep_id, type, status, completed_at")
      .eq("id", id)
      .maybeSingle();

    if (findErr) return res.status(500).json({ ok: false, error: findErr.message });
    if (!row) return res.status(404).json({ ok: false, error: "not_found" });
    if (String((row as any).rep_id) !== userId) {
      return res.status(403).json({ ok: false, error: "forbidden_not_owner" });
    }

    // Prevent double-complete (idempotent): if already completed, return the latest row
    const alreadyCompleted =
      String((row as any).status || "") === "completed" ||
      Boolean((row as any).completed_at);

    if (alreadyCompleted) {
      const { data: current, error: curErr } = await supa
        .from("assignments")
        .select(SELECT_FIELDS)
        .eq("id", id)
        .maybeSingle();

      if (curErr) return res.status(500).json({ ok: false, error: curErr.message });
      if (!current) return res.status(404).json({ ok: false, error: "not_found" });
      return res.json({ ok: true, assignment: current });
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

    // -------------------------------
    // XP (best-effort; never block completion)
    // -------------------------------
    // Default XP values (tweak later):
    // custom=10, call_review=25, sparring=50
    try {
      const t = String((row as any)?.type || "");
      const xp = t === "sparring" ? 50 : t === "call_review" ? 25 : 10;

      // If your rep_xp_events table doesn't have assignment_id, remove it (see note below).
      await supa.from("rep_xp_events").insert({
        rep_id: userId,
        xp,
        source: "assignment_complete",
        assignment_id: id,
        created_at: new Date().toISOString(),
      } as any);

      console.log("[xp.event]", {
        event: "xp_awarded",
        repId: userId,
        xp,
        source: "assignment_complete",
        assignmentId: id,
      });
    } catch (e: any) {
      console.log("[xp.event]", {
        event: "xp_award_failed",
        repId: userId,
        source: "assignment_complete",
        assignmentId: id,
        error: e?.message || String(e),
      });
    }

    console.log("[assign.lifecycle]", {
      event: "manually_completed",
      assignmentId: id,
      repId: userId,
      completed_by: "rep",
    });

    return res.json({ ok: true, assignment: data });
  });

  // DELETE /v1/assignments/:id
  // Manager can delete assignments they created (ownership enforced)
  r.delete("/:id", requireManager, async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const managerId = String((req as any).authUserId || "");
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "missing_id" });

    const { data: row, error: findErr } = await supa
      .from("assignments")
      .select("id, rep_id, manager_id, type, status, completed_at")
      .eq("id", id)
      .maybeSingle();

    if (findErr) return res.status(500).json({ ok: false, error: findErr.message });
    if (!row) return res.status(404).json({ ok: false, error: "not_found" });

    if (!managerId || String((row as any).manager_id || "") !== managerId) {
      return res.status(403).json({ ok: false, error: "forbidden_not_owner" });
    }

    const { error } = await supa.from("assignments").delete().eq("id", id);
    if (error) return res.status(500).json({ ok: false, error: error.message });

    console.log("[assign.lifecycle]", {
      event: "deleted",
      assignmentId: id,
      managerId,
    });

    return res.json({ ok: true });
  });

  // GET /v1/assignments/manager?rep_id=&status=&limit=&cursor=
  // Manager views assignments they created (optional filters)
  // Pagination is cursor-based for scale hardening.
  // Cursor format: `${created_at}|${id}` (created_at is ISO string)
  r.get("/manager", requireManager, async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const managerId = String((req as any).authUserId || "");
    const repId = String((req.query.rep_id as string) || "").trim();
    const status = String((req.query.status as string) || "").trim();

    const rawLimit = String((req.query.limit as any) || "").trim();
    let limit = Number(rawLimit || 0);
    if (!Number.isFinite(limit) || limit <= 0) limit = 25; // safe default
    limit = Math.min(Math.max(limit, 1), 200); // clamp

    const cursor = String((req.query.cursor as any) || "").trim();
    const [cursorCreatedAt, cursorId] = cursor ? cursor.split("|") : ["", ""];

    let q = supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .eq("manager_id", managerId)
      .order("created_at", { ascending: false })
      .order("id", { ascending: false });

    if (repId) q = q.eq("rep_id", repId);
    if (status) q = q.eq("status", status);

    // Cursor-based pagination (created_at desc, id desc)
    if (cursorCreatedAt && cursorId) {
      // Fetch rows strictly "after" the cursor in the ordered list.
      // created_at < cursorCreatedAt OR (created_at = cursorCreatedAt AND id < cursorId)
      q = q.or(
        `created_at.lt.${cursorCreatedAt},and(created_at.eq.${cursorCreatedAt},id.lt.${cursorId})`
      );
    }

    // Fetch one extra row to compute nextCursor.
    const { data, error } = await q.limit(limit + 1);

    if (error) return res.status(500).json({ ok: false, error: error.message });

    const rows = (data || []) as any[];
    const page = rows.slice(0, limit);

    const last = page.length ? page[page.length - 1] : null;
    const nextCursor = rows.length > limit && last
      ? `${String(last.created_at || "")}|${String(last.id || "")}`
      : null;

    // Back-compat: keep `assignments` for existing UI, but also return `items`.
    return res.json({
      ok: true,
      managerId,
      items: page,
      assignments: page,
      nextCursor,
    });
  });

  // GET /v1/assignments/manager/signals
  // Lightweight manager signals: overdue, completion rate, stale reps
  r.get("/manager/signals", requireManager, async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

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

  // GET /v1/assignments/manager/trust
  // Compact trust payload for manager confidence UI
  r.get("/manager/trust", requireManager, async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const managerId = String((req as any).authUserId || "");
    const now = new Date();
    const nowIso = now.toISOString();
    const sevenDaysAgoIso = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();

    const { data: rows, error } = await supa
      .from("assignments")
      .select("rep_id,status,due_at,created_at,completed_at")
      .eq("manager_id", managerId);

    if (error) return res.status(500).json({ ok: false, error: error.message });

    const list = (rows || []) as any[];

    const overdue = list.filter((a) => {
      if (String(a.status) === "completed") return false;
      if (!a.due_at) return false;
      return String(a.due_at) < nowIso;
    }).length;

    const assigned7d = list.filter((a) => String(a.created_at) >= sevenDaysAgoIso).length;
    const completed7d = list.filter(
      (a) => String(a.status) === "completed" && String(a.completed_at) >= sevenDaysAgoIso
    ).length;

    const repsTouched = Array.from(
      new Set(list.map((a) => String(a.rep_id || "")).filter(Boolean))
    );

    const repsCompleted7d = new Set(
      list
        .filter((a) => String(a.status) === "completed" && String(a.completed_at) >= sevenDaysAgoIso)
        .map((a) => String(a.rep_id))
    );

    const stale = repsTouched.filter((id) => !repsCompleted7d.has(id));

    return res.json({
      ok: true,
      managerId,
      trust: {
        overdue,
        assigned_7d: assigned7d,
        completed_7d: completed7d,
        stale_reps: stale,
      },
    });
  });

  return r;
}