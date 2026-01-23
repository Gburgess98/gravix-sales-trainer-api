// src/routes/assignments.ts
import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import { createClient } from "@supabase/supabase-js";
import crypto from "crypto";

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

function formatDayMon(d: Date) {
  try {
    return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "short" }).format(d);
  } catch {
    const mm = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${String(d.getDate()).padStart(2, "0")} ${mm[d.getMonth()]}`;
  }
}

function safeTypeNudgeLabel(t: string) {
  const type = String(t || "").toLowerCase();
  if (type === "sparring") return "sparring drill";
  if (type === "call_review") return "call review";
  return "task";
}

async function sendSlackWebhook(text: string) {
  const url = String(process.env.SLACK_WEBHOOK_URL || "").trim();
  if (!url) throw new Error("missing_slack_webhook_url");

  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ text }),
  });

  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`slack_webhook_failed:${resp.status}:${body.slice(0, 200)}`);
  }
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

  // POST /v1/assignments/:id/nudge
  // Manager-only: sends a Slack nudge with context (Task, Explanation, Due date)
  r.post("/:id/nudge", requireManager, async (req: Request, res: Response) => {
    try {
      const supa = getSupaAdmin();

      const managerId = String((req as any).authUserId || "").trim();
      if (!managerId) return res.status(401).json({ ok: false, error: "missing_user" });

      const id = String(req.params.id || "").trim();
      if (!id) return res.status(400).json({ ok: false, error: "missing_id" });

      // Fetch assignment
      const { data: a, error: aErr } = await supa
        .from("assignments")
        .select("id,rep_id,manager_id,type,title,status,due_at,created_at,completed_at")
        .eq("id", id)
        .maybeSingle();

      if (aErr) return res.status(500).json({ ok: false, error: aErr.message });
      if (!a) return res.status(404).json({ ok: false, error: "not_found" });

      // Ownership guard: only creator manager can nudge
      if (String((a as any).manager_id || "") !== managerId) {
        return res.status(403).json({ ok: false, error: "forbidden_not_owner" });
      }

      const repId = String((a as any).rep_id || "").trim();
      const type = String((a as any).type || "").toLowerCase();
      const title = String((a as any).title || "").trim();
      const status = String((a as any).status || "").toLowerCase();
      const dueAtIso = (a as any).due_at ? String((a as any).due_at) : "";

      // Best-effort rep name
      let repName: string | null = null;
      try {
        const { data: rep, error: repErr } = await supa
          .from("reps")
          .select("id,name")
          .eq("id", repId)
          .maybeSingle();
        if (!repErr && rep) repName = String((rep as any).name || "").trim() || null;
      } catch {
        // ignore
      }

      // Explanation heuristic (v1)
      let explanation = "Please complete this task.";

      const now = new Date();
      const nowIso = now.toISOString();
      const isOverdue = status !== "completed" && dueAtIso && dueAtIso < nowIso;

      if (isOverdue) {
        explanation = "This task is overdue and needs attention.";
      }

      // Missed 2+ times this week: count open assignments of same type for this rep in last 7d
      try {
        const since7d = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
        const { data: rows, error: rowsErr } = await supa
          .from("assignments")
          .select("id,status,type,created_at")
          .eq("rep_id", repId)
          .eq("manager_id", managerId)
          .eq("type", type)
          .gte("created_at", since7d);

        if (!rowsErr && Array.isArray(rows)) {
          const openCount = rows.filter((r: any) => String(r.status || "").toLowerCase() !== "completed").length;
          if (openCount >= 2) {
            explanation = "You’ve missed this twice this week and it’s holding back your close rate.";
          }
        }
      } catch {
        // ignore
      }

      const taskLabel = safeTypeNudgeLabel(type);
      const taskLine = title
        ? `Complete your ${taskLabel}: “${title}”`
        : `Complete your ${taskLabel}`;

      const dueLabel = (() => {
        const d = dueAtIso ? new Date(dueAtIso) : now;
        const dayMon = formatDayMon(d);

        if (!dueAtIso) return `Today (${dayMon})`;

        const today = new Date();
        const sameDay =
          d.getFullYear() === today.getFullYear() &&
          d.getMonth() === today.getMonth() &&
          d.getDate() === today.getDate();

        return sameDay ? `Today (${dayMon})` : dayMon;
      })();

      // EXACT Slack message format (FINAL)
      const headerLine = repName
        ? `👋 Quick nudge from your manager (${repName})`
        : "👋 Quick nudge from your manager";

      const text = [
        headerLine,
        "",
        "Task:",
        taskLine,
        "",
        "Explanation:",
        explanation,
        "",
        "Due date:",
        dueLabel,
        "",
        "— Gravix Coach",
      ].join("\n");

      await sendSlackWebhook(text);

      console.log("[nudge]", {
        event: "sent",
        assignmentId: id,
        repId,
        managerId,
        via: "slack_webhook",
        digest: crypto.createHash("sha1").update(text).digest("hex").slice(0, 10),
      });

      return res.json({ ok: true, sent: "slack" });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "nudge_failed" });
    }
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
  // Compact trust payload for manager confidence UI (24h + 7d)
  r.get("/manager/trust", requireManager, async (req: Request, res: Response) => {
    try {
      const supa = getSupaAdmin();

      const managerId = String((req as any).authUserId || "");
      if (!managerId) return res.status(401).json({ ok: false, error: "missing_user" });

      const nowMs = Date.now();
      const since24h = new Date(nowMs - 24 * 60 * 60 * 1000).toISOString();
      const since7d = new Date(nowMs - 7 * 24 * 60 * 60 * 1000).toISOString();

      // Pull 7d window (plus some open items) and compute trust in-memory.
      // Keep it simple and stable (no new RPC required).
      const { data: rows, error } = await supa
        .from("assignments")
        .select("id,rep_id,type,target_id,title,status,due_at,created_at,completed_at,completed_by")
        .eq("manager_id", managerId)
        .gte("created_at", since7d)
        .order("created_at", { ascending: false });

      if (error) {
        return res.status(500).json({ ok: false, error: error.message || "trust_query_failed" });
      }

      const list = (rows || []) as any[];

      const ts = (iso: any) => {
        const t = new Date(String(iso || "")).getTime();
        return Number.isFinite(t) ? t : 0;
      };

      const inRange = (iso: any, startIso: string) => {
        const t = ts(iso);
        return t > 0 && t >= ts(startIso);
      };

      const isToday = (iso: any) => {
        const d = new Date(String(iso || ""));
        if (Number.isNaN(d.getTime())) return false;
        const n = new Date();
        return (
          d.getFullYear() === n.getFullYear() &&
          d.getMonth() === n.getMonth() &&
          d.getDate() === n.getDate()
        );
      };

      const created24 = list.filter((a) => inRange(a.created_at, since24h));
      const created7 = list;

      const completed24 = list.filter((a) => inRange(a.completed_at, since24h));
      const completed7 = list.filter((a) => inRange(a.completed_at, since7d));

      const isAuto = (a: any) => String(a.completed_by || "").toLowerCase() === "system";
      const auto24 = completed24.filter(isAuto);
      const auto7 = completed7.filter(isAuto);

      const pct = (done: number, created: number) => {
        if (!created) return 0;
        return Math.round((done / created) * 1000) / 10; // 1dp
      };

      // Stuck reasons over OPEN assignments
      const open = list.filter((a) => String(a.status || "").toLowerCase() === "assigned");

      const isPast = (iso: any) => {
        const t = ts(iso);
        return t > 0 && t < nowMs;
      };

      const reasonFor = (a: any): string => {
        const type = String(a.type || "").toLowerCase();
        const hasTarget = !!String(a.target_id || "").trim();

        if (type === "custom" && isPast(a.due_at)) return "Overdue custom task";
        if (type === "sparring") return "Sparring not completed";
        if (type === "call_review" && !hasTarget) return "No call picked";
        if (type === "call_review" && hasTarget) return "Call review not completed";
        if (isPast(a.due_at)) return "Overdue";
        return "Not completed";
      };

      const reasonCounts: Record<string, number> = {};
      for (const a of open) {
        const r = reasonFor(a);
        reasonCounts[r] = (reasonCounts[r] || 0) + 1;
      }

      const topStuckReason =
        Object.entries(reasonCounts)
          .sort((a, b) => b[1] - a[1])
          .map(([reason, count]) => ({ reason, count }))[0] || { reason: "None", count: 0 };

      // Needs help today: top 5 reps by overdue then open, then oldest completion
      const repAgg: Record<
        string,
        { rep_id: string; overdue: number; open: number; completed_today: number; last_completed_at: string | null }
      > = {};

      for (const a of list) {
        const repId = String(a.rep_id || "").trim();
        if (!repId) continue;

        if (!repAgg[repId]) {
          repAgg[repId] = {
            rep_id: repId,
            overdue: 0,
            open: 0,
            completed_today: 0,
            last_completed_at: null,
          };
        }

        if (String(a.status || "").toLowerCase() === "assigned") {
          repAgg[repId].open += 1;
          if (isPast(a.due_at)) repAgg[repId].overdue += 1;
        }

        if (a.completed_at) {
          if (isToday(a.completed_at)) repAgg[repId].completed_today += 1;

          if (!repAgg[repId].last_completed_at) repAgg[repId].last_completed_at = String(a.completed_at);
          else {
            const prev = ts(repAgg[repId].last_completed_at);
            const cur = ts(a.completed_at);
            if (cur > prev) repAgg[repId].last_completed_at = String(a.completed_at);
          }
        }
      }

      const needsHelpToday = Object.values(repAgg)
        .filter((r) => r.open > 0)
        .sort((a, b) => {
          if (b.overdue !== a.overdue) return b.overdue - a.overdue;
          if (b.open !== a.open) return b.open - a.open;
          const at = ts(a.last_completed_at);
          const bt = ts(b.last_completed_at);
          return at - bt; // oldest completion first
        })
        .slice(0, 5);

      return res.json({
        ok: true,
        managerId,
        window: { since24h, since7d },
        completion: {
          created_24h: created24.length,
          completed_24h: completed24.length,
          completion_rate_24h: pct(completed24.length, created24.length),
          created_7d: created7.length,
          completed_7d: completed7.length,
          completion_rate_7d: pct(completed7.length, created7.length),
        },
        auto_completed: {
          auto_24h: auto24.length,
          auto_7d: auto7.length,
        },
        stuck: {
          top_reason: topStuckReason,
        },
        needs_help_today: needsHelpToday,
      });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "trust_failed" });
    }
  });

  return r;
}

// -----------------------------
// Reps: tiny self endpoint
// -----------------------------

export function repsRoutes() {
  const r = Router();

  // GET /v1/reps/me
  // Returns lightweight XP summary for the currently authenticated rep.
  r.get("/me", async (req: Request, res: Response) => {
    try {
      const supa = getSupaAdmin();

      const userId = getUserId(req);
      if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

      // Total XP comes from reps.xp (safe default 0)
      const { data: rep, error: repErr } = await supa
        .from("reps")
        .select("id,name,tier,xp")
        .eq("id", userId)
        .maybeSingle();

      if (repErr) return res.status(500).json({ ok: false, error: repErr.message });

      const xp_total = Number((rep as any)?.xp) || 0;

      // XP today: sum rep_xp_events.xp since start of day (best-effort)
      let xp_today = 0;
      try {
        const start = new Date();
        start.setHours(0, 0, 0, 0);

        const { data: events, error: evErr } = await supa
          .from("rep_xp_events")
          .select("xp,created_at")
          .eq("rep_id", userId)
          .gte("created_at", start.toISOString());

        if (!evErr && Array.isArray(events)) {
          xp_today = events.reduce((sum: number, e: any) => sum + (Number(e?.xp) || 0), 0);
        }
      } catch {
        // ignore
      }

      return res.json({
        ok: true,
        rep: {
          id: String((rep as any)?.id || userId),
          name: (rep as any)?.name ?? null,
          tier: (rep as any)?.tier ?? null,
          xp_total,
          xp_today,
        },
      });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "reps_me_failed" });
    }
  });

  return r;
}