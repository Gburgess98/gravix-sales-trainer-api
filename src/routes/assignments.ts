// src/routes/assignments.ts
import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import { createClient } from "@supabase/supabase-js";
import crypto from "crypto";
import {
  isCompanyManager,
  isOfficeManager,
  canAccessOffice,
  canAccessCompany,
  type UserContext,
} from "../lib/permissions.ts";
import { logAuditEvent } from "../lib/audit";

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

// 🔥 FAILURE ESCALATION FUNCTION (Day 66)
function getFailureMultiplier(failureCount: number) {
  if (!failureCount || failureCount <= 1) return 1;

  if (failureCount === 2) return 1.3;   // repeated
  if (failureCount === 3) return 1.7;   // concerning
  if (failureCount >= 4) return 2.2;    // 🔥 major issue

  return 1;
}

function calculateWeaknessPriority(args: {
  section: string;
  severity: "low" | "critical";
  failureCount: number;
  lastFailedAt?: string | null;
}) {
  const SECTION_WEIGHTS: Record<string, number> = {
    close: 10,        // 🔥 revenue critical
    objection: 7,     // high impact
    discovery: 5,     // mid impact
    intro: 2,         // low impact
  };

  const SEVERITY_WEIGHTS = {
    critical: 5,
    low: 2,
  };

  const sectionScore = SECTION_WEIGHTS[args.section] || 1;
  const severityScore = SEVERITY_WEIGHTS[args.severity] || 1;

  const frequencyScore = args.failureCount || 0;

  let recencyScore = 0;
  if (args.lastFailedAt) {
    const diff = Date.now() - new Date(args.lastFailedAt).getTime();
    if (diff < 24 * 60 * 60 * 1000) recencyScore = 3;
    else if (diff < 72 * 60 * 60 * 1000) recencyScore = 1;
  }

  const businessImpactMultiplier =
    args.section === "close" ? 1.5 :
      args.section === "objection" ? 1.2 :
        1;

  const failureMultiplier = getFailureMultiplier(args.failureCount || 1);

  return (
    ((sectionScore * severityScore * businessImpactMultiplier) * failureMultiplier) +
    (frequencyScore * 2) +
    recencyScore
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

// 🔥 DRILL MEMORY AGGREGATION
async function getRepSectionMemory(supa: any, repId: string) {
  const { data, error } = await supa
    .from("crm_activities")
    .select("meta, created_at")
    .eq("type", "drill_memory")
    .eq("rep_id", repId)
    .limit(2000);

  if (error) throw error;

  const memory: Record<string, any> = {};

  for (const row of data || []) {
    const m = row.meta || {};
    const section = m.section || "general";

    if (!memory[section]) {
      memory[section] = {
        attempts: 0,
        successes: 0,
        last_completed_at: null,
      };
    }

    memory[section].attempts++;

    if (m.completed) {
      memory[section].successes++;
      memory[section].last_completed_at = row.created_at;
    }
  }

  return memory;
}

// 🔥 REP WEAKNESS RANKING (Day 66)
async function getRepWeaknessRanking(supa: any, repId: string) {
  const { data, error } = await supa
    .from("crm_activities")
    .select("meta, created_at")
    .eq("type", "review_flag")
    .eq("rep_id", repId)
    .limit(2000);

  if (error) throw error;

  const bySection: Record<string, any> = {};

  for (const row of data || []) {
    const m = row.meta || {};
    const section = m.flag_section || "general";
    const severity = m.flag_severity || "low";

    if (!bySection[section]) {
      bySection[section] = {
        section,
        failures: 0,
        critical: 0,
        last_failed_at: row.created_at,
      };
    }

    bySection[section].failures++;

    if (severity === "critical") {
      bySection[section].critical++;
    }

    if (new Date(row.created_at) > new Date(bySection[section].last_failed_at)) {
      bySection[section].last_failed_at = row.created_at;
    }
  }

  const ranked = Object.values(bySection)
    .map((s: any) => ({
      ...s,
      priority: calculateWeaknessPriority({
        section: s.section,
        severity: s.critical > 0 ? "critical" : "low",
        failureCount: s.failures,
        lastFailedAt: s.last_failed_at,
      }),
    }))
    .sort((a: any, b: any) => b.priority - a.priority);

  return ranked.slice(0, 3);
}

async function getManagerForRep(supa: any, repId: string) {
  const { data: user } = await supa
    .from("users")
    .select("manager_id, office_id, company_id")
    .eq("id", repId)
    .maybeSingle();

  if (!user?.manager_id) {
    return {
      manager: null,
      office_id: user?.office_id || null,
      company_id: user?.company_id || null,
    };
  }

  const { data: manager } = await supa
    .from("users")
    .select("id, email, role, office_id, company_id")
    .eq("id", user.manager_id)
    .maybeSingle();

  return {
    manager: manager || null,
    office_id: user?.office_id || null,
    company_id: user?.company_id || null,
  };
}


async function getUserHierarchy(supa: any, userId: string) {
  const { data } = await supa
    .from("users")
    .select("office_id, company_id")
    .eq("id", userId)
    .maybeSingle();

  return {
    office_id: data?.office_id || null,
    company_id: data?.company_id || null,
  };
}

async function getUserContext(supa: any, userId: string): Promise<UserContext | null> {
  const { data } = await supa
    .from("users")
    .select("id, role, office_id, company_id, is_admin")
    .eq("id", userId)
    .maybeSingle();

  if (!data) return null;

  return {
    id: String(data.id),
    role: String(data.role || "rep"),
    office_id: data.office_id || null,
    company_id: data.company_id || null,
    is_admin: Boolean(data.is_admin),
  };
}

// 🔥 IMPROVEMENT SCORE
function getImprovementScore(sectionMemory: any) {
  if (!sectionMemory) return 0;

  const { attempts, successes } = sectionMemory;
  if (!attempts) return 0;

  return successes / attempts;
}

// 🔥 AUTO DIFFICULTY ENGINE
function getNextDifficulty(sectionMemory: any) {
  const score = getImprovementScore(sectionMemory);

  if (score >= 0.8) return "brutal";
  if (score >= 0.5) return "hard";
  if (score >= 0.25) return "medium";
  return "easy";
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

function parseTargetIds(value: unknown): string[] {
  const raw = String(value || "").trim();
  if (!raw) return [];
  return Array.from(
    new Set(
      raw
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean)
    )
  ).slice(0, 100);
}

// Day 155 — merge a whitelisted sparring completion proof into existing assignment
// meta without replacing it. Returns the merged meta, or null when there is no
// valid proof to persist (so the caller leaves meta untouched). Only known keys
// are copied across; unknown keys are ignored.
export function mergeCompletionProof(
  existingMeta: unknown,
  proof: unknown
): Record<string, any> | null {
  if (!proof || typeof proof !== "object") return null;
  const p = proof as Record<string, any>;
  const sessionId =
    typeof p.matched_sparring_session_id === "string" ? p.matched_sparring_session_id.trim() : "";
  if (!sessionId) return null;

  const merged: Record<string, any> =
    existingMeta && typeof existingMeta === "object" ? { ...(existingMeta as Record<string, any>) } : {};

  merged.completed_via = "sparring_session_match";
  merged.matched_sparring_session_id = sessionId;
  if (typeof p.completion_score === "number" && Number.isFinite(p.completion_score)) {
    merged.completion_score = p.completion_score;
  }
  if (typeof p.completed_session_at === "string" && p.completed_session_at.trim()) {
    merged.completed_session_at = p.completed_session_at.trim();
  }
  merged.completed_from_dashboard = p.completed_from_dashboard === true;
  return merged;
}

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
    "id,rep_id,manager_id,type,target_id,title,status,due_at,created_at,completed_at,completed_by,source,meta";

  function normalizeAssignment(row: any) {
    const meta = row && typeof row.meta === "object" && row.meta ? row.meta : null;
    const thresholdBand =
      typeof row?.threshold_band === "string"
        ? String(row.threshold_band)
        : typeof meta?.threshold_band === "string"
          ? String(meta.threshold_band)
          : null;
    const needsManagerReview =
      typeof row?.needs_manager_review === "boolean"
        ? Boolean(row.needs_manager_review)
        : Boolean(meta?.needs_manager_review);
    const flaggedCall =
      typeof row?.flagged_call === "boolean"
        ? Boolean(row.flagged_call)
        : Boolean(meta?.flagged_call);

    return {
      id: String(row?.id || ""),
      rep_id: row?.rep_id ? String(row.rep_id) : null,
      manager_id: row?.manager_id ? String(row.manager_id) : null,
      type: row?.type ? String(row.type) : null,
      target_id: row?.target_id ? String(row.target_id) : null,
      title: row?.title ? String(row.title) : "",
      status: row?.status ? String(row.status) : "assigned",
      due_at: row?.due_at ?? null,
      created_at: row?.created_at ?? null,
      completed_at: row?.completed_at ?? null,
      completed_by: row?.completed_by ?? null,

      // 🔥 CORE SOURCE TRACKING
      source: row?.source
        ? String(row.source)
        : (typeof meta?.assignment_origin === "string"
          ? String(meta.assignment_origin)
          : null),

      meta,

      // 🔥 FLAG + SCORING SIGNALS
      flagged_call: flaggedCall,
      threshold_band: thresholdBand,
      needs_manager_review: needsManagerReview,

      // 🔥 NEW — ANALYTICS FIELDS (CRITICAL)
      assignment_origin:
        typeof meta?.assignment_origin === "string"
          ? String(meta.assignment_origin)
          : null,

      flag_section:
        typeof meta?.flag_section === "string"
          ? String(meta.flag_section)
          : null,

      score_before:
        typeof meta?.score_before === "number"
          ? meta.score_before
          : null,
    };
  }

  function sortOpenAssignments(rows: any[]) {
    return rows.slice().sort((a, b) => {
      const aCritical = a?.needs_manager_review || a?.threshold_band === "critical" ? 1 : 0;
      const bCritical = b?.needs_manager_review || b?.threshold_band === "critical" ? 1 : 0;
      if (aCritical !== bCritical) return bCritical - aCritical;

      const aFlagged = a?.flagged_call || a?.threshold_band ? 1 : 0;
      const bFlagged = b?.flagged_call || b?.threshold_band ? 1 : 0;
      if (aFlagged !== bFlagged) return bFlagged - aFlagged;

      const aDue = a?.due_at ? new Date(a.due_at).getTime() : Number.MAX_SAFE_INTEGER;
      const bDue = b?.due_at ? new Date(b.due_at).getTime() : Number.MAX_SAFE_INTEGER;
      if (aDue !== bDue) return aDue - bDue;

      const aCreated = a?.created_at ? new Date(a.created_at).getTime() : 0;
      const bCreated = b?.created_at ? new Date(b.created_at).getTime() : 0;
      return bCreated - aCreated;
    });
  }

  function buildAssignmentsSummary(rows: any[]) {
    const now = Date.now();
    const normalized = rows.map(normalizeAssignment);
    const open = normalized.filter((r) => String(r.status || "").toLowerCase() !== "completed");
    const completed = normalized.filter((r) => String(r.status || "").toLowerCase() === "completed");
    const overdue = open.filter((r) => {
      if (!r.due_at) return false;
      const t = new Date(String(r.due_at)).getTime();
      return Number.isFinite(t) && t < now;
    });

    const dueToday = open.filter((r) => {
      if (!r.due_at) return false;
      const d = new Date(String(r.due_at));
      if (Number.isNaN(d.getTime())) return false;
      const today = new Date();
      return (
        d.getFullYear() === today.getFullYear() &&
        d.getMonth() === today.getMonth() &&
        d.getDate() === today.getDate()
      );
    });

    const flagged = normalized.filter(
      (r) => Boolean(r.flagged_call) || Boolean(r.threshold_band) || Boolean(r.needs_manager_review)
    );
    const critical = normalized.filter(
      (r) => r.threshold_band === "critical" || Boolean(r.needs_manager_review)
    );

    const todayFocus = sortOpenAssignments(open)[0] || null;

    return {
      total: normalized.length,
      open: open.length,
      completed: completed.length,
      overdue: overdue.length,

      flagged: flagged.length,
      critical: critical.length,

      open_count: open.length,
      completed_count: completed.length,
      overdue_count: overdue.length,
      due_today_count: dueToday.length,

      flagged_count: flagged.length,
      critical_count: critical.length,

      auto_created_count: normalized.filter(r => String(r.source || '').toLowerCase() === 'flagged_call_auto').length,
      manual_created_count: normalized.filter(r => String(r.source || '').toLowerCase() !== 'flagged_call_auto').length,

      completion_rate: normalized.length
        ? Math.round((completed.length / normalized.length) * 100)
        : 0,

      today_focus: todayFocus,
    };
  }

  // GET /v1/assignments/by-target?target_ids=<id1,id2,...>&type=call_review
  // Batch lookup used by the call library to decorate cards with assignment state.
  // Manager sees assignments they created. Rep sees assignments assigned to them.
  r.get("/by-target", async (req: Request, res: Response) => {
    try {
      const supa = getSupaAdmin();

      const userId = getUserId(req);
      if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

      const targetIds = parseTargetIds(req.query.target_ids);
      if (targetIds.length === 0) {
        return res.json({ ok: true, items: [], assignments: [], summary: buildAssignmentsSummary([]), today_focus: null });
      }

      const type = String((req.query.type as string) || "call_review").trim().toLowerCase() || "call_review";
      const manager = await isManagerUser(userId);

      const managerContext = manager
        ? await getUserContext(supa, userId)
        : null;

      let q = supa
        .from("assignments")
        .select("id,target_id,status,rep_id,manager_id,type,created_at,completed_at,due_at")
        .in("target_id", targetIds)
        .eq("type", type)
        .order("created_at", { ascending: false });

      if (manager && managerContext) {
        if (isOfficeManager(managerContext)) {
          q = q.eq("office_id", managerContext.office_id);
        }

        if (isCompanyManager(managerContext)) {
          q = q.eq("company_id", managerContext.company_id);
        }
      } else {
        q = q.eq("rep_id", userId);
      }

      const { data, error } = await q;
      if (error) return res.status(500).json({ ok: false, error: error.message });

      const grouped = new Map<string, any[]>();
      for (const row of (data || []) as any[]) {
        const key = String(row?.target_id || "").trim();
        if (!key) continue;
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key)!.push(row);
      }

      const items = targetIds.map((target_id) => {
        const rows = grouped.get(target_id) || [];
        const open = rows.find((r) => String(r?.status || "").toLowerCase() !== "completed") || null;
        const completed = rows.find((r) => String(r?.status || "").toLowerCase() === "completed") || null;
        const latest = rows[0] || null;

        return {
          target_id,
          has_assignment: rows.length > 0,
          has_open: Boolean(open),
          has_completed: Boolean(completed),
          status: open
            ? "assigned"
            : completed
              ? "completed"
              : null,
          latest_assignment_id: latest ? String(latest.id || "") : null,
          open_assignment_id: open ? String(open.id || "") : null,
          completed_assignment_id: completed ? String(completed.id || "") : null,
        };
      });

      return res.json({
        ok: true,
        items,
        assignments: items,
        summary: buildAssignmentsSummary([]),
        today_focus: null,
      });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "assignments_by_target_failed" });
    }
  });

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
      const managerContext = await getUserContext(supa, managerId);

      if (managerContext) {
        if (isOfficeManager(managerContext)) {
          q = q.eq("office_id", managerContext.office_id);
        }

        if (isCompanyManager(managerContext)) {
          q = q.eq("company_id", managerContext.company_id);
        }
      }
    }

    const { data, error } = await q;

    if (error) return res.status(500).json({ ok: false, error: error.message });

    const items = (data || []).map(normalizeAssignment);
    const summary = buildAssignmentsSummary(items);

    return res.json({
      ok: true,
      repId,
      items,
      assignments: items,
      summary,
      today_focus: summary.today_focus,
    });
  });

  // GET /v1/assignments/summary?repId=<uuid>
  // Unified lightweight summary for rep dashboard and assignment widgets.
  r.get("/summary", async (req: Request, res: Response) => {
    const supa = getSupaAdmin();

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const repIdFromQuery = String((req.query.repId as string) || "").trim();
    const repId = repIdFromQuery || userId;

    if (repId !== userId) {
      try {
        const ok = await isManagerUser(userId);
        if (!ok) {
          return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
        }
        (req as any).authUserId = userId;
      } catch (e: any) {
        return res.status(500).json({ ok: false, error: e?.message || "manager_gate_failed" });
      }
    }

    let q = supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .eq("rep_id", repId)
      .order("created_at", { ascending: false });

    if (repId !== userId) {
      const managerId = String((req as any).authUserId || "").trim();
      const managerContext = await getUserContext(supa, managerId);

      if (managerContext) {
        if (isOfficeManager(managerContext)) {
          q = q.eq("office_id", managerContext.office_id);
        }

        if (isCompanyManager(managerContext)) {
          q = q.eq("company_id", managerContext.company_id);
        }
      }
    }

    const { data, error } = await q;
    if (error) return res.status(500).json({ ok: false, error: error.message });

    const items = (data || []).map(normalizeAssignment);
    const summary = buildAssignmentsSummary(items);

    return res.json({
      ok: true,
      repId,
      summary,
      today_focus: summary.today_focus,
      items,
      assignments: items,
    });
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
    const {
      rep_id,
      type,
      target_id,
      target,
      title,
      due_at,
      source,
      notes,
      meta,
    } = req.body ?? {};

    if (!rep_id || typeof rep_id !== "string") {
      return res.status(400).json({ ok: false, error: "rep_id_required" });
    }
    if (
      !type ||
      typeof type !== "string" ||
      ![
        "call_review",
        "sparring",
        "custom",
        "drill",
        "replay",
      ].includes(type)
    ) {
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

    if (target !== undefined && target !== null && typeof target !== "string") {
      return res.status(400).json({ ok: false, error: "target_must_be_string" });
    }

    if (notes !== undefined && notes !== null && typeof notes !== "string") {
      return res.status(400).json({ ok: false, error: "notes_must_be_string" });
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

    const safeMeta = typeof meta === "object" && meta ? meta : {};

    const cleanNotes =
      typeof notes === "string"
        ? notes.trim().slice(0, 4000)
        : null;

    const cleanTarget =
      typeof target === "string"
        ? target.trim().slice(0, 1000)
        : null;

    // 🧠 ADVANCED MEMORY-BASED DIFFICULTY
    let difficulty = "easy";
    let sectionMemory: any = null;

    try {
      const repMemory = await getRepSectionMemory(supa, rep_id);
      sectionMemory = repMemory[safeMeta?.flag_section || "general"];

      difficulty = getNextDifficulty(sectionMemory);
    } catch (e) {
      console.log("[memory.fetch.failed]", e);
    }

    // 🔥 STEP 2 — RANK WEAKNESSES (Day 66)
    let rankedWeaknesses: any[] = [];

    try {
      rankedWeaknesses = await getRepWeaknessRanking(supa, rep_id);
    } catch (e) {
      console.log("[weakness.ranking.failed]", e);
    }

    if (!rankedWeaknesses.length) {
      rankedWeaknesses = [
        {
          section: safeMeta?.flag_section || "general",
          severity:
            safeMeta?.threshold_band === "critical" ? "critical" : "low",
          failureCount: safeMeta?.failure_count || 1,
          lastFailedAt: new Date().toISOString(),
          priority: 1,
        },
      ];
    }

    const primaryWeakness = rankedWeaknesses[0];

    // 🔥 STEP 1 — Derive drill type from section
    const sectionForDrill = primaryWeakness?.section || safeMeta?.flag_section;

    const drillType =
      sectionForDrill === "objection"
        ? "objection_handling"
        : sectionForDrill === "close"
          ? "closing"
          : sectionForDrill === "discovery"
            ? "discovery"
            : "general";

    // 🔥 STEP 2 — Build uniqueness key (prevents duplicate drills)
    const uniquenessKey = [
      rep_id,
      safeMeta?.flag_section || "general",
      drillType,
    ].join(":");

    // 🔥 STEP 3 — Check for existing active assignment (same weakness)
    const { data: existing } = await supa
      .from("assignments")
      .select("id,status,meta")
      .eq("rep_id", rep_id)
      .neq("status", "completed")
      .limit(20);

    const alreadyExists = (existing || []).some((a: any) => {
      const m = a.meta || {};
      return (
        m?.flag_section === safeMeta?.flag_section &&
        m?.drill_type === drillType
      );
    });

    if (alreadyExists) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "duplicate_active_drill",
      });
    }

    // 🔥 STEP 4 — Final payload
    const managerContext = await getManagerForRep(supa, rep_id);
    const manager = managerContext?.manager;

    const hierarchy = await getUserHierarchy(supa, rep_id);

    const repOfficeId = hierarchy?.office_id || null;
    const repCompanyId = hierarchy?.company_id || null;

    if (!repOfficeId) {
      return res.status(400).json({
        ok: false,
        error: "rep_missing_office",
      });
    }

    if (!repCompanyId) {
      return res.status(400).json({
        ok: false,
        error: "rep_missing_company",
      });
    }

    const payload: any = {
      rep_id,
      manager_id: manager?.id || managerId,
      office_id: repOfficeId,
      company_id: repCompanyId,
      type,
      title: cleanTitle,
      due_at: cleanDueAt,
      status: "assigned",
      source: source || safeMeta?.assignment_origin || "manual",
      meta: {
        ...safeMeta,
        // CORE
        assignment_origin: safeMeta?.assignment_origin || source || "manual",
        flag_section: safeMeta?.flag_section || null,
        score_before: safeMeta?.score_before ?? null,
        // 🔥 DRILL INTELLIGENCE
        drill_type: drillType,
        uniqueness_key: uniquenessKey,
        // FLAGS
        threshold_band: safeMeta?.threshold_band ?? null,
        needs_manager_review: safeMeta?.needs_manager_review ?? false,
        flagged_call: safeMeta?.flagged_call ?? false,

        // 🧠 MEMORY TRACKING
        failure_count:
          (safeMeta?.failure_count ?? 1) +
          (sectionMemory?.attempts || 0),
        last_failure_at: new Date().toISOString(),
        difficulty,

        // 🎯 SPARRING FEED
        sparring_context: {
          section: safeMeta?.flag_section || "general",
          difficulty,
          scenario_seed: `${safeMeta?.flag_section || "general"}_${Date.now()}`
        },

        // 🚀 COACHING ASSIGNMENT INTELLIGENCE
        coaching_notes: cleanNotes,
        coaching_target: cleanTarget,
        coaching_assignment_type: type,

        replay_context:
          type === "replay"
            ? {
              replay_call_id: cleanTarget,
              replay_reason:
                safeMeta?.replay_reason ||
                "Replay failed sales moment",
            }
            : null,

        drill_context:
          type === "drill"
            ? {
              drill_name: cleanTarget,
              drill_focus:
                safeMeta?.flag_section || "general",
              adaptive_difficulty: difficulty,
            }
            : null,
      },
    };

    if (target_id !== undefined) payload.target_id = target_id;
    if (cleanTarget && !payload.target_id) {
      payload.target_id = cleanTarget;
    }
    if (cleanDueAt) payload.due_at = cleanDueAt;

    const { data, error } = await supa
      .from("assignments")
      .insert(payload)
      .select(SELECT_FIELDS)
      .single();

    if (error) return res.status(500).json({ ok: false, error: error.message });

    // Day 95: audit trail for review-driven coaching (fail-soft).
    const assignmentOrigin = String(payload?.meta?.assignment_origin || payload?.source || "");
    if (type === "call_review" && assignmentOrigin === "manager_review") {
      void logAuditEvent({
        actorUserId: managerId,
        targetUserId: rep_id,
        action: "manager.coaching_assigned_from_call",
        entityType: "assignment",
        entityId: String((data as any)?.id || ""),
        metadata: {
          rep_id,
          call_id: payload?.target_id ?? safeMeta?.source_call_id ?? null,
          flag_section: safeMeta?.flag_section ?? null,
          priority: safeMeta?.priority ?? null,
          company_id: repCompanyId,
          office_id: repOfficeId,
        },
      });
    }

    // 🔥 FAILURE TRACKING (CRITICAL)
    try {
      const section = payload?.meta?.flag_section || "general";

      await supa.from("crm_activities").insert({
        type: "drill_memory",
        rep_id: rep_id,
        meta: {
          section,
          completed: false,
          created_from_assignment: true,
          assignment_id: (data as any)?.id,
          created_at: new Date().toISOString(),
        },
      });
    } catch (e) {
      console.log("[memory.failure.track.failed]", e);
    }

    console.log("[assign.lifecycle]", {
      event: "created",
      assignmentId: (data as any)?.id,
      repId: (data as any)?.rep_id,
      managerId,
      type: (data as any)?.type,
      due_at: (data as any)?.due_at ?? null,
    });

    const item = normalizeAssignment(data);
    return res.json({ ok: true, item, assignment: item });
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
      const item = normalizeAssignment(current);
      return res.json({ ok: true, item, assignment: item });
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

    // MEMORY TRACKING: Insert drill_memory activity for this section
    try {
      const section = (data as any)?.meta?.flag_section;
      if (section) {
        await supa.from("crm_activities").insert({
          type: "drill_memory",
          rep_id: userId,
          meta: {
            section,
            completed: true,
            completed_at: new Date().toISOString(),
          },
        });
      }
    } catch (e) {
      console.log("[memory.update.failed]", e);
    }

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

    const item = normalizeAssignment(data);
    return res.json({ ok: true, item, assignment: item });
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

  // PATCH /v1/assignments/manager/:id
  // Manager can update assignments they created (ownership enforced)
  r.patch("/manager/:id", async (req: Request, res: Response) => {
    try {
      const id = String(req.params.id || "").trim();
      if (!id) return res.status(400).json({ ok: false, error: "missing_id" });

      const managerId = req.header("x-user-id") || "";
      if (!managerId) return res.status(401).json({ ok: false, error: "unauthorized" });

      const supa = getSupaAdmin();
      const { data: current, error: curErr } = await supa
        .from("assignments")
        .select("id, manager_id, rep_id, status, due_at, completed_at, completed_by, title, type, meta")
        .eq("id", id)
        .maybeSingle();

      if (curErr) throw curErr;
      if (!current) return res.status(404).json({ ok: false, error: "not_found" });
      if (String((current as any).manager_id || "") !== String(managerId)) {
        return res.status(403).json({ ok: false, error: "forbidden" });
      }

      const body = (req.body || {}) as {
        status?: string;
        due_at?: string | null;
        completed_at?: string | null;
        completed_by?: string | null;
        title?: string;
        completion_proof?: Record<string, any> | null;
      };

      const patch: Record<string, any> = {};

      if (typeof body.title === "string") {
        patch.title = body.title.trim();
      }

      if (Object.prototype.hasOwnProperty.call(body, "due_at")) {
        patch.due_at = body.due_at ?? null;
      }

      if (typeof body.status === "string" && body.status.trim()) {
        patch.status = body.status.trim().toLowerCase();
      }

      if (Object.prototype.hasOwnProperty.call(body, "completed_at")) {
        patch.completed_at = body.completed_at ?? null;
      }

      if (Object.prototype.hasOwnProperty.call(body, "completed_by")) {
        patch.completed_by = body.completed_by ?? null;
      }

      if (patch.status === "completed") {
        if (!Object.prototype.hasOwnProperty.call(patch, "completed_at")) {
          patch.completed_at = new Date().toISOString();
        }
        if (!Object.prototype.hasOwnProperty.call(patch, "completed_by")) {
          patch.completed_by = "manager";
        }
      }

      // Persist completion proof metadata only when completing the assignment.
      // mergeCompletionProof whitelists keys and merges into existing meta.
      if (patch.status === "completed") {
        const mergedMeta = mergeCompletionProof((current as any).meta, body.completion_proof);
        if (mergedMeta) patch.meta = mergedMeta;
      }

      const { data, error } = await supa
        .from("assignments")
        .update(patch as any)
        .eq("id", id)
        .select("id, rep_id, manager_id, type, target_id, title, status, due_at, created_at, completed_at, completed_by, source, meta")
        .maybeSingle();

      if (error) throw error;
      const item = normalizeAssignment(data);
      return res.json({ ok: true, item, assignment: item });
    } catch (e: any) {
      return res.status(400).json({ ok: false, error: e?.message || "patch_failed" });
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

    const managerContext = await getUserContext(supa, managerId);

    let q = supa
      .from("assignments")
      .select(SELECT_FIELDS)
      .order("created_at", { ascending: false })
      .order("id", { ascending: false });

    if (managerContext) {
      if (isOfficeManager(managerContext)) {
        q = q.eq("office_id", managerContext.office_id);
      }

      if (isCompanyManager(managerContext)) {
        q = q.eq("company_id", managerContext.company_id);
      }
    }

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
    const page = rows.slice(0, limit).map(normalizeAssignment);

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
      summary: buildAssignmentsSummary(page),
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
      .select("rep_id,status,due_at,created_at,completed_at,completed_by")
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

    // Completion rate (last 7d): real completions only (exclude manager overrides)
    const created7d = list.filter((a) => String(a.created_at || "") >= sevenDaysAgoIso);
    const assigned_7d = created7d.length;
    const completed_7d = created7d.filter(
      (a) =>
        String(a.status || "") === "completed" &&
        String(a.completed_by || "").toLowerCase() !== "manager"
    ).length;
    const manager_overrides_7d = created7d.filter(
      (a) =>
        String(a.status || "") === "completed" &&
        String(a.completed_by || "").toLowerCase() === "manager"
    ).length;
    const completion_rate_7d = assigned_7d > 0 ? Number((completed_7d / assigned_7d).toFixed(2)) : 0;

    // Stale reps = reps with at least 1 assignment in last 30d and 0 completions in last 7d
    const repsTouched = Array.from(new Set(list.map((a) => String(a.rep_id || "")).filter(Boolean)));

    const completedByRepLast7d = new Set(
      list
        .filter(
          (a) =>
            String(a.status || "") === "completed" &&
            String(a.completed_at || "") >= sevenDaysAgoIso &&
            String(a.completed_by || "").toLowerCase() !== "manager"
        )
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
        manager_overrides_7d,
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

      const completed24 = list.filter(
        (a) => inRange(a.completed_at, since24h) && String(a.completed_by || "").toLowerCase() !== "manager"
      );
      const completed7 = list.filter(
        (a) => inRange(a.completed_at, since7d) && String(a.completed_by || "").toLowerCase() !== "manager"
      );
      const overrides24 = list.filter(
        (a) => inRange(a.completed_at, since24h) && String(a.completed_by || "").toLowerCase() === "manager"
      );
      const overrides7 = list.filter(
        (a) => inRange(a.completed_at, since7d) && String(a.completed_by || "").toLowerCase() === "manager"
      );

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
        manager_overrides: {
          overrides_24h: overrides24.length,
          overrides_7d: overrides7.length,
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

  // GET /v1/assignments/reporting
  // Aggregated assignment reporting (rep + company level)
  r.get("/reporting", async (req: Request, res: Response) => {
    try {
      const supa = getSupaAdmin();

      const userId = getUserId(req);
      if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

      const isManager = await isManagerUser(userId);

      let q = supa
        .from("assignments")
        .select(SELECT_FIELDS)
        .order("created_at", { ascending: false })
        .limit(5000);

      if (!isManager) {
        q = q.eq("rep_id", userId);
      } else {
        const managerContext = await getUserContext(supa, userId);

        if (managerContext) {
          if (isOfficeManager(managerContext)) {
            q = q.eq("office_id", managerContext.office_id);
          }

          if (isCompanyManager(managerContext)) {
            q = q.eq("company_id", managerContext.company_id);
          }
        }
      }

      const { data, error } = await q;
      if (error) return res.status(500).json({ ok: false, error: error.message });

      const rows = (data || []).map(normalizeAssignment);

      const byRep = new Map<string, any>();

      for (const r of rows) {
        const repId = String(r.rep_id || "");
        if (!repId) continue;

        if (!byRep.has(repId)) {
          byRep.set(repId, {
            rep_id: repId,
            total: 0,
            open: 0,
            completed: 0,
            overdue: 0,
            flagged: 0,
            critical: 0,
            auto_created: 0,
            by_section: {},
          });
        }

        const item = byRep.get(repId);

        item.total++;

        if (String(r.status).toLowerCase() === "completed") item.completed++;
        else item.open++;

        if (r.due_at && new Date(r.due_at).getTime() < Date.now()) item.overdue++;

        if (r.flagged_call || r.threshold_band || r.needs_manager_review) item.flagged++;
        if (r.threshold_band === "critical" || r.needs_manager_review) item.critical++;

        if (String(r.source || "").toLowerCase() === "flagged_call_auto") item.auto_created++;
      }

      const reps = Array.from(byRep.values()).map((r) => ({
        ...r,
        completion_rate: r.total ? Math.round((r.completed / r.total) * 100) : 0,
      }));

      const company = buildAssignmentsSummary(rows);

      return res.json({
        ok: true,
        scope: isManager ? "company" : "rep",
        reps,
        company,
      });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "assignment_reporting_failed" });
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