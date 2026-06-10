// src/routes/manager.ts
//
// SPRINT 4 — Manager Value Layer (Day 90)
//
// /v1/manager/* — manager-facing aggregate endpoints.
// Gate: requireManager (reps.tier). Data scope: getUserContext +
// applyHierarchyFilters (users.role hierarchy), same pattern as
// /v1/dashboard/kpis and /v1/assignments/manager.
//
// Day 90 note: "reviewed" calls are counted as scored calls until the
// call_manager_reviews table lands on Day 91.

import { Router, type Request, type Response } from "express";
import { createClient } from "@supabase/supabase-js";
import { requireManager } from "../middleware/requireManager";
import {
  isCompanyManager,
  isOfficeManager,
  type UserContext,
} from "../lib/permissions";

const router = Router();

const SUPABASE_URL = process.env.SUPABASE_URL as string;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY as string;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing – /v1/manager/* will fail.");
}

const supa = (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

// All routes require manager tier (Manager/Owner/PartnerAdmin/SuperAdmin).
router.use(requireManager);

// ── Identity / scoping helpers (same patterns as assignments.ts / dashboard.ts) ──

function getUserId(req: Request): string {
  const anyReq = req as any;
  return String(
    anyReq.user?.id ||
    anyReq.userId ||
    anyReq.authUserId ||
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

async function getUserContext(db: any, userId: string): Promise<UserContext | null> {
  if (!userId) return null;

  const { data } = await db
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

function applyHierarchyFilters(query: any, user: UserContext | null) {
  if (!user) return query;

  if (isOfficeManager(user)) {
    return query.eq("office_id", user.office_id);
  }

  if (isCompanyManager(user)) {
    return query.eq("company_id", user.company_id);
  }

  return query;
}

function isoDaysAgo(days: number) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString();
}

// ── Rule-based MVP logic (Sprint 4 thresholds) ──────────────────────────────

type SkillKey = "intro" | "discovery" | "pitch" | "objection" | "close";

const SKILL_KEYS: SkillKey[] = ["intro", "discovery", "pitch", "objection", "close"];

const SKILL_LABELS: Record<SkillKey, string> = {
  intro: "Intro",
  discovery: "Discovery",
  pitch: "Pitch",
  objection: "Objection",
  close: "Close",
};

const RECOMMENDED_ACTIONS: Record<SkillKey, string> = {
  discovery: "Assign Discovery Drill",
  objection: "Assign Objection Handling Drill",
  close: "Assign Closing Drill",
  intro: "Assign Opening Script Practice",
  pitch: "Review recent calls",
};

const REVIEW_SCORE_THRESHOLD = 70;
const CRITICAL_STAGE_THRESHOLD = 50;
const WEAK_STAGE_THRESHOLD = 70;
const MINUTES_SAVED_PER_REVIEWED_CALL = 20;

type ScoredCallRow = {
  id: string;
  user_id: string;
  filename: string | null;
  score_overall: number | null;
  analysis_json: any;
  created_at: string;
};

// Stage scores live in analysis_json.stages ({ intro: { score }, ... }) with
// rubric-shaped fallbacks for older rows.
export function extractStageScores(analysis: any): Partial<Record<SkillKey, number>> {
  const out: Partial<Record<SkillKey, number>> = {};
  const stages = analysis?.stages ?? analysis?.rubric ?? null;
  if (!stages || typeof stages !== "object") return out;

  for (const key of SKILL_KEYS) {
    const raw = (stages as any)[key];
    const score = Number(raw?.score ?? raw);
    if (Number.isFinite(score)) out[key] = score;
  }
  return out;
}

export function weakestSkillOf(stageScores: Partial<Record<SkillKey, number>>): SkillKey | null {
  let weakest: SkillKey | null = null;
  let lowest = Infinity;
  for (const key of SKILL_KEYS) {
    const score = stageScores[key];
    if (typeof score === "number" && score < lowest) {
      lowest = score;
      weakest = key;
    }
  }
  return weakest;
}

// Reasons a scored call needs manager review (empty array = does not need review).
export function reviewReasons(call: ScoredCallRow): string[] {
  const reasons: string[] = [];

  const overall = Number(call.score_overall);
  if (Number.isFinite(overall) && overall < REVIEW_SCORE_THRESHOLD) {
    reasons.push(`Score below ${REVIEW_SCORE_THRESHOLD}`);
  }

  const stages = extractStageScores(call.analysis_json);
  for (const key of SKILL_KEYS) {
    const score = stages[key];
    if (typeof score === "number" && score < CRITICAL_STAGE_THRESHOLD) {
      reasons.push(`${SKILL_LABELS[key]} below ${CRITICAL_STAGE_THRESHOLD}`);
    }
  }

  if (call.analysis_json?.needs_manager_review === true && reasons.length === 0) {
    reasons.push("Flagged for manager review");
  }

  return reasons;
}

export function needsReview(call: ScoredCallRow): boolean {
  return reviewReasons(call).length > 0;
}

// ── Manager review history (call_manager_reviews, Day 91) ───────────────────
// Fail-soft: until sql/20260610_call_manager_reviews.sql has been run in the
// Supabase SQL editor the table is missing — treat as "no reviews yet" so the
// command centre keeps working, and report availability to the caller.

function isMissingReviewTableError(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("call_manager_reviews") && (
    msg.includes("does not exist") ||
    msg.includes("could not find") ||
    msg.includes("schema cache")
  );
}

async function fetchReviewedCallIds(
  db: any,
  since: string
): Promise<{ ids: Set<string>; available: boolean }> {
  // Reviews are always created after their call, so filtering reviews by the
  // same `since` window covers every in-window call. Hierarchy scoping is
  // applied by intersecting with the already-scoped call set in the caller.
  const { data, error } = await db
    .from("call_manager_reviews")
    .select("call_id")
    .gte("created_at", since)
    .limit(5000);

  if (error) {
    if (isMissingReviewTableError(error)) return { ids: new Set(), available: false };
    throw error;
  }

  const ids = new Set<string>();
  for (const row of data ?? []) {
    const callId = String((row as any)?.call_id ?? "").trim();
    if (callId) ids.add(callId);
  }
  return { ids, available: true };
}

export function computeTeamHealthStatus(args: {
  averageScore: number | null;
  callsNeedingReview: number;
  overdueAssignments: number;
}): "green" | "amber" | "red" {
  const { averageScore, callsNeedingReview, overdueAssignments } = args;

  if (averageScore !== null && averageScore < 55) return "red";
  if (overdueAssignments > 0 && callsNeedingReview > 5) return "red";
  if (averageScore !== null && averageScore < 70) return "amber";
  if (callsNeedingReview > 0) return "amber";
  return "green";
}

export function computeRepRisk(args: {
  averageScore: number | null;
  callsBelow50: number;
  overdueAssignments: number;
}): { riskLevel: "green" | "amber" | "red"; riskReason: string } {
  const { averageScore, callsBelow50, overdueAssignments } = args;

  if (averageScore !== null && averageScore < 55) {
    return { riskLevel: "red", riskReason: "Average score below 55" };
  }
  if (callsBelow50 >= 2) {
    return { riskLevel: "red", riskReason: `${callsBelow50} calls scored below 50` };
  }
  if (overdueAssignments > 0) {
    return { riskLevel: "red", riskReason: "Overdue coaching assignments" };
  }
  if (averageScore !== null && averageScore < 70) {
    return { riskLevel: "amber", riskReason: "Average score below 70" };
  }
  return { riskLevel: "green", riskReason: "On track" };
}

export function recommendedActionFor(weakest: SkillKey | null): string {
  if (!weakest) return "Review recent calls";
  return RECOMMENDED_ACTIONS[weakest] ?? "Review recent calls";
}

function assignmentPriority(dueAt: string | null, now: number): "low" | "medium" | "high" {
  if (!dueAt) return "low";
  const dueMs = new Date(dueAt).getTime();
  if (!Number.isFinite(dueMs)) return "low";
  if (dueMs < now) return "high";
  if (dueMs < now + 24 * 3600 * 1000) return "medium";
  return "low";
}

// Day 93: prefer the manager-chosen meta.priority, fall back to due-date rule.
function resolveAssignmentPriority(
  meta: any,
  dueAt: string | null,
  now: number
): "low" | "medium" | "high" {
  const stored = String(meta?.priority || "").toLowerCase();
  if (stored === "low" || stored === "medium" || stored === "high") return stored;
  return assignmentPriority(dueAt, now);
}

// Day 93: normalised origin for assignment tracking badges.
function assignmentOrigin(
  source: string | null | undefined,
  meta: any
): { source: "manager_review" | "auto" | "manual" | "unknown"; label: string } {
  const src = String(source || meta?.assignment_origin || "").toLowerCase();
  if (src === "manager_review") return { source: "manager_review", label: "Assigned via review" };
  if (src.includes("auto")) return { source: "auto", label: "Auto-created" };
  if (src) return { source: "manual", label: "Manual assignment" };
  return { source: "unknown", label: "Unknown" };
}

// Day 93: linked source call — meta.source_call_id, else target_id for call reviews.
function assignmentSourceCallId(row: {
  type?: string | null;
  target_id?: string | null;
  meta?: any;
}): string | null {
  const fromMeta = String(row.meta?.source_call_id || "").trim();
  if (fromMeta) return fromMeta;
  if (String(row.type || "") === "call_review" && row.target_id) return String(row.target_id);
  return null;
}

// ── GET /v1/manager/command-centre ───────────────────────────────────────────

router.get("/command-centre", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const since = isoDaysAgo(days);
    const now = Date.now();

    const userContext = await getUserContext(supa, userId);

    let callsQuery = supa
      .from("calls")
      .select("id, user_id, filename, status, score_overall, analysis_json, created_at, office_id, company_id")
      .eq("status", "scored")
      .gte("created_at", since)
      .order("created_at", { ascending: false })
      .limit(2000);
    callsQuery = applyHierarchyFilters(callsQuery, userContext);

    let assignmentsQuery = supa
      .from("assignments")
      .select("id, rep_id, title, status, due_at, created_at, type, target_id, source, meta, office_id, company_id")
      .eq("status", "assigned")
      .order("due_at", { ascending: true, nullsFirst: false })
      .limit(1000);
    assignmentsQuery = applyHierarchyFilters(assignmentsQuery, userContext);

    // Day 94: previous matching window (the N days before `since`) for skill trends.
    const prevSince = isoDaysAgo(days * 2);
    let prevCallsQuery = supa
      .from("calls")
      .select("id, analysis_json, office_id, company_id")
      .eq("status", "scored")
      .gte("created_at", prevSince)
      .lt("created_at", since)
      .limit(2000);
    prevCallsQuery = applyHierarchyFilters(prevCallsQuery, userContext);

    // Day 94: completed assignments in the current window (count only).
    let completedCountQuery = supa
      .from("assignments")
      .select("id", { count: "exact", head: true })
      .eq("status", "completed")
      .gte("completed_at", since);
    completedCountQuery = applyHierarchyFilters(completedCountQuery, userContext);

    const [callsResult, assignmentsResult, reviewHistory, prevCallsResult, completedResult] =
      await Promise.all([
        callsQuery,
        assignmentsQuery,
        fetchReviewedCallIds(supa, since),
        prevCallsQuery,
        completedCountQuery,
      ]);

    if (callsResult.error) throw callsResult.error;
    if (assignmentsResult.error) throw assignmentsResult.error;
    // Trend/impact data is enrichment — fail soft so the command centre still loads.
    if (prevCallsResult.error) console.warn("[manager.command-centre] prev window query failed", prevCallsResult.error.message);
    const completedAssignments = completedResult.error ? 0 : (completedResult.count ?? 0);
    if (completedResult.error) console.warn("[manager.command-centre] completed count failed", completedResult.error.message);

    const scoredCalls = (callsResult.data ?? []) as ScoredCallRow[];
    const openAssignmentRows = (assignmentsResult.data ?? []) as Array<{
      id: string;
      rep_id: string | null;
      title: string | null;
      status: string;
      due_at: string | null;
      created_at: string;
      type: string | null;
      target_id: string | null;
      source: string | null;
      meta: any;
    }>;

    // ── Rep name lookup (best-effort, single query) ──
    const repIds = new Set<string>();
    for (const c of scoredCalls) if (c.user_id) repIds.add(String(c.user_id));
    for (const a of openAssignmentRows) if (a.rep_id) repIds.add(String(a.rep_id));

    const repNames = new Map<string, string>();
    if (repIds.size > 0) {
      const { data: repRows } = await supa
        .from("reps")
        .select("id, name")
        .in("id", Array.from(repIds));
      for (const r of repRows ?? []) {
        const name = String((r as any).name || "").trim();
        if (name) repNames.set(String((r as any).id), name);
      }
    }
    const nameOf = (id: string | null | undefined) =>
      (id && repNames.get(String(id))) || "Unknown rep";

    // ── Assignment aggregates ──
    const overdueAssignmentRows = openAssignmentRows.filter((a) => {
      const dueMs = a.due_at ? new Date(a.due_at).getTime() : NaN;
      return Number.isFinite(dueMs) && dueMs < now;
    });
    const overdueRepIds = new Set(overdueAssignmentRows.map((a) => String(a.rep_id || "")));

    // ── Per-rep call aggregates ──
    type RepAgg = {
      repId: string;
      scoreSum: number;
      scoreCount: number;
      callsBelow50: number;
      stageSums: Partial<Record<SkillKey, { sum: number; count: number }>>;
    };
    const repAggs = new Map<string, RepAgg>();
    const skillAgg = new Map<SkillKey, { sum: number; count: number; weakCount: number }>();

    let teamScoreSum = 0;
    let teamScoreCount = 0;

    for (const call of scoredCalls) {
      const repId = String(call.user_id || "");
      const overall = Number(call.score_overall);
      const hasOverall = Number.isFinite(overall);

      if (hasOverall) {
        teamScoreSum += overall;
        teamScoreCount += 1;
      }

      let agg = repAggs.get(repId);
      if (!agg) {
        agg = { repId, scoreSum: 0, scoreCount: 0, callsBelow50: 0, stageSums: {} };
        repAggs.set(repId, agg);
      }
      if (hasOverall) {
        agg.scoreSum += overall;
        agg.scoreCount += 1;
        if (overall < 50) agg.callsBelow50 += 1;
      }

      const stages = extractStageScores(call.analysis_json);
      for (const key of SKILL_KEYS) {
        const score = stages[key];
        if (typeof score !== "number") continue;

        const repStage = agg.stageSums[key] ?? { sum: 0, count: 0 };
        repStage.sum += score;
        repStage.count += 1;
        agg.stageSums[key] = repStage;

        const teamStage = skillAgg.get(key) ?? { sum: 0, count: 0, weakCount: 0 };
        teamStage.sum += score;
        teamStage.count += 1;
        if (score < WEAK_STAGE_THRESHOLD) teamStage.weakCount += 1;
        skillAgg.set(key, teamStage);
      }
    }

    const teamAverage = teamScoreCount > 0 ? Math.round(teamScoreSum / teamScoreCount) : null;

    // ── Manager review history (Day 91) ──
    // Reviewed = a row in call_manager_reviews for a call in the scoped window.
    // Intersecting with the hierarchy-scoped call set keeps tenant isolation.
    const reviewedScopedCalls = scoredCalls.filter((c) => reviewHistory.ids.has(String(c.id)));
    const reviewedCount = reviewedScopedCalls.length;

    // ── Calls needing review (unreviewed only, lowest score first, cap 10) ──
    const unreviewedNeedingReview = scoredCalls.filter(
      (c) => !reviewHistory.ids.has(String(c.id)) && needsReview(c)
    );
    const reviewCalls = unreviewedNeedingReview
      .slice()
      .sort((a, b) => Number(a.score_overall ?? 101) - Number(b.score_overall ?? 101))
      .slice(0, 10)
      .map((call) => {
        const stages = extractStageScores(call.analysis_json);
        const weakest = weakestSkillOf(stages);
        return {
          callId: String(call.id),
          repId: call.user_id ? String(call.user_id) : null,
          repName: nameOf(call.user_id),
          title: String(call.filename || "Untitled call"),
          overallScore: Number.isFinite(Number(call.score_overall)) ? Number(call.score_overall) : 0,
          weakestSkill: weakest ? SKILL_LABELS[weakest] : "Unknown",
          createdAt: String(call.created_at || ""),
        };
      });
    const callsNeedingReviewCount = unreviewedNeedingReview.length;

    // ── Reps needing attention (red first, then lowest average, cap 10) ──
    // Exclude the requesting manager's own calls from the attention list.
    const repsNeedingAttention = Array.from(repAggs.values())
      .filter((agg) => agg.repId && agg.repId !== userId)
      .map((agg) => {
        const averageScore = agg.scoreCount > 0 ? Math.round(agg.scoreSum / agg.scoreCount) : null;
        const { riskLevel, riskReason } = computeRepRisk({
          averageScore,
          callsBelow50: agg.callsBelow50,
          overdueAssignments: overdueRepIds.has(agg.repId) ? 1 : 0,
        });

        const repStageAverages: Partial<Record<SkillKey, number>> = {};
        for (const key of SKILL_KEYS) {
          const s = agg.stageSums[key];
          if (s && s.count > 0) repStageAverages[key] = s.sum / s.count;
        }
        const weakest = weakestSkillOf(repStageAverages);

        return {
          repId: agg.repId,
          repName: nameOf(agg.repId),
          averageScore: averageScore ?? 0,
          riskLevel,
          riskReason,
          recommendedAction: recommendedActionFor(weakest),
        };
      })
      .filter((rep) => rep.riskLevel !== "green")
      .sort((a, b) => {
        const rank = { red: 0, amber: 1, green: 2 } as const;
        if (rank[a.riskLevel] !== rank[b.riskLevel]) return rank[a.riskLevel] - rank[b.riskLevel];
        return a.averageScore - b.averageScore;
      })
      .slice(0, 10);

    // ── Open assignments (cap 10: overdue first, then nearest due date) ──
    const openAssignments = openAssignmentRows
      .map((a) => {
        const dueMs = a.due_at ? new Date(a.due_at).getTime() : NaN;
        const isOverdue = Number.isFinite(dueMs) && dueMs < now;
        const origin = assignmentOrigin(a.source, a.meta);
        const notes = String(a.meta?.coaching_notes || "").trim() || null;
        return {
          assignmentId: String(a.id),
          repId: a.rep_id ? String(a.rep_id) : null,
          repName: nameOf(a.rep_id),
          title: String(a.title || "Coaching assignment"),
          status: (isOverdue ? "overdue" : "open") as "open" | "overdue",
          dueAt: a.due_at ?? null,
          priority: resolveAssignmentPriority(a.meta, a.due_at, now),
          type: String(a.type || "custom"),
          source: origin.source,
          sourceCallId: assignmentSourceCallId(a),
          originLabel: origin.label,
          notes,
        };
      })
      .sort((a, b) => {
        if (a.status !== b.status) return a.status === "overdue" ? -1 : 1;
        const aDue = a.dueAt ? new Date(a.dueAt).getTime() : Infinity;
        const bDue = b.dueAt ? new Date(b.dueAt).getTime() : Infinity;
        return aDue - bDue;
      })
      .slice(0, 10);

    // ── Previous-window skill averages (Day 94 trends) ──
    const prevSkillAgg = new Map<SkillKey, { sum: number; count: number }>();
    for (const call of (prevCallsResult.data ?? []) as Array<{ analysis_json: any }>) {
      const stages = extractStageScores(call.analysis_json);
      for (const key of SKILL_KEYS) {
        const score = stages[key];
        if (typeof score !== "number") continue;
        const agg = prevSkillAgg.get(key) ?? { sum: 0, count: 0 };
        agg.sum += score;
        agg.count += 1;
        prevSkillAgg.set(key, agg);
      }
    }

    // ── Weakest skills (weak occurrences, lowest average first) ──
    // Trend: vs the previous matching window. Lower score = weaker, so
    // "up" (delta >= 3) means the skill improved.
    const weakestSkills = Array.from(skillAgg.entries())
      .map(([key, agg]) => {
        const averageScore = agg.count > 0 ? Math.round(agg.sum / agg.count) : 0;
        const prev = prevSkillAgg.get(key);
        const previousAverageScore =
          prev && prev.count > 0 ? Math.round(prev.sum / prev.count) : null;

        let trend: "up" | "down" | "flat" | "new";
        let trendLabel: string;
        let delta: number | null = null;

        if (previousAverageScore === null) {
          trend = "new";
          trendLabel = "New this period";
        } else {
          delta = averageScore - previousAverageScore;
          if (delta >= 3) {
            trend = "up";
            trendLabel = `↑ from ${previousAverageScore}`;
          } else if (delta <= -3) {
            trend = "down";
            trendLabel = `↓ from ${previousAverageScore}`;
          } else {
            trend = "flat";
            trendLabel = "No major change";
          }
        }

        return {
          skill: SKILL_LABELS[key],
          count: agg.weakCount,
          averageScore,
          previousAverageScore,
          delta,
          trend,
          trendLabel,
        };
      })
      .filter((s) => s.count > 0)
      .sort((a, b) => a.averageScore - b.averageScore);

    // ── Coaching impact (Day 94: cheap rule-based summary) ──
    const skillsImproving = weakestSkills.filter((s) => s.trend === "up").length;
    const skillsDeclining = weakestSkills.filter((s) => s.trend === "down").length;
    const coachingImpact = {
      completedAssignments,
      skillsImproving,
      skillsDeclining,
      summary: `${skillsImproving} skill${skillsImproving === 1 ? "" : "s"} improving, ${skillsDeclining} declining`,
    };

    // ── ROI (Day 91: real manager reviews from call_manager_reviews) ──
    const callsReviewed = reviewedCount;
    const estimatedMinutesSaved = callsReviewed * MINUTES_SAVED_PER_REVIEWED_CALL;
    const estimatedHoursSaved = Math.round((estimatedMinutesSaved / 60) * 10) / 10;

    return res.json({
      ok: true,
      windowDays: days,
      reviewHistoryAvailable: reviewHistory.available,
      teamHealth: {
        status: computeTeamHealthStatus({
          averageScore: teamAverage,
          callsNeedingReview: callsNeedingReviewCount,
          overdueAssignments: overdueAssignmentRows.length,
        }),
        averageScore: teamAverage ?? 0,
        reviewedCalls: reviewedCount,
        callsNeedingReview: callsNeedingReviewCount,
        openAssignments: openAssignmentRows.length,
        overdueAssignments: overdueAssignmentRows.length,
      },
      repsNeedingAttention,
      callsNeedingReview: reviewCalls,
      openAssignments,
      weakestSkills,
      coachingImpact,
      roi: {
        callsReviewed,
        estimatedMinutesSaved,
        estimatedHoursSaved,
      },
    });
  } catch (e: any) {
    const msg = e?.message || "command_centre_failed";
    console.error("[manager.command-centre] error", e);
    if (msg === "server_missing_supabase_env") {
      return res.status(500).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg });
  }
});

// ── GET /v1/manager/review-queue ─────────────────────────────────────────────
// Scored calls that still need a manager review (excludes call_manager_reviews).

router.get("/review-queue", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const since = isoDaysAgo(days);

    const rawLimit = parseInt(String(req.query.limit ?? "25"), 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 25, 1), 100);

    const userContext = await getUserContext(supa, userId);

    let callsQuery = supa
      .from("calls")
      .select("id, user_id, filename, status, score_overall, analysis_json, created_at, office_id, company_id")
      .eq("status", "scored")
      .gte("created_at", since)
      .order("created_at", { ascending: false })
      .limit(2000);
    callsQuery = applyHierarchyFilters(callsQuery, userContext);

    const [callsResult, reviewHistory] = await Promise.all([
      callsQuery,
      fetchReviewedCallIds(supa, since),
    ]);
    if (callsResult.error) throw callsResult.error;

    const scoredCalls = (callsResult.data ?? []) as ScoredCallRow[];

    const queue = scoredCalls
      .filter((c) => !reviewHistory.ids.has(String(c.id)))
      .map((call) => ({ call, reasons: reviewReasons(call) }))
      .filter(({ reasons }) => reasons.length > 0)
      .sort((a, b) => {
        const scoreDelta =
          Number(a.call.score_overall ?? 101) - Number(b.call.score_overall ?? 101);
        if (scoreDelta !== 0) return scoreDelta;
        // Newest first as tie-breaker
        return String(b.call.created_at || "").localeCompare(String(a.call.created_at || ""));
      })
      .slice(0, limit);

    // Rep names (single best-effort lookup)
    const repIds = Array.from(new Set(queue.map(({ call }) => String(call.user_id || "")).filter(Boolean)));
    const repNames = new Map<string, string>();
    if (repIds.length > 0) {
      const { data: repRows } = await supa.from("reps").select("id, name").in("id", repIds);
      for (const r of repRows ?? []) {
        const name = String((r as any).name || "").trim();
        if (name) repNames.set(String((r as any).id), name);
      }
    }

    const items = queue.map(({ call, reasons }) => {
      const stages = extractStageScores(call.analysis_json);
      const weakest = weakestSkillOf(stages);
      return {
        callId: String(call.id),
        repId: call.user_id ? String(call.user_id) : null,
        repName: (call.user_id && repNames.get(String(call.user_id))) || "Unknown rep",
        title: String(call.filename || "Untitled call"),
        overallScore: Number.isFinite(Number(call.score_overall)) ? Number(call.score_overall) : 0,
        weakestSkill: weakest ? SKILL_LABELS[weakest] : "Unknown",
        createdAt: String(call.created_at || ""),
        reasons,
      };
    });

    return res.json({
      ok: true,
      windowDays: days,
      reviewHistoryAvailable: reviewHistory.available,
      items,
      count: items.length,
    });
  } catch (e: any) {
    console.error("[manager.review-queue] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "review_queue_failed" });
  }
});

export default router;
