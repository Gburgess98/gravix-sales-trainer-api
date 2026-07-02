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
import { sparringHardeningColumns } from "../sparring";
import { whispererTablesAvailable, discoverTriggerCandidates, blendTriggerCandidates, suppressKnownCandidates, type DiscoveryItem } from "../whisperer";
import { logAuditEvent } from "../lib/audit";
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
    // Seeded demo orgs have office managers with no office assigned; filtering
    // .eq("office_id", null) is a Postgres uuid error, so fall back to company
    // scope (then unscoped, matching the null-context behaviour above).
    if (user.office_id) return query.eq("office_id", user.office_id);
    if (user.company_id) return query.eq("company_id", user.company_id);
    return query;
  }

  if (isCompanyManager(user)) {
    if (user.company_id) return query.eq("company_id", user.company_id);
    return query;
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

// ── GET /v1/manager/sparring-sessions ────────────────────────────────────────
// TIER 2A Day 105: completed sparring sessions for the manager's scope.
// sparring_sessions has no tenant columns yet (Day 100 data plan), so scoping
// goes through the rep's users-row hierarchy: fetch in-window sessions, resolve
// each rep's office/company, and apply the same office/company-manager rules
// as applyHierarchyFilters before returning anything.

router.get("/sparring-sessions", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const since = isoDaysAgo(days);
    const rawLimit = parseInt(String(req.query.limit ?? "10"), 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 10, 1), 50);

    const userContext = await getUserContext(supa, userId);

    // Day 106 — prefer the first-class columns when the hardening migration
    // has been applied; fall back to the Day 105 meta-based path otherwise.
    const cols = await sparringHardeningColumns(supa).catch(() => ({ sessions: false, turns: false }));
    const selectFields = cols.sessions
      ? "id, rep_id, persona_id, difficulty, total_score, summary, created_at, meta, status, completed_at, assignment_id, company_id, office_id"
      : "id, rep_id, persona_id, difficulty, total_score, summary, created_at, meta";

    const { data: sessionRows, error: sessErr } = await supa
      .from("sparring_sessions")
      .select(selectFields)
      .gte("created_at", since)
      .order("created_at", { ascending: false })
      .limit(500);
    if (sessErr) throw sessErr;

    // Completed = status column (post-migration) or ended/completed_at in
    // meta / persisted summary/score (pre-migration and pre-backfill rows).
    const completed = (sessionRows ?? []).filter((s: any) => {
      if (s?.status === "completed" || s?.completed_at) return true;
      const m = s?.meta && typeof s.meta === "object" ? s.meta : {};
      return Boolean(m.ended || m.completed_at || s.summary || typeof s.total_score === "number");
    });

    // Resolve rep hierarchy for tenant scoping (users row per rep).
    const repIds = Array.from(
      new Set(completed.map((s: any) => String(s.rep_id || "")).filter(Boolean))
    );
    const repHierarchy = new Map<string, { office_id: string | null; company_id: string | null }>();
    if (repIds.length > 0) {
      const { data: userRows } = await supa
        .from("users")
        .select("id, office_id, company_id")
        .in("id", repIds);
      for (const u of userRows ?? []) {
        repHierarchy.set(String((u as any).id), {
          office_id: (u as any).office_id || null,
          company_id: (u as any).company_id || null,
        });
      }
    }

    const inScope = completed.filter((s: any) => {
      if (!userContext) return true; // same semantics as applyHierarchyFilters
      // Column-first (Day 106 backfilled/new rows), rep hierarchy fallback (Day 105)
      const officeId = s.office_id || repHierarchy.get(String(s.rep_id || ""))?.office_id || null;
      const companyId = s.company_id || repHierarchy.get(String(s.rep_id || ""))?.company_id || null;
      if (isOfficeManager(userContext)) return officeId === userContext.office_id;
      if (isCompanyManager(userContext)) return companyId === userContext.company_id;
      return true;
    });

    // Rep names (best-effort)
    const repNames = new Map<string, string>();
    if (repIds.length > 0) {
      const { data: repRows } = await supa.from("reps").select("id, name").in("id", repIds);
      for (const r of repRows ?? []) {
        const name = String((r as any).name || "").trim();
        if (name) repNames.set(String((r as any).id), name);
      }
    }

    const normaliseDifficultyLabel = (raw: any): string => {
      const d = String(raw || "").toLowerCase();
      if (d === "easy" || d === "hard" || d === "nightmare") return d;
      if (d === "normal" || d === "standard") return "standard";
      return "unknown";
    };

    const items = inScope
      .map((s: any) => {
        const m = s?.meta && typeof s.meta === "object" ? s.meta : {};
        const ss = m.session_summary && typeof m.session_summary === "object" ? m.session_summary : null;
        const assignmentId =
          (s.assignment_id ? String(s.assignment_id) : "") ||
          String(m.assignment_id || "").trim() ||
          null;
        const completedAt = s.completed_at || m.completed_at || null;

        return {
          sessionId: String(s.id),
          repId: s.rep_id ? String(s.rep_id) : null,
          repName: (s.rep_id && repNames.get(String(s.rep_id))) || "Unknown rep",
          assignmentId,
          persona: s.persona_id ? String(s.persona_id) : null,
          difficulty: normaliseDifficultyLabel(s.difficulty || m.difficulty),
          overall:
            typeof ss?.overall === "number"
              ? ss.overall
              : typeof s.total_score === "number"
                ? s.total_score
                : 0,
          weakestDimension: ss?.weakestDimension || "unknown",
          strongestDimension: ss?.strongestDimension || "unknown",
          recommendedDrill: ss?.recommendedDrill || null,
          summaryText: ss?.summaryText || String(s.summary || "") || "No summary available.",
          nextBestAction: ss?.nextBestAction || "",
          completedAt,
          turnCount: typeof ss?.turnCount === "number" ? ss.turnCount : 0,
          source: (assignmentId ? "assignment" : "manual") as "assignment" | "manual" | "unknown",
          _sortKey: String(completedAt || s.created_at || ""),
        };
      })
      .sort((a, b) => b._sortKey.localeCompare(a._sortKey))
      .slice(0, limit)
      .map(({ _sortKey, ...item }) => item);

    return res.json({ ok: true, windowDays: days, items, count: items.length });
  } catch (e: any) {
    console.error("[manager.sparring-sessions] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "sparring_sessions_failed" });
  }
});

// ── GET /v1/manager/whisperer-sessions ───────────────────────────────────────
// TIER 2B Day 114: Live Whisperer activity for the manager's scope.
// whisperer_sessions carries tenant columns from day one (Day 111), so scoping
// uses direct DB filters via applyHierarchyFilters — no rep-hierarchy join.

router.get("/whisperer-sessions", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const since = isoDaysAgo(days);
    const rawLimit = parseInt(String(req.query.limit ?? "10"), 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 10, 1), 50);

    const emptySummary = {
      sessionCount: 0,
      triggerCount: 0,
      topTriggerTypes: [] as Array<{ type: string; count: number }>,
      avgLatencyMs: null as number | null,
      activeSessions: 0,
      staleSessions: 0,
      endedSessions: 0,
    };

    // Day 118: an "active" session older than 30 min was never ended cleanly.
    // We classify it as stale in the response (no DB mutation here).
    const STALE_AFTER_MS = 30 * 60 * 1000;
    const nowMs = Date.now();
    const isStaleSession = (s: any): boolean =>
      s.status === "active" &&
      Number.isFinite(new Date(s.started_at).getTime()) &&
      nowMs - new Date(s.started_at).getTime() > STALE_AFTER_MS;

    // Fail-soft if the migration hasn't been applied in this environment
    const available = await whispererTablesAvailable(supa).catch(() => false);
    if (!available) {
      return res.json({
        ok: true,
        persistence: false,
        windowDays: days,
        items: [],
        summary: emptySummary,
        count: 0,
      });
    }

    const userContext = await getUserContext(supa, userId);

    let sessionQuery = supa
      .from("whisperer_sessions")
      .select("id, rep_id, user_id, status, started_at, ended_at, latency_p50_ms, latency_p95_ms, meta, office_id, company_id")
      .gte("started_at", since)
      .order("started_at", { ascending: false })
      .limit(limit);
    sessionQuery = applyHierarchyFilters(sessionQuery, userContext);

    const { data: sessionRows, error: sessErr } = await sessionQuery;
    if (sessErr) throw sessErr;
    const sessions = sessionRows ?? [];

    if (sessions.length === 0) {
      return res.json({ ok: true, persistence: true, windowDays: days, items: [], summary: emptySummary, count: 0 });
    }

    const sessionIds = sessions.map((s: any) => String(s.id));

    // Triggers for these sessions (one query, grouped in code)
    // Day 124: meta carries customTriggerId for custom-vs-built-in breakdown.
    const baseTrigCols = "session_id, type, phrase, confidence, suggestion, latency_ms, detected_at, meta";
    // Day 122: include suggestion_outcome; fail-soft if column not migrated yet.
    const trigWide = await supa
      .from("whisperer_triggers")
      .select(`${baseTrigCols}, suggestion_outcome`)
      .in("session_id", sessionIds)
      .order("detected_at", { ascending: true })
      .limit(5000);
    let triggers: any[] = trigWide.data ?? [];
    if (trigWide.error && /suggestion_outcome/i.test(String(trigWide.error.message || ""))) {
      const trigNarrow = await supa
        .from("whisperer_triggers")
        .select(baseTrigCols)
        .in("session_id", sessionIds)
        .order("detected_at", { ascending: true })
        .limit(5000);
      triggers = trigNarrow.data ?? [];
    }

    // Rep names (best-effort)
    const repIds = Array.from(new Set(sessions.map((s: any) => String(s.rep_id || s.user_id || "")).filter(Boolean)));
    const repNames = new Map<string, string>();
    if (repIds.length > 0) {
      const { data: repRows } = await supa.from("reps").select("id, name").in("id", repIds);
      for (const r of repRows ?? []) {
        const name = String((r as any).name || "").trim();
        if (name) repNames.set(String((r as any).id), name);
      }
    }

    const avg = (xs: number[]) => (xs.length ? Math.round(xs.reduce((a, b) => a + b, 0) / xs.length) : null);
    const p95 = (xs: number[]) => {
      if (!xs.length) return null;
      const sorted = [...xs].sort((a, b) => a - b);
      return sorted[Math.min(sorted.length - 1, Math.floor(0.95 * sorted.length))];
    };
    const topTypes = (rows: any[], n: number) => {
      const counts = new Map<string, number>();
      for (const t of rows) counts.set(String(t.type), (counts.get(String(t.type)) || 0) + 1);
      return Array.from(counts.entries())
        .map(([type, count]) => ({ type, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, n);
    };

    const bySession = new Map<string, any[]>();
    for (const t of triggers) {
      const sid = String(t.session_id);
      if (!bySession.has(sid)) bySession.set(sid, []);
      bySession.get(sid)!.push(t);
    }

    const items = sessions.map((s: any) => {
      const sid = String(s.id);
      const sessionTriggers = bySession.get(sid) ?? [];
      const latencies = sessionTriggers.map((t) => Number(t.latency_ms)).filter(Number.isFinite);
      const latest = sessionTriggers[sessionTriggers.length - 1] || null;
      const repId = s.rep_id ? String(s.rep_id) : s.user_id ? String(s.user_id) : null;

      return {
        sessionId: sid,
        repId,
        repName: (repId && repNames.get(repId)) || "Unknown rep",
        status: isStaleSession(s) ? "stale" : (s.status === "active" || s.status === "ended") ? s.status : "unknown",
        isStale: isStaleSession(s),
        startedAt: s.started_at,
        endedAt: s.ended_at ?? null,
        triggerCount: sessionTriggers.length,
        topTriggerTypes: topTypes(sessionTriggers, 3),
        avgLatencyMs: typeof s.latency_p50_ms === "number" ? s.latency_p50_ms : avg(latencies),
        p95LatencyMs: typeof s.latency_p95_ms === "number" ? s.latency_p95_ms : p95(latencies),
        latestSuggestion: latest
          ? {
              type: String(latest.type),
              title: latest.suggestion?.title || "Suggestion",
              urgency: latest.suggestion?.urgency || "low",
              phrase: latest.phrase ?? null,
              createdAt: latest.detected_at,
              // Day 122: usefulness signal (null = unrated)
              suggestionOutcome: (latest as any).suggestion_outcome ?? null,
            }
          : null,
        source: (s.meta?.source as string) || "unknown",
      };
    });

    const allLatencies = triggers.map((t: any) => Number(t.latency_ms)).filter(Number.isFinite);

    // Day 122: suggestion quality signal across this window's triggers.
    const outcomeCounts = { used: 0, ignored: 0, notRelevant: 0, unrated: 0 };
    for (const t of triggers) {
      switch (String((t as any).suggestion_outcome || "")) {
        case "used": outcomeCounts.used += 1; break;
        case "ignored": outcomeCounts.ignored += 1; break;
        case "not_relevant": outcomeCounts.notRelevant += 1; break;
        default: outcomeCounts.unrated += 1; break;
      }
    }
    const ratedTotal = outcomeCounts.used + outcomeCounts.ignored + outcomeCounts.notRelevant;
    const usedRate = ratedTotal > 0 ? Math.round((outcomeCounts.used / ratedTotal) * 100) : null;

    // Day 124: usefulness breakdown by trigger type + custom-vs-built-in.
    // A tally accumulates outcome counts and derives shown + usedRate.
    type Tally = { shown: number; used: number; ignored: number; notRelevant: number; unrated: number };
    const newTally = (): Tally => ({ shown: 0, used: 0, ignored: 0, notRelevant: 0, unrated: 0 });
    const addToTally = (t: Tally, outcome: string) => {
      t.shown += 1;
      switch (outcome) {
        case "used": t.used += 1; break;
        case "ignored": t.ignored += 1; break;
        case "not_relevant": t.notRelevant += 1; break;
        default: t.unrated += 1; break;
      }
    };
    const tallyUsedRate = (t: Tally): number | null => {
      const rated = t.used + t.ignored + t.notRelevant;
      return rated > 0 ? Math.round((t.used / rated) * 100) : null;
    };
    const isCustomTrigger = (t: any): boolean =>
      Boolean(t?.meta?.customTriggerId || t?.meta?.custom_trigger_id || t?.meta?.custom);

    const byType = new Map<string, Tally>();
    const customByType = new Map<string, Tally>(); // Day 125: custom-only, for health flags
    const custom = newTally();
    const builtIn = newTally();
    for (const t of triggers) {
      const outcome = String((t as any).suggestion_outcome || "");
      const type = String((t as any).type || "unknown");
      if (!byType.has(type)) byType.set(type, newTally());
      addToTally(byType.get(type)!, outcome);
      if (isCustomTrigger(t)) {
        if (!customByType.has(type)) customByType.set(type, newTally());
        addToTally(customByType.get(type)!, outcome);
        addToTally(custom, outcome);
      } else {
        addToTally(builtIn, outcome);
      }
    }
    const usefulnessByType = Array.from(byType.entries())
      .map(([type, t]) => ({ type, shown: t.shown, used: t.used, ignored: t.ignored, notRelevant: t.notRelevant, unrated: t.unrated, usedRate: tallyUsedRate(t) }))
      .sort((a, b) => b.shown - a.shown || a.type.localeCompare(b.type));
    const customVsBuiltIn = {
      custom: { ...custom, usedRate: tallyUsedRate(custom) },
      builtIn: { ...builtIn, usedRate: tallyUsedRate(builtIn) },
    };

    // Day 125: flag custom triggers that look noisy so managers know what to edit.
    // MVP thresholds — low used rate with enough rated volume, or repeatedly
    // marked not relevant. unrated is excluded from usedRate.
    const needsEditing = Array.from(customByType.entries())
      .map(([type, t]) => {
        const rated = t.used + t.ignored + t.notRelevant;
        const rate = tallyUsedRate(t);
        const lowUsed = t.shown >= 5 && rated >= 3 && rate !== null && rate < 30;
        const oftenNotRelevant = t.shown >= 5 && t.notRelevant >= 3;
        if (!lowUsed && !oftenNotRelevant) return null;
        return {
          type,
          shown: t.shown, used: t.used, ignored: t.ignored, notRelevant: t.notRelevant, unrated: t.unrated,
          usedRate: rate,
          reason: oftenNotRelevant ? "Often marked not relevant" : "Low used rate",
          severity: "warning" as const,
        };
      })
      .filter((r): r is NonNullable<typeof r> => r !== null)
      .sort((a, b) => b.notRelevant - a.notRelevant || (a.usedRate ?? 101) - (b.usedRate ?? 101) || b.shown - a.shown);
    const customTriggerHealth = { needsEditing };

    const summary = {
      sessionCount: sessions.length,
      triggerCount: triggers.length,
      topTriggerTypes: topTypes(triggers, 5),
      avgLatencyMs: avg(allLatencies),
      activeSessions: sessions.filter((s: any) => s.status === "active" && !isStaleSession(s)).length,
      staleSessions: sessions.filter((s: any) => isStaleSession(s)).length,
      endedSessions: sessions.filter((s: any) => s.status === "ended").length,
      suggestionOutcomes: outcomeCounts,
      usedRate,
      usefulnessByType,
      customVsBuiltIn,
      customTriggerHealth,
    };

    return res.json({ ok: true, persistence: true, windowDays: days, items, summary, count: items.length });
  } catch (e: any) {
    console.error("[manager.whisperer-sessions] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "whisperer_sessions_failed" });
  }
});

// Day 133: detect the candidate-decisions table being absent (migration not yet
// applied in this environment). Mirrors whispererLibraryTableMissing.
function candidateDecisionsTableMissing(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("whisperer_trigger_candidate_decisions") &&
    (msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache"));
}

const CANDIDATE_DECISIONS = new Set(["approved", "dismissed", "rejected"]);

// Day 144: detect the raw-segments table being absent (migration not applied in
// this environment) so discovery falls back to trigger-text mining. Mirrors
// candidateDecisionsTableMissing.
function whispererSegmentsTableMissing(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("whisperer_segments") &&
    (msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache"));
}

// ── GET /v1/manager/whisperer-trigger-candidates ─────────────────────────────
// TIER 2B Day 130: AI Trigger Discovery — READ-ONLY. Mines stored trigger
// segment text across the manager's scope and surfaces recurring sales moments
// as *candidates* only. No DB writes, no activation, no LLM. Managers approve
// anything into the custom trigger library by hand (existing POST endpoint).
// Day 133: candidates with a persisted decision (approved/dismissed/rejected)
// in scope are suppressed so actioned suggestions don't re-appear.
router.get("/whisperer-trigger-candidates", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");

    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(90, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const rawLimit = parseInt(String(req.query.limit ?? "10"), 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 10, 1), 50);
    const since = isoDaysAgo(days);

    const notEnough = (sourceMoments: number) => ({
      candidateCount: 0,
      sourceWindowDays: days,
      sourceMoments,
      note: "Not enough Whisperer data yet.",
    });

    const available = await whispererTablesAvailable(supa).catch(() => false);
    if (!available) {
      return res.json({ ok: true, persistence: false, items: [], count: 0, summary: notEnough(0) });
    }

    const userContext = await getUserContext(supa, userId);

    // Day 145: BLEND both discovery sources so managers see raw blind spots AND
    // recurring triggered moments. Raw blind spots rank first; a pattern present
    // in both becomes a single "mixed" candidate. Both sources are mined every
    // time (no raw-first hiding). Raw stays fail-soft — a missing
    // whisperer_segments table simply contributes no raw candidates. Offline,
    // tenant-scoped, no LLM, no DB writes.
    let rawSegmentsConsidered = 0;
    let untriggeredSegmentsConsidered = 0;
    let triggerMomentsConsidered = 0;

    // ── Source 1: raw whisperer_segments (untriggered blind spots) ──
    let rawItems: DiscoveryItem[] = [];
    try {
      let segQuery = supa
        .from("whisperer_segments")
        .select("id, session_id, text, triggers_count, speaker, speaker_role, created_at")
        .eq("is_final", true)
        .gte("created_at", since)
        .order("created_at", { ascending: false })
        .limit(5000);
      segQuery = applyHierarchyFilters(segQuery, userContext);
      const { data: segRows, error: segErr } = await segQuery;
      if (segErr) {
        if (!whispererSegmentsTableMissing(segErr)) throw segErr;
        // Raw table not migrated here → no raw candidates, trigger source still runs.
      } else {
        rawSegmentsConsidered = (segRows ?? []).length;
        const eligible = (segRows ?? []).filter((r: any) => {
          const role = String(r.speaker_role || "").toLowerCase();
          const sp = String(r.speaker || "").toLowerCase();
          if (role === "rep" || sp === "rep") return false; // never mine rep speech
          return String(r.text || "").trim().length >= 8;
        });
        const untriggered = eligible.filter((r: any) => Number(r.triggers_count) === 0);
        untriggeredSegmentsConsidered = untriggered.length;
        rawItems = untriggered.map((r: any) => ({
          text: String(r.text || ""),
          sessionId: r.session_id ? String(r.session_id) : null,
          detectedAt: r.created_at ? String(r.created_at) : null,
          segmentId: r.id ? String(r.id) : null,
          source: "raw_segment",
          untriggered: true,
        }));
      }
    } catch (e: any) {
      console.warn("[manager.whisperer-trigger-candidates] raw segment mine failed", e?.message || e);
    }

    // ── Source 2: stored trigger segment text (recurring triggered moments) ──
    let triggerItems: DiscoveryItem[] = [];
    let sessionQuery = supa
      .from("whisperer_sessions")
      .select("id")
      .eq("status", "ended")
      .gte("started_at", since)
      .order("started_at", { ascending: false })
      .limit(500);
    sessionQuery = applyHierarchyFilters(sessionQuery, userContext);
    const { data: sessionRows, error: sessErr } = await sessionQuery;
    if (sessErr) throw sessErr;
    const sessionIds = (sessionRows ?? []).map((s: any) => String(s.id));
    if (sessionIds.length > 0) {
      const { data: trigRows, error: trigErr } = await supa
        .from("whisperer_triggers")
        .select("segment_text, session_id, detected_at")
        .in("session_id", sessionIds)
        .order("detected_at", { ascending: false })
        .limit(5000);
      if (trigErr) throw trigErr;
      triggerItems = (trigRows ?? [])
        .map((r: any) => ({
          text: String(r.segment_text || ""),
          sessionId: r.session_id ? String(r.session_id) : null,
          detectedAt: r.detected_at ? String(r.detected_at) : null,
          source: "trigger_segment" as const,
          untriggered: false,
        }))
        .filter((it: DiscoveryItem) => it.text.trim().length > 0);
      triggerMomentsConsidered = triggerItems.length;
    }

    // Discover each source independently, then blend + rank (blind spots first).
    const rawCandidates = discoverTriggerCandidates({ items: rawItems, minSeenCount: 2, limit });
    const triggerCandidates = discoverTriggerCandidates({ items: triggerItems, minSeenCount: 2, limit });
    const rawCandidateCount = rawCandidates.length;
    const triggerCandidateCount = triggerCandidates.length;
    let candidates = blendTriggerCandidates(rawCandidates, triggerCandidates);

    // Summary source reflects which sources actually produced candidates.
    const source: "raw_segments" | "trigger_segments" | "mixed" =
      rawCandidateCount > 0 && triggerCandidateCount > 0
        ? "mixed"
        : rawCandidateCount > 0
          ? "raw_segments"
          : "trigger_segments";

    // Day 132: suppress candidates that already overlap an enabled custom
    // trigger in scope, so actioned suggestions stop re-appearing. Fail-soft —
    // a missing/erroring library never breaks discovery (no suppression then).
    let suppressedExistingCount = 0;
    try {
      let lq = supa
        .from("whisperer_trigger_library")
        .select("match_phrases, match_keywords, office_id, company_id, org_id")
        .eq("enabled", true)
        .limit(500);
      lq = applyLibraryScope(lq, userContext);
      const { data: libRows, error: libErr } = await lq;
      if (libErr) {
        if (!whispererLibraryTableMissing(libErr)) throw libErr;
      } else if (libRows && libRows.length) {
        const existing = libRows.map((r: any) => ({ phrases: r.match_phrases ?? [], keywords: r.match_keywords ?? [] }));
        const { kept, suppressedCount } = suppressKnownCandidates(candidates, existing);
        candidates = kept;
        suppressedExistingCount = suppressedCount;
      }
    } catch (e: any) {
      console.warn("[manager.whisperer-trigger-candidates] dedupe failed", e?.message || e);
    }

    // Day 133: suppress candidates a manager has already actioned (any persisted
    // decision in scope). Fail-soft — a missing decisions table → no suppression.
    let suppressedDecisionCount = 0;
    try {
      let dq = supa
        .from("whisperer_trigger_candidate_decisions")
        .select("candidate_id, office_id, company_id, org_id")
        .limit(1000);
      dq = applyLibraryScope(dq, userContext);
      const { data: decRows, error: decErr } = await dq;
      if (decErr) {
        if (!candidateDecisionsTableMissing(decErr)) throw decErr;
      } else if (decRows && decRows.length) {
        const decided = new Set(decRows.map((r: any) => String(r.candidate_id)));
        const before = candidates.length;
        candidates = candidates.filter((c) => !decided.has(c.id));
        suppressedDecisionCount = before - candidates.length;
      }
    } catch (e: any) {
      console.warn("[manager.whisperer-trigger-candidates] decision filter failed", e?.message || e);
    }

    // Day 145: apply the limit after blending + suppression so blind spots and
    // triggered moments compete for the same slots (blind spots already rank first).
    candidates = candidates.slice(0, limit);
    const mixedCandidateCount = candidates.filter((c) => c.source === "mixed").length;

    // Provenance + counters shared across the empty and populated paths.
    const discoverySummary = {
      source,
      sourceWindowDays: days,
      sourceMoments: rawItems.length + triggerItems.length,
      rawSegmentsConsidered,
      untriggeredSegmentsConsidered,
      triggerMomentsConsidered,
      rawCandidateCount,
      triggerCandidateCount,
      mixedCandidateCount,
      suppressedExistingCount,
      suppressedDecisionCount,
    };

    if (candidates.length === 0) {
      return res.json({
        ok: true,
        persistence: true,
        items: [],
        count: 0,
        summary: {
          ...discoverySummary,
          candidateCount: 0,
          note: "Not enough Whisperer data yet.",
        },
      });
    }

    return res.json({
      ok: true,
      persistence: true,
      items: candidates,
      count: candidates.length,
      summary: {
        ...discoverySummary,
        candidateCount: candidates.length,
        note: "Candidates are suggestions only. Managers must approve triggers before they go live.",
      },
    });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-candidates] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "trigger_candidates_failed" });
  }
});

// POST /v1/manager/whisperer-trigger-candidates/:id/decision
// TIER 2B Day 133: persist a manager's lifecycle decision for a candidate
// (approved / dismissed / rejected). This NEVER creates or enables a trigger —
// activation stays with the Custom Trigger Library save flow. Tenant-stamped
// from the manager's context; one live decision per candidate per scope.
router.post("/whisperer-trigger-candidates/:id/decision", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const candidateId = String(req.params.id || "").trim();
    if (!candidateId) return res.status(400).json({ ok: false, error: "invalid_candidate_id" });

    const decision = String((req.body as any)?.decision || "").trim().toLowerCase();
    if (!CANDIDATE_DECISIONS.has(decision)) {
      return res.status(400).json({ ok: false, error: "invalid_decision" });
    }
    const noteRaw = (req.body as any)?.note;
    const note = typeof noteRaw === "string" ? noteRaw.trim().slice(0, 1000) : null;
    const sourceRaw = (req.body as any)?.source;
    const source = sourceRaw && typeof sourceRaw === "object" ? sourceRaw : {};
    const candidateTypeRaw = (req.body as any)?.candidateType;
    const candidateType = typeof candidateTypeRaw === "string" ? candidateTypeRaw.trim().slice(0, 40) : null;

    const ctx = await getUserContext(supa, userId);
    const officeId = ctx?.office_id ?? null;
    const companyId = ctx?.company_id ?? null;

    const nowIso = new Date().toISOString();
    const table = "whisperer_trigger_candidate_decisions";

    // Manual upsert on (org_id, company_id, office_id, candidate_id) so we don't
    // depend on NULLS NOT DISTINCT onConflict support across PG versions.
    let sel = supa.from(table).select("id").eq("candidate_id", candidateId).is("org_id", null);
    sel = officeId ? sel.eq("office_id", officeId) : sel.is("office_id", null);
    sel = companyId ? sel.eq("company_id", companyId) : sel.is("company_id", null);
    const { data: existing, error: selErr } = await sel.maybeSingle();
    if (selErr) {
      if (candidateDecisionsTableMissing(selErr)) {
        return res.status(503).json({ ok: false, error: "migration_required", hint: "Run sql/20260617_whisperer_trigger_candidate_decisions.sql" });
      }
      throw selErr;
    }

    let createdAt = nowIso;
    if (existing) {
      const { error: uErr } = await supa
        .from(table)
        .update({ decision, note, source, candidate_type: candidateType, decided_by: userId, updated_at: nowIso })
        .eq("id", (existing as any).id);
      if (uErr) throw uErr;
    } else {
      const { error: iErr } = await supa.from(table).insert({
        org_id: null,
        company_id: companyId,
        office_id: officeId,
        candidate_id: candidateId,
        candidate_type: candidateType,
        decision,
        decided_by: userId,
        note,
        source,
        created_at: nowIso,
        updated_at: nowIso,
      });
      if (iErr) {
        if (candidateDecisionsTableMissing(iErr)) {
          return res.status(503).json({ ok: false, error: "migration_required", hint: "Run sql/20260617_whisperer_trigger_candidate_decisions.sql" });
        }
        throw iErr;
      }
    }

    void logAuditEvent({
      actorUserId: userId,
      action: "whisperer.candidate_decision",
      entityType: "whisperer_trigger_candidate",
      entityId: candidateId,
      metadata: { decision, company_id: companyId, office_id: officeId },
    });

    return res.json({
      ok: true,
      decision: { candidateId, decision, decidedBy: userId, createdAt },
    });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-candidate decision] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "candidate_decision_failed" });
  }
});

// GET /v1/manager/whisperer-trigger-candidate-decisions?days=30&limit=20
// TIER 2B Day 136: reviewed candidate history. Lists the manager's persisted
// candidate decisions (approved/dismissed/rejected) in scope so they can see
// what they've actioned and restore dismissed/rejected ones. Read-only.
// Fail-soft when the table hasn't been migrated (persistence:false, no 500).
router.get("/whisperer-trigger-candidate-decisions", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const days = Math.max(1, Math.min(90, parseInt(String(req.query.days ?? "30"), 10) || 30));
    const rawLimit = parseInt(String(req.query.limit ?? "20"), 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 20, 1), 50);
    const since = isoDaysAgo(days);

    const emptySummary = { dismissed: 0, rejected: 0, approved: 0, sourceWindowDays: days };

    const ctx = await getUserContext(supa, userId);
    let q = supa
      .from("whisperer_trigger_candidate_decisions")
      .select("id, candidate_id, candidate_type, decision, decided_by, note, source, created_at, updated_at")
      .gte("created_at", since)
      .order("updated_at", { ascending: false })
      .limit(limit);
    q = applyLibraryScope(q, ctx);

    const { data, error } = await q;
    if (error) {
      if (candidateDecisionsTableMissing(error)) {
        return res.json({ ok: true, persistence: false, items: [], count: 0, summary: emptySummary });
      }
      throw error;
    }

    const items = (data ?? []).map((r: any) => ({
      id: String(r.id),
      candidateId: String(r.candidate_id),
      candidateType: r.candidate_type ?? null,
      decision: String(r.decision),
      decidedBy: r.decided_by ?? null,
      note: r.note ?? null,
      source: r.source ?? {},
      createdAt: r.created_at ?? null,
      updatedAt: r.updated_at ?? null,
    }));

    const summary = { ...emptySummary };
    for (const it of items) {
      if (it.decision === "dismissed") summary.dismissed += 1;
      else if (it.decision === "rejected") summary.rejected += 1;
      else if (it.decision === "approved") summary.approved += 1;
    }

    return res.json({ ok: true, persistence: true, items, count: items.length, summary });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-candidate-decisions] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "candidate_decisions_failed" });
  }
});

// DELETE /v1/manager/whisperer-trigger-candidates/:id/decision
// TIER 2B Day 136: un-dismiss / reopen a candidate. Removes the persisted
// decision row for this candidate in the manager's scope so it becomes eligible
// to surface again in discovery. NEVER deletes a custom trigger or activates
// anything — it only deletes the candidate decision row.
router.delete("/whisperer-trigger-candidates/:id/decision", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const candidateId = String(req.params.id || "").trim();
    if (!candidateId) return res.status(400).json({ ok: false, error: "invalid_candidate_id" });

    const ctx = await getUserContext(supa, userId);
    const officeId = ctx?.office_id ?? null;
    const companyId = ctx?.company_id ?? null;
    const table = "whisperer_trigger_candidate_decisions";

    // Match the same scope the POST decision stamps with.
    let sel = supa.from(table).select("id").eq("candidate_id", candidateId).is("org_id", null);
    sel = officeId ? sel.eq("office_id", officeId) : sel.is("office_id", null);
    sel = companyId ? sel.eq("company_id", companyId) : sel.is("company_id", null);
    const { data: existing, error: selErr } = await sel.maybeSingle();
    if (selErr) {
      if (candidateDecisionsTableMissing(selErr)) {
        return res.status(503).json({ ok: false, error: "migration_required", hint: "Run sql/20260617_whisperer_trigger_candidate_decisions.sql" });
      }
      throw selErr;
    }

    if (!existing) {
      return res.json({ ok: true, candidateId, restored: false });
    }

    const { error: dErr } = await supa.from(table).delete().eq("id", (existing as any).id);
    if (dErr) {
      if (candidateDecisionsTableMissing(dErr)) {
        return res.status(503).json({ ok: false, error: "migration_required", hint: "Run sql/20260617_whisperer_trigger_candidate_decisions.sql" });
      }
      throw dErr;
    }

    void logAuditEvent({
      actorUserId: userId,
      action: "whisperer.candidate_decision_restore",
      entityType: "whisperer_trigger_candidate",
      entityId: candidateId,
      metadata: { company_id: companyId, office_id: officeId },
    });

    return res.json({ ok: true, candidateId, restored: true });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-candidate decision DELETE] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "candidate_decision_restore_failed" });
  }
});

// ── Custom Whisperer trigger library (Tier 2B Day 119) ───────────────────────
// Manager CRUD, tenant-scoped. requireManager gates the whole router. New rules
// are stamped with the manager's org/company/office; managers only act within
// their scope. Fail-soft when the table hasn't been migrated.

// Base columns present since Day 119. Day 139 adds the source-link columns,
// kept in a separate constant so GET/POST can fall back to the base set when
// the Day 139 migration hasn't been applied in an environment yet.
const WTL_FIELDS_BASE =
  "id, org_id, company_id, office_id, created_by, name, description, type, match_phrases, match_keywords, suggestion_title, suggestion_response, urgency, emoji, enabled, priority, created_at, updated_at";
const WTL_FIELDS = `${WTL_FIELDS_BASE}, source_candidate_id, source_meta`;

function whispererLibraryTableMissing(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("whisperer_trigger_library") &&
    (msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache"));
}

// Day 139: detect the source-link columns being absent (migration not applied
// in this environment) so library reads/writes fail soft to the base columns.
function librarySourceColumnsMissing(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return (msg.includes("source_candidate_id") || msg.includes("source_meta")) &&
    (msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache") || msg.includes("column"));
}

function normaliseLibraryItem(row: any) {
  const sourceCandidateId = row.source_candidate_id ?? null;
  return {
    id: String(row.id),
    name: row.name,
    description: row.description ?? null,
    type: row.type,
    matchPhrases: row.match_phrases ?? [],
    matchKeywords: row.match_keywords ?? [],
    suggestionTitle: row.suggestion_title,
    suggestionResponse: row.suggestion_response,
    urgency: row.urgency,
    emoji: row.emoji ?? null,
    enabled: row.enabled,
    priority: row.priority,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    // Day 139 — approved candidate → source link (null/{} when unmigrated).
    sourceCandidateId,
    sourceMeta: row.source_meta ?? {},
    source: sourceCandidateId ? "trigger_candidate" : null,
  };
}

// Day 139: build the optional source-link patch for a custom trigger create.
// Only used on POST (source fields are immutable after create). Stores a small
// summary snapshot — never the full example transcript text.
function buildLibrarySourcePatch(body: any): Record<string, any> {
  const patch: Record<string, any> = {};
  const rawId = body?.sourceCandidateId;
  if (typeof rawId === "string" && rawId.trim()) {
    patch.source_candidate_id = rawId.trim().slice(0, 200);
  }
  const rawMeta = body?.sourceMeta;
  if (rawMeta && typeof rawMeta === "object" && !Array.isArray(rawMeta)) {
    const m = rawMeta as Record<string, any>;
    const clampStr = (v: any, n: number) => (typeof v === "string" ? v.slice(0, n) : undefined);
    const clampNum = (v: any) => (Number.isFinite(Number(v)) ? Number(v) : undefined);
    const meta: Record<string, any> = {
      source: clampStr(m.source, 40) ?? "trigger_candidate",
      candidateId: clampStr(m.candidateId, 200),
      suggestedName: clampStr(m.suggestedName ?? m.title, 200),
      type: clampStr(m.type, 40),
      seenCount: clampNum(m.seenCount),
      confidence: clampNum(m.confidence),
      examplesCount: clampNum(m.examplesCount),
      reason: clampStr(m.reason, 500),
      suggestedPhrases: Array.isArray(m.suggestedPhrases) ? m.suggestedPhrases.slice(0, 5).map((p: any) => String(p).slice(0, 120)) : undefined,
      suggestedKeywords: Array.isArray(m.suggestedKeywords) ? m.suggestedKeywords.slice(0, 10).map((k: any) => String(k).slice(0, 60)) : undefined,
    };
    // Drop undefined keys so the snapshot stays compact.
    for (const k of Object.keys(meta)) if (meta[k] === undefined) delete meta[k];
    patch.source_meta = meta;
  }
  return patch;
}

// Body → DB column patch with validation. Returns {error} or {patch}.
function validateLibraryBody(body: any, partial: boolean): { error?: string; patch?: Record<string, any> } {
  const patch: Record<string, any> = {};
  const has = (k: string) => body[k] !== undefined;

  if (!partial || has("name")) {
    const name = String(body.name || "").trim();
    if (!name) return { error: "name_required" };
    patch.name = name;
  }
  if (!partial || has("suggestionTitle")) {
    const t = String(body.suggestionTitle || "").trim();
    if (!t) return { error: "suggestion_title_required" };
    patch.suggestion_title = t;
  }
  if (!partial || has("suggestionResponse")) {
    const r = String(body.suggestionResponse || "").trim();
    if (!r) return { error: "suggestion_response_required" };
    patch.suggestion_response = r;
  }
  if (has("description")) patch.description = body.description ? String(body.description).slice(0, 1000) : null;
  if (has("type")) patch.type = String(body.type || "custom").trim() || "custom";
  if (has("matchPhrases")) patch.match_phrases = Array.isArray(body.matchPhrases) ? body.matchPhrases.map((p: any) => String(p).trim()).filter(Boolean) : [];
  if (has("matchKeywords")) patch.match_keywords = Array.isArray(body.matchKeywords) ? body.matchKeywords.map((k: any) => String(k).trim()).filter(Boolean) : [];
  if (has("urgency")) {
    const u = String(body.urgency || "").toLowerCase();
    patch.urgency = (u === "low" || u === "high") ? u : "medium";
  }
  if (has("emoji")) patch.emoji = body.emoji ? String(body.emoji).slice(0, 8) : null;
  if (has("enabled")) patch.enabled = Boolean(body.enabled);
  if (has("priority")) patch.priority = Math.max(0, Math.min(100, parseInt(String(body.priority), 10) || 50));

  // On create, at least one phrase or keyword must be present.
  if (!partial) {
    const phrases = patch.match_phrases ?? [];
    const keywords = patch.match_keywords ?? [];
    if (phrases.length === 0 && keywords.length === 0) return { error: "match_phrase_or_keyword_required" };
  }
  return { patch };
}

// Scope filter shared by GET/PATCH/DELETE: a manager sees rules in their office
// or company (mirrors applyHierarchyFilters semantics for this table).
function applyLibraryScope(query: any, ctx: any) {
  if (!ctx) return query;
  if (isOfficeManager(ctx)) return query.eq("office_id", ctx.office_id);
  if (isCompanyManager(ctx)) return query.eq("company_id", ctx.company_id);
  return query;
}

// GET /v1/manager/whisperer-trigger-library
router.get("/whisperer-trigger-library", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const ctx = await getUserContext(supa, userId);
    const runSelect = (fields: string) => {
      let q = supa.from("whisperer_trigger_library").select(fields).order("priority", { ascending: false }).limit(200);
      q = applyLibraryScope(q, ctx);
      return q;
    };

    let { data, error } = await runSelect(WTL_FIELDS);
    if (error && librarySourceColumnsMissing(error)) {
      // Day 139 migration not applied here — fall back to the base columns.
      ({ data, error } = await runSelect(WTL_FIELDS_BASE));
    }
    if (error) {
      if (whispererLibraryTableMissing(error)) return res.json({ ok: true, persistence: false, items: [] });
      throw error;
    }
    return res.json({ ok: true, persistence: true, items: (data ?? []).map(normaliseLibraryItem) });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-library GET] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "library_list_failed" });
  }
});

// POST /v1/manager/whisperer-trigger-library
router.post("/whisperer-trigger-library", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const { error: vErr, patch } = validateLibraryBody(req.body ?? {}, false);
    if (vErr) return res.status(400).json({ ok: false, error: vErr });

    const ctx = await getUserContext(supa, userId);
    const base = {
      ...patch,
      org_id: null,
      company_id: ctx?.company_id ?? null,
      office_id: ctx?.office_id ?? null,
      created_by: userId,
      updated_at: new Date().toISOString(),
    };
    // Day 139 — optional source link (immutable after create).
    const sourcePatch = buildLibrarySourcePatch(req.body ?? {});

    const insert = (fields: string, withSource: boolean) =>
      supa
        .from("whisperer_trigger_library")
        .insert(withSource ? { ...base, ...sourcePatch } : base)
        .select(fields)
        .single();

    let { data, error } = await insert(WTL_FIELDS, true);
    if (error && librarySourceColumnsMissing(error)) {
      // Source columns not migrated here — still create the trigger without
      // them (fail-soft), so manual approval/save is never blocked.
      ({ data, error } = await insert(WTL_FIELDS_BASE, false));
    }
    if (error) {
      if (whispererLibraryTableMissing(error)) {
        return res.status(503).json({ ok: false, error: "migration_required", hint: "Run sql/20260615_whisperer_trigger_library.sql" });
      }
      throw error;
    }
    return res.json({ ok: true, item: normaliseLibraryItem(data) });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-library POST] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "library_create_failed" });
  }
});

// PATCH /v1/manager/whisperer-trigger-library/:id
router.patch("/whisperer-trigger-library/:id", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });
    const id = String(req.params.id || "").trim();

    const { error: vErr, patch } = validateLibraryBody(req.body ?? {}, true);
    if (vErr) return res.status(400).json({ ok: false, error: vErr });
    if (!patch || Object.keys(patch).length === 0) return res.status(400).json({ ok: false, error: "no_fields" });

    const ctx = await getUserContext(supa, userId);
    // Scope guard: only update if the row is in the manager's scope
    let sel = supa.from("whisperer_trigger_library").select("id").eq("id", id);
    sel = applyLibraryScope(sel, ctx);
    const { data: existing, error: selErr } = await sel.maybeSingle();
    if (selErr && !whispererLibraryTableMissing(selErr)) throw selErr;
    if (!existing) return res.status(404).json({ ok: false, error: "not_found_or_out_of_scope" });

    patch.updated_at = new Date().toISOString();
    // Note: source fields are intentionally not editable via PATCH (immutable
    // after create). Fall back to base columns when the Day 139 migration is
    // absent so enable/disable/edit keeps working.
    const update = (fields: string) => supa.from("whisperer_trigger_library").update(patch).eq("id", id).select(fields).single();
    let { data, error } = await update(WTL_FIELDS);
    if (error && librarySourceColumnsMissing(error)) {
      ({ data, error } = await update(WTL_FIELDS_BASE));
    }
    if (error) throw error;
    return res.json({ ok: true, item: normaliseLibraryItem(data) });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-library PATCH] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "library_update_failed" });
  }
});

// DELETE /v1/manager/whisperer-trigger-library/:id
router.delete("/whisperer-trigger-library/:id", async (req: Request, res: Response) => {
  try {
    if (!supa) throw new Error("server_missing_supabase_env");
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });
    const id = String(req.params.id || "").trim();

    const ctx = await getUserContext(supa, userId);
    let sel = supa.from("whisperer_trigger_library").select("id").eq("id", id);
    sel = applyLibraryScope(sel, ctx);
    const { data: existing, error: selErr } = await sel.maybeSingle();
    if (selErr && !whispererLibraryTableMissing(selErr)) throw selErr;
    if (!existing) return res.status(404).json({ ok: false, error: "not_found_or_out_of_scope" });

    const { error } = await supa.from("whisperer_trigger_library").delete().eq("id", id);
    if (error) throw error;
    return res.json({ ok: true, deleted: id });
  } catch (e: any) {
    console.error("[manager.whisperer-trigger-library DELETE] error", e);
    return res.status(500).json({ ok: false, error: e?.message || "library_delete_failed" });
  }
});

export default router;
