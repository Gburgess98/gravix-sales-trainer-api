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

export function needsReview(call: ScoredCallRow): boolean {
  const overall = Number(call.score_overall);
  if (Number.isFinite(overall) && overall < REVIEW_SCORE_THRESHOLD) return true;

  const stages = extractStageScores(call.analysis_json);
  return SKILL_KEYS.some((key) => {
    const score = stages[key];
    return typeof score === "number" && score < CRITICAL_STAGE_THRESHOLD;
  });
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
  if (dueMs < now + 48 * 3600 * 1000) return "medium";
  return "low";
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
      .select("id, rep_id, title, status, due_at, created_at, office_id, company_id")
      .eq("status", "assigned")
      .order("due_at", { ascending: true, nullsFirst: false })
      .limit(1000);
    assignmentsQuery = applyHierarchyFilters(assignmentsQuery, userContext);

    const [callsResult, assignmentsResult] = await Promise.all([callsQuery, assignmentsQuery]);

    if (callsResult.error) throw callsResult.error;
    if (assignmentsResult.error) throw assignmentsResult.error;

    const scoredCalls = (callsResult.data ?? []) as ScoredCallRow[];
    const openAssignmentRows = (assignmentsResult.data ?? []) as Array<{
      id: string;
      rep_id: string | null;
      title: string | null;
      status: string;
      due_at: string | null;
      created_at: string;
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

    // ── Calls needing review (lowest score first, cap 10) ──
    const reviewCalls = scoredCalls
      .filter(needsReview)
      .sort((a, b) => Number(a.score_overall ?? 101) - Number(b.score_overall ?? 101))
      .slice(0, 10)
      .map((call) => {
        const stages = extractStageScores(call.analysis_json);
        const weakest = weakestSkillOf(stages);
        return {
          callId: String(call.id),
          repName: nameOf(call.user_id),
          title: String(call.filename || "Untitled call"),
          overallScore: Number.isFinite(Number(call.score_overall)) ? Number(call.score_overall) : 0,
          weakestSkill: weakest ? SKILL_LABELS[weakest] : "Unknown",
          createdAt: String(call.created_at || ""),
        };
      });
    const callsNeedingReviewCount = scoredCalls.filter(needsReview).length;

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
        return {
          assignmentId: String(a.id),
          repName: nameOf(a.rep_id),
          title: String(a.title || "Coaching assignment"),
          status: (isOverdue ? "overdue" : "open") as "open" | "overdue",
          dueAt: a.due_at ?? null,
          priority: assignmentPriority(a.due_at, now),
        };
      })
      .sort((a, b) => {
        if (a.status !== b.status) return a.status === "overdue" ? -1 : 1;
        const aDue = a.dueAt ? new Date(a.dueAt).getTime() : Infinity;
        const bDue = b.dueAt ? new Date(b.dueAt).getTime() : Infinity;
        return aDue - bDue;
      })
      .slice(0, 10);

    // ── Weakest skills (weak occurrences, lowest average first) ──
    const weakestSkills = Array.from(skillAgg.entries())
      .map(([key, agg]) => ({
        skill: SKILL_LABELS[key],
        count: agg.weakCount,
        averageScore: agg.count > 0 ? Math.round(agg.sum / agg.count) : 0,
      }))
      .filter((s) => s.count > 0)
      .sort((a, b) => a.averageScore - b.averageScore);

    // ── ROI (Day 90: reviewed = scored; switches to call_manager_reviews on Day 91) ──
    const callsReviewed = scoredCalls.length;
    const estimatedMinutesSaved = callsReviewed * MINUTES_SAVED_PER_REVIEWED_CALL;
    const estimatedHoursSaved = Math.round((estimatedMinutesSaved / 60) * 10) / 10;

    return res.json({
      ok: true,
      windowDays: days,
      teamHealth: {
        status: computeTeamHealthStatus({
          averageScore: teamAverage,
          callsNeedingReview: callsNeedingReviewCount,
          overdueAssignments: overdueAssignmentRows.length,
        }),
        averageScore: teamAverage ?? 0,
        reviewedCalls: scoredCalls.length,
        callsNeedingReview: callsNeedingReviewCount,
        openAssignments: openAssignmentRows.length,
        overdueAssignments: overdueAssignmentRows.length,
      },
      repsNeedingAttention,
      callsNeedingReview: reviewCalls,
      openAssignments,
      weakestSkills,
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

export default router;
