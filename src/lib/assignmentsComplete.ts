import { createClient } from "@supabase/supabase-js";

type CompletedBy = "rep" | "system";

type AssignmentType = "sparring" | "call_review" | "custom";

async function awardXp(opts: {
  repId: string;
  xp: number;
  source: "sparring" | "call_review";
}) {
  const { repId, xp, source } = opts;
  if (!repId || !xp || xp <= 0) return;

  const supa = getSupa();

  await supa.from("rep_xp_events").insert({
    rep_id: repId,
    xp,
    source,
    created_at: new Date().toISOString(),
  });
}

function getSupa() {
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
  return createClient(url, key, { auth: { persistSession: false } });
}

async function markCompletedById(opts: {
  assignmentId: string;
  repId: string;
  type?: AssignmentType;
  completedBy?: CompletedBy;
}) {
  const { assignmentId, repId, type, completedBy = "system" } = opts;
  if (!assignmentId || !repId) return { ok: false as const, updated: 0 };

  const supa = getSupa();

  let q = supa
    .from("assignments")
    .update({
      status: "completed",
      completed_at: new Date().toISOString(),
      completed_by: completedBy,
    })
    .eq("id", assignmentId)
    .eq("rep_id", repId)
    .neq("status", "completed");

  if (type) q = q.eq("type", type);

  const { data, error } = await q.select("id");
  if (error) throw error;

  if ((data || []).length > 0 && type) {
    if (type === "sparring") {
      await awardXp({ repId, xp: 50, source: "sparring" });
    }
    if (type === "call_review") {
      await awardXp({ repId, xp: 25, source: "call_review" });
    }
  }

  return { ok: true as const, updated: (data || []).length };
}

async function markCompletedLatestAssigned(opts: {
  repId: string;
  type: AssignmentType;
  completedBy?: CompletedBy;
}) {
  const { repId, type, completedBy = "system" } = opts;
  if (!repId) return { ok: false as const, updated: 0 };

  const supa = getSupa();

  const { data: latest, error: selErr } = await supa
    .from("assignments")
    .select("id")
    .eq("rep_id", repId)
    .eq("type", type)
    .eq("status", "assigned")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (selErr) throw selErr;
  if (!latest?.id) return { ok: true as const, updated: 0 };

  return markCompletedById({
    assignmentId: latest.id,
    repId,
    type,
    completedBy,
  });
}

/**
 * Existing behaviour (idempotent): mark assignments completed by (rep_id, type, target_id).
 * 
 * Extended:
 * - If `assignmentId` is provided, we complete that exact assignment for the rep (preferred path).
 */
export async function completeAssignmentsForTarget(opts: {
  repId: string;
  type: AssignmentType;
  targetId: string | null;
  assignmentId?: string | null;
  completedBy?: CompletedBy;
}) {
  const {
    repId,
    type,
    targetId,
    assignmentId = null,
    completedBy = "system",
  } = opts;

  if (!repId) return;

  // 1) Exact assignment completion (best signal)
  if (assignmentId) {
    await markCompletedById({
      assignmentId,
      repId,
      type,
      completedBy,
    });
    return;
  }

  // 2) Back-compat: complete by target match
  const supa = getSupa();

  const { error } = await supa
    .from("assignments")
    .update({
      status: "completed",
      completed_at: new Date().toISOString(),
      completed_by: completedBy,
    })
    .eq("rep_id", repId)
    .eq("type", type)
    .eq("target_id", targetId)
    .neq("status", "completed");

  if (error) throw error;

  if (type === "sparring") {
    await awardXp({ repId, xp: 50, source: "sparring" });
  }
  if (type === "call_review") {
    await awardXp({ repId, xp: 25, source: "call_review" });
  }
}

/**
 * THE MAGIC v1 for call scoring:
 * When a call is scored, we try to auto-complete a `call_review` assignment.
 * 
 * Priority:
 *  1) assignmentId (if supplied)
 *  2) match target_id === callId
 *  3) latest assigned call_review for the rep
 */
export async function completeCallReviewOnScore(opts: {
  repId: string;
  callId: string;
  assignmentId?: string | null;
  completedBy?: CompletedBy;
}) {
  const { repId, callId, assignmentId = null, completedBy = "system" } = opts;
  if (!repId) return { ok: true as const, updated: 0 };

  // 1) Exact assignment
  if (assignmentId) {
    const result = await markCompletedById({
      assignmentId,
      repId,
      type: "call_review",
      completedBy,
    });
    if (result.updated > 0) {
      await awardXp({ repId, xp: 25, source: "call_review" });
    }
    return result;
  }

  // 2) target_id === callId
  const supa = getSupa();
  const { data, error } = await supa
    .from("assignments")
    .update({
      status: "completed",
      completed_at: new Date().toISOString(),
      completed_by: completedBy,
    })
    .eq("rep_id", repId)
    .eq("type", "call_review")
    .eq("target_id", callId)
    .eq("status", "assigned")
    .select("id");

  if (error) throw error;
  if ((data || []).length > 0) {
    await awardXp({ repId, xp: 25, source: "call_review" });
    return { ok: true as const, updated: (data || []).length };
  }

  // 3) latest assigned call_review
  const result = await markCompletedLatestAssigned({
    repId,
    type: "call_review",
    completedBy,
  });
  if (result.updated > 0) {
    await awardXp({ repId, xp: 25, source: "call_review" });
  }
  return result;
}

/**
 * THE MAGIC v1 for sparring completion:
 * When a sparring session is completed, we try to auto-complete a `sparring` assignment.
 * 
 * Priority:
 *  1) assignmentId (if supplied)
 *  2) latest assigned sparring for the rep
 */
export async function completeSparringOnComplete(opts: {
  repId: string;
  assignmentId?: string | null;
  completedBy?: CompletedBy;
}) {
  const { repId, assignmentId = null, completedBy = "system" } = opts;
  if (!repId) return { ok: true as const, updated: 0 };

  if (assignmentId) {
    const result = await markCompletedById({
      assignmentId,
      repId,
      type: "sparring",
      completedBy,
    });
    if (result.updated > 0) {
      await awardXp({ repId, xp: 50, source: "sparring" });
    }
    return result;
  }

  const result = await markCompletedLatestAssigned({
    repId,
    type: "sparring",
    completedBy,
  });
  if (result.updated > 0) {
    await awardXp({ repId, xp: 50, source: "sparring" });
  }
  return result;
}