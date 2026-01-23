import { createClient } from "@supabase/supabase-js";

type CompletedBy = "rep" | "system";

type AssignmentType = "sparring" | "call_review" | "custom";

async function awardXp(opts: {
  repId: string;
  xp: number;
  source: "sparring" | "call_review" | "custom";
  assignmentId: string;
}) {
  const { repId, xp, source, assignmentId } = opts;
  if (!repId || !assignmentId || !xp || xp <= 0) return { xpAwarded: 0 };

  const supa = getSupa();

  // Idempotency guard: if we already logged XP for this assignment, do nothing.
  // (Requires rep_xp_events.assignment_id — if missing, this will harmlessly fail and we’ll fall back to insert.)
  try {
    const existing = await supa
      .from("rep_xp_events")
      .select("id")
      .eq("assignment_id", assignmentId)
      .limit(1)
      .maybeSingle();

    if (!existing.error && existing.data?.id) {
      return { xpAwarded: 0 };
    }
  } catch {
    // ignore
  }

  const { error } = await supa.from("rep_xp_events").insert({
    rep_id: repId,
    xp,
    source,
    assignment_id: assignmentId,
    created_at: new Date().toISOString(),
  } as any);

  if (error) {
    // If a unique constraint exists, duplicates will land here too.
    return { xpAwarded: 0, error: error.message };
  }

  return { xpAwarded: xp };
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

  let xpAwardedTotal = 0;

  if ((data || []).length > 0 && type) {
    const ids = (data || []).map((r: any) => r.id).filter(Boolean);

    for (const id of ids) {
      if (type === "sparring") {
        const xpRes = await awardXp({
          repId,
          xp: 50,
          source: "sparring",
          assignmentId: String(id),
        });
        xpAwardedTotal += Number((xpRes as any)?.xpAwarded || 0);
      }
      if (type === "call_review") {
        const xpRes = await awardXp({
          repId,
          xp: 25,
          source: "call_review",
          assignmentId: String(id),
        });
        xpAwardedTotal += Number((xpRes as any)?.xpAwarded || 0);
      }
    }
  }

  const updated = (data || []).length;
  return { ok: true as const, updated, completedCount: updated, xpAwardedTotal };
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
}): Promise<{ ok: true; updated: number; completedCount: number; xpAwardedTotal: number }> {
  const {
    repId,
    type,
    targetId,
    assignmentId = null,
    completedBy = "system",
  } = opts;

  if (!repId) return { ok: true as const, updated: 0, completedCount: 0, xpAwardedTotal: 0 };

  // 1) Exact assignment completion (best signal)
  if (assignmentId) {
    const result = await markCompletedById({
      assignmentId,
      repId,
      type,
      completedBy,
    });
    return {
      ok: true as const,
      updated: result.updated,
      completedCount: (result as any).completedCount ?? result.updated,
      xpAwardedTotal: (result as any).xpAwardedTotal ?? 0,
    };
  }

  // 2) Back-compat: complete by target match
  const supa = getSupa();

  const { data, error } = await supa
    .from("assignments")
    .update({
      status: "completed",
      completed_at: new Date().toISOString(),
      completed_by: completedBy,
    })
    .eq("rep_id", repId)
    .eq("type", type)
    .eq("target_id", targetId)
    .neq("status", "completed")
    .select("id");

  if (error) throw error;

  const updated = (data || []).length;
  let xpAwardedTotal = 0;

  if (updated > 0) {
    const ids = (data || []).map((r: any) => r.id).filter(Boolean);

    for (const id of ids) {
      if (type === "sparring") {
        const xpRes = await awardXp({
          repId,
          xp: 50,
          source: "sparring",
          assignmentId: String(id),
        });
        xpAwardedTotal += Number((xpRes as any)?.xpAwarded || 0);
      }
      if (type === "call_review") {
        const xpRes = await awardXp({
          repId,
          xp: 25,
          source: "call_review",
          assignmentId: String(id),
        });
        xpAwardedTotal += Number((xpRes as any)?.xpAwarded || 0);
      }
    }
  }

  return { ok: true as const, updated, completedCount: updated, xpAwardedTotal };
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
    return { ok: true as const, updated: (data || []).length };
  }

  // 3) latest assigned call_review
  const result = await markCompletedLatestAssigned({
    repId,
    type: "call_review",
    completedBy,
  });
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
    return result;
  }

  const result = await markCompletedLatestAssigned({
    repId,
    type: "sparring",
    completedBy,
  });
  return result;
}