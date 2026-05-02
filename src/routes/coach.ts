import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";

console.log("[coach.ts] module loaded", new Date().toISOString());

const r = Router();
const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

const UUID = z.string().uuid();
const Create = z.object({
  callId: UUID,
  assigneeUserId: UUID,
  drillId: z.string().min(1),
  notes: z.string().max(500).optional(),
});

function uid(req: any) { const v = (req.header("x-user-id") || "").trim(); if (!v) throw new Error("Missing x-user-id"); return v; }

r.post("/assign", async (req, res) => {
  try {
    const body = Create.parse(req.body);
    const requester = uid(req);
    let activityDebug: any = { attempted: false, ok: false, error: null };

    console.log("[coach/assign] handler reached", {
      callId: body.callId,
      assigneeUserId: body.assigneeUserId,
      requester,
    });

    // verify requester owns the call OR is same org (simplest: owner only)
    const { data: call, error } = await supa
      .from("calls")
      .select("id,user_id,org_id,analysis_json,score_overall")
      .eq("id", body.callId)
      .single();
    if (error || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const reviewFlags = Array.isArray((call as any)?.analysis_json?.review_flags)
      ? (call as any).analysis_json.review_flags
      : [];
    const thresholdBand =
      typeof (call as any)?.analysis_json?.threshold_band === "string"
        ? String((call as any).analysis_json.threshold_band)
        : null;
    const needsManagerReview = Boolean((call as any)?.analysis_json?.needs_manager_review);
    const isFlaggedCall = reviewFlags.length > 0 || Boolean(thresholdBand) || needsManagerReview;

    const assignmentSource = isFlaggedCall ? "flagged_call" : "manual";
    const activityTitle = isFlaggedCall ? "Flagged call coaching assignment created" : "Coach assignment created";
    const activitySummary = isFlaggedCall
      ? `Created coaching assignment from flagged call (${thresholdBand || "flagged"})`
      : "Created manual coaching assignment";

    const duplicateCheck = await supa
      .from("coach_assignments")
      .select("id,call_id,assignee_user_id,drill_id,notes,org_id,status,source,meta,created_at")
      .eq("call_id", body.callId)
      .eq("assignee_user_id", body.assigneeUserId)
      .eq("drill_id", body.drillId)
      .neq("status", "completed")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (duplicateCheck.error) {
      const msg = String((duplicateCheck.error as any)?.message ?? "").toLowerCase();
      const schemaCacheIssue = msg.includes("schema cache") || msg.includes("could not find");
      if (!schemaCacheIssue) {
        console.warn("[coach/assign] duplicate check failed", duplicateCheck.error);
      }
    }

    if (!duplicateCheck.error && duplicateCheck.data) {
      console.log("[coach/assign] skipped duplicate open assignment", {
        existingId: (duplicateCheck.data as any)?.id ?? null,
        call_id: body.callId,
        assignee_user_id: body.assigneeUserId,
      });

      return res.json({
        ok: true,
        deduped: true,
        item: duplicateCheck.data,
        reporting: {
          assignment_origin: String((duplicateCheck.data as any)?.source || assignmentSource),
          flagged_call: Boolean((duplicateCheck.data as any)?.meta?.flagged_call ?? isFlaggedCall),
          threshold_band: (duplicateCheck.data as any)?.meta?.threshold_band ?? thresholdBand,
          review_flag_count: Array.isArray((duplicateCheck.data as any)?.meta?.review_flags)
            ? (duplicateCheck.data as any).meta.review_flags.length
            : reviewFlags.length,
          needs_manager_review: Boolean((duplicateCheck.data as any)?.meta?.needs_manager_review ?? needsManagerReview),
        },
        activity_debug: {
          attempted: false,
          ok: true,
          error: null,
          skipped: "duplicate_open_assignment",
        },
      });
    }

    const assignmentPayload = {
      call_id: body.callId,
      assignee_user_id: body.assigneeUserId,
      drill_id: body.drillId,
      notes: body.notes ?? null,
      org_id: (call as any)?.org_id ?? null,
      status: "open",
      source: assignmentSource,
      meta: {
        source: assignmentSource,
        action_type: "assignment_created",
        assignment_origin: assignmentSource,
        dedupe_key: `${body.callId}:${body.assigneeUserId}:${body.drillId}`,
        flagged_call: isFlaggedCall,
        threshold_band: thresholdBand,
        needs_manager_review: needsManagerReview,
        review_flags: reviewFlags,
        score_overall: (call as any)?.score_overall ?? null,
        // NEW: tracking fields for analytics
        flag_sections: reviewFlags.map((f: any) => f.section).filter(Boolean),
        score_before: (call as any)?.score_overall ?? null,
      },
    } as any;

    console.log("[coach/assign] inserting assignment", assignmentPayload);

    let { data, error: insErr } = await supa
      .from("coach_assignments")
      .insert(assignmentPayload)
      .select()
      .single();

    if (insErr) {
      const msg = String((insErr as any)?.message ?? "").toLowerCase();
      const missingOrg = msg.includes("org_id") && msg.includes("schema cache");
      const fallbackPayload = {
        call_id: body.callId,
        assignee_user_id: body.assigneeUserId,
        drill_id: body.drillId,
        notes: body.notes ?? null,
        status: "open",
        source: assignmentSource,
        meta: {
          source: assignmentSource,
          action_type: "assignment_created",
          assignment_origin: assignmentSource,
          dedupe_key: `${body.callId}:${body.assigneeUserId}:${body.drillId}`,
          flagged_call: isFlaggedCall,
          threshold_band: thresholdBand,
          needs_manager_review: needsManagerReview,
          review_flags: reviewFlags,
          score_overall: (call as any)?.score_overall ?? null,
        },
      } as any;

      if (missingOrg) {
        console.warn("[coach/assign] retrying assignment insert without org_id");
        const retry = await supa
          .from("coach_assignments")
          .insert(fallbackPayload)
          .select()
          .single();
        data = retry.data as any;
        insErr = retry.error as any;
      }
    }

    if (insErr) {
      console.error("[coach/assign] coach_assignments insert failed", insErr);
      throw insErr;
    }

    // Day 64/65: write linked CRM activity with full linkage fields
    try {
      const activityPayload = {
        org_id: (call as any)?.org_id ?? null,
        user_id: body.assigneeUserId,
        rep_id: body.assigneeUserId,
        call_id: body.callId,
        type: "coach_assignment",
        title: activityTitle,
        summary: activitySummary,
        status: "open",
        source: "coach_assignment",
        meta: {
          coach_assignment_id: (data as any)?.id ?? null,
          drill_id: body.drillId,
          requester,
          assignee_user_id: body.assigneeUserId,
          notes: body.notes ?? null,
          source: assignmentSource,
          action_type: "assignment_created",
          assignment_origin: assignmentSource,
          dedupe_key: `${body.callId}:${body.assigneeUserId}:${body.drillId}`,
          flagged_call: isFlaggedCall,
          threshold_band: thresholdBand,
          needs_manager_review: needsManagerReview,
          review_flags: reviewFlags,
          review_flag_count: reviewFlags.length,
          score_overall: (call as any)?.score_overall ?? null,
          // NEW: tracking fields for analytics
          flag_sections: reviewFlags.map((f: any) => f.section).filter(Boolean),
          score_before: (call as any)?.score_overall ?? null,
        },
      } as any;

      activityDebug.attempted = true;
      console.log("[coach/assign] inserting activity", activityPayload);

      const insertRes = await supa.from("crm_activities").insert(activityPayload);
      if (insertRes.error) {
        activityDebug.ok = false;
        activityDebug.error = String((insertRes.error as any)?.message ?? insertRes.error);
        console.warn("[coach/assign] crm_activities insert failed", insertRes.error);
      } else {
        activityDebug.ok = true;
      }
    } catch (activityErr: any) {
      activityDebug.attempted = true;
      activityDebug.ok = false;
      activityDebug.error = String(activityErr?.message ?? activityErr);
      console.warn("[coach/assign] activity write failed", activityErr);
    }

    console.log("[coach/assign] created", {
      coachAssignmentId: (data as any)?.id ?? null,
      call_id: body.callId,
      org_id: (call as any)?.org_id ?? null,
      rep_id: body.assigneeUserId,
      created_by: requester,
      flagged_call: isFlaggedCall,
      threshold_band: thresholdBand,
      review_flag_count: reviewFlags.length,
    });
    res.json({
      ok: true,
      item: data,
      reporting: {
        assignment_origin: assignmentSource,
        flagged_call: isFlaggedCall,
        threshold_band: thresholdBand,
        review_flag_count: reviewFlags.length,
        needs_manager_review: needsManagerReview,
        deduped: false,
      },
      activity_debug: activityDebug,
    });
  } catch (e: any) {
    console.error("[coach/assign] failed", e);
    res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

export default r;