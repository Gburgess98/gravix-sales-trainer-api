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
      .select("id,user_id,org_id")
      .eq("id", body.callId)
      .single();
    if (error || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const assignmentPayload = {
      call_id: body.callId,
      assignee_user_id: body.assigneeUserId,
      drill_id: body.drillId,
      notes: body.notes ?? null,
      org_id: (call as any)?.org_id ?? null,
      status: "open",
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
        title: "Coach assignment created",
        status: "open",
        source: "coach_assignment",
        meta: {
          coach_assignment_id: (data as any)?.id ?? null,
          drill_id: body.drillId,
          requester,
          assignee_user_id: body.assigneeUserId,
          notes: body.notes ?? null,
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
    });
    res.json({ ok: true, item: data, activity_debug: activityDebug });
  } catch (e: any) {
    console.error("[coach/assign] failed", e);
    res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

export default r;