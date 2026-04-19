import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";

const r = Router();
const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

const UUID = z.string().uuid();
const Create = z.object({
  callId: UUID,
  assigneeUserId: UUID,
  drillId: z.string().min(1),
  notes: z.string().max(500).optional(),
});

function uid(req:any){ const v=(req.header("x-user-id")||"").trim(); if(!v) throw new Error("Missing x-user-id"); return v; }

r.post("/assign", async (req, res) => {
  try {
    const body = Create.parse(req.body);
    const requester = uid(req);

    // verify requester owns the call OR is same org (simplest: owner only)
    const { data: call, error } = await supa
      .from("calls")
      .select("id,user_id,org_id")
      .eq("id", body.callId)
      .single();
    if (error || !call) return res.status(404).json({ ok:false, error:"not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok:false, error:"forbidden" });

    const { data, error: insErr } = await supa.from("coach_assignments").insert({
      call_id: body.callId,
      assignee_user_id: body.assigneeUserId,
      drill_id: body.drillId,
      notes: body.notes ?? null,
      created_by: requester,
    }).select().single();
    if (insErr) throw insErr;

    // Day 64/63 stability: write linked CRM activity with org_id + call_id + rep_id
    try {
      const activityPayload = {
        org_id: (call as any)?.org_id ?? null,
        user_id: body.assigneeUserId,
        rep_id: body.assigneeUserId,
        call_id: body.callId,
        type: "coach_assignment",
        title: "Coach assignment created",
        description: body.notes ?? `Assigned drill: ${body.drillId}`,
        status: "open",
        source: "coach_assignment",
        meta: {
          coach_assignment_id: (data as any)?.id ?? null,
          drill_id: body.drillId,
          created_by: requester,
          requester,
          assignee_user_id: body.assigneeUserId,
        },
      } as any;

      const insertWithRep = await supa.from("crm_activities").insert(activityPayload);
      if (insertWithRep.error) {
        const msg = String((insertWithRep.error as any)?.message ?? "").toLowerCase();
        const missingOrg = msg.includes("org_id") && msg.includes("does not exist");
        const missingRep = msg.includes("rep_id") && msg.includes("does not exist");
        const missingMeta = msg.includes("meta") && msg.includes("does not exist");
        const missingSource = msg.includes("source") && msg.includes("does not exist");

        if (missingOrg || missingRep || missingMeta || missingSource) {
          const fallbackPayload = {
            user_id: body.assigneeUserId,
            call_id: body.callId,
            type: "coach_assignment",
            title: "Coach assignment created",
            description: body.notes ?? `Assigned drill: ${body.drillId}`,
            status: "open",
          } as any;

          console.warn("[coach/assign] crm_activities retrying with fallback payload", {
            missingOrg,
            missingRep,
            missingMeta,
            missingSource,
          });

          const retry = await supa.from("crm_activities").insert(fallbackPayload);
          if (retry.error) {
            console.warn("[coach/assign] crm_activities fallback insert failed", retry.error);
          }
        } else {
          console.warn("[coach/assign] crm_activities insert failed", insertWithRep.error);
        }
      }
    } catch (activityErr) {
      console.warn("[coach/assign] activity write failed", activityErr);
    }

    console.log("[coach/assign] created", {
      coachAssignmentId: (data as any)?.id ?? null,
      call_id: body.callId,
      org_id: (call as any)?.org_id ?? null,
      rep_id: body.assigneeUserId,
      created_by: requester,
    });
    res.json({ ok:true, item:data });
  } catch (e:any) {
    res.status(400).json({ ok:false, error: e?.message || "bad_request" });
  }
});

export default r;