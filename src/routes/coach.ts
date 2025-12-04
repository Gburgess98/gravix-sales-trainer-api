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
    const { data: call, error } = await supa.from("calls").select("user_id").eq("id", body.callId).single();
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
    res.json({ ok:true, item:data });
  } catch (e:any) {
    res.status(400).json({ ok:false, error: e?.message || "bad_request" });
  }
});

export default r;