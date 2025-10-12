import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function uidFromHeader(req: any): string {
  const uid = req.header("x-user-id");
  if (!uid || !UUID_RE.test(uid)) throw new Error("Missing or invalid x-user-id");
  return uid;
}

const CreatePinSchema = z.object({
  callId: z.string().uuid(),
  t: z.number().int().min(0).max(24 * 60 * 60), // up to 24h audio
  note: z.string().trim().max(500).nullable().optional(),
});

/* -------------------------------------------
   GET /v1/pins?callId=...
   -> list pins for a call (ownership enforced)
-------------------------------------------- */
router.get("/", async (req, res) => {
  try {
    const requester = uidFromHeader(req);
    const callId = String(req.query.callId || "");
    if (!UUID_RE.test(callId)) {
      return res.status(400).json({ ok: false, error: "invalid callId" });
    }

    // Verify the call belongs to requester
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id,user_id")
      .eq("id", callId)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const { data: pins, error } = await supa
      .from("pins")
      .select("*")
      .eq("call_id", callId)
      .order("t", { ascending: true })
      .limit(500);

    if (error) throw error;

    res.json({ ok: true, pins: pins ?? [] });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* -------------------------------------------
   POST /v1/pins
   body: { callId, t, note }
   -> create a pin (ownership enforced)
-------------------------------------------- */
router.post("/", async (req, res) => {
  try {
    const requester = uidFromHeader(req);
    const body = CreatePinSchema.parse(req.body);

    // Verify call ownership
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id,user_id")
      .eq("id", body.callId)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const insert = {
      call_id: body.callId,
      user_id: requester,
      t: body.t,
      note: body.note ?? null,
    };

    const { data, error } = await supa
      .from("pins")
      .insert(insert)
      .select()
      .single();

    if (error) throw error;

    res.json({ ok: true, pin: data });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* -------------------------------------------
   DELETE /v1/pins/:id
   -> delete if the pin belongs to requester
-------------------------------------------- */
router.delete("/:id", async (req, res) => {
  try {
    const requester = uidFromHeader(req);
    const id = String(req.params.id);
    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid id" });
    }

    // Fetch pin + join call to confirm ownership
    const { data: pinRow, error: pinErr } = await supa
      .from("pins")
      .select("id, call_id")
      .eq("id", id)
      .single();

    if (pinErr || !pinRow) return res.status(404).json({ ok: false, error: "not_found" });

    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("user_id")
      .eq("id", pinRow.call_id)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const { error } = await supa.from("pins").delete().eq("id", id);
    if (error) throw error;

    res.json({ ok: true });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

export default router;