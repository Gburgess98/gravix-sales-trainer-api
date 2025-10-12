import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";
import { postSlack } from "../lib/slack";
const router = Router();
const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const CreateCallSchema = z.object({
    userId: z.string().uuid(),
    userEmail: z.string().email(),
    fileName: z.string(),
    storagePath: z.string(),
    sizeBytes: z.number().optional(),
    durationSec: z.number().optional(),
});
/* ✅ NEW: score payload */
const ScoreSchema = z.object({
    score_overall: z.number().min(0).max(100),
    rubric: z.any().optional(),
});
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
function getUserIdHeader(req) {
    const uid = req.header("x-user-id");
    if (!uid || !UUID_RE.test(uid))
        throw new Error("Missing or invalid x-user-id");
    return uid;
}
/* ---------------------------------------------
   POST /v1/calls  → insert (upsert) + Slack ping
---------------------------------------------- */
router.post("/", async (req, res) => {
    try {
        const body = CreateCallSchema.parse(req.body);
        const insert = {
            user_id: body.userId,
            org_id: process.env.DEFAULT_ORG_ID ?? null, // adapt if NOT NULL
            filename: body.fileName,
            storage_path: body.storagePath,
            size_bytes: body.sizeBytes ?? null,
            // keep other fields null; your /v1/upload flow also inserts, but this route is
            // specifically for "upload from web already done" → Slack + mirror row if needed.
            status: "uploaded",
            kind: "audio",
            mime_type: null,
            sha256: null,
            audio_path: body.storagePath,
        };
        // If you already insert in /v1/upload, upsert prevents duplicates by storage_path
        const { data, error } = await supa
            .from("calls")
            .upsert(insert, { onConflict: "storage_path" })
            .select()
            .single();
        if (error)
            throw error;
        const appUrl = process.env.WEB_APP_URL || process.env.WEB_ORIGIN || "http://localhost:3000";
        const recentLink = `${appUrl}/recent-calls`;
        await postSlack(`New call from ${body.userEmail}`, [
            {
                type: "section",
                text: {
                    type: "mrkdwn",
                    text: `*New call uploaded* by \`${body.userEmail}\``,
                },
            },
            {
                type: "section",
                fields: [
                    { type: "mrkdwn", text: `*File:*\n${body.fileName}` },
                    { type: "mrkdwn", text: `*Size:*\n${body.sizeBytes ?? "?"} bytes` },
                ],
            },
            {
                type: "actions",
                elements: [
                    {
                        type: "button",
                        text: { type: "plain_text", text: "Open Recent Calls" },
                        url: recentLink,
                    },
                ],
            },
        ]);
        res.json({ ok: true, call: data });
    }
    catch (e) {
        console.error(e);
        res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
    }
});
/* -----------------------------------------------------------
   GET /v1/calls  → list recent by query ?userId=...&limit=10
   (simple list you already had; kept as-is)
------------------------------------------------------------ */
router.get("/", async (req, res) => {
    try {
        const userId = String(req.query.userId ?? "");
        const limit = Math.min(Number(req.query.limit ?? 10), 50);
        if (!userId)
            return res.status(400).json({ ok: false, error: "userId required" });
        const { data, error } = await supa
            .from("calls")
            .select("*")
            .eq("user_id", userId)
            .order("created_at", { ascending: false })
            .limit(limit);
        if (error)
            throw error;
        res.json({ ok: true, calls: data });
    }
    catch (e) {
        res.status(400).json({ ok: false, error: e.message });
    }
});
/* -----------------------------------------------------------------
   GET /v1/calls/paged?limit=10&cursor=<ISO or epoch>  → cursor paging
   - Uses x-user-id header for the requester (safer for service-role API)
------------------------------------------------------------------ */
router.get("/paged", async (req, res) => {
    try {
        const requester = getUserIdHeader(req);
        const rawLimit = Number(req.query.limit ?? 10);
        const limit = Math.min(Math.max(rawLimit, 1), 50);
        const cursor = req.query.cursor ? String(req.query.cursor) : null;
        let q = supa
            .from("calls")
            .select("*")
            .eq("user_id", requester)
            .order("created_at", { ascending: false })
            .limit(limit + 1); // one extra to compute nextCursor
        if (cursor) {
            const cursorDate = isNaN(Number(cursor)) ? new Date(cursor) : new Date(Number(cursor));
            if (isNaN(cursorDate.getTime())) {
                return res.status(400).json({ ok: false, error: "invalid cursor" });
            }
            q = q.lt("created_at", cursorDate.toISOString());
        }
        const { data, error } = await q;
        if (error)
            throw error;
        let nextCursor = null;
        let items = data ?? [];
        if (items.length > limit) {
            const last = items[limit - 1];
            nextCursor = last.created_at;
            items = items.slice(0, limit);
        }
        res.json({ ok: true, calls: items, nextCursor });
    }
    catch (e) {
        res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
    }
});
/* ----------------------------------------------------------------
   GET /v1/calls/:id/audio-url → signed URL for secure audio playback
   (ORDERED BEFORE /:id so it doesn't get captured by the :id route)
----------------------------------------------------------------- */
router.get("/:id/audio-url", async (req, res) => {
    try {
        const id = String(req.params.id);
        if (!UUID_RE.test(id)) {
            return res.status(400).json({ ok: false, error: "invalid id" });
        }
        const requester = getUserIdHeader(req);
        // ensure call exists and belongs to requester
        const { data: call, error } = await supa
            .from("calls")
            .select("user_id,audio_path")
            .eq("id", id)
            .single();
        if (error || !call)
            return res.status(404).json({ ok: false, error: "not_found" });
        if (call.user_id !== requester) {
            return res.status(403).json({ ok: false, error: "forbidden" });
        }
        if (!call.audio_path) {
            return res.status(400).json({ ok: false, error: "no_audio_path" });
        }
        const bucket = process.env.SUPABASE_STORAGE_BUCKET || "calls";
        const ttl = Number(process.env.SIGNED_URL_TTL_SEC || 3600); // seconds
        const { data: signed, error: signErr } = await supa
            .storage
            .from(bucket)
            .createSignedUrl(call.audio_path, ttl);
        if (signErr || !signed?.signedUrl) {
            return res
                .status(500)
                .json({ ok: false, error: `sign_failed: ${signErr?.message ?? "unknown"}` });
        }
        res.json({ ok: true, url: signed.signedUrl, ttl });
    }
    catch (e) {
        res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
    }
});
/* ---------------------------------------------
   POST /v1/calls/:id/score → set score (+rubric)
   (ORDERED BEFORE /:id to avoid route capture)
---------------------------------------------- */
router.post("/:id/score", async (req, res) => {
    try {
        const id = String(req.params.id);
        if (!UUID_RE.test(id))
            return res.status(400).json({ ok: false, error: "invalid id" });
        const requester = getUserIdHeader(req);
        const body = ScoreSchema.parse(req.body);
        // verify ownership
        const { data: call, error: callErr } = await supa
            .from("calls")
            .select("id,user_id")
            .eq("id", id)
            .single();
        if (callErr || !call)
            return res.status(404).json({ ok: false, error: "not_found" });
        if (call.user_id !== requester)
            return res.status(403).json({ ok: false, error: "forbidden" });
        const patch = {
            score_overall: body.score_overall,
            status: "scored",
        };
        if (typeof body.rubric !== "undefined")
            patch.rubric = body.rubric;
        const { data, error } = await supa
            .from("calls")
            .update(patch)
            .eq("id", id)
            .select()
            .single();
        if (error)
            throw error;
        res.json({ ok: true, call: data });
    }
    catch (e) {
        res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
    }
});
/* -------------------------------------------
   GET /v1/calls/:id → call detail (ownership)
-------------------------------------------- */
router.get("/:id", async (req, res) => {
    try {
        const id = String(req.params.id);
        if (!UUID_RE.test(id)) {
            return res.status(400).json({ ok: false, error: "invalid id" });
        }
        const requester = getUserIdHeader(req);
        const { data: call, error } = await supa
            .from("calls")
            .select("*")
            .eq("id", id)
            .single();
        if (error || !call)
            return res.status(404).json({ ok: false, error: "not_found" });
        if (call.user_id !== requester) {
            return res.status(403).json({ ok: false, error: "forbidden" });
        }
        res.json({ ok: true, call });
    }
    catch (e) {
        res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
    }
});
export default router;
