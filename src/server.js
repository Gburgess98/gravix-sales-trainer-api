// api/src/server.ts
import "dotenv/config";
import express from "express";
import multer from "multer";
import crypto from "crypto";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";
import { postSlack } from "./lib/slack";
import { scoreWithLLM } from "./lib/scoring";
import callsRouter from "./routes/calls"; // existing (provides /v1/calls/:id and /v1/calls/:id/pins)
import pinsRouter from "./routes/pins"; // standalone /v1/pins CRUD
const app = express();
app.use((req, _res, next) => {
    console.log(`[api] ${req.method} ${req.path}`);
    next();
});
/* ----------------------------
   CORS (allow Vercel + tunnel)
----------------------------- */
const allowedOrigins = [
    process.env.WEB_ORIGIN || "http://localhost:3000", // local web
    process.env.VERCEL_ORIGIN, // e.g. https://gravix-staging.vercel.app
    process.env.TUNNEL_ORIGIN // e.g. https://abc123.trycloudflare.com
].filter(Boolean);
console.log("CORS allow-list:", allowedOrigins);
app.use(cors({
    origin: (origin, callback) => {
        if (!origin)
            return callback(null, true); // same-origin / server-to-server
        if (allowedOrigins.includes(origin))
            return callback(null, true);
        return callback(new Error(`CORS blocked for origin: ${origin}`));
    },
    methods: ["GET", "POST", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "x-user-id"],
    credentials: true,
}));
/* Handle preflight */
app.use((req, res, next) => {
    if (req.method === "OPTIONS")
        return res.sendStatus(204);
    next();
});
/* JSON body */
app.use(express.json());
/* Supabase server client */
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });
const DEFAULT_ORG_ID = process.env.DEFAULT_ORG_ID;
if (!DEFAULT_ORG_ID) {
    console.warn("⚠️ DEFAULT_ORG_ID not set — calls.org_id NOT NULL will fail.");
}
/* Debug on boot */
(async () => {
    console.log("Supabase URL:", process.env.SUPABASE_URL);
    const { data: buckets, error } = await supabase.storage.listBuckets();
    if (error)
        console.error("List buckets error:", error.message);
    else
        console.log("Buckets found:", buckets?.map((b) => b.name));
})();
/* Uploads */
const BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "calls";
const upload = multer({ storage: multer.memoryStorage() });
function getUserId(req) {
    const uid = req.header("x-user-id") || process.env.DEV_TEST_UID;
    if (!uid ||
        !/^[0-9a-fA-F-]{8}-[0-9a-fA-F-]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(uid)) {
        throw new Error("Missing or invalid x-user-id");
    }
    return uid;
}
app.post("/v1/upload", upload.single("file"), async (req, res) => {
    try {
        console.log("UPLOAD HIT at", new Date().toISOString());
        const userId = getUserId(req);
        const f = req.file;
        if (!f)
            return res.status(400).json({ ok: false, error: "No file uploaded" });
        const isJson = f.mimetype.includes("json");
        const kind = isJson ? "json" : "audio";
        const id = crypto.randomUUID();
        const ext = f.originalname.includes(".")
            ? f.originalname.slice(f.originalname.lastIndexOf("."))
            : isJson
                ? ".json"
                : ".wav";
        const key = `${userId}/${id}${ext}`;
        const hash = crypto.createHash("sha256").update(f.buffer).digest("hex");
        // 1) Storage
        const { error: upErr } = await supabase.storage.from(BUCKET).upload(key, f.buffer, {
            contentType: f.mimetype,
            upsert: false,
        });
        if (upErr) {
            return res.status(500).json({ ok: false, error: `Storage upload failed: ${upErr.message}` });
        }
        // 2) DB row
        const { error: dbErrCall } = await supabase.from("calls").insert({
            id,
            user_id: userId,
            org_id: DEFAULT_ORG_ID,
            filename: f.originalname,
            storage_path: key,
            mime_type: f.mimetype,
            size_bytes: f.size,
            sha256: hash,
            kind,
            status: "queued",
            audio_path: key,
        });
        if (dbErrCall) {
            await supabase.storage.from(BUCKET).remove([key]).catch(() => { });
            return res.status(500).json({ ok: false, error: `DB insert failed: ${dbErrCall.message}` });
        }
        // 3) Job row
        const jobId = crypto.randomUUID();
        const { error: dbErrJob } = await supabase.from("jobs").insert({
            id: jobId,
            call_id: id,
            user_id: userId,
            kind: "transcribe",
            status: "queued",
        });
        if (dbErrJob) {
            return res.status(500).json({ ok: false, error: `Job insert failed: ${dbErrJob.message}` });
        }
        // 4) Sim worker (stub)
        simulateTranscription(jobId, id, key, userId).catch((e) => console.error("Sim worker failed:", e.message));
        return res.json({
            ok: true,
            callId: id,
            jobId,
            kind,
            filename: f.originalname,
            storagePath: key,
            size: f.size,
            mime: f.mimetype,
            sha256: hash,
        });
    }
    catch (e) {
        return res.status(400).json({ ok: false, error: e.message || "Upload failed" });
    }
});
/* Health (supports /health and /v1/health + HEAD) */
app.get(["/health", "/v1/health"], (_req, res) => res.json({ ok: true }));
app.head(["/health", "/v1/health"], (_req, res) => res.status(200).end());
/* Jobs (unchanged) */
app.get("/v1/jobs/:id", async (req, res) => {
    const { data, error } = await supabase.from("jobs").select("*").eq("id", req.params.id).single();
    if (error)
        return res.status(404).json({ ok: false, error: error.message });
    res.json({ ok: true, job: data });
});
/* 🔧 Admin: enqueue a score job for a call (dev only) — returns jobId */
app.post("/v1/admin/score/:id", async (req, res) => {
    try {
        const id = String(req.params.id);
        const { data: call, error } = await supabase
            .from("calls")
            .select("id, user_id")
            .eq("id", id)
            .single();
        if (error || !call)
            return res.status(404).json({ ok: false, error: "call_not_found" });
        console.log("[admin] enqueue score for", { callId: call.id, userId: call.user_id });
        const jobId = await enqueueScoreJob(call.id, call.user_id);
        res.json({ ok: true, callId: call.id, jobId });
    }
    catch (e) {
        console.error("[admin] score error:", e?.message || e);
        res.status(400).json({ ok: false, error: e?.message || "enqueue_failed" });
    }
});
/* 🔧 Admin: force-score now (no queue) */
app.post("/v1/admin/force-score/:id", async (req, res) => {
    try {
        const id = String(req.params.id);
        const { data: call, error } = await supabase
            .from("calls")
            .select("id, filename")
            .eq("id", id)
            .single();
        if (error || !call)
            return res.status(404).json({ ok: false, error: "call_not_found" });
        const result = await scoreWithLLM({ supabase, callId: id });
        const { error: upErr } = await supabase
            .from("calls")
            .update({ status: "scored", updated_at: new Date().toISOString() })
            .eq("id", id);
        if (upErr)
            throw new Error(`status_update_failed: ${upErr.message}`);
        try {
            const appUrl = process.env.WEB_APP_URL || process.env.WEB_ORIGIN || "http://localhost:3000";
            await postSlack(`Call scored: ${result.overall}`, [
                {
                    type: "section",
                    text: {
                        type: "mrkdwn",
                        text: `*Call scored:* *${result.overall}*/100\nModel: \`${result.model}\``,
                    },
                },
                {
                    type: "actions",
                    elements: [
                        {
                            type: "button",
                            text: { type: "plain_text", text: "Open Call" },
                            url: `${appUrl}/calls/${id}`,
                            style: "primary",
                        },
                    ],
                },
            ]);
        }
        catch {
            /* ignore Slack failures */
        }
        res.json({ ok: true, callId: id, result });
    }
    catch (e) {
        console.error("[admin] force-score error:", e?.message || e);
        res.status(400).json({ ok: false, error: String(e?.message || e) });
    }
});
/* Routers */
app.use("/v1/calls", callsRouter);
app.use("/v1/pins", pinsRouter);
/* ------------------------------------------
   Sim workers: transcription + auto-scoring
-------------------------------------------*/
async function setJobStatus(jobId, status, patch = {}) {
    const { error } = await supabase
        .from("jobs")
        .update({ status, ...patch, updated_at: new Date().toISOString() })
        .eq("id", jobId);
    if (error)
        throw new Error(`setJobStatus failed: ${error.message}`);
}
async function enqueueScoreJob(callId, userId) {
    const jobId = crypto.randomUUID();
    console.log("[score] enqueue", { callId, userId, jobId });
    const { error } = await supabase.from("jobs").insert({
        id: jobId,
        call_id: callId,
        user_id: userId,
        kind: "score",
        status: "queued",
    });
    if (error)
        throw new Error(`enqueue score failed: ${error.message}`);
    simulateScore(jobId, callId).catch((e) => console.error("Sim score failed:", e.message));
    return jobId;
}
async function simulateScore(jobId, callId) {
    console.log("[score] start", { jobId, callId });
    await setJobStatus(jobId, "processing", { attempts: 1 });
    try {
        const { data: callRow, error: callErr } = await supabase
            .from("calls")
            .select("id, filename, status")
            .eq("id", callId)
            .single();
        if (callErr || !callRow)
            throw new Error(`score: call not found`);
        const result = await scoreWithLLM({ supabase, callId });
        await setJobStatus(jobId, "succeeded", {
            result: {
                model: result.model,
                overall: result.overall,
                sections: {
                    intro: result.intro.score,
                    discovery: result.discovery.score,
                    objection: result.objection.score,
                    close: result.close.score,
                },
            },
        });
        const { error: upErr } = await supabase
            .from("calls")
            .update({ status: "scored", updated_at: new Date().toISOString() })
            .eq("id", callId);
        if (upErr)
            throw new Error(`score status update failed: ${upErr.message}`);
        console.log("[score] wrote", { callId, overall: result.overall, model: result.model });
        try {
            const appUrl = process.env.WEB_APP_URL || process.env.WEB_ORIGIN || "http://localhost:3000";
            await postSlack(`Call scored: ${result.overall}`, [
                {
                    type: "section",
                    text: {
                        type: "mrkdwn",
                        text: `*Call scored:* *${result.overall}*/100\n` +
                            `Model: \`${result.model}\`\n` +
                            `File: \`${(typeof callRow?.filename === "string" && callRow.filename) || callId}\``,
                    },
                },
                {
                    type: "actions",
                    elements: [
                        {
                            type: "button",
                            text: { type: "plain_text", text: "Open Call" },
                            url: `${appUrl}/calls/${callId}`,
                            style: "primary",
                        },
                        {
                            type: "button",
                            text: { type: "plain_text", text: "Recent Calls" },
                            url: `${appUrl}/recent-calls`,
                        },
                    ],
                },
            ]);
        }
        catch (e) {
            console.warn("Slack score ping failed:", e?.message || e);
        }
    }
    catch (e) {
        console.error("[score] failed:", e?.message || e);
        await setJobStatus(jobId, "failed", { error: String(e?.message || e) });
    }
}
async function simulateTranscription(jobId, callId, _storagePath, userId) {
    console.log("[transcribe] start", { jobId, callId });
    await setJobStatus(jobId, "processing", { attempts: 1 });
    await new Promise((r) => setTimeout(r, 3000));
    const fakeTranscript = {
        model: "whisper-stub",
        text: "This is a stub transcript. Replace with real Whisper/Deepgram later.",
        words: [],
    };
    await setJobStatus(jobId, "succeeded", { result: fakeTranscript });
    const { error } = await supabase.from("calls").update({ status: "processed" }).eq("id", callId);
    if (error)
        throw new Error(`update call failed: ${error.message}`);
    console.log("[transcribe] done → enqueue score", { callId });
    try {
        await enqueueScoreJob(callId, userId);
    }
    catch (e) {
        console.error("[transcribe] enqueue score failed:", e?.message || e);
    }
}
/* Boot */
const port = Number(process.env.PORT || 4000);
app.listen(port, () => console.log(`🚀 Gravix API listening on :${port}`));
