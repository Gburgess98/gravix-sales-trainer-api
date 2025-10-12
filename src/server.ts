// api/src/server.ts
import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

import { postSlack /* , postScoreSummary */ } from "./lib/slack";
import { scoreWithLLM } from "./lib/scoring";

import callsRouter from "./routes/calls";
import pinsRouter from "./routes/pins";
import crmRouter from "./routes/crm";

// Local file shape to avoid Express.Multer namespace types
type UploadedFile = {
  fieldname: string;
  originalname: string;
  encoding: string;
  mimetype: string;
  size: number;
  buffer: Buffer;
};

const app = express();

/* ------------------------------------------------
   Basic request log (helps see preflights/uploads)
------------------------------------------------- */
app.use((req, _res, next) => {
  console.log(
    `[api] ${req.method} ${req.path}  Origin=${req.headers.origin || "-"}  UA=${req.headers["user-agent"] || "-"}`
  );
  next();
});

/* ----------------------------
   CORS (allow Vercel + local)
----------------------------- */
const vercelProd  = "https://gravix-sales-trainer-web.vercel.app";
const vercelStage = "https://gravix-staging.vercel.app";
const localWeb    = process.env.WEB_ORIGIN?.trim() || "http://localhost:3000";

const extra = [
  process.env.VERCEL_ORIGIN?.trim(),
  process.env.VERCEL_ORIGIN_STAGING?.trim(),
].filter(Boolean) as string[];

const allowedOrigins = Array.from(new Set([vercelProd, vercelStage, localWeb, ...extra]));
console.log("CORS allow-list:", allowedOrigins);

const corsOptions: cors.CorsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // curl/Postman/server-to-server
    if (allowedOrigins.includes(origin)) return cb(null, true);
    return cb(new Error(`CORS: origin not allowed: ${origin}`));
  },
  methods: ["GET", "POST", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "x-user-id"],
  credentials: false,
  maxAge: 86400,
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions));

/* ----------------
   JSON body
----------------- */
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/* ------------------------
   Supabase server client
------------------------- */
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const DEFAULT_ORG_ID = process.env.DEFAULT_ORG_ID!;
if (!DEFAULT_ORG_ID) {
  console.warn("⚠️ DEFAULT_ORG_ID not set — calls.org_id NOT NULL will fail.");
}

/* Debug on boot */
(async () => {
  console.log("Supabase URL:", process.env.SUPABASE_URL);
  const { data: buckets, error } = await supabase.storage.listBuckets();
  if (error) console.error("List buckets error:", error.message);
  else console.log("Buckets found:", buckets?.map((b) => b.name));
})();

/* ------------------------
   Uploads (multipart)
------------------------- */
const BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "calls";
const upload = multer({ storage: multer.memoryStorage() });

/* --- Helpers --- */
function getUserId(req: express.Request): string {
  const uid = req.header("x-user-id") || process.env.DEV_TEST_UID;
  if (
    !uid ||
    !/^[0-9a-fA-F-]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(uid)
  ) throw new Error("Missing or invalid x-user-id");
  return uid;
}

function requireAdmin(
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) {
  if (process.env.ALLOW_ADMIN_ENDPOINTS === "true") return next();
  return res.status(403).json({ ok: false, error: "admin_required" });
}

/* --- Slack summary builder (used after scoring) --- */
async function buildSlackSummaryBlocks(opts: {
  supabase: ReturnType<typeof createClient>;
  callId: string;
  filename?: string | null;
  overall: number;
  sections: { intro?: number; discovery?: number; objection?: number; close?: number };
  siteUrl: string;
}) {
  const { supabase, callId, filename, overall, sections, siteUrl } = opts;

  const { data: callRow } = await supabase
    .from("calls")
    .select("id, contact_id, account_id")
    .eq("id", callId)
    .maybeSingle();

  let contact: { id: string; name: string } | null = null;
  let account: { id: string; name: string } | null = null;

  if (callRow?.contact_id) {
    const { data } = await supabase
      .from("contacts")
      .select("id, first_name, last_name, email")
      .eq("id", callRow.contact_id)
      .maybeSingle();
    if (data) {
      const name =
        [data.first_name, data.last_name].filter(Boolean).join(" ").trim() ||
        data.email || data.id;
      contact = { id: data.id, name };
    }
  }

  if (callRow?.account_id) {
    const { data } = await supabase
      .from("accounts")
      .select("id, name, domain")
      .eq("id", callRow.account_id)
      .maybeSingle();
    if (data) {
      account = { id: data.id, name: data.name || data.domain || data.id };
    }
  }

  const callUrl = `${siteUrl}/calls/${callId}`;
  const crmPanelUrl = `${callUrl}?panel=crm`;

  const sectionLines: string[] = [];
  if (typeof sections.intro === "number") sectionLines.push(`• *Intro:* ${Math.round(sections.intro)}%`);
  if (typeof sections.discovery === "number") sectionLines.push(`• *Discovery:* ${Math.round(sections.discovery)}%`);
  if (typeof sections.objection === "number") sectionLines.push(`• *Objection:* ${Math.round(sections.objection)}%`);
  if (typeof sections.close === "number") sectionLines.push(`• *Close:* ${Math.round(sections.close)}%`);
  const sectionText = sectionLines.join("\n");

  const chips: string[] = [];
  if (contact) chips.push(`<${crmPanelUrl}#contact=${contact.id}|👤 ${contact.name}>`);
  if (account) chips.push(`<${crmPanelUrl}#account=${account.id}|🏢 ${account.name}>`);
  if (!contact || !account) chips.push(`<${crmPanelUrl}|🔗 Link contact/account>`);

  const blocks: any[] = [
    { type: "section", text: { type: "mrkdwn", text: `*Call scored:* <${callUrl}|${filename || callId}>\n*Overall:* ${Math.round(overall)}%` } },
  ];

  if (sectionText) {
    blocks.push({ type: "section", text: { type: "mrkdwn", text: sectionText } });
  }

  blocks.push({ type: "divider" });
  blocks.push({ type: "context", elements: chips.map((txt) => ({ type: "mrkdwn", text: txt })) });
  blocks.push({
    type: "actions",
    elements: [
      { type: "button", text: { type: "plain_text", text: "Open Call" }, url: callUrl },
      { type: "button", text: { type: "plain_text", text: "Link/Review CRM" }, url: crmPanelUrl },
    ],
  });

  return { blocks };
}

/* ------------------------------------------
   Signed Uploads (optional, behind flag)
-------------------------------------------*/
app.post("/v1/upload/signed", async (req, res) => {
  try {
    const userId = getUserId(req);
    const { filename, mime, size, sha256 } = (req.body || {}) as {
      filename: string; mime?: string; size?: number; sha256?: string;
    };
    if (!filename) return res.status(400).json({ ok: false, error: "missing_filename" });

    const isJson = (mime || "").includes("json") || filename.toLowerCase().endsWith(".json");
    const kind = isJson ? "json" : "audio";
    const id = crypto.randomUUID();
    const ext = filename.includes(".")
      ? filename.slice(filename.lastIndexOf("."))
      : (isJson ? ".json" : ".wav");
    const path = `${userId}/${id}${ext}`;

    const { data, error } = await supabase.storage.from(BUCKET).createSignedUploadUrl(path);
    if (error || !data?.signedUrl) {
      return res.status(500).json({ ok: false, error: error?.message || "signed_url_failed" });
    }

    res.json({
      ok: true,
      path,
      url: data.signedUrl,
      id,
      kind,
      sha256: sha256 || null,
      size: size || null,
      filename,
      mime: mime || null,
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

app.post("/v1/upload/finalize", async (req, res) => {
  try {
    const userId = getUserId(req);
    const { path, filename, mime, size, sha256 } = (req.body || {}) as {
      path: string; filename: string; mime?: string; size?: number; sha256?: string;
    };
    if (!path || !filename) return res.status(400).json({ ok: false, error: "missing_path_or_filename" });
    if (!path.startsWith(`${userId}/`)) return res.status(400).json({ ok: false, error: "path_user_mismatch" });

    // best-effort existence check
    try {
      const test = await supabase.storage.from(BUCKET).createSignedUrl(path, 30);
      if (test.error) throw test.error;
      await fetch(test.data.signedUrl, { method: "HEAD" }).catch(() => {});
    } catch { /* ignore */ }

    const id = path.split("/").pop()!.split(".")[0];
    const isJson = (mime || "").includes("json") || filename.toLowerCase().endsWith(".json");
    const kind = isJson ? "json" : "audio";
    const hash = (sha256 && /^[a-f0-9]{64}$/i.test(sha256)) ? sha256.toLowerCase() : null;

    // 1) DB row
    const { error: dbErrCall } = await supabase.from("calls").insert({
      id,
      user_id: userId,
      org_id: DEFAULT_ORG_ID,
      filename,
      storage_path: path,
      mime_type: mime || null,
      size_bytes: size || null,
      sha256: hash,
      kind,
      status: "queued",
      audio_path: path,
    });
    if (dbErrCall) return res.status(500).json({ ok: false, error: `DB insert failed: ${dbErrCall.message}` });

    // 2) Job row
    const jobId = crypto.randomUUID();
    const { error: dbErrJob } = await supabase.from("jobs").insert({
      id: jobId, call_id: id, user_id: userId, kind: "transcribe", status: "queued",
    });
    if (dbErrJob) return res.status(500).json({ ok: false, error: `Job insert failed: ${dbErrJob.message}` });

    // 3) Sim worker
    simulateTranscription(jobId, id, path, userId).catch((e) => console.error("Sim worker failed:", e.message));

    res.json({ ok: true, callId: id, jobId, filename, storagePath: path, size: size || null, mime: mime || null, sha256: hash });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

/* ------------------------------------------
   Direct Upload (current default)
-------------------------------------------*/
app.post(
  "/v1/upload",
  upload.single("file"),
  async (req: express.Request & { file?: UploadedFile }, res) => {
    try {
      console.log("UPLOAD HIT at", new Date().toISOString());
      const userId = getUserId(req);
      const f = req.file;
      if (!f) return res.status(400).json({ ok: false, error: "No file uploaded" });

      const isJson = f.mimetype.includes("json");
      const kind = isJson ? "json" : "audio";
      const id = crypto.randomUUID();
      const ext = f.originalname.includes(".")
        ? f.originalname.slice(f.originalname.lastIndexOf("."))
        : isJson ? ".json" : ".wav";
      const key = `${userId}/${id}${ext}`;
      const hash = crypto.createHash("sha256").update(f.buffer).digest("hex");

      // 1) Storage
      const { error: upErr } = await supabase.storage.from(BUCKET).upload(key, f.buffer, {
        contentType: f.mimetype, upsert: false,
      });
      if (upErr) return res.status(500).json({ ok: false, error: `Storage upload failed: ${upErr.message}` });

      // 2) DB row
      const { error: dbErrCall } = await supabase.from("calls").insert({
        id, user_id: userId, org_id: DEFAULT_ORG_ID,
        filename: f.originalname, storage_path: key, mime_type: f.mimetype,
        size_bytes: f.size, sha256: hash, kind, status: "queued", audio_path: key,
      });
      if (dbErrCall) {
        await supabase.storage.from(BUCKET).remove([key]).catch(() => {});
        return res.status(500).json({ ok: false, error: `DB insert failed: ${dbErrCall.message}` });
      }

      // 3) Job row
      const jobId = crypto.randomUUID();
      const { error: dbErrJob } = await supabase.from("jobs").insert({
        id: jobId, call_id: id, user_id: userId, kind: "transcribe", status: "queued",
      });
      if (dbErrJob) return res.status(500).json({ ok: false, error: `Job insert failed: ${dbErrJob.message}` });

      // 4) Sim worker (stub)
      simulateTranscription(jobId, id, key, userId).catch((e) =>
        console.error("Sim worker failed:", e.message)
      );

      return res.json({
        ok: true, callId: id, jobId, kind,
        filename: f.originalname, storagePath: key, size: f.size, mime: f.mimetype, sha256: hash,
      });
    } catch (e: any) {
      return res.status(400).json({ ok: false, error: e.message || "Upload failed" });
    }
  }
);

/* --------------------------------
   Health
--------------------------------- */
app.get(["/health", "/v1/health"], (_req, res) => res.json({ ok: true }));
app.head(["/health", "/v1/health"], (_req, res) => res.status(200).end());

/* Jobs */
app.get("/v1/jobs/:id", async (req, res) => {
  const { data, error } = await supabase.from("jobs").select("*").eq("id", req.params.id).single();
  if (error) return res.status(404).json({ ok: false, error: error.message });
  res.json({ ok: true, job: data });
});

/* 🔧 Admin: enqueue score job for a call (dev only) — returns jobId */
app.post("/v1/admin/score/:id", requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id);
    const { data: call, error } = await supabase
      .from("calls").select("id, user_id").eq("id", id).single();
    if (error || !call) return res.status(404).json({ ok: false, error: "call_not_found" });

    console.log("[admin] enqueue score for", { callId: call.id, userId: call.user_id });
    const jobId = await enqueueScoreJob(call.id, call.user_id);
    res.json({ ok: true, callId: call.id, jobId });
  } catch (e: any) {
    console.error("[admin] score error:", e?.message || e);
    res.status(400).json({ ok: false, error: e?.message || "enqueue_failed" });
  }
});

/* 🔧 Admin: force-score now (no queue) */
app.post("/v1/admin/force-score/:id", requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id);

    const { data: call, error } = await supabase
      .from("calls").select("id, filename").eq("id", id).single();
    if (error || !call) return res.status(404).json({ ok: false, error: "call_not_found" });

    const result = await scoreWithLLM({ supabase, callId: id });

    const { error: upErr } = await supabase
      .from("calls").update({ status: "scored", updated_at: new Date().toISOString() }).eq("id", id);
    if (upErr) throw new Error(`status_update_failed: ${upErr.message}`);

    // Slack summary
    try {
      const WEB = process.env.SITE_URL
        || process.env.WEB_BASE_URL
        || process.env.WEB_APP_URL
        || process.env.WEB_ORIGIN
        || "http://localhost:3000";

      const blocksPayload = await buildSlackSummaryBlocks({
        supabase,
        callId: id,
        filename: call.filename,
        overall: result.overall,
        sections: {
          intro: result.intro.score,
          discovery: result.discovery.score,
          objection: result.objection.score,
          close: result.close.score,
        },
        siteUrl: WEB,
      });

      await postSlack("Call scored ✅", blocksPayload.blocks);
    } catch { /* ignore Slack failures */ }

    res.json({ ok: true, callId: id, result });
  } catch (e: any) {
    console.error("[admin] force-score error:", e?.message || e);
    res.status(400).json({ ok: false, error: String(e?.message || e) });
  }
});

/* ------------------------
   Mount feature routers
------------------------- */
app.use("/v1/calls", callsRouter);
app.use("/v1/pins", pinsRouter);
app.use("/v1/crm", crmRouter);

/* ------------------------------------------
   Sim workers: transcription + auto-scoring
-------------------------------------------*/
async function setJobStatus(
  jobId: string,
  status: "queued" | "processing" | "succeeded" | "failed",
  patch: Record<string, unknown> = {}
) {
  const { error } = await supabase
    .from("jobs")
    .update({ status, ...patch, updated_at: new Date().toISOString() })
    .eq("id", jobId);
  if (error) throw new Error(`setJobStatus failed: ${error.message}`);
}

async function enqueueScoreJob(callId: string, userId: string) {
  const jobId = crypto.randomUUID();
  console.log("[score] enqueue", { callId, userId, jobId });

  const { error } = await supabase.from("jobs").insert({
    id: jobId, call_id: callId, user_id: userId, kind: "score", status: "queued",
  });
  if (error) throw new Error(`enqueue score failed: ${error.message}`);

  simulateScore(jobId, callId).catch((e) => console.error("Sim score failed:", e.message));
  return jobId;
}

async function simulateScore(jobId: string, callId: string) {
  console.log("[score] start", { jobId, callId });
  await setJobStatus(jobId, "processing", { attempts: 1 });

  try {
    const { data: callRow, error: callErr } = await supabase
      .from("calls").select("id, filename, status").eq("id", callId).single();
    if (callErr || !callRow) throw new Error(`score: call not found`);

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
      .from("calls").update({ status: "scored", updated_at: new Date().toISOString() }).eq("id", callId);
    if (upErr) throw new Error(`score status update failed: ${upErr.message}`);

    console.log("[score] wrote", { callId, overall: result.overall, model: result.model });

    try {
      const WEB = process.env.SITE_URL
        || process.env.WEB_BASE_URL
        || process.env.WEB_APP_URL
        || process.env.WEB_ORIGIN
        || "http://localhost:3000";

      const blocksPayload = await buildSlackSummaryBlocks({
        supabase,
        callId,
        filename: callRow.filename,
        overall: result.overall,
        sections: {
          intro: result.intro.score,
          discovery: result.discovery.score,
          objection: result.objection.score,
          close: result.close.score,
        },
        siteUrl: WEB,
      });

      await postSlack("Call scored ✅", blocksPayload.blocks);
    } catch (e: any) {
      console.warn("Slack score summary failed:", e?.message || e);
    }
  } catch (e: any) {
    console.error("[score] failed:", e?.message || e);
    await setJobStatus(jobId, "failed", { error: String(e?.message || e) });
  }
}

async function simulateTranscription(jobId: string, callId: string, _storagePath: string, userId: string) {
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
  if (error) throw new Error(`update call failed: ${error.message}`);

  console.log("[transcribe] done → enqueue score", { callId });

  try {
    await enqueueScoreJob(callId, userId);
  } catch (e: any) {
    console.error("[transcribe] enqueue score failed:", e?.message || e);
  }
}

/* Boot */
const port = Number(process.env.PORT || 4000);
app.listen(port, () => console.log(`🚀 Gravix API listening on :${port}`));
