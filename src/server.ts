// api/src/server.ts
import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import crypto from "crypto";
import { createClient } from "@supabase/supabase-js";

import { randomUUID } from "crypto";
import { postSlack, postAssignNotification /* , postScoreSummary */ } from "./lib/slack";
import { scoreWithLLM } from "./lib/scoring";

import callsRouter from "./routes/calls";
import pinsRouter from "./routes/pins";
import crmRouter from "./routes/crm.ts";
import dashboardRoutes from "./routes/dashboard";
import sparringRouter from "./routes/sparring";

import coachRoutes from "./routes/coach";
import teamRoutes from "./routes/team";
import repsRouter from "./routes/reps";
import { rewardsRoutes } from "./routes/rewards";
import { PERSONAS } from "./personas";
import whispererRouter from "./routes/whisperer";
import adminRouter from "./routes/admin";
import { assignmentsRoutes } from "./routes/assignments";
import debugRouter from "./routes/debug";

const REQUIRED_ENV = ['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
for (const k of REQUIRED_ENV) {
  if (!process.env[k] || String(process.env[k]).trim() === '') {
    console.error(`❌ Missing required env: ${k}`);
  }
}
console.log('Supabase URL:', process.env.SUPABASE_URL);

// ----- JSON error helper (server-wide) -------------------------------------
function sendJsonError(
  res: express.Response,
  status: number,
  message: string,
  extra?: Record<string, unknown>
) {
  try {
    // Try to pull the same x-request-id (set in request id middleware)
    const rid = ((res as any).req?.rid) || ((res as any).req?.headers?.["x-request-id"]) || "no-rid";
    res.status(status).json({ ok: false, error: message, rid, ...(extra || {}) });
  } catch {
    try { res.status(status).end(); } catch { }
  }
}

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

app.get("/__probe", (req, res) => {
  res.setHeader("x-probe-server", "gravix-api-live");
  return res.status(200).json({ ok: true, probe: "gravix-api-live" });
});

app.use((req, res, next) => {
  res.setHeader("x-probe-server", "gravix-api-live");
  next();
});

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
const vercelProd = "https://gravix-sales-trainer-web.vercel.app";
const vercelStage = "https://gravix-staging.vercel.app";
const localWeb = process.env.WEB_ORIGIN?.trim() || "http://localhost:3000";

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
  allowedHeaders: ["Content-Type", "Authorization", "x-user-id", "x-org-id", "x-request-id"],
  credentials: true,
  maxAge: 86400,
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions));

// --- Request ID + timing ---
app.use((req, res, next) => {
  const rid = req.headers["x-request-id"]?.toString() || randomUUID();
  (req as any).rid = rid;
  const start = Date.now();
  res.setHeader("x-request-id", rid);
  res.on("finish", () => {
    const ms = Date.now() - start;
    console.log(`[${rid}] ${req.method} ${req.originalUrl} -> ${res.statusCode} (${ms}ms)`);
  });
  next();
});

// --- Auth context (defensive): derive a stable user id early ---
// We keep routes responsible for *authorisation* (manager/rep), but we ensure
// identity is consistently available and mismatches are caught loudly.

type AuthCtx = { userId: string | null; via: "header" | "jwt" | "env" | null };

function tryDecodeJwtSub(token: string | null): string | null {
  if (!token) return null;
  try {
    const parts = token.split(".");
    if (parts.length < 2) return null;
    const b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "===".slice((b64.length + 3) % 4);
    const json = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    const sub = typeof json?.sub === "string" ? json.sub : null;
    return sub && sub.length > 10 ? sub : null;
  } catch {
    return null;
  }
}

function getBearerToken(req: express.Request): string | null {
  const raw = (req.header("authorization") || req.header("Authorization") || "").trim();
  if (!raw) return null;
  const m = raw.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

function isUuid(v: string | null | undefined) {
  if (!v) return false;
  return /^[0-9a-fA-F-]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(v);
}

// Attach req.userId early so middleware ordering changes can't silently break auth.
app.use((req, res, next) => {
  const headerUid = (req.header("x-user-id") || "").trim() || null;
  const token = getBearerToken(req);
  const jwtUid = tryDecodeJwtSub(token);

  // DEV escape hatch (local only)
  const envUid = (process.env.DEV_TEST_UID || "").trim() || null;

  let ctx: AuthCtx = { userId: null, via: null };

  // Priority: explicit header > jwt > env
  if (isUuid(headerUid)) ctx = { userId: headerUid, via: "header" };
  else if (isUuid(jwtUid)) ctx = { userId: jwtUid, via: "jwt" };
  else if (isUuid(envUid)) ctx = { userId: envUid, via: "env" };

  (req as any).auth = ctx;
  (req as any).userId = ctx.userId;
  (res.locals as any).userId = ctx.userId;
  (res.locals as any).authVia = ctx.via;

  // Guardrail: if both header + jwt exist and disagree, fail loudly.
  if (isUuid(headerUid) && isUuid(jwtUid) && headerUid !== jwtUid) {
    return sendJsonError(res, 401, "auth_mismatch", {
      headerUid,
      jwtUid,
      hint: "x-user-id and Authorization Bearer token sub do not match",
    });
  }

  return next();
});

// Root: simple status page for browsers/Slack bots
app.get('/', (_req, res) => {
  const ts = new Date().toISOString();
  const version = process.env.GIT_SHA || 'dev';
  res.status(200).send(`🚀 Gravix API v1 — Up and Running\nTime: ${ts}\nCommit: ${version}`);
});

app.get('/v1/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

app.get('/index.html', (_req, res) => {
  const ts = new Date().toISOString();
  const version = process.env.GIT_SHA || 'dev';
  res.setHeader('content-type', 'text/html; charset=utf-8');
  res.status(200).send(`<!doctype html><meta charset="utf-8"/><title>Gravix API</title><pre>🚀 Gravix API v1 — Up and Running\nTime: ${ts}\nCommit: ${version}</pre>`);
});

/* ----------------
   JSON body
----------------- */
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use("/v1/dashboard", dashboardRoutes);

// --- Dashboard: Leaderboard (top reps by avg score) ---
app.get("/v1/dashboard/leaderboard", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 10), 50);
    const days = Math.min(Number(req.query.days || 90), 365);
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    // Pull recent calls (capped) — filter to scored
    const { data: calls, error: cErr } = await supabase
      .from('calls')
      .select('user_id, score_overall, created_at')
      .not('score_overall', 'is', null)
      .gte('created_at', sinceIso)
      .order('created_at', { ascending: false })
      .limit(5000);
    if (cErr) return res.status(500).json({ ok: false, error: cErr.message });

    type Agg = { sum: number; n: number };
    const byRep = new Map<string, Agg>();
    for (const r of calls || []) {
      const uid = String((r as any).user_id || '');
      const sc = Number((r as any).score_overall);
      if (!uid || !Number.isFinite(sc)) continue;
      const cur = byRep.get(uid) || { sum: 0, n: 0 };
      cur.sum += sc; cur.n += 1;
      byRep.set(uid, cur);
    }

    const repIds = Array.from(byRep.keys()).filter(Boolean);
    const repsById = new Map<string, { id: string; name: string }>();

    if (repIds.length) {
      // Prefer profiles.display_name, fall back to users.full_name
      const { data: profs } = await supabase
        .from('profiles')
        .select('id, display_name')
        .in('id', repIds);
      for (const r of profs || []) {
        const id = String((r as any).id);
        const name = (r as any).display_name || 'Rep';
        repsById.set(id, { id, name });
      }
      const missing = repIds.filter(id => !repsById.has(id));
      if (missing.length) {
        const { data: users } = await supabase
          .from('users')
          .select('id, full_name')
          .in('id', missing);
        for (const u of users || []) {
          const id = String((u as any).id);
          const name = (u as any).full_name || 'Rep';
          if (!repsById.has(id)) repsById.set(id, { id, name });
        }
      }
    }

    const items = Array.from(byRep.entries())
      .map(([user_id, agg]) => ({
        user_id,
        name: repsById.get(user_id)?.name || 'Rep',
        avg_score: agg.n ? agg.sum / agg.n : null,
        calls: agg.n,
      }))
      .filter(x => typeof x.avg_score === 'number')
      .sort((a, b) => (b.avg_score! - a.avg_score!))
      .slice(0, limit);

    res.set("Cache-Control", "public, max-age=15");
    return res.json({ ok: true, items, since: sinceIso });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'leaderboard_failed' });
  }
});

// --- Rep Overview (KPIs + recent) ---
app.get("/v1/reps/:id/overview", async (req, res) => {
  try {
    const repId = String(req.params.id || "");
    if (!repId) return res.status(400).json({ ok: false, error: "rep_id required" });
    const days = Math.min(Number(req.query.days || 90), 365);
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    // Rep name (profiles.display_name -> users.full_name)
    let name = "Rep";
    try {
      const { data: prof } = await supabase.from("profiles").select("id,display_name").eq("id", repId).maybeSingle();
      if (prof?.display_name) name = prof.display_name;
      if (!prof?.display_name) {
        const { data: usr } = await supabase.from("users").select("id,full_name").eq("id", repId).maybeSingle();
        if (usr?.full_name) name = usr.full_name;
      }
    } catch { }

    // Calls since window
    const { data: calls } = await supabase
      .from("calls")
      .select("id, filename, created_at, score_overall, status, account_id, contact_id")
      .eq("user_id", repId)
      .gte("created_at", sinceIso)
      .order("created_at", { ascending: false })
      .limit(500);

    const scored = (calls || []).filter((c: any) => typeof c.score_overall === "number");
    const callsCount = (calls || []).length;
    const avgScore = scored.length
      ? Math.round(scored.reduce((a: number, c: any) => a + Number(c.score_overall), 0) / scored.length)
      : null;

    const recent_calls = (calls || []).slice(0, 20);

    const { data: assignsRows } = await supabase
      .from("coach_assignments")
      .select("id, call_id, drill_id, status, created_at, completed_at")
      .eq("assignee_user_id", repId)
      .order("created_at", { ascending: false })
      .limit(20);

    // Activities for this rep’s calls
    let activities: any[] = [];
    try {
      const repCallIds = (calls || []).map((c: any) => c.id);
      if (repCallIds.length) {
        const { data: act } = await supabase
          .from("activities")
          .select("id, type, summary, created_at, call_id")
          .in("call_id", repCallIds.slice(0, 200))
          .order("created_at", { ascending: false })
          .limit(50);
        activities = act || [];
      }
    } catch { }

    // Build a simple score trend (avg per day) from recent scored calls
    const byDay = new Map<string, { sum: number; n: number }>();
    for (const c of (calls || [])) {
      const sc = (c as any).score_overall;
      if (typeof sc !== "number" || !Number.isFinite(sc)) continue;
      const d = new Date((c as any).created_at);
      const day = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);
      const cur = byDay.get(day) || { sum: 0, n: 0 };
      cur.sum += Number(sc); cur.n += 1;
      byDay.set(day, cur);
    }
    const scoreTrend = Array.from(byDay.entries())
      .map(([date, agg]) => ({ date, value: Math.round(agg.sum / Math.max(agg.n, 1)) }))
      .sort((a, b) => a.date.localeCompare(b.date));

    // Recent blocks in the new 'recent' envelope (still keep the original top-level keys too)
    const recent = {
      calls: (recent_calls || []).map((c: any) => ({
        id: c.id,
        created_at: c.created_at,
        score: typeof c.score_overall === "number" ? Number(c.score_overall) : 0
      })),
      assignments: (assignsRows || []).map((a: any) => ({
        id: a.id,
        title: a.drill_id || "Drill",
        status: a.status || "open",
        created_at: a.created_at
      })),
      activity: (activities || []).map((ev: any) => ({
        id: ev.id,
        t: ev.created_at,
        text: ev.summary || ev.type || ""
      }))
    };

    const payload = {
      // --- Original keys (back-compat) ---
      ok: true,
      rep: { id: repId, name },
      kpis: { since: sinceIso, calls: callsCount, avg_score: avgScore },
      recent_calls,
      assignments: assignsRows || [],
      activities,

      // --- New enriched shape expected by web Rep Profile page ---
      xp: 0,
      tier: "SalesRep",
      totals: {
        calls: callsCount,
        avgScore: typeof avgScore === "number" ? avgScore : 0,
        winRate: null
      },
      trends: {
        xp: [],           // leave empty until XP daily is wired
        score: scoreTrend,
        voice: []         // leave empty until Voice score is wired
      },
      recent
    };

    res.set("Cache-Control", "public, max-age=15");
    return res.json(payload);
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "rep_overview_failed" });
  }
});

// --- Dashboard: Top Objections (from assignments.drill_id starting 'objection-') ---
app.get("/v1/dashboard/objections/top", async (req, res) => {
  try {
    const days = Math.min(Number(req.query.days || 90), 365);
    const limit = Math.min(Number(req.query.limit || 5), 20);
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const { data: assigns, error } = await supabase
      .from("coach_assignments")
      .select("drill_id, created_at")
      .gte("created_at", sinceIso)
      .ilike("drill_id", "objection-%")
      .order("created_at", { ascending: false })
      .limit(2000);
    if (error) return res.status(500).json({ ok: false, error: error.message });

    const byLabel = new Map<string, number>();
    for (const a of assigns || []) {
      const label = String((a as any).drill_id || "");
      if (!label) continue;
      byLabel.set(label, (byLabel.get(label) || 0) + 1);
    }

    const items = Array.from(byLabel.entries())
      .map(([label, count]) => ({ label, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, limit);

    res.set("Cache-Control", "public, max-age=15");
    return res.json({ ok: true, items, since: sinceIso });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "top_objections_failed" });
  }
});


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
  const fromCtx = (req as any)?.auth?.userId || (req as any)?.userId;
  const headerUid = req.header("x-user-id");
  const uid = (String(fromCtx || headerUid || process.env.DEV_TEST_UID || "").trim()) || null;

  if (!uid || !/^[0-9a-fA-F-]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(uid)) {
    throw new Error("Missing or invalid user id");
  }

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
      await fetch(test.data.signedUrl, { method: "HEAD" }).catch(() => { });
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
        await supabase.storage.from(BUCKET).remove([key]).catch(() => { });
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

// --- Health & Version ---
app.get("/v1/version", (_req, res) => {
  res.json({
    ok: true,
    version: process.env.GIT_SHA || "dev",
    buildTime: process.env.BUILD_TIME || null,
  });
});

/* --------------------------------
   Health
--------------------------------- */
// Canonical Health (JSON) + HEAD
app.get(["/health", "/v1/health"], (_req, res) => {
  res.json({
    ok: true,
    service: "gravix-api",
    uptime_s: Math.round(process.uptime()),
    ts: new Date().toISOString(),
  });
});
app.head(["/health", "/v1/health"], (_req, res) => res.status(200).end());

/* Jobs */
app.get("/v1/jobs/:id", async (req, res) => {
  const { data, error } = await supabase.from("jobs").select("*").eq("id", req.params.id).single();
  if (error) return res.status(404).json({ ok: false, error: error.message });
  res.json({ ok: true, job: data });
});

// --- DEMO: paged recent calls (for offline/dev) ---
app.get("/v1/calls/paged", (req, res) => {
  const limit = Math.min(Number(req.query.limit || 20), 50);
  const cursorRaw = req.query.cursor ? String(req.query.cursor) : null;
  const start = cursorRaw ? Number(cursorRaw) : 0;

  // Build demo data in the exact shape the web expects
  const all = Array.from({ length: 120 }).map((_, i) => {
    const idx = i + 1;
    const createdAt = new Date(Date.now() - i * 90_000).toISOString();
    const scored = i % 3 !== 0; // 2/3 scored
    const status = scored ? "scored" as const : (i % 2 === 0 ? "processed" as const : "queued" as const);
    const overall = scored ? 55 + (i % 45) : null;
    const duration = 60 + (i % 900);
    return {
      id: `demo-${String(idx).padStart(3, "0")}`,
      filename: `demo-${String(idx).padStart(3, "0")}.wav`,
      storage_path: null,
      status,
      created_at: createdAt,
      duration_sec: duration,
      score_overall: overall,
      ai_model: scored ? "gpt-4o-mini" : null,
    };
  });

  const slice = all.slice(start, start + limit);
  const nextCursor = start + limit < all.length ? String(start + limit) : null;
  return res.json({ ok: true, items: slice, nextCursor });
});

// --- ADD: CRM search stub ---


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
      const WEB = process.env.PUBLIC_WEB_BASE
        || process.env.SITE_URL
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

/* 🔧 Admin: preview Slack payload (no post) */
app.get("/v1/admin/preview-slack", requireAdmin, async (req, res) => {
  try {
    const callId = String(req.query.callId || crypto.randomUUID());
    const overall = Number(req.query.overall || 78);
    const WEB = process.env.PUBLIC_WEB_BASE
      || process.env.SITE_URL
      || process.env.WEB_BASE_URL
      || process.env.WEB_APP_URL
      || process.env.WEB_ORIGIN
      || "http://localhost:3000";

    const blocksPayload = await buildSlackSummaryBlocks({
      supabase,
      callId,
      filename: null,
      overall,
      sections: {
        intro: Number(req.query.intro ?? 82),
        discovery: Number(req.query.discovery ?? 76),
        objection: Number(req.query.objection ?? 74),
        close: Number(req.query.close ?? 73),
      },
      siteUrl: WEB,
    });
    return res.json({ ok: true, body: { text: "Call scored ✅", blocks: blocksPayload.blocks } });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message || "preview_failed" });
  }
});

/* 🔧 Admin: post a test Slack message (respects slack.ts dry-run) */
app.post("/v1/admin/post-slack", requireAdmin, async (req, res) => {
  try {
    const { callId, overall, sections } = (req.body || {}) as {
      callId?: string;
      overall?: number;
      sections?: { intro?: number; discovery?: number; objection?: number; close?: number };
    };
    if (!callId) return res.status(400).json({ ok: false, error: "callId required" });

    const WEB = process.env.PUBLIC_WEB_BASE
      || process.env.SITE_URL
      || process.env.WEB_BASE_URL
      || process.env.WEB_APP_URL
      || process.env.WEB_ORIGIN
      || "http://localhost:3000";

    const blocksPayload = await buildSlackSummaryBlocks({
      supabase,
      callId,
      filename: null,
      overall: typeof overall === "number" ? overall : 80,
      sections: {
        intro: sections?.intro ?? 82,
        discovery: sections?.discovery ?? 76,
        objection: sections?.objection ?? 74,
        close: sections?.close ?? 73,
      },
      siteUrl: WEB,
    });

    const result = await postSlack("Call scored ✅", blocksPayload.blocks);
    return res.json({ ok: true, result });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message || "post_failed" });
  }
});

// --- Admin: Daily Digest (Slack) ---
app.post("/v1/admin/digest/daily", requireAdmin, async (req, res) => {
  try {
    const days = Math.min(Number(req.query.days || 1), 7);
    const sinceIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const { data: calls } = await supabase
      .from("calls")
      .select("user_id, score_overall, created_at, filename, id")
      .not("score_overall", "is", null)
      .gte("created_at", sinceIso)
      .order("created_at", { ascending: false })
      .limit(2000);

    // Aggregate by rep
    const agg = new Map<string, { sum: number; n: number }>();
    for (const r of calls || []) {
      const uid = String((r as any).user_id || "");
      const sc = Number((r as any).score_overall);
      if (!uid || !Number.isFinite(sc)) continue;
      const cur = agg.get(uid) || { sum: 0, n: 0 };
      cur.sum += sc; cur.n += 1; agg.set(uid, cur);
    }

    // Names
    const repIds = Array.from(agg.keys());
    const names = new Map<string, string>();
    if (repIds.length) {
      const { data: profs } = await supabase.from("profiles").select("id, display_name").in("id", repIds);
      for (const p of profs || []) names.set(String((p as any).id), (p as any).display_name || "Rep");
      const missing = repIds.filter((id) => !names.has(id));
      if (missing.length) {
        const { data: users } = await supabase.from("users").select("id, full_name").in("id", missing);
        for (const u of users || []) names.set(String((u as any).id), (u as any).full_name || "Rep");
      }
    }

    const top = Array.from(agg.entries())
      .map(([id, a]) => ({ id, name: names.get(id) || "Rep", avg: a.n ? Math.round(a.sum / a.n) : null, calls: a.n }))
      .filter(x => typeof x.avg === "number")
      .sort((a, b) => b.avg! - a.avg!)
      .slice(0, 5);

    const worst = (calls || [])
      .filter((c: any) => typeof c.score_overall === "number")
      .sort((a: any, b: any) => Number(a.score_overall) - Number(b.score_overall))
      .slice(0, 3)
      .map((c: any) => `• ${c.filename || c.id}: ${Math.round(c.score_overall)}%`);

    const blocks: any[] = [
      { type: "header", text: { type: "plain_text", text: "Daily Digest (last 24h)", emoji: true } },
      { type: "section", text: { type: "mrkdwn", text: `*Top Reps (avg)*\n${top.map(t => `• ${t.name}: ${t.avg}% (${t.calls} calls)`).join("\n") || "_No scored calls_"}` } },
      { type: "divider" },
      { type: "section", text: { type: "mrkdwn", text: `*Bottom Calls*\n${worst.join("\n") || "_None_"}` } },
    ];

    const result = await postSlack("📊 Daily Digest", blocks);
    return res.json({ ok: true, posted: !!result, blocks });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "digest_failed" });
  }
});

// --- Coach Assign (persist to DB) ---
app.post("/v1/coach/assign", async (req, res) => {
  try {
    const { callId, assigneeUserId, drillId, notes } = req.body || {};
    if (!callId || !assigneeUserId || !drillId) {
      return res.status(400).json({ ok: false, error: "callId, assigneeUserId, drillId required" });
    }

    const { data, error } = await supabase
      .from("coach_assignments")
      .insert({
        call_id: callId,
        assignee_user_id: assigneeUserId,
        drill_id: drillId,
        notes: notes || null,
        org_id: req.header('x-org-id') || DEFAULT_ORG_ID,
      })
      .select("*")
      .single();

    // Optional: notify assignee via Slack webhook (if configured)
    try {
      await postAssignNotification({
        callId,
        drillId,
        notes,
        assigneeName: null, // TODO: wire real name when available
        assigneeWebhook: process.env.SLACK_ASSIGN_WEBHOOK || null,
      });
    } catch (e: any) {
      console.warn("[coach.assign] notify failed:", e?.message || e);
    }

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, assignment: data });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "assign_failed" });
  }
});

// --- Coach Assignments: list by call ---
app.get("/v1/coach/assignments", async (req, res) => {
  try {
    const callId = req.query.callId ? String(req.query.callId) : null;
    const accountId = req.query.accountId ? String(req.query.accountId) : null;
    const contactId = req.query.contactId ? String(req.query.contactId) : null;
    const assigneeUserId = (req.query.assigneeUserId || req.query.repId) ? String(req.query.assigneeUserId || req.query.repId) : null;
    const status = req.query.status ? String(req.query.status) as ("open" | "completed") : null;
    const orgId = req.query.orgId ? String(req.query.orgId) : null;

    const limit = Math.min(Number(req.query.limit || 50), 100);

    // Determine mode
    let mode: "byCall" | "byAccount" | "byContact" | "byAssignee" | null = null;
    if (callId) mode = "byCall";
    else if (accountId) mode = "byAccount";
    else if (contactId) mode = "byContact";
    else if (assigneeUserId) mode = "byAssignee";

    if (!mode) {
      return res.status(400).json({ ok: false, error: "one of callId, accountId, contactId, assigneeUserId|repId required" });
    }

    // Collect call ids if needed
    let callIds: string[] = [];
    if (mode === "byCall") {
      callIds = [callId!];
    } else if (mode === "byAccount") {
      const q = supabase
        .from('calls')
        .select('id, org_id')
        .eq('account_id', accountId!)
        .order('created_at', { ascending: false })
        .limit(500);
      const { data, error } = await (orgId ? q.eq('org_id', orgId) : q);
      if (error) return res.status(500).json({ ok: false, error: error.message });
      callIds = (data || []).map((r: any) => String(r.id));
    } else if (mode === "byContact") {
      const q = supabase
        .from('calls')
        .select('id, org_id')
        .eq('contact_id', contactId!)
        .order('created_at', { ascending: false })
        .limit(500);
      const { data, error } = await (orgId ? q.eq('org_id', orgId) : q);
      if (error) return res.status(500).json({ ok: false, error: error.message });
      callIds = (data || []).map((r: any) => String(r.id));
    }

    // Base assignments query
    let aQ = supabase
      .from('coach_assignments')
      .select('id, call_id, assignee_user_id, drill_id, notes, status, created_at, completed_at, org_id')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (orgId) aQ = aQ.eq('org_id', orgId);
    if (status) aQ = aQ.eq('status', status);

    if (mode === "byAssignee") {
      aQ = aQ.eq('assignee_user_id', assigneeUserId!);
    } else {
      if (!callIds.length) return res.json({ ok: true, items: [] });
      aQ = aQ.in('call_id', callIds);
    }

    const { data: assigns, error: aErr } = await aQ;
    if (aErr) return res.status(500).json({ ok: false, error: aErr.message });

    // Enrich with call filename (best-effort)
    const need = Array.from(new Set((assigns || []).map((x: any) => String(x.call_id))));
    const labelByCall = new Map<string, string>();
    if (need.length) {
      const cq = supabase
        .from('calls')
        .select('id, filename')
        .in('id', need)
        .limit(need.length);
      const { data: callRows } = await cq;
      for (const c of callRows || []) {
        labelByCall.set(String((c as any).id), String((c as any).filename || (c as any).id));
      }
    }

    const items = (assigns || []).map((x: any) => ({
      id: x.id,
      call_id: x.call_id,
      call_label: labelByCall.get(String(x.call_id)) || x.call_id,
      assignee_user_id: x.assignee_user_id,
      drill_id: x.drill_id,
      notes: x.notes,
      status: x.status,
      created_at: x.created_at,
      completed_at: x.completed_at || null,
    }));

    res.set('Cache-Control', 'public, max-age=10');
    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'assignments_list_failed' });
  }
});

// --- Coach Assignments: delete by id ---
app.delete("/v1/coach/assignments/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ ok: false, error: "id required" });
    const { error } = await supabase.from("coach_assignments").delete().eq("id", id);
    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "delete_failed" });
  }
});

// --- Coach Assignments: mark complete / reopen ---
app.patch("/v1/coach/assignments/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "");
    if (!id) return res.status(400).json({ ok: false, error: "id required" });

    const { status, note } = (req.body || {}) as {
      status?: "open" | "completed";
      note?: string | null;
    };
    if (!status || !["open", "completed"].includes(status)) {
      return res.status(400).json({ ok: false, error: "status must be 'open' or 'completed'" });
    }

    const patch: any = { status };
    if (status === "completed") patch.completed_at = new Date().toISOString();
    if (status === "open") patch.completed_at = null;

    // Update assignment
    const { data: updated, error } = await supabase
      .from("coach_assignments")
      .update(patch)
      .eq("id", id)
      .select("id, call_id, drill_id, status, completed_at, created_at")
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    if (!updated) return res.status(404).json({ ok: false, error: "not_found" });

    // Activity log (best-effort)
    try {
      const type = status === "completed" ? "assign_completed" : "assign_reopened";
      const summary =
        status === "completed"
          ? `✅ Drill completed: ${updated.drill_id}`
          : `↩️ Drill reopened: ${updated.drill_id}`;

      await supabase.from("activities").insert({
        type,
        summary,
        call_id: updated.call_id,
        // account_id/contact_id can be added later if you resolve them here
        created_at: new Date().toISOString(),
      });
    } catch (e: any) {
      console.warn("[coach.assignments.patch] activity insert failed:", e?.message || e);
    }

    return res.json({ ok: true, assignment: updated });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "patch_failed" });
  }
});

// --- Coach Drills catalog (static for now) ---
app.get("/v1/coach/drills", (_req, res) => {
  res.json({
    ok: true,
    items: [
      { id: "intro-basics", label: "Intro: Basics" },
      { id: "discovery-5qs", label: "Discovery: Top 5 Qs" },
      { id: "objection-too-expensive", label: "Objection: “Too expensive”" },
      { id: "close-trial", label: "Close: Trial close" },
    ],
  });
});



// --- Call score trend (demo data; replace with real query later) ---
app.get("/v1/calls/:id/scores", async (req, res) => {
  try {
    const { id } = req.params;
    // TODO: replace with real DB-backed history. For now return a deterministic demo series.
    const n = Math.min(Number(req.query.n || 12), 50);
    const base = (id?.length || 7) * 3;
    const values = Array.from({ length: n }).map((_, i) => 58 + ((i * 7 + base) % 32));
    res.json({ ok: true, values });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e?.message || "scores_failed" });
  }
});
// --- Coach Notes (persist simple text per call) ---
app.get("/v1/coach/notes", async (req, res) => {
  try {
    const callId = String(req.query.callId || "");
    if (!callId) return res.status(400).json({ ok: false, error: "callId required" });

    const { data, error } = await supabase
      .from("coach_notes")
      .select("id, call_id, notes, updated_at")
      .eq("call_id", callId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, note: data || null });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "fetch_failed" });
  }
});

app.post("/v1/coach/notes", async (req, res) => {
  try {
    const { callId, notes } = (req.body || {}) as { callId?: string; notes?: string };
    if (!callId) return res.status(400).json({ ok: false, error: "callId required" });

    // Upsert by call_id
    const { data, error } = await supabase
      .from("coach_notes")
      .upsert(
        { call_id: callId, notes: notes ?? null, updated_at: new Date().toISOString() },
        { onConflict: "call_id" }
      )
      .select("id, call_id, notes, updated_at")
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, note: data });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "save_failed" });
  }
});

// --- CRM Overview: Account ---
app.get('/v1/crm/accounts/:id/overview', async (req, res) => {
  try {
    const id = String(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: 'account_id required' });

    const { data: account, error: aErr } = await supabase
      .from('accounts')
      .select('id, name, domain')
      .eq('id', id)
      .maybeSingle();
    if (aErr) return res.status(500).json({ ok: false, error: aErr.message });
    if (!account) return res.status(404).json({ ok: false, error: 'account_not_found' });

    const { data: calls } = await supabase
      .from('calls')
      .select('id, filename, created_at, score_overall, status, contact_id')
      .eq('account_id', id)
      .order('created_at', { ascending: false })
      .limit(20);

    const { data: activities } = await supabase
      .from('activities')
      .select('id, type, summary, created_at, contact_id')
      .eq('account_id', id)
      .order('created_at', { ascending: false })
      .limit(50);

    // Light contact list (prefer explicit link via contacts.account_id)
    const { data: contacts } = await supabase
      .from('contacts')
      .select('id, first_name, last_name, email')
      .eq('account_id', id)
      .order('first_name', { ascending: true })
      .limit(10);

    return res.json({
      ok: true,
      account,
      recent_calls: calls || [],
      activities: activities || [],
      contacts: contacts || [],
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'account_overview_failed' });
  }
});

// --- CRM Overview: Contact ---
app.get('/v1/crm/contacts/:id/overview', async (req, res) => {
  try {
    const id = String(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: 'contact_id required' });

    // Contact + its account_id
    const { data: contact, error: cErr } = await supabase
      .from('contacts')
      .select('id, first_name, last_name, email, account_id')
      .eq('id', id)
      .maybeSingle();
    if (cErr) return res.status(500).json({ ok: false, error: cErr.message });
    if (!contact) return res.status(404).json({ ok: false, error: 'contact_not_found' });

    // Optional account (if linked)
    let account: { id: string; name?: string; domain?: string } | null = null;
    if (contact.account_id) {
      const { data: acc } = await supabase
        .from('accounts')
        .select('id, name, domain')
        .eq('id', contact.account_id)
        .maybeSingle();
      if (acc) account = acc as any;
    }

    // Recent calls (keep light; include account_id for completeness)
    const { data: calls } = await supabase
      .from('calls')
      .select('id, filename, created_at, score_overall, status, account_id')
      .eq('contact_id', id)
      .order('created_at', { ascending: false })
      .limit(20);

    // Activities
    const { data: activities } = await supabase
      .from('activities')
      .select('id, type, summary, created_at')
      .eq('contact_id', id)
      .order('created_at', { ascending: false })
      .limit(50);

    return res.json({
      ok: true,
      contact,
      account,                // optional
      recent_calls: calls || [],
      activities: activities || [],
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'contact_overview_failed' });
  }
});

// --- Coach Assignments aggregate by account/contact ---
app.get('/v1/coach/assignments/by-entity', async (req, res) => {
  try {
    const accountId = req.query.accountId ? String(req.query.accountId) : null;
    const contactId = req.query.contactId ? String(req.query.contactId) : null;
    const limit = Math.min(Number(req.query.limit || 20), 50);

    if (!accountId && !contactId) {
      return res.status(400).json({ ok: false, error: 'accountId or contactId required' });
    }

    // 1) Related calls
    let callsResp;
    if (accountId) {
      callsResp = await supabase
        .from('calls')
        .select('id')
        .eq('account_id', accountId)
        .order('created_at', { ascending: false })
        .limit(200);
    } else {
      callsResp = await supabase
        .from('calls')
        .select('id')
        .eq('contact_id', contactId)
        .order('created_at', { ascending: false })
        .limit(200);
    }
    if (callsResp.error) return res.status(500).json({ ok: false, error: callsResp.error.message });

    const callIds = (callsResp.data || []).map((r: any) => r.id);
    if (!callIds.length) return res.json({ ok: true, items: [] });

    // 2) Assignments for those calls
    const { data: assigns, error: aErr } = await supabase
      .from('coach_assignments')
      .select('id, call_id, drill_id, notes, created_at')
      .in('call_id', callIds)
      .order('created_at', { ascending: false })
      .limit(limit);
    if (aErr) return res.status(500).json({ ok: false, error: aErr.message });

    // 3) Enrich with call filename (optional)
    const map = new Map<string, string>();
    try {
      const need = Array.from(new Set((assigns || []).map((x: any) => x.call_id)));
      if (need.length) {
        const { data: callRows } = await supabase
          .from('calls')
          .select('id, filename')
          .in('id', need);
        for (const c of callRows || []) {
          map.set(String((c as any).id), String((c as any).filename || (c as any).id));
        }
      }
    } catch { }

    const items = (assigns || []).map((x: any) => ({
      id: x.id,
      call_id: x.call_id,
      call_label: map.get(String(x.call_id)) || x.call_id,
      drill_id: x.drill_id,
      notes: x.notes,
      created_at: x.created_at,
    }));

    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'assignments_aggregate_failed' });
  }
});

// --- Admin: index hints (DDL) — returns SQL strings, does not execute ---
app.get("/v1/admin/index-hints", requireAdmin, (_req, res) => {
  const ddl = [
    "CREATE INDEX IF NOT EXISTS idx_calls_account ON calls(account_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_calls_contact ON calls(contact_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_activities_account ON activities(account_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_activities_contact ON activities(contact_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_assignments_call ON coach_assignments(call_id, created_at DESC);",
    // Optional: faster KPIs by user and account
    "CREATE INDEX IF NOT EXISTS idx_calls_user ON calls(user_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_opps_stage_closed ON opportunities(stage, closed_at DESC);"
  ];
  res.json({ ok: true, ddl });
});


// --- SQL Triggers (documentation) ---
// -- Run in Supabase SQL editor:
//
// CREATE OR REPLACE FUNCTION trg_assign_created()
// RETURNS TRIGGER AS $$
// BEGIN
//   INSERT INTO activities(type, summary, call_id, created_at)
//   VALUES('assign_created', CONCAT('📝 Drill assigned: ', NEW.drill_id), NEW.call_id, NOW());
//   RETURN NEW;
// END; $$ LANGUAGE plpgsql;
//
// CREATE TRIGGER on_assign_insert AFTER INSERT ON coach_assignments
// FOR EACH ROW EXECUTE FUNCTION trg_assign_created();
//
// CREATE OR REPLACE FUNCTION trg_assign_status()
// RETURNS TRIGGER AS $$
// DECLARE
//   msg TEXT;
// BEGIN
//   IF NEW.status = 'completed' AND (OLD.status IS DISTINCT FROM NEW.status) THEN
//     msg := CONCAT('✅ Drill completed: ', NEW.drill_id);
//   ELSIF NEW.status = 'open' AND (OLD.status IS DISTINCT FROM NEW.status) THEN
//     msg := CONCAT('↩️ Drill reopened: ', NEW.drill_id);
//   ELSE
//     RETURN NEW;
//   END IF;
//   INSERT INTO activities(type, summary, call_id, created_at)
//   VALUES('assign_status', msg, NEW.call_id, NOW());
//   RETURN NEW;
// END; $$ LANGUAGE plpgsql;
//
// CREATE TRIGGER on_assign_update AFTER UPDATE ON coach_assignments
// FOR EACH ROW EXECUTE FUNCTION trg_assign_status();
//
// CREATE OR REPLACE FUNCTION trg_call_scored()
// RETURNS TRIGGER AS $$
// BEGIN
//   IF NEW.status = 'scored' AND (OLD.status IS DISTINCT FROM NEW.status) THEN
//     INSERT INTO activities(type, summary, call_id, created_at)
//     VALUES('call_scored', CONCAT('Scored ', COALESCE(NEW.score_overall, 0)::int, '%'), NEW.id, NOW());
//   END IF;
//   RETURN NEW;
// END; $$ LANGUAGE plpgsql;
//
// CREATE TRIGGER on_call_update AFTER UPDATE ON calls
// FOR EACH ROW EXECUTE FUNCTION trg_call_scored();

// --- Whisperer: offline preview (regex + silence heuristic) ---



/* ------------------------
   Mount feature routers
------------------------- */
app.use("/v1/calls", callsRouter);
app.use("/v1/pins", pinsRouter);
app.use("/v1/crm", crmRouter);
app.use("/v1/coach", coachRoutes);
app.use("/v1/team", teamRoutes);
app.use("/v1/reps", repsRouter);
app.use("/v1/rewards", rewardsRoutes);
app.use("/v1/sparring", sparringRouter);
app.use("/v1/whisperer", whispererRouter);
app.use("/v1/admin", adminRouter);
app.use("/v1/assignments", assignmentsRoutes());
app.use("/v1/debug", debugRouter);

// NOTE: we’re no longer mounting personasRouter here –
// /v1/sparring/personas is still handled inside sparringRouter

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

    // Activity: call scored (best-effort)
    try {
      const summary = `Scored ${Math.round(result.overall)} — Intro ${Math.round(result.intro.score)} / Disc ${Math.round(result.discovery.score)} / Obj ${Math.round(result.objection.score)} / Close ${Math.round(result.close.score)}`;
      await supabase.from("activities").insert({
        type: "call_scored",
        summary,
        call_id: callId,
        created_at: new Date().toISOString(),
      });
    } catch (e: any) {
      console.warn("[activity] call_scored insert failed:", e?.message || e);
    }

    console.log("[score] wrote", { callId, overall: result.overall, model: result.model });

    try {
      const WEB = process.env.PUBLIC_WEB_BASE
        || process.env.SITE_URL
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

// 404 Not Found (must be after all routes)
app.use((req, res) => {
  return sendJsonError(res, 404, "Not found", { path: req.originalUrl });
});

// Global error handler (last middleware)
app.use((err: any, req: any, res: express.Response, _next: express.NextFunction) => {
  const rid = (req as any)?.rid || req?.headers?.["x-request-id"] || "no-rid";
  console.error(`[${rid}] Uncaught`, err);
  return sendJsonError(res, 500, "Internal error");
});

/* Boot */
const port = Number(process.env.PORT || 4000);
app.listen(port, () => console.log(`🚀 Gravix API listening on :${port}`));
