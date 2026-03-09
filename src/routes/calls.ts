import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";
import { postSlack } from "../lib/slack";
import { completeAssignmentsForTarget } from "../lib/assignmentsComplete";
import 'dotenv/config';

const router = Router();
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

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
  transcript_text: z.string().optional(),
  // Optional: if the client launched this score action from an assignment CTA (used for auto-complete)
  assignmentId: z.string().uuid().optional(),
});

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getUserIdHeader(req: any): string {
  // Accept a few aliases to be resilient across proxies/CDNs
  const cand =
    req.header("x-user-id") ||
    req.header("x-gravix-user-id") ||
    req.header("x-forwarded-user-id") ||
    null;

  // Optional DEV-only escape hatch via query string (ONLY if you enable it)
  const allowQs = process.env.ALLOW_DEV_UID_QS === "1";
  const qsUid = allowQs ? (req.query?.dev_uid as string | undefined) : undefined;

  const uid = (cand || qsUid || "").trim();

  if (!uid || !UUID_RE.test(uid)) {
    throw new Error("Missing or invalid x-user-id");
  }
  return uid;
}

const FILLER_WORDS = [
  "um",
  "uh",
  "like",
  "you know",
  "sort of",
  "kind of",
  "basically",
  "actually",
  "literally",
  "i mean",
];

function clampScore(n: number): number {
  return Math.max(0, Math.min(100, Math.round(n)));
}

function analyseFillers(text: string) {
  const raw = String(text || "").toLowerCase();
  const words = raw.match(/\b[\w']+\b/g) ?? [];
  const totalWords = words.length;

  let totalFillers = 0;
  const hits = new Map<string, number>();

  for (const filler of FILLER_WORDS) {
    const escaped = filler.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`\\b${escaped}\\b`, "gi");
    const matches = raw.match(re) ?? [];
    if (matches.length) {
      totalFillers += matches.length;
      hits.set(filler, matches.length);
    }
  }

  return {
    total_words: totalWords,
    filler_count: totalFillers,
    filler_words: Array.from(hits.keys()),
    filler_breakdown: Object.fromEntries(hits.entries()),
    filler_density: totalWords > 0 ? totalFillers / totalWords : 0,
  };
}

function detectWeakClose(args: { closeScore: number | null; text: string }) {
  const closeScore = typeof args.closeScore === "number" ? args.closeScore : null;
  const text = String(args.text || "").toLowerCase();

  const strongCloseSignals = [
    "next step",
    "book a time",
    "schedule",
    "calendar",
    "let's do",
    "shall we",
    "send over",
    "move forward",
    "get started",
    "does that sound fair",
    "how does that sound",
  ];

  const hasStrongCloseLanguage = strongCloseSignals.some((s) => text.includes(s));

  if (closeScore !== null && closeScore < 60) return true;
  if (text && !hasStrongCloseLanguage) return true;
  return false;
}

function buildVoiceScore(args: {
  transcriptText: string;
  closeScore: number | null;
}) {
  const transcriptText = String(args.transcriptText || "");
  const filler = analyseFillers(transcriptText);
  const weakClose = detectWeakClose({ closeScore: args.closeScore, text: transcriptText });

  const fillerPenalty = filler.filler_density * 220;
  const fillerScore = clampScore(100 - fillerPenalty);
  const clarity = clampScore(92 - filler.filler_density * 160);
  const tone = clampScore(82 - filler.filler_density * 70 + (weakClose ? -4 : 4));
  const confidence = clampScore(84 - filler.filler_density * 110 - (weakClose ? 10 : 0));
  const close = clampScore(args.closeScore ?? (weakClose ? 50 : 78));

  const voice_rubric = {
    tone,
    clarity,
    confidence,
    filler: fillerScore,
    close,
  };

  const voice_score = clampScore(
    tone * 0.2 + clarity * 0.25 + confidence * 0.2 + fillerScore * 0.15 + close * 0.2
  );

  const review_tags = {
    filler_words: filler.filler_words,
    filler_count: filler.filler_count,
    filler_density: Number(filler.filler_density.toFixed(4)),
    weak_close: weakClose,
  };

  return { voice_score, voice_rubric, review_tags };
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

    if (error) throw error;
    if (!data) throw new Error("upsert returned no row");

    const appUrl =
      process.env.WEB_APP_URL || process.env.WEB_ORIGIN || "http://localhost:3000";
    const recentLink = `${appUrl}/recent-calls`;
    const callLink = `${appUrl}/calls/${data.id}`;

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
          ...(data?.id
            ? [
              {
                type: "button",
                text: { type: "plain_text", text: "Open This Call" },
                url: callLink,
              } as const,
            ]
            : []),
        ],
      },
    ]);

    res.json({ ok: true, call: data });
  } catch (e: any) {
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

    if (!userId) return res.status(400).json({ ok: false, error: "userId required" });

    const { data, error } = await supa
      .from("calls")
      .select("*")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    res.json({ ok: true, calls: data });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/* -----------------------------------------------------------------
   GET /v1/calls/paged?limit=10&cursor=...
------------------------------------------------------------------ */
router.get("/paged", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const rawLimit = Number(req.query.limit ?? 10);
    const limit = Math.min(Math.max(rawLimit, 1), 50);
    const cursor = req.query.cursor ? String(req.query.cursor) : null;
    const search = (req.query.q as string | undefined)?.trim() ?? "";

    let q = supa
      .from("calls")
      .select(
        `
          id,
          user_id,
          org_id,
          filename,
          storage_path,
          status,
          created_at,
          duration_sec,
          duration_ms,
          score_overall,
          ai_model,
          rep_name,
          tags,
          summary,
          flags
        `
      )
      .eq("user_id", requester)
      .order("created_at", { ascending: false })
      .limit(limit + 1);

    // cursor-based pagination (keep existing behaviour)
    if (cursor) {
      const cursorDate = isNaN(Number(cursor))
        ? new Date(cursor)
        : new Date(Number(cursor));
      if (isNaN(cursorDate.getTime())) {
        return res.status(400).json({ ok: false, error: "invalid cursor" });
      }
      q = q.lt("created_at", cursorDate.toISOString());
    }

    // simple search on filename + summary
    if (search) {
      q = q.or(
        `filename.ilike.%${search}%,summary.ilike.%${search}%`
      );
    }

    const { data, error } = await q;
    if (error) throw error;

    let items = data ?? [];
    let nextCursor: string | null = null;

    if (items.length > limit) {
      const last = items[limit - 1];
      nextCursor = last.created_at;
      items = items.slice(0, limit);
    }

    // Shape payload for web: keep existing fields, add new UX bits
    const mapped = items.map((c) => ({
      id: c.id,
      filename: c.filename,
      status: c.status,
      created_at: c.created_at,
      duration_sec: c.duration_sec,
      duration_ms: c.duration_ms ?? null,
      score_overall: c.score_overall,
      ai_model: c.ai_model,
      type: c.storage_path ? "upload" : "live",
      rep_name: c.rep_name ?? null,
      tags: c.tags ?? null,
      summary: c.summary ?? null,
      flags: c.flags ?? [],
    }));

    return res.json({ ok: true, calls: mapped, nextCursor });
  } catch (e: any) {
    console.error(e);
    return res
      .status(400)
      .json({ ok: false, error: e.message ?? "bad_request" });
  }
});
/* ----------------------------------------------------------------
   GET /v1/calls/:id/signed-audio → signed URL for audio playback
   (ORDERED BEFORE /:id so it doesn't get captured by the :id route)
----------------------------------------------------------------- */
router.get("/:id/signed-audio", async (req, res) => {
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

    if (error || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (!call.audio_path) {
      return res.status(400).json({ ok: false, error: "no_audio_path" });
    }

    const bucket = process.env.SUPABASE_STORAGE_BUCKET || "calls";
    const ttl = Number(process.env.SIGNED_URL_TTL_SEC || 300); // default 5 min

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
  } catch (e: any) {
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
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid id" });

    const requester = getUserIdHeader(req);
    const body = ScoreSchema.parse(req.body);

    // Back-compat: allow assignmentId via query string too
    const assignmentIdFromQuery = typeof req.query.assignmentId === "string" ? req.query.assignmentId : undefined;
    const effectiveAssignmentId = body.assignmentId || assignmentIdFromQuery || undefined;

    // verify ownership
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id,user_id,summary")
      .eq("id", id)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const reviewText = String(body.transcript_text ?? (call as any)?.summary ?? "");
    const closeScore = typeof (body.rubric as any)?.close === "number" ? Number((body.rubric as any).close) : null;
    const { voice_score, voice_rubric, review_tags } = buildVoiceScore({
      transcriptText: reviewText,
      closeScore,
    });

    const patch: any = {
      score_overall: body.score_overall,
      status: "scored",
      voice_score,
      voice_rubric,
      review_tags,
    };

    patch.rubric = {
      ...(typeof body.rubric === "object" && body.rubric ? body.rubric : {}),
      voice_score,
      voice_rubric,
      review_tags,
    };

    let updatePatch = { ...patch };

    let { data, error } = await supa
      .from("calls")
      .update(updatePatch)
      .eq("id", id)
      .select()
      .single();

    if (error) {
      const msg = String((error as any)?.message ?? "").toLowerCase();
      const missingVoiceCols =
        (msg.includes("voice_score") || msg.includes("voice_rubric") || msg.includes("review_tags")) &&
        (msg.includes("column") || msg.includes("schema cache"));

      if (missingVoiceCols) {
        updatePatch = { ...patch };
        delete updatePatch.voice_score;
        delete updatePatch.voice_rubric;
        delete updatePatch.review_tags;

        const retry = await supa
          .from("calls")
          .update(updatePatch)
          .eq("id", id)
          .select()
          .single();

        data = retry.data as any;
        error = retry.error as any;
      }
    }

    if (error) throw error;

    // --- Assignment Engine v2: auto-complete + XP (never blocks scoring) ---
    // If the client passed assignmentId, we complete that.
    // Otherwise, we fall back to matching targetId (call id) or latest open call_review.
    // Award baseline XP based on score band.
    let completedCount = 0;
    let xpAwardedTotal = 0;
    try {
      const score = Number(body.score_overall);
      const xpAwarded = Number.isFinite(score)
        ? score >= 80
          ? 35
          : score >= 60
            ? 25
            : 15
        : 25;

      const result = await completeAssignmentsForTarget({
        repId: call.user_id,
        assignmentId: effectiveAssignmentId || null,
        type: "call_review",
        targetId: id,
        completedVia: "call_review",
        xpAwarded,
        // helpful metadata for audits
        metaPatch: { call_id: id, score_overall: body.score_overall },
      });

      completedCount =
        typeof (result as any)?.completedCount === "number"
          ? (result as any).completedCount
          : 0;
      xpAwardedTotal =
        typeof (result as any)?.xpAwardedTotal === "number"
          ? (result as any).xpAwardedTotal
          : 0;

      if (result?.completedCount) {
        console.info("[assignments:lifecycle]", {
          event: "auto_completed",
          via: "call_review",
          rep_id: call.user_id,
          call_id: id,
          assignment_id: effectiveAssignmentId || null,
          completed_count: result.completedCount,
          method: result.method || null,
          xp_awarded: xpAwardedTotal,
        });
      } else {
        console.info("[assignments:lifecycle]", {
          event: "auto_complete_noop",
          via: "call_review",
          rep_id: call.user_id,
          call_id: id,
          assignment_id: effectiveAssignmentId || null,
          reason: result?.reason || "no_matching_open_assignment",
        });
      }
    } catch (e: any) {
      console.warn("[assignments:lifecycle]", {
        event: "auto_complete_failed",
        via: "call_review",
        rep_id: call.user_id,
        call_id: id,
        assignment_id: effectiveAssignmentId || null,
        error: e?.message || String(e),
      });
    }

    // ✅ NEW: append a score snapshot to call_scores
    const hist = {
      call_id: id,
      user_id: requester,
      score: body.score_overall,
      rubric: {
        ...(typeof body.rubric === "object" && body.rubric ? body.rubric : {}),
        voice_score,
        voice_rubric,
        review_tags,
      },
    };
    const { error: histErr } = await supa.from("call_scores").insert(hist);
    if (histErr) {
      // don't fail the whole request for history insert
      console.warn("[score history] insert failed", histErr);
    }

    // ---------------------------------------------------------
    // DAY 52 — AUTO COACHING TRIGGERS
    // Detect weak close / objection and auto-create CRM tasks
    // Future-safe: include coaching_reason metadata when schema supports it
    // ---------------------------------------------------------
    try {
      const rubric: any = body.rubric ?? {};

      const closeScore = typeof rubric?.close === "number" ? rubric.close : null;
      const objectionScore = typeof rubric?.objection === "number" ? rubric.objection : null;

      const due = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();

      const ensureCoachingTask = async (args: {
        title: string;
        reason: "weak_close" | "objection_handling";
      }) => {
        const { title, reason } = args;

        // Prevent duplicate open coaching tasks for the same reason/title
        const existingTask = await supa
          .from("crm_activities")
          .select("id")
          .eq("user_id", requester)
          .eq("title", title)
          .eq("status", "open")
          .limit(1)
          .maybeSingle();

        if (existingTask.data) return;

        const payloadWithMeta: any = {
          type: "task",
          title,
          status: "open",
          due_at: due,
          user_id: requester,
          opportunity_id: null,
          contact_id: null,
          account_id: null,
          meta: {
            coaching_reason: reason,
            source: "call_score",
            source_call_id: id,
          },
        };

        const insertWithMeta = await supa.from("crm_activities").insert(payloadWithMeta);
        if (!insertWithMeta.error) return;

        const metaErr = String((insertWithMeta.error as any)?.message ?? "").toLowerCase();

        // Backwards-compatible fallback if meta column does not exist yet
        if (metaErr.includes("column") && metaErr.includes("meta")) {
          const payloadNoMeta = {
            type: "task",
            title,
            status: "open",
            due_at: due,
            user_id: requester,
            opportunity_id: null,
            contact_id: null,
            account_id: null,
          };

          const retry = await supa.from("crm_activities").insert(payloadNoMeta);
          if (retry.error) throw retry.error;
          return;
        }

        throw insertWithMeta.error;
      };

      // Weak close trigger
      if (closeScore !== null && closeScore < 60) {
        await ensureCoachingTask({
          title: "Review and strengthen close before next call",
          reason: "weak_close",
        });
      }

      // Weak objection handling trigger
      if (objectionScore !== null && objectionScore < 60) {
        await ensureCoachingTask({
          title: "Practise objection handling before next follow-up",
          reason: "objection_handling",
        });
      }
    } catch (coachErr) {
      console.warn("[coaching auto-task] failed", coachErr);
    }

    res.json({
      ok: true,
      call: data,
      voice_score,
      voice_rubric,
      review_tags,
      assignment: effectiveAssignmentId
        ? { assignmentId: effectiveAssignmentId, via: "call_review" }
        : undefined,
      completed_count: completedCount,
      xp_awarded: xpAwardedTotal,
      xp_awarded_total: xpAwardedTotal,
    });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* -----------------------------------------------------------
   GET /v1/calls/:id/scores → score history for sparklines
   (ORDERED BEFORE /:id to avoid route capture)
------------------------------------------------------------ */
router.get("/:id/scores", async (req, res) => {
  try {
    const id = String(req.params.id);
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid id" });

    const requester = getUserIdHeader(req);

    // verify ownership
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id,user_id")
      .eq("id", id)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const { data, error } = await supa
      .from("call_scores")
      .select("score, created_at")
      .eq("call_id", id)
      .order("created_at", { ascending: true })
      .limit(100);

    if (error) throw error;

    res.json({ ok: true, items: data ?? [] });
  } catch (e: any) {
    console.error("[GET /:id/scores] error", e);
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* -------------------------------------------
   GET /v1/calls/:id → call detail (ownership)
   (KEEP THIS LAST among /:id* routes)
-------------------------------------------- */
router.get("/:id", async (req, res) => {
  try {
    const id = String(req.params.id);
    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid id" });
    }

    const requester = getUserIdHeader(req);

    const baseSelect = `
      id,
      user_id,
      org_id,
      filename,
      storage_path,
      status,
      created_at,
      duration_sec,
      duration_ms,
      score_overall,
      ai_model,
      rep_name,
      tags,
      summary,
      flags,
      rubric
    `;

    const extendedSelect = `
      ${baseSelect},
      voice_score,
      voice_rubric,
      review_tags
    `;

    let { data: call, error } = await supa
      .from("calls")
      .select(extendedSelect)
      .eq("id", id)
      .single();

    if (error) {
      const msg = String((error as any)?.message ?? "").toLowerCase();
      const missingVoiceCols =
        (msg.includes("voice_score") || msg.includes("voice_rubric") || msg.includes("review_tags")) &&
        (msg.includes("column") || msg.includes("schema cache"));

      if (missingVoiceCols) {
        const retry = await supa
          .from("calls")
          .select(baseSelect)
          .eq("id", id)
          .single();

        call = retry.data as any;
        error = retry.error as any;
      }
    }

    if (error || !call) {
      return res.status(404).json({ ok: false, error: "not_found" });
    }

    if (call.user_id !== requester) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }

    const mapped = {
      id: call.id,
      filename: call.filename,
      status: call.status,
      created_at: call.created_at,
      duration_sec: call.duration_sec,
      duration_ms: call.duration_ms ?? null,
      score_overall: call.score_overall,
      ai_model: call.ai_model,
      type: call.storage_path ? "upload" : "live",
      rep_name: call.rep_name ?? null,
      tags: call.tags ?? null,
      summary: call.summary ?? null,
      flags: call.flags ?? [],
      rubric: (call as any).rubric ?? null,
      voice_score: (call as any).voice_score ?? null,
      voice_rubric: (call as any).voice_rubric ?? null,
      review_tags: (call as any).review_tags ?? null,
      user_id: call.user_id,
      org_id: call.org_id,
    };

    return res.json({ ok: true, call: mapped });
  } catch (e: any) {
    console.error(e);
    return res
      .status(400)
      .json({ ok: false, error: e.message ?? "bad_request" });
  }
});

module.exports = router;