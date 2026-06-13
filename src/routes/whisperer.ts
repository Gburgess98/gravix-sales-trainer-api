import express from "express";
import OpenAI from "openai";
import { createClient } from "@supabase/supabase-js";
import { v4 as uuidv4 } from "uuid";
// TIER 2B Day 111 — Live Whisperer stub loop
import {
  detectWhispererTriggers,
  whispererTablesAvailable,
  type RecentTrigger,
} from "../whisperer";
import { logAuditEvent } from "../lib/audit";

const router = express.Router();

// Service-role client for whisperer session persistence (Day 111)
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false, autoRefreshToken: false } }
);

// Identity: same convention as the rest of the API (proxy injects x-user-id;
// global middleware may set req.userId).
function getWhispererUserId(req: express.Request): string {
  const anyReq = req as any;
  return String(
    anyReq.user?.id ||
      anyReq.userId ||
      req.header("x-user-id") ||
      req.header("x-forwarded-user-id") ||
      req.header("x-gravix-user-id") ||
      ""
  ).trim();
}

// In-memory fallback store until sql/20260612_whisperer_stub_loop.sql is run
// in an environment (manual Supabase SQL editor workflow). Responses are
// flagged persistence:false so the gap is visible. Capped to avoid growth.
type MemSession = { session: Record<string, any>; triggers: Record<string, any>[] };
const memSessions = new Map<string, MemSession>();
const MEM_CAP = 200;

function memPut(id: string, value: MemSession) {
  if (memSessions.size >= MEM_CAP) {
    const oldest = memSessions.keys().next().value;
    if (oldest) memSessions.delete(oldest);
  }
  memSessions.set(id, value);
}

function percentile(sorted: number[], p: number): number | null {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

type TranscriptTurn = { role: string; text: string };

function normalisePersonaId(raw: string): string {
  const id = (raw || "").toLowerCase().trim();
  switch (id) {
    case "angry":
    case "angry_buyer":
      return "angry";
    case "silent":
    case "ultra_silent":
      return "silent";
    case "cfo":
      return "cfo";
    case "procurement":
    case "procurement_wall":
      return "procurement";
    default:
      return "price_sensitive";
  }
}

function buildSuggestions(opts: {
  personaId: string;
  lastBuyerText: string;
  transcript: TranscriptTurn[];
}): string[] {
  const { personaId, lastBuyerText } = opts;
  const text = lastBuyerText.toLowerCase();
  const suggestions: string[] = [];

  const mentionsPrice = /price|cost|expensive|budget|roi|return on investment/.test(
    text,
  );
  const thinking =
    /think about it|need to think|send me info|email me/.test(text);
  const pushback =
    /not interested|happy with|already using/i.test(lastBuyerText);

  if (personaId === "price_sensitive") {
    if (mentionsPrice) {
      suggestions.push(
        "Acknowledge their price concern, then reframe around ROI and time-to-value. Finish with a binary question like: “If it paid for itself in 60 days, would it be worth exploring?”",
      );
    }
    if (pushback && !mentionsPrice) {
      suggestions.push(
        "Surface the real cost of staying the same. Ask a question that quantifies their current spend or lost revenue, then link it to your solution.",
      );
    }
  }

  if (personaId === "angry") {
    suggestions.push(
      "Lower your tone, acknowledge their frustration, and take responsibility for clarity. Then ask one calm, specific question to regain control.",
    );
  }

  if (personaId === "silent") {
    suggestions.push(
      "Use a light pattern interrupt and ask a short, direct question that forces a choice, like: “Is this more about timing or fit right now?”",
    );
  }

  if (thinking) {
    suggestions.push(
      "Respect the need to think, but lock in a next step: “Totally — why don’t we pencil in 10 minutes on Thursday to see if this is actually worth moving forward?”",
    );
  }

  if (!suggestions.length) {
    suggestions.push(
      "Summarise what they said in one sentence, then ask a sharp follow-up that uncovers the real constraint — budget, timing, or trust.",
    );
  }

  return suggestions;
}

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

async function rewriteSuggestionWithLLM(
  base: string,
  personaId: string,
  opts: { repId: string | null },
): Promise<string> {
  try {
    if (!process.env.OPENAI_API_KEY) return base;

    const personaLabel = personaId.replace(/_/g, " ");
    const repTag = opts.repId ? `The rep id is ${opts.repId}.` : "";

    const resp = await openai.responses.create({
      model: "gpt-4.1-mini",
      input: [
        {
          role: "system",
          content:
            "You rewrite coaching tips for sales reps. Keep them punchy, 1–2 sentences max, concrete and tactical. Do not add disclaimers or emojis.",
        },
        {
          role: "user",
          content: `Persona: ${personaLabel}. ${repTag} Rewrite this coaching tip in a sharper, clearer way:\n\n${base}`,
        },
      ],
      max_output_tokens: 120,
    });

    const out =
      (resp.output[0].content[0] as any)?.text?.trim?.() ??
      (resp.output[0].content as any)?.[0]?.text?.trim?.();

    if (out && typeof out === "string") return out;
    return base;
  } catch (e: any) {
    console.warn(
      "[whisperer] rewriteSuggestionWithLLM failed:",
      e?.message || e,
    );
    return base;
  }
}

// --- Route: POST /v1/whisperer/preview -------------------
router.post("/preview", express.json(), async (req, res) => {
  res.setHeader("Cache-Control", "no-store");

  try {
    const body = (req.body || {}) as {
      personaId?: string;
      transcript?: string | TranscriptTurn[];
    };

    const personaId = normalisePersonaId(body.personaId || "price_sensitive");

    const repIdHeader = req.header("x-user-id");
    const repId =
      repIdHeader && String(repIdHeader).trim().length > 0
        ? String(repIdHeader).trim()
        : null;

    const transcriptRaw = body.transcript;
    let transcript: TranscriptTurn[] = [];

    if (Array.isArray(transcriptRaw)) {
      transcript = (transcriptRaw as any[])
        .filter((t) => t && typeof (t as any).text === "string")
        .map((t) => ({
          role: String((t as any).role || "").toLowerCase(),
          text: String((t as any).text),
        }));
    } else if (typeof transcriptRaw === "string") {
      transcript = [
        {
          role: "buyer",
          text: transcriptRaw,
        },
      ];
    }

    if (!transcript.length) {
      return res.json({
        ok: true,
        personaId,
        suggestions: [],
      });
    }

    const lastBuyerTurn =
      [...transcript].reverse().find((t) => t.role.includes("buyer")) ||
      transcript[transcript.length - 1];

    if (!lastBuyerTurn || !lastBuyerTurn.text?.trim()) {
      return res.json({
        ok: true,
        personaId,
        suggestions: [],
      });
    }

    const baseSuggestions = buildSuggestions({
      personaId,
      lastBuyerText: lastBuyerTurn.text,
      transcript,
    });

    const enhanced: string[] = [];
    for (const s of baseSuggestions) {
      const rewritten = await rewriteSuggestionWithLLM(s, personaId, { repId });
      enhanced.push(rewritten);
    }

    return res.json({
      ok: true,
      personaId,
      suggestions: enhanced,
    });
  } catch (err: any) {
    console.error("[whisperer] preview failed", err);
    return res.status(500).json({
      ok: false,
      error: "whisperer_preview_failed",
      message: err?.message || "Failed to build Whisperer suggestions.",
    });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// TIER 2B Day 111 — Live Whisperer session routes (transcript stub loop).
// No Deepgram, no mic, no push channels: typed segments are POSTed and the
// trigger/suggestion response rides back inline. Access is conservative for
// Day 111: only the session owner (creating user) can read/write a session;
// the manager surface arrives later with hierarchy scoping.
// ═══════════════════════════════════════════════════════════════════════════

// Shared loader: DB row or in-memory fallback. Returns null when not found.
async function loadWhispererSession(id: string): Promise<{
  session: Record<string, any> | null;
  persisted: boolean;
}> {
  const persisted = await whispererTablesAvailable(supa);
  if (!persisted) {
    return { session: memSessions.get(id)?.session ?? null, persisted };
  }
  const { data } = await supa
    .from("whisperer_sessions")
    .select("*")
    .eq("id", id)
    .maybeSingle();
  return { session: data ?? null, persisted };
}

// POST /v1/whisperer/deepgram-token — mint a short-lived client token (Day 112)
// Uses Deepgram's Grant Token API so the browser never holds the permanent key.
// Requires a normal authenticated user (not a manager).
router.post("/deepgram-token", express.json(), async (req, res) => {
  try {
    const userId = getWhispererUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const key = String(process.env.DEEPGRAM_API_KEY || "").trim();
    if (!key) {
      return res.status(503).json({ ok: false, error: "deepgram_not_configured" });
    }

    const ttl = 30; // seconds — Deepgram grant tokens are short-lived by design
    const resp = await fetch("https://api.deepgram.com/v1/auth/grant", {
      method: "POST",
      headers: { Authorization: `Token ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ ttl_seconds: ttl }),
    });

    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      console.warn("[whisperer/deepgram-token] grant failed", resp.status, body.slice(0, 200));
      // Controlled error — never fall back to the permanent key
      return res.status(502).json({
        ok: false,
        error: "deepgram_token_unsupported",
        status: resp.status,
      });
    }

    const data = (await resp.json().catch(() => ({}))) as any;
    const token = String(data?.access_token || "").trim();
    if (!token) {
      return res.status(502).json({ ok: false, error: "deepgram_token_unsupported" });
    }

    return res.json({
      ok: true,
      token,
      expiresInSeconds: Number(data?.expires_in) || ttl,
      model: "nova-3",
      language: "en",
    });
  } catch (e: any) {
    console.error("[whisperer/deepgram-token] error", e?.message || e);
    return res.status(500).json({ ok: false, error: "deepgram_token_failed" });
  }
});

// POST /v1/whisperer/sessions — start a live session
router.post("/sessions", express.json(), async (req, res) => {
  try {
    const userId = getWhispererUserId(req) || String((req.body as any)?.repId || "").trim();
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const callId = String((req.body as any)?.callId || "").trim() || null;

    // Tenant stamping from the user's hierarchy row (Tier 2A lesson: day one)
    let companyId: string | null = null;
    let officeId: string | null = null;
    try {
      const { data: u } = await supa
        .from("users")
        .select("company_id, office_id")
        .eq("id", userId)
        .maybeSingle();
      companyId = (u as any)?.company_id || null;
      officeId = (u as any)?.office_id || null;
    } catch { /* best-effort */ }

    const nowIso = new Date().toISOString();
    const session: Record<string, any> = {
      id: uuidv4(),
      rep_id: userId,
      user_id: userId,
      org_id: null,
      company_id: companyId,
      office_id: officeId,
      call_id: callId,
      status: "active",
      started_at: nowIso,
      ended_at: null,
      latency_p50_ms: null,
      latency_p95_ms: null,
      meta: { source: "simulator" },
      created_at: nowIso,
      updated_at: nowIso,
    };

    const persisted = await whispererTablesAvailable(supa);
    if (persisted) {
      const { error } = await supa.from("whisperer_sessions").insert(session);
      if (error) return res.status(500).json({ ok: false, error: error.message });
    } else {
      memPut(session.id, { session, triggers: [] });
    }

    void logAuditEvent({
      actorUserId: userId,
      action: "whisperer.session_started",
      entityType: "whisperer_session",
      entityId: session.id,
      metadata: { company_id: companyId, office_id: officeId, call_id: callId, persisted },
    });

    return res.json({
      ok: true,
      persistence: persisted,
      session: { id: session.id, status: session.status, startedAt: session.started_at },
    });
  } catch (e: any) {
    console.error("[whisperer/sessions POST] error", e);
    return res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

// GET /v1/whisperer/sessions/:id — session + triggers (owner only)
router.get("/sessions/:id", async (req, res) => {
  try {
    const userId = getWhispererUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const id = String(req.params.id || "").trim();
    const { session, persisted } = await loadWhispererSession(id);
    if (!session) return res.status(404).json({ ok: false, error: "not_found" });
    if (String(session.user_id) !== userId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }

    let triggers: Record<string, any>[] = [];
    if (persisted) {
      const { data } = await supa
        .from("whisperer_triggers")
        .select("id, segment_text, type, phrase, confidence, suggestion, latency_ms, detected_at")
        .eq("session_id", id)
        .order("created_at", { ascending: true })
        .limit(200);
      triggers = data ?? [];
    } else {
      triggers = memSessions.get(id)?.triggers ?? [];
    }

    return res.json({ ok: true, persistence: persisted, session, triggers });
  } catch (e: any) {
    console.error("[whisperer/sessions GET] error", e);
    return res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

// POST /v1/whisperer/sessions/:id/segments — the heart of the stub loop
router.post("/sessions/:id/segments", express.json(), async (req, res) => {
  try {
    const userId = getWhispererUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const id = String(req.params.id || "").trim();
    const { session, persisted } = await loadWhispererSession(id);
    if (!session) return res.status(404).json({ ok: false, error: "not_found" });
    if (String(session.user_id) !== userId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (session.status === "ended") {
      return res.status(409).json({ ok: false, error: "session_ended" });
    }

    const text = String((req.body as any)?.text || "").trim();
    if (!text) return res.status(400).json({ ok: false, error: "text_required" });
    const speaker = ["rep", "prospect", "unknown"].includes(String((req.body as any)?.speaker))
      ? ((req.body as any).speaker as "rep" | "prospect" | "unknown")
      : "prospect";

    const receivedAt = new Date();

    // De-dup context: recent triggers from the last 60s
    let recentTriggers: RecentTrigger[] = [];
    if (persisted) {
      const { data } = await supa
        .from("whisperer_triggers")
        .select("type, phrase, detected_at")
        .eq("session_id", id)
        .gte("detected_at", new Date(receivedAt.getTime() - 60_000).toISOString());
      recentTriggers = (data ?? []).map((t: any) => ({
        type: t.type,
        phrase: t.phrase,
        detectedAt: t.detected_at,
      }));
    } else {
      recentTriggers = (memSessions.get(id)?.triggers ?? []).map((t: any) => ({
        type: t.type,
        phrase: t.phrase,
        detectedAt: t.detected_at,
      }));
    }

    const detected = detectWhispererTriggers({
      sessionId: id,
      text,
      speaker,
      now: receivedAt,
      recentTriggers,
    });

    // Latency: spoken/typed moment → trigger ready (client clock when given)
    const clientSentAtRaw = String((req.body as any)?.clientSentAt || "").trim();
    const clientSentMs = clientSentAtRaw ? new Date(clientSentAtRaw).getTime() : NaN;
    const latencyMs = Number.isFinite(clientSentMs)
      ? Math.max(0, Date.now() - clientSentMs)
      : Date.now() - receivedAt.getTime();

    const triggerRows = detected.map((t) => ({
      id: uuidv4(),
      session_id: id,
      segment_text: text.slice(0, 2000),
      type: t.type,
      phrase: t.phrase,
      confidence: t.confidence,
      suggestion: t.suggestion,
      latency_ms: latencyMs,
      detected_at: receivedAt.toISOString(),
      meta: { speaker },
      created_at: receivedAt.toISOString(),
    }));

    if (triggerRows.length > 0) {
      if (persisted) {
        const { error } = await supa.from("whisperer_triggers").insert(triggerRows);
        if (error) console.warn("[whisperer/segments] trigger insert failed", error.message);
      } else {
        const mem = memSessions.get(id);
        if (mem) mem.triggers.push(...triggerRows);
      }
    }

    return res.json({
      ok: true,
      persistence: persisted,
      segment: {
        text,
        speaker,
        receivedAt: receivedAt.toISOString(),
        processedAt: new Date().toISOString(),
      },
      triggers: triggerRows.map((r) => ({
        id: r.id,
        type: r.type,
        phrase: r.phrase,
        confidence: r.confidence,
        suggestion: r.suggestion,
        detectedAt: r.detected_at,
        latencyMs: r.latency_ms,
      })),
      latencyMs,
    });
  } catch (e: any) {
    console.error("[whisperer/segments POST] error", e);
    return res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

// POST /v1/whisperer/sessions/:id/end — close the session
router.post("/sessions/:id/end", express.json(), async (req, res) => {
  try {
    const userId = getWhispererUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user_identity" });

    const id = String(req.params.id || "").trim();
    const { session, persisted } = await loadWhispererSession(id);
    if (!session) return res.status(404).json({ ok: false, error: "not_found" });
    if (String(session.user_id) !== userId) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }

    // Latency percentiles from this session's triggers
    let latencies: number[] = [];
    if (persisted) {
      const { data } = await supa
        .from("whisperer_triggers")
        .select("latency_ms")
        .eq("session_id", id)
        .not("latency_ms", "is", null);
      latencies = (data ?? []).map((t: any) => Number(t.latency_ms)).filter(Number.isFinite);
    } else {
      latencies = (memSessions.get(id)?.triggers ?? [])
        .map((t: any) => Number(t.latency_ms))
        .filter(Number.isFinite);
    }
    latencies.sort((a, b) => a - b);

    const nowIso = new Date().toISOString();
    const patch = {
      status: "ended",
      ended_at: nowIso,
      latency_p50_ms: percentile(latencies, 50),
      latency_p95_ms: percentile(latencies, 95),
      updated_at: nowIso,
    };

    if (persisted) {
      const { error } = await supa.from("whisperer_sessions").update(patch).eq("id", id);
      if (error) return res.status(500).json({ ok: false, error: error.message });
    } else {
      const mem = memSessions.get(id);
      if (mem) Object.assign(mem.session, patch);
    }

    void logAuditEvent({
      actorUserId: userId,
      action: "whisperer.session_ended",
      entityType: "whisperer_session",
      entityId: id,
      metadata: {
        trigger_count: latencies.length,
        latency_p50_ms: patch.latency_p50_ms,
        persisted,
      },
    });

    return res.json({ ok: true, persistence: persisted, session: { id, ...patch } });
  } catch (e: any) {
    console.error("[whisperer/end POST] error", e);
    return res.status(400).json({ ok: false, error: e?.message || "bad_request" });
  }
});

export default router;