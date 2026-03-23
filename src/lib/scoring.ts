// src/lib/scoring.ts
import type { SupabaseClient } from "@supabase/supabase-js";
import { createClient } from "@supabase/supabase-js";
import { getOpenAI, AI_MODEL, OPENAI_TIMEOUT_MS } from "./openai";
import { postScoreSummary } from "./slack";

export const RUBRIC_VERSION = "v1"; // bump when rubric changes
export const SCORING_PROMPT_VERSION = "v1";
export const SCORING_MODEL_VERSION = `${AI_MODEL}:${SCORING_PROMPT_VERSION}:${RUBRIC_VERSION}`;

function stableHash(input: string): string {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export type RubricSection = { score: number; notes: string };
export type LlmScore = {
  model: string;
  overall: number;
  summary: string;
  intro: RubricSection;
  discovery: RubricSection;
  objection: RubricSection;
  close: RubricSection;
};

function sectionSchema() {
  return {
    type: "object",
    properties: {
      score: { type: "integer", minimum: 0, maximum: 100 },
      notes: { type: "string", maxLength: 300 },
    },
    required: ["score", "notes"],
    additionalProperties: false,
  };
}

const JSON_SCHEMA = {
  name: "SalesCallScore",
  schema: {
    type: "object",
    properties: {
      model: { type: "string" },
      overall: { type: "integer", minimum: 0, maximum: 100 },
      summary: { type: "string", maxLength: 220 },
      intro: sectionSchema(),
      discovery: sectionSchema(),
      objection: sectionSchema(),
      close: sectionSchema(),
    },
    required: ["model", "overall", "summary", "intro", "discovery", "objection", "close"],
    additionalProperties: false,
  },
  strict: true,
};

function clamp(n: number) {
  return Math.max(0, Math.min(100, Math.round(n)));
}

function normaliseTranscriptForDeterminism(transcript: string): string {
  return String(transcript || "")
    .replace(/\r\n/g, "\n")
    .replace(/\s+/g, " ")
    .trim();
}

function buildDeterministicPromptKey(params: {
  callId: string;
  filename?: string | null;
  sha256?: string | null;
  transcript?: string | null;
}) {
  const transcript = normaliseTranscriptForDeterminism(params.transcript || "");
  const transcriptHash = stableHash(transcript || params.sha256 || params.callId);
  return {
    transcript,
    transcriptHash,
    key: [
      `rubric=${RUBRIC_VERSION}`,
      `prompt=${SCORING_PROMPT_VERSION}`,
      `model=${SCORING_MODEL_VERSION}`,
      `filename=${params.filename || params.callId}`,
      `sha256=${params.sha256 || "missing"}`,
      `transcriptHash=${transcriptHash}`,
    ].join("|"),
  };
}

function buildRubricWithMeta(args: {
  intro: RubricSection;
  discovery: RubricSection;
  objection: RubricSection;
  close: RubricSection;
  callSha256: string | null;
  transcriptHash: string | null;
  transcriptPresent: boolean;
  modelVersion: string;
}) {
  return {
    intro: args.intro,
    discovery: args.discovery,
    objection: args.objection,
    close: args.close,
    _meta: {
      rubric_version: RUBRIC_VERSION,
      prompt_version: SCORING_PROMPT_VERSION,
      model_version: args.modelVersion,
      call_sha256: args.callSha256,
      transcript_hash: args.transcriptHash,
      transcript_present: args.transcriptPresent,
    },
  };
}

async function readScoreCache(
  supabase: SupabaseClient,
  cacheKey: string
): Promise<LlmScore | null> {
  try {
    const { data, error } = await supabase
      .from("score_cache")
      .select("result_json")
      .eq("cache_key", cacheKey)
      .maybeSingle();

    if (error) {
      const msg = String((error as any)?.message ?? "").toLowerCase();
      const missing =
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find") && msg.includes("score_cache")) ||
        (msg.includes("schema cache") && msg.includes("score_cache"));
      if (missing) return null;
      console.warn("[score-cache] read failed:", error.message);
      return null;
    }

    const raw = (data as any)?.result_json;
    if (!raw) return null;
    return raw as LlmScore;
  } catch (e: any) {
    console.warn("[score-cache] read error:", e?.message || e);
    return null;
  }
}

async function writeScoreCache(
  supabase: SupabaseClient,
  args: {
    cacheKey: string;
    callSha256: string | null;
    transcriptHash: string | null;
    result: LlmScore;
  }
) {
  try {
    const { error } = await supabase.from("score_cache").upsert(
      {
        cache_key: args.cacheKey,
        call_sha256: args.callSha256,
        transcript_hash: args.transcriptHash,
        rubric_version: RUBRIC_VERSION,
        prompt_version: SCORING_PROMPT_VERSION,
        model_version: SCORING_MODEL_VERSION,
        result_json: args.result,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "cache_key" }
    );

    if (error) {
      const msg = String((error as any)?.message ?? "").toLowerCase();
      const missing =
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find") && msg.includes("score_cache")) ||
        (msg.includes("schema cache") && msg.includes("score_cache"));
      if (!missing) console.warn("[score-cache] write failed:", error.message);
    }
  } catch (e: any) {
    console.warn("[score-cache] write error:", e?.message || e);
  }
}

export function heuristicScoreFallback(): LlmScore {
  const s = {
    score: 68,
    notes: "Deterministic fallback based on minimal call metadata.",
  };
  return {
    model: "heuristic:v1",
    overall: s.score,
    summary: "Solid overall structure, but a fuller transcript is needed for a reliable coaching summary.",
    intro: { ...s },
    discovery: { ...s },
    objection: { ...s },
    close: { ...s },
  };
}

/** Write a score row into call_scores (non-blocking if table missing) */
async function writeScoreHistory(
  supabase: SupabaseClient,
  callId: string,
  model: string,
  overall: number,
  rubric: any
) {
  try {
    const { data: callRow, error: callErr } = await supabase
      .from("calls")
      .select("user_id")
      .eq("id", callId)
      .maybeSingle();

    if (callErr) {
      console.warn("[score] call lookup for history failed:", callErr.message);
    }

    const { error } = await supabase.from("call_scores").insert({
      call_id: callId,
      user_id: (callRow as any)?.user_id ?? null,
      ai_model: model,
      rubric_version: RUBRIC_VERSION,
      overall,
      rubric,
    });
    if (error) console.warn("[score] call_scores insert failed:", error.message);
  } catch (e: any) {
    console.warn("[score] call_scores insert error:", e?.message || e);
  }
}

/** Best-effort Slack notifier (safe: silently skips if webhook unset) */
async function notifySlack(opts: {
  supabase: SupabaseClient;
  callId: string;
  scores: {
    intro: number;
    discovery: number;
    objection: number;
    close: number;
    overall: number;
  };
  durationSec?: number | null;
  repIdFallback?: string | null;
}) {
  try {
    // Try to resolve a human name for the rep if you store it in profiles
    let repName: string | undefined = undefined;
    if (opts.repIdFallback) {
      try {
        const { data } = await opts.supabase
          .from("profiles")
          .select("full_name")
          .eq("id", opts.repIdFallback)
          .maybeSingle();
        repName = (data as any)?.full_name || opts.repIdFallback || undefined;
      } catch {
        repName = opts.repIdFallback || undefined;
      }
    }

    const WEB =
      (process.env.PUBLIC_WEB_BASE ||
        process.env.WEB_BASE_URL ||
        "http://localhost:3000").replace(/\/$/, "");

    await postScoreSummary({
      callId: opts.callId,
      overallScore: opts.scores.overall,
      section: {
        intro: opts.scores.intro,
        discovery: opts.scores.discovery,
        objection: opts.scores.objection,
        close: opts.scores.close,
      },
      durationSec: typeof opts.durationSec === "number" ? opts.durationSec : undefined,
      repName,
      callUrl: `${WEB}/calls/${opts.callId}`,
      recentUrl: `${WEB}/recent-calls`,
      // webhookUrl optional — postScoreSummary will default to SLACK_WEBHOOK_URL
    });

    console.log("[score] Slack summary posted");
  } catch (err) {
    console.error("[score] Slack summary failed", err);
  }
}

export async function scoreWithLLM(opts: {
  supabase: SupabaseClient;
  callId: string;
}): Promise<LlmScore> {
  const { supabase, callId } = opts;

  try {
    // Pull minimal call meta (include duration for Slack; user_id to resolve rep)
    const { data: call, error: callErr } = await supabase
      .from("calls")
      .select("id, filename, user_id, duration_sec, sha256, transcript")
      .eq("id", callId)
      .single();
    if (callErr || !call) throw new Error("call_not_found");

    const deterministic = buildDeterministicPromptKey({
      callId,
      filename: (call as any)?.filename ?? null,
      sha256: ((call as any)?.sha256 as string | null) ?? null,
      transcript: ((call as any)?.transcript as string | null) ?? null,
    });

    const cached = await readScoreCache(supabase, deterministic.key);
    if (cached) {
      const cachedModelVersion = String((cached as any)?.model || SCORING_MODEL_VERSION);
      const rubric = buildRubricWithMeta({
        intro: cached.intro,
        discovery: cached.discovery,
        objection: cached.objection,
        close: cached.close,
        callSha256: ((call as any)?.sha256 as string | null) ?? null,
        transcriptHash: deterministic.transcriptHash,
        transcriptPresent: Boolean(deterministic.transcript),
        modelVersion: cachedModelVersion,
      });

      const { error: cacheUpErr } = await supabase
        .from("calls")
        .update({
          score_overall: cached.overall,
          summary: cached.summary,
          transcript: ((call as any)?.transcript as string | null) ?? null,
          rubric,
          ai_model: cachedModelVersion,
          rubric_version: RUBRIC_VERSION,
          scored_at: new Date().toISOString(),
        })
        .eq("id", callId);
      if (cacheUpErr) throw cacheUpErr;

      await writeScoreHistory(supabase, callId, cachedModelVersion, cached.overall, rubric);

      await notifySlack({
        supabase,
        callId,
        repIdFallback: (call as any)?.user_id ?? null,
        durationSec: (call as any)?.duration_sec ?? null,
        scores: {
          intro: cached.intro.score,
          discovery: cached.discovery.score,
          objection: cached.objection.score,
          close: cached.close.score,
          overall: cached.overall,
        },
      });

      return cached;
    }

    // Build prompt
    const userLines = [
      `CALL META: filename="${call.filename || call.id}"`,
      `CALL HASH: sha256="${(((call as any).sha256 as string | null) || "missing")}"`,
      `TRANSCRIPT HASH: stable="${deterministic.transcriptHash}"`,
      `DETERMINISM KEY: "${deterministic.key}"`,
      "TRANSCRIPT:",
      deterministic.transcript || "(not available in MVP)",
      "",
      "Rubric guide:",
      "- Intro: pattern interrupt, clear reason, agenda set.",
      "- Discovery: deep questions, pain/impact, budget/timeline, authority.",
      "- Objection: isolates true objection, reframes value, tests commitment.",
      "- Close: clear next step, assumptive/binary ask, time/date locked.",
      "Also return a short coaching summary in 1-2 sentences (max 220 chars) that explains the main strength and main weakness of the call.",
      "For identical transcripts, hashes, rubric versions, and prompt versions, return identical section scores, overall score, and materially consistent reasoning.",
    ];
    const user = userLines.join("\n");

    const system =
      "You are a strict sales call evaluator. Score from 0–100 overall and for Intro, Discovery, Objection Handling, Close. Be concise. Also provide a short coaching summary. Treat the transcript and determinism key as the source of truth. Identical calls scored under the same rubric and prompt version must return the same result. Output must match the provided JSON schema exactly.";

    const openai = getOpenAI();
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), OPENAI_TIMEOUT_MS);

    const resp = await openai.chat.completions.create(
      {
        model: AI_MODEL,
        messages: [
          { role: "system", content: system },
          { role: "user", content: user },
        ],
        response_format: { type: "json_schema", json_schema: JSON_SCHEMA as any },
        temperature: 0,
      },
      { signal: ctrl.signal }
    );
    clearTimeout(timer);

    const raw = resp.choices?.[0]?.message?.content;
    if (!raw) throw new Error("no_model_content");

    const parsed = JSON.parse(raw) as LlmScore;

    // Clamp & tidy
    parsed.model = AI_MODEL;
    parsed.overall = clamp(parsed.overall);
    (["intro", "discovery", "objection", "close"] as const).forEach((k) => {
      parsed[k].score = clamp(parsed[k].score);
      parsed[k].notes = (parsed[k].notes || "").slice(0, 300);
    });
    parsed.summary = String(parsed.summary || "").trim().slice(0, 220);
    if (!parsed.summary) {
      parsed.summary = "Good baseline structure, but this call needs clearer strengths and weaknesses captured in the summary.";
    }

    const rubric = buildRubricWithMeta({
      intro: parsed.intro,
      discovery: parsed.discovery,
      objection: parsed.objection,
      close: parsed.close,
      callSha256: ((call as any)?.sha256 as string | null) ?? null,
      transcriptHash: deterministic.transcriptHash,
      transcriptPresent: Boolean(deterministic.transcript),
      modelVersion: SCORING_MODEL_VERSION,
    });

    // Persist latest on calls
    const { error: upErr } = await supabase
      .from("calls")
      .update({
        score_overall: parsed.overall,
        summary: parsed.summary,
        transcript: ((call as any)?.transcript as string | null) ?? null,
        rubric,
        ai_model: SCORING_MODEL_VERSION,
        rubric_version: RUBRIC_VERSION,
        scored_at: new Date().toISOString(),
      })
      .eq("id", callId);
    if (upErr) throw upErr;

    // History row (non-blocking)
    await writeScoreHistory(supabase, callId, SCORING_MODEL_VERSION, parsed.overall, rubric);

    await writeScoreCache(supabase, {
      cacheKey: deterministic.key,
      callSha256: ((call as any)?.sha256 as string | null) ?? null,
      transcriptHash: deterministic.transcriptHash,
      result: parsed,
    });

    // CRM Activity: record a score event (best-effort; non-blocking)
    try {
      const svc = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);
      // fetch linkage for account/contact so the activity appears on their timelines
      const { data: callRow } = await svc
        .from('calls')
        .select('id, account_id, contact_id, score_overall')
        .eq('id', callId)
        .single();

      const overall = typeof (callRow as any)?.score_overall === 'number' ? (callRow as any).score_overall : parsed.overall;
      const summary = `Scored ${Math.round(overall)}`;

      await svc.from('activities').insert({
        type: 'score',
        summary,
        account_id: (callRow as any)?.account_id ?? null,
        contact_id: (callRow as any)?.contact_id ?? null,
      });
    } catch (e) {
      console.warn('[score] activity insert failed', e);
    }

    // Slack (best-effort)
    await notifySlack({
      supabase,
      callId,
      repIdFallback: (call as any)?.user_id ?? null,
      durationSec: (call as any)?.duration_sec ?? null,
      scores: {
        intro: parsed.intro.score,
        discovery: parsed.discovery.score,
        objection: parsed.objection.score,
        close: parsed.close.score,
        overall: parsed.overall,
      },
    });

    return parsed;
  } catch (err: any) {
    console.warn(
      "[scoreWithLLM] LLM failed, using heuristic:",
      err?.status ?? "",
      err?.code ?? "",
      err?.message ?? err
    );

    const fb = heuristicScoreFallback();

    const fallbackModelVersion = `${fb.model}:${SCORING_PROMPT_VERSION}:${RUBRIC_VERSION}`;
    const rubric = buildRubricWithMeta({
      intro: fb.intro,
      discovery: fb.discovery,
      objection: fb.objection,
      close: fb.close,
      callSha256: null,
      transcriptHash: null,
      transcriptPresent: false,
      modelVersion: fallbackModelVersion,
    });

    await opts.supabase
      .from("calls")
      .update({
        score_overall: fb.overall,
        summary: fb.summary,
        rubric,
        ai_model: fallbackModelVersion,
        rubric_version: RUBRIC_VERSION,
        scored_at: new Date().toISOString(),
      })
      .eq("id", opts.callId);

    // History row (non-blocking)
    await writeScoreHistory(opts.supabase, opts.callId, fallbackModelVersion, fb.overall, rubric);

    await writeScoreCache(opts.supabase, {
      cacheKey: stableHash(`fallback|${opts.callId}|${RUBRIC_VERSION}|${SCORING_PROMPT_VERSION}`),
      callSha256: null,
      transcriptHash: null,
      result: fb,
    });

    // CRM Activity: record a score event for fallback (best-effort)
    try {
      const svc = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);
      const { data: callRow } = await svc
        .from('calls')
        .select('id, account_id, contact_id, score_overall')
        .eq('id', opts.callId)
        .single();

      const overall = typeof (callRow as any)?.score_overall === 'number' ? (callRow as any).score_overall : fb.overall;
      const summary = `Scored ${Math.round(overall)}`;

      await svc.from('activities').insert({
        type: 'score',
        summary,
        account_id: (callRow as any)?.account_id ?? null,
        contact_id: (callRow as any)?.contact_id ?? null,
      });
    } catch (e) {
      console.warn('[score] activity insert failed (fallback)', e);
    }

    // Grab duration if present for Slack
    let durationSec: number | null = null;
    try {
      const { data } = await opts.supabase
        .from("calls")
        .select("duration_sec")
        .eq("id", opts.callId)
        .single();
      durationSec = (data as any)?.duration_sec ?? null;
    } catch {}

    // Slack fallback
    await notifySlack({
      supabase: opts.supabase,
      callId: opts.callId,
      repIdFallback: null,
      durationSec,
      scores: {
        intro: fb.intro.score,
        discovery: fb.discovery.score,
        objection: fb.objection.score,
        close: fb.close.score,
        overall: fb.overall,
      },
    });

    return fb;
  }
}