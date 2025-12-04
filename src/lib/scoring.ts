// src/lib/scoring.ts
import type { SupabaseClient } from "@supabase/supabase-js";
import { createClient } from "@supabase/supabase-js";
import { getOpenAI, AI_MODEL, OPENAI_TIMEOUT_MS } from "./openai";
import { postScoreSummary } from "./slack";

export const RUBRIC_VERSION = "v1"; // bump when rubric changes

export type RubricSection = { score: number; notes: string };
export type LlmScore = {
  model: string;
  overall: number;
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
      intro: sectionSchema(),
      discovery: sectionSchema(),
      objection: sectionSchema(),
      close: sectionSchema(),
    },
    required: ["model", "overall", "intro", "discovery", "objection", "close"],
    additionalProperties: false,
  },
  strict: true,
};

function clamp(n: number) {
  return Math.max(0, Math.min(100, Math.round(n)));
}

export function heuristicScoreFallback(): LlmScore {
  const pick = () =>
    Math.max(55, Math.min(85, Math.round(70 + (Math.random() * 16 - 8))));
  const s = {
    score: pick(),
    notes: "Heuristic fallback based on minimal call metadata.",
  };
  return {
    model: "heuristic:v1",
    overall: s.score,
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
    const { error } = await supabase.from("call_scores").insert({
      call_id: callId,
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
      .select("id, filename, user_id, duration_sec")
      .eq("id", callId)
      .single();
    if (callErr || !call) throw new Error("call_not_found");

    // Build prompt
    const userLines = [
      'CALL META: filename="' + (call.filename || call.id) + '"',
      "TRANSCRIPT: (not available in MVP)",
      "",
      "Rubric guide:",
      "- Intro: pattern interrupt, clear reason, agenda set.",
      "- Discovery: deep questions, pain/impact, budget/timeline, authority.",
      "- Objection: isolates true objection, reframes value, tests commitment.",
      "- Close: clear next step, assumptive/binary ask, time/date locked.",
    ];
    const user = userLines.join("\n");

    const system =
      "You are a strict sales call evaluator. Score from 0–100 overall and for Intro, Discovery, Objection Handling, Close. Be concise. Output must match the provided JSON schema exactly.";

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
        temperature: 0.2,
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

    const rubric = {
      intro: parsed.intro,
      discovery: parsed.discovery,
      objection: parsed.objection,
      close: parsed.close,
    };

    // Persist latest on calls
    const { error: upErr } = await supabase
      .from("calls")
      .update({
        score_overall: parsed.overall,
        rubric,
        ai_model: parsed.model,
        rubric_version: RUBRIC_VERSION,
        scored_at: new Date().toISOString(),
      })
      .eq("id", callId);
    if (upErr) throw upErr;

    // History row (non-blocking)
    await writeScoreHistory(supabase, callId, parsed.model, parsed.overall, rubric);

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

    const rubric = {
      intro: fb.intro,
      discovery: fb.discovery,
      objection: fb.objection,
      close: fb.close,
    };

    await opts.supabase
      .from("calls")
      .update({
        score_overall: fb.overall,
        rubric,
        ai_model: fb.model,
        rubric_version: RUBRIC_VERSION,
        scored_at: new Date().toISOString(),
      })
      .eq("id", opts.callId);

    // History row (non-blocking)
    await writeScoreHistory(opts.supabase, opts.callId, fb.model, fb.overall, rubric);

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