// src/lib/scoring.ts
import type { SupabaseClient } from "@supabase/supabase-js";
import { createClient } from "@supabase/supabase-js";
import { getOpenAI, AI_MODEL, OPENAI_TIMEOUT_MS } from "./openai";
import { postScoreSummary } from "./slack";
import type {
  CallAnalysis,
  CallAnalysisStages,
  CallMoment,
  StageScore,
  VoiceAnalysis,
} from "./types/call-analysis";

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

export type RubricSection = StageScore;
export type VoiceScore = VoiceAnalysis;
export type LlmScore = CallAnalysis & {
  model: string;
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

function stagesSchema() {
  return {
    type: "object",
    properties: {
      intro: sectionSchema(),
      discovery: sectionSchema(),
      objection: sectionSchema(),
      close: sectionSchema(),
    },
    required: ["intro", "discovery", "objection", "close"],
    additionalProperties: false,
  };
}

function momentsSchema() {
  return {
    type: "array",
    items: {
      type: "object",
      properties: {
        timestamp: { type: "number" },
        type: {
          type: "string",
          enum: ["objection", "mistake", "highlight", "closing_attempt"],
        },
        text: { type: "string", maxLength: 280 },
        severity: {
          type: "string",
          enum: ["low", "medium", "high"],
        },
      },
      required: ["type", "text"],
      additionalProperties: false,
    },
  };
}

function suggestionsSchema() {
  return {
    type: "array",
    items: { type: "string", maxLength: 220 },
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
      stages: stagesSchema(),
      moments: momentsSchema(),
      suggestions: suggestionsSchema(),
    },
    required: ["model", "overall", "summary", "stages", "moments", "suggestions"],
    additionalProperties: false,
  },
  strict: true,
};

function clamp(n: number) {
  return Math.max(0, Math.min(100, Math.round(n)));
}

function clampText(input: unknown, max = 300): string {
  return String(input || "").trim().slice(0, max);
}


function normaliseSection(section: any, key: string): RubricSection {
  if (!section || typeof section !== "object") {
    throw new Error(`invalid_score_schema:${key}`);
  }

  const score = Number(section.score);
  if (!Number.isFinite(score)) {
    throw new Error(`invalid_score_schema:${key}.score`);
  }

  const notes = clampText(section.notes, 300);
  if (!notes) {
    throw new Error(`invalid_score_schema:${key}.notes`);
  }

  return {
    score: clamp(score),
    notes,
  };
}

function normaliseStages(input: any): CallAnalysisStages {
  const source = input?.stages && typeof input.stages === "object" ? input.stages : input;
  return {
    intro: normaliseSection(source?.intro, "intro"),
    discovery: normaliseSection(source?.discovery, "discovery"),
    objection: normaliseSection(source?.objection, "objection"),
    close: normaliseSection(source?.close, "close"),
  };
}

function normaliseMoments(input: any): CallMoment[] {
  if (!Array.isArray(input)) return [];
  return input
    .map((moment: any) => {
      const type = String(moment?.type || "").trim();
      const text = clampText(moment?.text, 280);
      const severity = String(moment?.severity || "").trim();
      const timestamp = Number(moment?.timestamp);

      if (!type || !text) return null;
      if (!["objection", "mistake", "highlight", "closing_attempt"].includes(type)) return null;

      return {
        type: type as CallMoment["type"],
        text,
        severity: ["low", "medium", "high"].includes(severity)
          ? (severity as CallMoment["severity"])
          : undefined,
        timestamp: Number.isFinite(timestamp) ? timestamp : undefined,
      };
    })
    .filter(Boolean) as CallMoment[];
}

function normaliseSuggestions(input: any): string[] {
  if (!Array.isArray(input)) return [];
  return input
    .map((x) => clampText(x, 220))
    .filter(Boolean)
    .slice(0, 6);
}

function normaliseVoiceScore(input: any): VoiceScore {
  const fallback = computeVoiceScore("", null);
  if (!input || typeof input !== "object") return fallback;

  return {
    clarity: clamp(Number(input.clarity ?? fallback.clarity)),
    confidence: clamp(Number(input.confidence ?? fallback.confidence)),
    filler_density: clamp(Number(input.filler_density ?? fallback.filler_density)),
    pace: clamp(Number(input.pace ?? fallback.pace)),
    overall: clamp(Number(input.overall ?? fallback.overall)),
  };
}

function normaliseStoredScore(input: any): LlmScore {
  const stages = normaliseStages(input);
  const voice = normaliseVoiceScore(
    input?.voice ?? input?.rubric?._meta?.voice ?? null
  );

  return {
    model: clampText(input?.model, 120) || AI_MODEL,
    overall: clamp(Number(input?.overall ?? 0)),
    summary: clampText(input?.summary, 220) || "No summary available.",
    stages,
    moments: normaliseMoments(input?.moments),
    suggestions: normaliseSuggestions(input?.suggestions),
    voice,
  };
}

function parseAndValidateScoreResponse(raw: string): Omit<LlmScore, "voice"> {
  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("invalid_score_schema:json_parse_failed");
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("invalid_score_schema:root");
  }

  const overall = Number(parsed.overall);
  if (!Number.isFinite(overall)) {
    throw new Error("invalid_score_schema:overall");
  }

  const summary = clampText(parsed.summary, 220);
  if (!summary) {
    throw new Error("invalid_score_schema:summary");
  }

  return {
    model: clampText(parsed.model, 120) || AI_MODEL,
    overall: clamp(overall),
    summary,
    stages: normaliseStages(parsed.stages),
    moments: normaliseMoments(parsed.moments),
    suggestions: normaliseSuggestions(parsed.suggestions),
  };
}

function computeVoiceScore(transcript: string, durationSec?: number | null): VoiceScore {
  const text = String(transcript || "").trim();
  const words = text ? text.split(/\s+/).filter(Boolean) : [];
  const wordCount = words.length;

  if (!wordCount || !durationSec || durationSec <= 0) {
    return {
      clarity: 60,
      confidence: 55,
      filler_density: 0,
      pace: 50,
      overall: 55,
    };
  }

  const minutes = durationSec / 60;
  const wpm = minutes > 0 ? wordCount / minutes : 0;
  const fillerMatches =
    text.match(/\b(um|uh|erm|er|like|you know|sort of|kind of|basically|literally)\b/gi) || [];
  const fillerCount = fillerMatches.length;
  const fillerDensityRaw = wordCount > 0 ? (fillerCount / wordCount) * 100 : 0;

  const sentenceParts = text
    .split(/[.!?\n]+/)
    .map((x) => x.trim())
    .filter(Boolean);
  const sentenceCount = sentenceParts.length || 1;
  const avgWordsPerSentence = wordCount / sentenceCount;

  const pacePenalty = Math.min(45, Math.abs(wpm - 145) * 0.45);
  const pace = clamp(100 - pacePenalty);
  const clarity = clamp(100 - fillerDensityRaw * 4 - Math.max(0, avgWordsPerSentence - 26) * 1.5);
  const confidence = clamp(55 + pace * 0.25 + clarity * 0.2 - fillerDensityRaw * 2.5);
  const fillerDensity = clamp(100 - fillerDensityRaw * 8);
  const overall = clamp((clarity + confidence + fillerDensity + pace) / 4);

  return {
    clarity,
    confidence,
    filler_density: fillerDensity,
    pace,
    overall,
  };
}

function detectMomentsFromTranscript(
  transcript: string,
  segments?: Array<{ speaker?: string; start_sec?: number; end_sec?: number; text?: string }>
): CallMoment[] {
  const sourceSegments = Array.isArray(segments) && segments.length > 0
    ? segments
    : String(transcript || "")
        .split(/\n+/)
        .map((text, i) => ({
          speaker: i % 2 === 0 ? "Speaker 1" : "Speaker 2",
          start_sec: i * 4,
          end_sec: i * 4 + 4,
          text,
        }));

  const moments: CallMoment[] = [];

  for (const seg of sourceSegments) {
    const text = String(seg?.text || "").trim();
    if (!text) continue;

    const lower = text.toLowerCase();
    const timestamp =
      typeof seg?.start_sec === "number" ? seg.start_sec : undefined;

    if (
      lower.includes("not interested") ||
      lower.includes("we're okay") ||
      lower.includes("we are okay") ||
      lower.includes("no time") ||
      lower.includes("outside help")
    ) {
      moments.push({
        timestamp,
        type: "objection",
        text,
        severity: "high",
      });
      continue;
    }

    if (
      lower.includes("does that mean") ||
      lower.includes("why") ||
      lower.includes("how") ||
      lower.includes("what happens")
    ) {
      moments.push({
        timestamp,
        type: "highlight",
        text,
        severity: "medium",
      });
      continue;
    }

    if (
      lower.includes("call me back") ||
      lower.includes("couple weeks") ||
      lower.includes("send you an email") ||
      lower.includes("revisit this")
    ) {
      moments.push({
        timestamp,
        type: "closing_attempt",
        text,
        severity: "medium",
      });
      continue;
    }
  }

  // --- DEDUPE + CLEAN ---
  const seen = new Set<string>();

  const deduped = moments.filter((m) => {
    const key = `${m.type}-${m.text.toLowerCase().slice(0, 60)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Prioritise higher-severity moments first, then earlier timestamps.
  deduped.sort((a, b) => {
    const weight = { high: 3, medium: 2, low: 1 } as const;
    const severityDiff = (weight[b.severity || "low"] || 1) - (weight[a.severity || "low"] || 1);
    if (severityDiff !== 0) return severityDiff;

    const aTs = typeof a.timestamp === "number" ? a.timestamp : Number.MAX_SAFE_INTEGER;
    const bTs = typeof b.timestamp === "number" ? b.timestamp : Number.MAX_SAFE_INTEGER;
    return aTs - bTs;
  });

  return deduped.slice(0, 6);
}

function buildSuggestionsFromAnalysis(args: {
  overall: number;
  stages: CallAnalysisStages;
  moments: CallMoment[];
}): string[] {
  const suggestions: string[] = [];

  if (args.stages.discovery.score < 60) {
    suggestions.push("Ask 2 more discovery questions before pitching.");
  }

  if (args.stages.objection.score < 60) {
    suggestions.push("Slow down and isolate the real objection before rebutting.");
  }

  if (args.stages.close.score < 60) {
    suggestions.push("End with a clearer next step and a direct commitment ask.");
  }

  if (args.moments.some((m) => m.type === "objection")) {
    suggestions.push("Review the objection moments and rehearse a tighter value reframe.");
  }

  if (args.overall >= 70 && suggestions.length === 0) {
    suggestions.push("Keep the structure, but tighten follow-up questions to raise conversion odds.");
  }

  return suggestions.slice(0, 4);
}


async function updateCallScoreRow(
  supabase: SupabaseClient,
  callId: string,
  payload: Record<string, any>
) {
  const nextPayload = { ...payload };

  // Preserve existing structured transcript / analysis data when scoring writes analysis_json.
  if (nextPayload.analysis_json) {
    try {
      const { data: existingRow, error: existingErr } = await supabase
        .from("calls")
        .select("analysis_json")
        .eq("id", callId)
        .maybeSingle();

      if (!existingErr) {
        nextPayload.analysis_json = {
          ...(((existingRow as any)?.analysis_json as Record<string, any> | null) ?? {}),
          ...(nextPayload.analysis_json as Record<string, any>),
        };
      }
    } catch {
      // best effort only — do not block scoring if merge lookup fails
    }
  }

  let { error } = await supabase.from("calls").update(nextPayload).eq("id", callId);
  if (!error) return;

  const msg = String((error as any)?.message ?? "").toLowerCase();
  const missingVoiceCols =
    (msg.includes("voice_score") || msg.includes("voice_rubric")) &&
    (msg.includes("column") || msg.includes("schema cache") || msg.includes("could not find"));

  if (missingVoiceCols) {
    const retryPayload = { ...nextPayload };
    delete retryPayload.voice_score;
    delete retryPayload.voice_rubric;

    const retry = await supabase.from("calls").update(retryPayload).eq("id", callId);
    if (retry.error) throw retry.error;
    return;
  }

  throw error;
}

// --- Rep Memory Helper Types & Functions ---
type RepMemoryInput = {
  userId: string;
  companyId?: string | null;
  callId: string;
  overall: number;
  stages: CallAnalysisStages;
  voice?: VoiceScore | null;
  moments: CallMoment[];
};

function uniqueStrings(items: string[]): string[] {
  return Array.from(new Set(items.map((x) => String(x).trim()).filter(Boolean)));
}

function buildRepMemoryLabels(args: {
  overall: number;
  stages: CallAnalysisStages;
  moments: CallMoment[];
  voice?: VoiceScore | null;
}) {
  const strengths: string[] = [];
  const weaknesses: string[] = [];
  const coachingFocus: string[] = [];

  const { stages, moments, voice } = args;

  const stageEntries = [
    { name: "Intro", score: stages.intro.score },
    { name: "Discovery", score: stages.discovery.score },
    { name: "Objection handling", score: stages.objection.score },
    { name: "Close", score: stages.close.score },
  ].sort((a, b) => b.score - a.score);

  // Always keep the rep's strongest 1-2 stage areas, even if the overall call was weak.
  for (const s of stageEntries.slice(0, 2)) {
    if (s.score >= 40) strengths.push(s.name);
  }

  if ((voice?.clarity ?? 0) >= 70) strengths.push("Clarity");
  if ((voice?.confidence ?? 0) >= 70) strengths.push("Confidence");

  if (stages.intro.score < 45) weaknesses.push("Intro");
  if (stages.discovery.score < 60) weaknesses.push("Discovery");
  if (stages.objection.score < 60) weaknesses.push("Objection handling");
  if (stages.close.score < 60) weaknesses.push("Close");
  if ((voice?.clarity ?? 100) < 55) weaknesses.push("Clarity");
  if ((voice?.confidence ?? 100) < 55) weaknesses.push("Confidence");
  if ((voice?.pace ?? 100) < 50) weaknesses.push("Pace");

  if (stages.discovery.score < 60) {
    coachingFocus.push("Ask more discovery questions before pitching");
  }
  if (stages.objection.score < 60) {
    coachingFocus.push("Slow down and isolate objections before rebutting");
  }
  if (stages.close.score < 60) {
    coachingFocus.push("End with a clearer next step and direct commitment ask");
  }
  if (moments.some((m) => m.type === "objection")) {
    coachingFocus.push("Rehearse objection handling around the flagged pushback moments");
  }
  if ((voice?.confidence ?? 100) < 60) {
    coachingFocus.push("Improve vocal confidence and certainty in delivery");
  }

  return {
    strengths: uniqueStrings(strengths).slice(0, 5),
    weaknesses: uniqueStrings(weaknesses).slice(0, 5),
    coachingFocus: uniqueStrings(coachingFocus).slice(0, 5),
  };
}

async function upsertRepMemory(
  supabase: SupabaseClient,
  input: RepMemoryInput
) {
  const { userId, companyId, callId, overall, stages, voice, moments } = input;

  const { data: existing, error: existingErr } = await supabase
    .from("rep_memory")
    .select("*")
    .eq("user_id", userId)
    .maybeSingle();

  if (existingErr) throw existingErr;

  const labels = buildRepMemoryLabels({
    overall,
    stages,
    moments,
    voice,
  });

  const nextCallCount = Number((existing as any)?.call_count ?? 0) + 1;

  const rolling = (prev: any, next: number) => {
    const prevNum = Number(prev ?? 0);
    const countBefore = Math.max(0, nextCallCount - 1);
    if (countBefore === 0) return next;
    return Number((((prevNum * countBefore) + next) / nextCallCount).toFixed(2));
  };

  const avgScore = rolling((existing as any)?.avg_score, overall);
  const introScore = rolling((existing as any)?.intro_score, stages.intro.score);
  const discoveryScore = rolling((existing as any)?.discovery_score, stages.discovery.score);
  const objectionScore = rolling((existing as any)?.objection_score, stages.objection.score);
  const closeScore = rolling((existing as any)?.close_score, stages.close.score);

  const trendOverall =
    (existing as any)?.avg_score != null ? Number((overall - Number((existing as any).avg_score)).toFixed(2)) : 0;
  const trendObjection =
    (existing as any)?.objection_score != null ? Number((stages.objection.score - Number((existing as any).objection_score)).toFixed(2)) : 0;
  const trendClose =
    (existing as any)?.close_score != null ? Number((stages.close.score - Number((existing as any).close_score)).toFixed(2)) : 0;

  const payload = {
    user_id: userId,
    company_id: companyId ?? null,
    avg_score: avgScore,
    intro_score: introScore,
    discovery_score: discoveryScore,
    objection_score: objectionScore,
    close_score: closeScore,
    trend_overall: trendOverall,
    trend_objection: trendObjection,
    trend_close: trendClose,
    filler_word_rate: voice?.filler_density ?? null,
    talk_ratio: null,
    strengths: labels.strengths,
    weaknesses: labels.weaknesses,
    coaching_focus: labels.coachingFocus,
    call_count: nextCallCount,
    last_call_id: callId,
    last_updated: new Date().toISOString(),
  };

  if ((existing as any)?.id) {
    const { error: updateErr } = await supabase
      .from("rep_memory")
      .update(payload)
      .eq("id", (existing as any).id);

    if (updateErr) throw updateErr;
    return;
  }

  const { error: insertErr } = await supabase
    .from("rep_memory")
    .insert(payload);

  if (insertErr) throw insertErr;
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

  const voice = computeVoiceScore("", null);

  return {
    model: "heuristic:v1",
    overall: s.score,
    summary: "Solid overall structure, but a fuller transcript is needed for a reliable coaching summary.",
    stages: {
      intro: { ...s },
      discovery: { ...s },
      objection: { ...s },
      close: { ...s },
    },
    moments: [],
    suggestions: [],
    voice,
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
      overall: opts.scores.overall,
      sections: {
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
      .select("id, filename, user_id, duration_sec, sha256, transcript, analysis_json")
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
      const cachedScore = normaliseStoredScore(cached);
      const cachedModelVersion = String(cachedScore.model || SCORING_MODEL_VERSION);
      const voice = cachedScore.voice ?? computeVoiceScore(
        deterministic.transcript,
        (call as any)?.duration_sec ?? null
      );
      const transcriptSegments =
        (call as any)?.analysis_json?.transcript?.segments ?? [];

      const derivedMoments =
        cachedScore.moments?.length > 0
          ? cachedScore.moments
          : detectMomentsFromTranscript(
              deterministic.transcript,
              transcriptSegments
            );

      const derivedSuggestions =
        cachedScore.suggestions?.length > 0
          ? cachedScore.suggestions
          : buildSuggestionsFromAnalysis({
              overall: cachedScore.overall,
              stages: cachedScore.stages,
              moments: derivedMoments,
            });

      const rubric = buildRubricWithMeta({
        intro: cachedScore.stages.intro,
        discovery: cachedScore.stages.discovery,
        objection: cachedScore.stages.objection,
        close: cachedScore.stages.close,
        callSha256: ((call as any)?.sha256 as string | null) ?? null,
        transcriptHash: deterministic.transcriptHash,
        transcriptPresent: Boolean(deterministic.transcript),
        modelVersion: cachedModelVersion,
      });

      (rubric as any)._meta.voice = voice;

      await updateCallScoreRow(supabase, callId, {
        score_overall: cachedScore.overall,
        summary: cachedScore.summary,
        transcript: ((call as any)?.transcript as string | null) ?? null,
        voice_score: voice.overall,
        voice_rubric: voice,
        rubric,
        analysis_json: {
          overall: cachedScore.overall,
          stages: cachedScore.stages,
          moments: derivedMoments,
          suggestions: derivedSuggestions,
          summary: cachedScore.summary,
          voice,
        },
        ai_model: cachedModelVersion,
        rubric_version: RUBRIC_VERSION,
        scored_at: new Date().toISOString(),
      });

      await writeScoreHistory(supabase, callId, cachedModelVersion, cachedScore.overall, rubric);

      await notifySlack({
        supabase,
        callId,
        repIdFallback: (call as any)?.user_id ?? null,
        durationSec: (call as any)?.duration_sec ?? null,
        scores: {
          intro: cachedScore.stages.intro.score,
          discovery: cachedScore.stages.discovery.score,
          objection: cachedScore.stages.objection.score,
          close: cachedScore.stages.close.score,
          overall: cachedScore.overall,
        },
      });
      try {
        await upsertRepMemory(supabase, {
          userId: String((call as any)?.user_id),
          companyId: (call as any)?.org_id ?? null,
          callId,
          overall: cachedScore.overall,
          stages: cachedScore.stages,
          voice,
          moments: derivedMoments,
        });
      } catch (e: any) {
        console.warn("[rep_memory] cache-hit update failed", e?.message || e);
      }
      return {
        ...cachedScore,
        moments: derivedMoments,
        suggestions: derivedSuggestions,
        voice,
      };
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
      "Return a short coaching summary in 1-2 sentences (max 220 chars) that explains the main strength and main weakness of the call.",
      "Return stage scores under stages.intro, stages.discovery, stages.objection, and stages.close.",
      "Return moments as an array of notable call moments using timestamps when possible. Focus on objections, mistakes, highlights, and closing attempts.",
      "Return suggestions as an array of short actionable coaching suggestions based on the weakest stages and moments.",
      "For identical transcripts, hashes, rubric versions, and prompt versions, return identical stage scores, overall score, moments, suggestions, and materially consistent reasoning.",
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

    const validated = parseAndValidateScoreResponse(raw);
    const voice = computeVoiceScore(
      deterministic.transcript,
      (call as any)?.duration_sec ?? null
    );
    const parsed: LlmScore = {
      ...validated,
      model: AI_MODEL,
      voice,
    };

    const transcriptSegments =
      (call as any)?.analysis_json?.transcript?.segments ?? [];

    const derivedMoments = detectMomentsFromTranscript(
      deterministic.transcript,
      transcriptSegments
    );

    const derivedSuggestions =
      parsed.suggestions?.length > 0
        ? parsed.suggestions
        : buildSuggestionsFromAnalysis({
            overall: parsed.overall,
            stages: parsed.stages,
            moments: derivedMoments,
          });

    parsed.moments = derivedMoments;
    parsed.suggestions = derivedSuggestions;

    const rubric = buildRubricWithMeta({
      intro: parsed.stages.intro,
      discovery: parsed.stages.discovery,
      objection: parsed.stages.objection,
      close: parsed.stages.close,
      callSha256: ((call as any)?.sha256 as string | null) ?? null,
      transcriptHash: deterministic.transcriptHash,
      transcriptPresent: Boolean(deterministic.transcript),
      modelVersion: SCORING_MODEL_VERSION,
    });

    (rubric as any)._meta.voice = voice;

    // Persist latest on calls
    await updateCallScoreRow(supabase, callId, {
      score_overall: parsed.overall,
      summary: parsed.summary,
      transcript: ((call as any)?.transcript as string | null) ?? null,
      voice_score: voice.overall,
      voice_rubric: voice,
      rubric,
      analysis_json: {
        overall: parsed.overall,
        stages: parsed.stages,
        moments: parsed.moments,
        suggestions: parsed.suggestions,
        summary: parsed.summary,
        voice,
      },
      ai_model: SCORING_MODEL_VERSION,
      rubric_version: RUBRIC_VERSION,
      scored_at: new Date().toISOString(),
    });

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
        intro: parsed.stages.intro.score,
        discovery: parsed.stages.discovery.score,
        objection: parsed.stages.objection.score,
        close: parsed.stages.close.score,
        overall: parsed.overall,
      },
    });
    try {
      await upsertRepMemory(supabase, {
        userId: String((call as any)?.user_id),
        companyId: (call as any)?.org_id ?? null,
        callId,
        overall: parsed.overall,
        stages: parsed.stages,
        voice,
        moments: parsed.moments,
      });
    } catch (e: any) {
      console.warn("[rep_memory] success update failed", e?.message || e);
    }

    return parsed;
  } catch (err: any) {
    console.warn(
      "[scoreWithLLM] LLM failed, using heuristic:",
      err?.status ?? "",
      err?.code ?? "",
      err?.message ?? err
    );

    const fb = heuristicScoreFallback();

    const fallbackVoice = computeVoiceScore("", null);

    let transcriptSegments: any[] = [];
    let transcriptText = "";

    try {
      const { data: callRow } = await opts.supabase
        .from("calls")
        .select("transcript, analysis_json")
        .eq("id", opts.callId)
        .maybeSingle();

      transcriptText = String((callRow as any)?.transcript || "");
      transcriptSegments = (callRow as any)?.analysis_json?.transcript?.segments ?? [];
    } catch {}

    const fallbackMoments = detectMomentsFromTranscript(
      transcriptText,
      transcriptSegments
    );

    const fallbackSuggestions = buildSuggestionsFromAnalysis({
      overall: fb.overall,
      stages: fb.stages,
      moments: fallbackMoments,
    });

    const fallbackModelVersion = `${fb.model}:${SCORING_PROMPT_VERSION}:${RUBRIC_VERSION}`;
    const rubric = buildRubricWithMeta({
      intro: fb.stages.intro,
      discovery: fb.stages.discovery,
      objection: fb.stages.objection,
      close: fb.stages.close,
      callSha256: null,
      transcriptHash: null,
      transcriptPresent: false,
      modelVersion: fallbackModelVersion,
    });

    (rubric as any)._meta.voice = fallbackVoice;

    await updateCallScoreRow(opts.supabase, opts.callId, {
      score_overall: fb.overall,
      summary: fb.summary,
      voice_score: fallbackVoice.overall,
      voice_rubric: fallbackVoice,
      rubric,
      analysis_json: {
        overall: fb.overall,
        stages: fb.stages,
        moments: fallbackMoments,
        suggestions: fallbackSuggestions,
        summary: fb.summary,
        voice: fallbackVoice,
      },
      ai_model: fallbackModelVersion,
      rubric_version: RUBRIC_VERSION,
      scored_at: new Date().toISOString(),
    });

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
    } catch { }

    // Slack fallback
    await notifySlack({
      supabase: opts.supabase,
      callId: opts.callId,
      repIdFallback: null,
      durationSec,
      scores: {
        intro: fb.stages.intro.score,
        discovery: fb.stages.discovery.score,
        objection: fb.stages.objection.score,
        close: fb.stages.close.score,
        overall: fb.overall,
      },
    });
    try {
      const { data: memoryCall } = await opts.supabase
        .from("calls")
        .select("user_id, org_id")
        .eq("id", opts.callId)
        .maybeSingle();

      await upsertRepMemory(opts.supabase, {
        userId: String((memoryCall as any)?.user_id),
        companyId: (memoryCall as any)?.org_id ?? null,
        callId: opts.callId,
        overall: fb.overall,
        stages: fb.stages,
        voice: fallbackVoice,
        moments: fallbackMoments,
      });
    } catch (e: any) {
      console.warn("[rep_memory] fallback update failed", e?.message || e);
    }

    return {
      ...fb,
      moments: fallbackMoments,
      suggestions: fallbackSuggestions,
      voice: fallbackVoice,
    };
  }
}