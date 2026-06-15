// src/whisperer/customTriggers.ts
// TIER 2B — Day 119: manager-defined custom Whisperer triggers.
//
// Pure matching against tenant trigger-library rows, kept separate from the
// built-in semantic engine in triggers.ts. No LLM. Merged with built-ins at
// segment time; phrase hits beat keyword hits, priority breaks ties.

import {
  type WhispererTrigger,
  type WhispererTriggerType,
  type RecentTrigger,
  normaliseTranscriptText,
} from "./triggers";

// A library row as stored (snake_case from the DB) — normalised on load.
export type CustomTriggerRow = {
  id: string;
  type?: string | null;
  name?: string | null;
  match_phrases?: string[] | null;
  match_keywords?: string[] | null;
  suggestion_title?: string | null;
  suggestion_response?: string | null;
  urgency?: string | null;
  emoji?: string | null;
  enabled?: boolean | null;
  priority?: number | null;
};

export type CustomTriggerRule = {
  id: string;
  type: WhispererTriggerType;
  phrases: string[];   // normalised
  keywords: string[];  // normalised
  suggestionTitle: string;
  suggestionResponse: string;
  urgency: "low" | "medium" | "high";
  emoji: "👂" | "💥" | null;
  priority: number;    // 0–100
};

const clamp = (n: number) => Math.max(0, Math.min(100, Math.round(n)));

function coerceUrgency(u: unknown): "low" | "medium" | "high" {
  const s = String(u || "").toLowerCase();
  return s === "low" || s === "high" ? s : "medium";
}

function coerceEmoji(e: unknown): "👂" | "💥" | null {
  return e === "👂" || e === "💥" ? e : null;
}

// Normalise a raw DB row into a usable rule (phrases/keywords lowercased,
// empties dropped). Returns null for unusable rows.
export function normaliseCustomTrigger(row: CustomTriggerRow): CustomTriggerRule | null {
  if (!row || row.enabled === false) return null;
  const phrases = (row.match_phrases || []).map((p) => normaliseTranscriptText(String(p))).filter(Boolean);
  const keywords = (row.match_keywords || []).map((k) => normaliseTranscriptText(String(k))).filter(Boolean);
  if (phrases.length === 0 && keywords.length === 0) return null;
  const title = String(row.suggestion_title || "").trim();
  const response = String(row.suggestion_response || "").trim();
  if (!title || !response) return null;

  return {
    id: String(row.id),
    type: (String(row.type || "custom") as WhispererTriggerType),
    phrases,
    keywords,
    suggestionTitle: title,
    suggestionResponse: response,
    urgency: coerceUrgency(row.urgency),
    emoji: coerceEmoji(row.emoji),
    priority: clamp(typeof row.priority === "number" ? row.priority : 50),
  };
}

export type DetectCustomInput = {
  text: string;
  speaker?: "rep" | "prospect" | "unknown";
  now?: Date;
  recentTriggers?: RecentTrigger[];
};

const DEDUPE_WINDOW_MS = 30_000;

function isDuplicate(type: string, recent: RecentTrigger[] | undefined, now: Date): boolean {
  if (!recent?.length) return false;
  return recent.some((r) => {
    if (String(r.type) !== type) return false;
    const at = new Date(r.detectedAt).getTime();
    return Number.isFinite(at) && now.getTime() - at < DEDUPE_WINDOW_MS;
  });
}

// Match custom rules against a segment. Phrase hit → 90; ≥2 keywords → 85;
// 1 keyword → 75. Carries customTriggerId + priority in meta for sorting.
export function detectCustomWhispererTriggers(
  input: DetectCustomInput,
  rules: CustomTriggerRule[]
): WhispererTrigger[] {
  const text = normaliseTranscriptText(input.text);
  if (!text || input.speaker === "rep" || !rules.length) return [];
  const now = input.now || new Date();

  const out: WhispererTrigger[] = [];
  for (const rule of rules) {
    let confidence = 0;
    let phrase: string | null = null;

    const phraseHit = rule.phrases.find((p) => text.includes(p));
    if (phraseHit) {
      confidence = 90;
      phrase = phraseHit;
    } else {
      const hits = rule.keywords.filter((k) => text.includes(k));
      if (hits.length >= 2) { confidence = 85; phrase = hits.join(", "); }
      else if (hits.length === 1) { confidence = 75; phrase = hits[0]; }
    }

    if (!confidence || !phrase) continue;
    if (isDuplicate(rule.type, input.recentTriggers, now)) continue;

    out.push({
      type: rule.type,
      phrase,
      confidence: clamp(confidence),
      suggestion: {
        title: rule.suggestionTitle,
        response: rule.suggestionResponse,
        urgency: rule.urgency,
        emoji: rule.emoji,
      },
      // Tag custom origin + priority so the merge can sort/trace it.
      meta: { customTriggerId: rule.id, priority: rule.priority, custom: true },
    });
  }
  return out;
}

// Merge built-in + custom triggers: confidence desc, then custom priority desc.
export function mergeBuiltInAndCustomTriggers(
  builtIns: WhispererTrigger[],
  customMatches: WhispererTrigger[]
): WhispererTrigger[] {
  const all = [...builtIns, ...customMatches];
  return all.sort((a, b) => {
    if (b.confidence !== a.confidence) return b.confidence - a.confidence;
    const ap = Number((a as any).meta?.priority ?? 0);
    const bp = Number((b as any).meta?.priority ?? 0);
    return bp - ap;
  });
}
