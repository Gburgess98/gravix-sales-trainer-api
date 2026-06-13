// src/whisperer/triggers.ts
// TIER 2B — Day 111: pure deterministic trigger engine for the Live Whisperer.
//
// Same discipline as src/sparring/: no I/O, no LLM, unit-scripted. Unifies the
// legacy /v1/whisperer/preview regexes with the Tier 2A objection keyword sets.
// Suggestions are rule templates (UK copy) — nothing sits on the hot path that
// could blow the ≤1.8s spoken-word→suggestion latency budget.

export type WhispererTriggerType =
  | "price"
  | "timing"
  | "authority"
  | "trust"
  | "competitor"
  | "send_info"
  | "custom";

export type WhispererSuggestion = {
  title: string;
  response: string;
  urgency: "low" | "medium" | "high";
  emoji: "👂" | "💥" | null;
};

export type WhispererTrigger = {
  type: WhispererTriggerType;
  phrase: string;
  confidence: number; // 0–100
  suggestion: WhispererSuggestion;
};

export type RecentTrigger = {
  type: string;
  phrase?: string | null;
  detectedAt: string | Date;
};

export type DetectInput = {
  sessionId: string;
  text: string;
  speaker?: "rep" | "prospect" | "unknown";
  now?: Date;
  recentTriggers?: RecentTrigger[];
};

const clamp = (n: number) => Math.max(0, Math.min(100, Math.round(n)));

// ── Phrase rules (exact phrases score higher than loose keywords) ───────────

type Rule = {
  type: WhispererTriggerType;
  phrases: string[]; // exact-substring phrases → confidence 90
  keywords: RegExp | null; // looser match → confidence 70
};

const RULES: Rule[] = [
  {
    type: "price",
    phrases: ["too expensive", "costs too much", "price is high", "cheaper elsewhere", "can't afford", "over budget"],
    keywords: /\b(expensive|pricey|cost|budget|discount)\b/i,
  },
  {
    type: "timing",
    phrases: ["think about it", "not right now", "call me later", "next month", "next quarter", "bad time"],
    keywords: /\b(later|timing|busy right now)\b/i,
  },
  {
    type: "send_info",
    phrases: ["send me info", "send information", "email me details", "send me an email", "send something over"],
    keywords: /\b(send|email) (me|us|over|through)\b/i,
  },
  {
    type: "authority",
    phrases: ["speak to my partner", "ask my boss", "talk to my wife", "talk to my husband", "check with my team", "not my decision"],
    keywords: /\b(decision maker|sign.?off|my (boss|manager|partner|director))\b/i,
  },
  {
    type: "trust",
    phrases: ["not sure about this", "need to trust", "sounds like a scam", "never heard of you", "is this legit"],
    keywords: /\b(reviews?|scam|proof|guarantee|references?)\b/i,
  },
  {
    type: "competitor",
    phrases: ["using someone else", "already have a provider", "with a competitor", "already use", "current supplier"],
    keywords: /\b(competitor|another (provider|vendor|company))\b/i,
  },
];

// ── Suggestion templates (UK copy, concise, call-usable) ────────────────────

const SUGGESTIONS: Record<WhispererTriggerType, WhispererSuggestion> = {
  price: {
    title: "Handle price objection",
    response:
      "Acknowledge it, then reframe on value: “Fair enough — if it paid for itself in 60 days, would the price still be the blocker?”",
    urgency: "high",
    emoji: "💥",
  },
  competitor: {
    title: "Handle competitor mention",
    response:
      "Don't knock them. Ask: “What would have to be true for switching to be worth it?” — then anchor your one differentiator.",
    urgency: "high",
    emoji: "💥",
  },
  trust: {
    title: "Build trust fast",
    response:
      "Offer proof, not promises: “Completely fair — can I send one customer result like yours, and we judge from there?”",
    urgency: "high",
    emoji: "💥",
  },
  timing: {
    title: "Handle the stall",
    response:
      "Respect it, lock a step: “No problem — let's pencil 10 minutes Thursday so it doesn't drift. Morning or afternoon?”",
    urgency: "medium",
    emoji: "👂",
  },
  authority: {
    title: "Bring in the decision maker",
    response:
      "Make it easy: “Makes sense — would it help if we got them on a short call together so nothing is lost in translation?”",
    urgency: "medium",
    emoji: "👂",
  },
  send_info: {
    title: "Don't die by email",
    response:
      "Agree, then qualify: “Happy to — so I send the right thing, what's the one question the info must answer for you?”",
    urgency: "medium",
    emoji: "👂",
  },
  custom: {
    title: "Custom trigger",
    response: "Use your team's playbook response for this moment.",
    urgency: "low",
    emoji: null,
  },
};

export function suggestionFor(type: WhispererTriggerType): WhispererSuggestion {
  return SUGGESTIONS[type] || SUGGESTIONS.custom;
}

// ── De-duplication ───────────────────────────────────────────────────────────

const DEDUPE_WINDOW_MS = 30_000;

function isDuplicate(
  type: WhispererTriggerType,
  phrase: string,
  recent: RecentTrigger[] | undefined,
  now: Date
): boolean {
  if (!recent?.length) return false;
  return recent.some((r) => {
    if (String(r.type) !== type) return false;
    const at = new Date(r.detectedAt).getTime();
    if (!Number.isFinite(at)) return false;
    return now.getTime() - at < DEDUPE_WINDOW_MS;
  });
}

// ── Main entry ───────────────────────────────────────────────────────────────

export function detectWhispererTriggers(input: DetectInput): WhispererTrigger[] {
  const text = String(input.text || "").toLowerCase().trim();
  if (!text) return [];

  // Rep's own words don't fire prospect-objection triggers
  if (input.speaker === "rep") return [];

  const now = input.now || new Date();
  const out: WhispererTrigger[] = [];

  for (const rule of RULES) {
    let phrase: string | null = null;
    let confidence = 0;

    const exact = rule.phrases.find((p) => text.includes(p));
    if (exact) {
      phrase = exact;
      confidence = 90;
    } else if (rule.keywords) {
      const m = text.match(rule.keywords);
      if (m) {
        phrase = m[0].toLowerCase();
        confidence = 70;
      }
    }

    if (!phrase) continue;
    if (isDuplicate(rule.type, phrase, input.recentTriggers, now)) continue;

    out.push({
      type: rule.type,
      phrase,
      confidence: clamp(confidence),
      suggestion: suggestionFor(rule.type),
    });
  }

  return out;
}
