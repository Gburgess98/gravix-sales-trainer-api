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

// Below this, a match is too weak to surface a suggestion.
export const TRIGGER_THRESHOLD = 70;

export type IntentScore = { score: number; phrase: string | null };

// Normalise a raw transcript segment for meaning-based matching: lowercase,
// unify smart apostrophes/quotes, strip punctuation to spaces (keeping
// apostrophes so contractions survive), collapse whitespace.
export function normaliseTranscriptText(text: string): string {
  return String(text || "")
    .toLowerCase()
    .replace(/[‘’ʼ]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/[^a-z0-9'\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// ── Semantic intent definitions ─────────────────────────────────────────────
// Each type detects BUYER INTENT, not exact wording:
//   exact phrase            → 90
//   strong semantic combo   → 80–89
//   weaker semantic signal  → 65–79
// `patterns` are tried in order and the highest-scoring match wins.

type IntentPattern = { re: RegExp; score: number };
type IntentDef = { exact: string[]; patterns: IntentPattern[] };

const INTENTS: Record<Exclude<WhispererTriggerType, "custom">, IntentDef> = {
  price: {
    exact: ["too expensive", "the price is too high", "price is too high", "too pricey", "can't afford", "cannot afford"],
    patterns: [
      { re: /\b(price|cost|costs|pricing|quote|rate|fee)\b.*\b(too high|high|too much|much|steep|up there)\b/, score: 86 },
      { re: /\b(too high|too much|steep)\b.*\b(price|cost|costs|pricing|quote)\b/, score: 86 },
      { re: /\b(a bit |too |quite |rather )?steep\b/, score: 85 },
      { re: /\bexpensive\b/, score: 84 },
      { re: /\bpricey\b/, score: 84 },
      { re: /\b(out(side)?|over) (of )?(our )?budget\b/, score: 84 },
      { re: /\b(don'?t|do not|haven'?t|no|without) .*\bbudget\b/, score: 82 },
      { re: /\bafford\b/, score: 82 },
      { re: /\b(too )?(costly|dear)\b/, score: 82 },
      { re: /\bjustify\b.*\b(cost|price|spend|expense)\b/, score: 82 },
      { re: /\b(cost|costs|price|prices|spend)\b.*\b(tight|adding up|stretch)\b/, score: 80 },
      { re: /\bbudget\b.*\b(tight|stretched|limited|gone|blown)\b/, score: 80 },
      { re: /\bcheaper\b/, score: 80 },
      { re: /\bmore than (we |i )?(expected|thought|planned|budgeted)\b/, score: 80 },
    ],
  },
  timing: {
    exact: ["not right now", "maybe later", "bad timing", "bad time"],
    patterns: [
      { re: /\bthink about it\b/, score: 88 },
      { re: /\bnot (the )?right (time|moment)\b/, score: 86 },
      { re: /\b(not ready|aren'?t ready|we'?re not ready|isn'?t ready)\b/, score: 84 },
      { re: /\bnot (right )?now\b/, score: 82 },
      { re: /\b(circle back|touch base|follow up|reach out|come back)\b.*\b(next|later|month|quarter|week|year)\b/, score: 82 },
      { re: /\b(hold off|on hold|park (it|this)|put .* on hold|push (it|this) back)\b/, score: 80 },
      { re: /\b(revisit|reconsider|look at this)\b.*\b(later|next|month|quarter)\b/, score: 80 },
      { re: /\b(later|next (month|quarter|week|year)|down the (line|road))\b/, score: 76 },
      { re: /\bbusy (right )?now\b/, score: 74 },
    ],
  },
  authority: {
    exact: ["not my decision", "don't make the final decision", "not up to me"],
    patterns: [
      { re: /\b(ask|check|speak|talk|run (it|this) by|clear (it|this) with|loop in|check with)\b.*\b(my |the |our )?(boss|manager|partner|wife|husband|team|director|board|cfo|ceo|spouse|colleague|other half|higher ups?)\b/, score: 86 },
      { re: /\b(my |the |our )(boss|manager|partner|wife|husband|team|director|board|cfo|ceo) .*\b(decide|decides|approve|approves|sign off|needs to|has to|wants? to)\b/, score: 86 },
      { re: /\b(don'?t|do not|not the one to) make (the )?(final )?(decision|call)\b/, score: 86 },
      { re: /\bneeds? (sign.?off|approval|to (approve|decide|sign off))\b/, score: 82 },
      { re: /\b(decision maker|sign.?off|approval)\b/, score: 80 },
      { re: /\bnot (really )?my (call|decision|department)\b/, score: 82 },
    ],
  },
  trust: {
    exact: ["is this legit", "sounds like a scam", "don't trust", "do not trust"],
    patterns: [
      { re: /\bhow (do|can|would) (i|we) (know|trust|be sure)\b/, score: 84 },
      { re: /\b(don'?t|do not|not sure (i|we)) trust\b/, score: 84 },
      { re: /\b(reviews?|testimonials?|case stud(y|ies)|references?)\b/, score: 82 },
      { re: /\b(sounds?|seems?|feels?|looks?) (risky|dodgy|too good|sketchy|suspicious)\b/, score: 82 },
      { re: /\b(not sure|unsure|sceptical|skeptical)\b.*\b(trust|work|works|legit|real|you)\b/, score: 80 },
      { re: /\b(scam|legit|legitimate|trustworthy)\b/, score: 80 },
      { re: /\brisky\b/, score: 76 },
      { re: /\b(proof|guarantee|evidence)\b/, score: 74 },
    ],
  },
  competitor: {
    exact: ["we use a competitor", "already have a provider", "happy with our current"],
    patterns: [
      { re: /\b(already|currently) (use|using|have|got|work(ing)? with|on)\b/, score: 84 },
      { re: /\b(another|different|other|existing|current) (provider|vendor|supplier|company|tool|platform|solution|system|setup)\b/, score: 84 },
      { re: /\b(we'?re|we are|i'?m) (with|using) (another|a different|someone)\b/, score: 84 },
      { re: /\balready (have|got) (this|that|it|everything) (covered|sorted|handled|in place)\b/, score: 84 },
      { re: /\bcompetitor\b/, score: 82 },
      { re: /\b(have|got) (a |someone |another )?(provider|supplier|vendor) (already|in place)?\b/, score: 80 },
      { re: /\bhappy with (our|what we|the)\b/, score: 78 },
    ],
  },
  send_info: {
    exact: ["send me details", "email me something", "put it in an email", "send me info"],
    patterns: [
      { re: /\b(send|email|forward|share|ping)\b.*\b(info|information|details?|brochure|deck|pdf|one.?pager|spec|material|stuff|something)\b/, score: 84 },
      { re: /\b(put|drop) (it|this|that) in an email\b/, score: 84 },
      { re: /\b(can you|could you|would you|just) (send|email|forward|share)\b/, score: 82 },
      { re: /\bemail me\b/, score: 82 },
      { re: /\bsend (it |that |me |us )?(over|across|through)\b/, score: 80 },
      { re: /\b(brochure|one.?pager|spec sheet|some literature)\b/, score: 78 },
    ],
  },
};

// Score one intent type against already-normalised text.
function detectIntent(type: Exclude<WhispererTriggerType, "custom">, text: string): IntentScore {
  const def = INTENTS[type];
  const exactHit = def.exact.find((p) => text.includes(p));
  if (exactHit) return { score: 90, phrase: exactHit };

  let best: IntentScore = { score: 0, phrase: null };
  for (const { re, score } of def.patterns) {
    const m = text.match(re);
    if (m && score > best.score) best = { score, phrase: m[0].trim() };
  }
  return best;
}

// Public per-type detectors (text is normalised internally so they're usable standalone)
export const detectPriceIntent = (text: string): IntentScore => detectIntent("price", normaliseTranscriptText(text));
export const detectTimingIntent = (text: string): IntentScore => detectIntent("timing", normaliseTranscriptText(text));
export const detectAuthorityIntent = (text: string): IntentScore => detectIntent("authority", normaliseTranscriptText(text));
export const detectTrustIntent = (text: string): IntentScore => detectIntent("trust", normaliseTranscriptText(text));
export const detectCompetitorIntent = (text: string): IntentScore => detectIntent("competitor", normaliseTranscriptText(text));
export const detectSendInfoIntent = (text: string): IntentScore => detectIntent("send_info", normaliseTranscriptText(text));

export function scoreTriggerIntent(text: string, type: Exclude<WhispererTriggerType, "custom">): number {
  return detectIntent(type, normaliseTranscriptText(text)).score;
}

const DETECT_ORDER: Array<Exclude<WhispererTriggerType, "custom">> = [
  "price", "competitor", "trust", "authority", "timing", "send_info",
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
  const text = normaliseTranscriptText(input.text);
  if (!text) return [];

  // Rep's own words don't fire prospect-objection triggers
  if (input.speaker === "rep") return [];

  const now = input.now || new Date();

  // Score every intent semantically, keep those above threshold.
  const candidates: WhispererTrigger[] = [];
  for (const type of DETECT_ORDER) {
    const { score, phrase } = detectIntent(type, text);
    if (score < TRIGGER_THRESHOLD || !phrase) continue;
    if (isDuplicate(type, phrase, input.recentTriggers, now)) continue;
    candidates.push({
      type,
      phrase,
      confidence: clamp(score),
      suggestion: suggestionFor(type),
    });
  }

  // Highest-confidence trigger first; separate types may co-occur.
  candidates.sort((a, b) => b.confidence - a.confidence);
  return candidates;
}
