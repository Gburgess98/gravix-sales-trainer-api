// src/whisperer/discovery.ts
// TIER 2B — Day 130: AI Trigger Discovery (read-only foundation).
//
// Pure, deterministic candidate discovery over STORED Whisperer text. No I/O,
// no LLM, no live detection — this never touches the live hot path and never
// creates or enables triggers. It surfaces recurring sales moments so a manager
// can later approve them into the existing custom trigger library by hand.
//
// Data note (Day 130): only trigger segment text is persisted today (a raw
// transcript table does not exist), so discovery mines `whisperer_triggers`
// segment text. A future LLM classifier could refine clustering — see the
// placeholder in classifyDiscoveryCandidate — but no LLM is wired today.

import {
  type WhispererTriggerType,
  normaliseTranscriptText,
  suggestionFor,
  detectPriceIntent,
  detectTimingIntent,
  detectAuthorityIntent,
  detectTrustIntent,
  detectCompetitorIntent,
  detectSendInfoIntent,
} from "./triggers";

const clamp = (n: number) => Math.max(0, Math.min(100, Math.round(n)));

// Types a candidate can be classified into (mirrors the live engine + custom).
export type DiscoveryCandidateType = WhispererTriggerType; // includes "custom"

// One stored unit of text to mine (built from a whisperer_triggers row).
export type DiscoveryItem = {
  text: string;
  sessionId?: string | null;
  detectedAt?: string | null;
};

export type DiscoveryExample = {
  text: string;
  sessionId: string | null;
  detectedAt: string | null;
};

export type TriggerCandidate = {
  id: string;
  type: DiscoveryCandidateType;
  title: string;
  suggestedName: string;
  suggestedDescription: string;
  suggestedPhrases: string[];
  suggestedKeywords: string[];
  suggestedResponse: string;
  suggestedUrgency: "low" | "medium" | "high";
  confidence: number; // 0–100
  seenCount: number;
  examples: DiscoveryExample[];
  reason: string;
  status: "candidate";
};

export type DiscoverInput = {
  items: DiscoveryItem[];
  minSeenCount?: number; // default 2 — one-offs are ignored
  limit?: number; // default 10
};

// ── Type metadata (discovery-flavoured labels/keywords) ──────────────────────
// Response/urgency are reused from the live suggestion templates (suggestionFor)
// so a manager sees the same coaching wording they'd get live.
type TypeMeta = { label: string; title: string; description: string; keywords: string[] };

const TYPE_META: Record<Exclude<DiscoveryCandidateType, "custom">, TypeMeta> = {
  price: {
    label: "Price",
    title: "Budget concern",
    description: "Prospects repeatedly mention budget/cost concerns.",
    keywords: ["budget", "cost", "price", "expensive", "afford", "cheaper", "pricey"],
  },
  timing: {
    label: "Timing",
    title: "Timing stall",
    description: "Prospects repeatedly defer or ask for more time.",
    keywords: ["later", "next month", "next quarter", "not ready", "think about it", "busy"],
  },
  authority: {
    label: "Authority",
    title: "Decision-maker blocker",
    description: "Prospects repeatedly defer to a boss, partner or board.",
    keywords: ["boss", "manager", "partner", "team", "board", "approval", "sign off"],
  },
  trust: {
    label: "Trust",
    title: "Trust / proof needed",
    description: "Prospects repeatedly ask for proof, reviews or reassurance.",
    keywords: ["trust", "reviews", "proof", "guarantee", "legit", "risky", "references"],
  },
  competitor: {
    label: "Competitor",
    title: "Existing provider",
    description: "Prospects repeatedly mention a competitor or current provider.",
    keywords: ["competitor", "provider", "supplier", "vendor", "already use", "current"],
  },
  send_info: {
    label: "Send info",
    title: "Send-me-info brush-off",
    description: "Prospects repeatedly ask to be sent information instead of engaging.",
    keywords: ["send", "email", "info", "details", "brochure", "deck"],
  },
};

const DISCOVERY_THRESHOLD = 65; // below this a line is too weak to type-classify

// ── Public helpers ───────────────────────────────────────────────────────────

export function normaliseDiscoveryText(text: string): string {
  return normaliseTranscriptText(text);
}

type Classification = { type: DiscoveryCandidateType; score: number; phrase: string | null };

// Deterministically classify one line by reusing the live semantic detectors,
// keeping the highest-scoring intent. Returns "custom" when nothing is strong
// enough. (Future: an offline LLM classifier could replace this — not today.)
export function classifyDiscoveryCandidate(text: string): Classification {
  const detectors: Array<[Exclude<DiscoveryCandidateType, "custom">, (t: string) => { score: number; phrase: string | null }]> = [
    ["price", detectPriceIntent],
    ["competitor", detectCompetitorIntent],
    ["trust", detectTrustIntent],
    ["authority", detectAuthorityIntent],
    ["timing", detectTimingIntent],
    ["send_info", detectSendInfoIntent],
  ];

  let best: Classification = { type: "custom", score: 0, phrase: null };
  for (const [type, detect] of detectors) {
    const { score, phrase } = detect(text);
    if (score > best.score) best = { type, score, phrase };
  }
  if (best.score < DISCOVERY_THRESHOLD) return { type: "custom", score: best.score, phrase: null };
  return best;
}

type Cluster = {
  type: DiscoveryCandidateType;
  token: string; // strongest keyword token, for naming/ids
  items: Array<DiscoveryItem & { classification: Classification }>;
};

// Group classified items by type; within a type pick the strongest (most
// frequent) keyword token for labelling. Items NOT grouped on the token so that
// a recurring type (e.g. price) clusters together even with varied wording.
export function groupSimilarPhrases(
  items: Array<DiscoveryItem & { classification: Classification }>
): Cluster[] {
  const byType = new Map<DiscoveryCandidateType, Array<DiscoveryItem & { classification: Classification }>>();
  for (const it of items) {
    const arr = byType.get(it.classification.type) ?? [];
    arr.push(it);
    byType.set(it.classification.type, arr);
  }

  const clusters: Cluster[] = [];
  for (const [type, group] of byType) {
    const meta = type === "custom" ? null : TYPE_META[type];
    let token = "general";
    if (meta) {
      const counts = new Map<string, number>();
      for (const it of group) {
        const norm = normaliseTranscriptText(it.text);
        for (const kw of meta.keywords) {
          if (norm.includes(kw)) counts.set(kw, (counts.get(kw) ?? 0) + 1);
        }
      }
      let bestN = -1;
      for (const [kw, n] of counts) if (n > bestN) { bestN = n; token = kw; }
    }
    clusters.push({ type, token, items: group });
  }
  return clusters;
}

let _candidateSeq = 0;
function nextSeq(): string {
  _candidateSeq = (_candidateSeq % 999) + 1;
  return String(_candidateSeq).padStart(3, "0");
}

export function buildTriggerCandidateFromCluster(cluster: Cluster): TriggerCandidate {
  const { type, token, items } = cluster;
  const meta = type === "custom" ? null : TYPE_META[type];
  const suggestion = suggestionFor(type);

  // Distinct matched phrases (up to 5) and keywords present across the cluster.
  const phraseSet = new Set<string>();
  for (const it of items) {
    const p = it.classification.phrase;
    if (p) phraseSet.add(p);
  }
  const suggestedPhrases = [...phraseSet].slice(0, 5);

  const keywordSet = new Set<string>();
  if (meta) {
    for (const it of items) {
      const norm = normaliseTranscriptText(it.text);
      for (const kw of meta.keywords) if (norm.includes(kw)) keywordSet.add(kw);
    }
  }
  const suggestedKeywords = [...keywordSet].slice(0, 6);

  const avgScore = items.reduce((s, it) => s + it.classification.score, 0) / Math.max(1, items.length);
  const confidence = clamp(avgScore);

  const examples: DiscoveryExample[] = items.slice(0, 3).map((it) => ({
    text: it.text.slice(0, 280),
    sessionId: it.sessionId ?? null,
    detectedAt: it.detectedAt ?? null,
  }));

  const title = meta ? meta.title : "Recurring phrase";
  const label = meta ? meta.label : "Custom";

  return {
    id: `candidate-${type}-${token}-${nextSeq()}`,
    type,
    title,
    suggestedName: `${label}: ${title}`,
    suggestedDescription: meta ? meta.description : "A phrase recurs across recent sessions with no dedicated trigger.",
    suggestedPhrases,
    suggestedKeywords,
    suggestedResponse: suggestion.response,
    suggestedUrgency: suggestion.urgency,
    confidence,
    seenCount: items.length,
    examples,
    reason: meta
      ? `Repeated ${token} language across recent sessions.`
      : "Phrase recurs across recent sessions without a useful trigger.",
    status: "candidate",
  };
}

// ── Main entry ───────────────────────────────────────────────────────────────

export function discoverTriggerCandidates(input: DiscoverInput): TriggerCandidate[] {
  const minSeen = Math.max(1, input.minSeenCount ?? 2);
  const limit = Math.max(1, input.limit ?? 10);

  const classified = (input.items ?? [])
    .map((it) => ({ ...it, classification: classifyDiscoveryCandidate(it.text) }))
    .filter((it) => normaliseTranscriptText(it.text).length > 0);

  const clusters = groupSimilarPhrases(classified).filter((c) => c.items.length >= minSeen);

  const candidates = clusters.map(buildTriggerCandidateFromCluster);

  // Strongest patterns first: most-seen, then most-confident.
  candidates.sort((a, b) => (b.seenCount - a.seenCount) || (b.confidence - a.confidence));

  return candidates.slice(0, limit);
}
