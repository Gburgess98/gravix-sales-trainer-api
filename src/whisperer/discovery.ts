// src/whisperer/discovery.ts
// TIER 2B — Day 130: AI Trigger Discovery (read-only foundation).
//
// Pure, deterministic candidate discovery over STORED Whisperer text. No I/O,
// no LLM, no live detection — this never touches the live hot path and never
// creates or enables triggers. It surfaces recurring sales moments so a manager
// can later approve them into the existing custom trigger library by hand.
//
// Data note (Day 144): discovery now mines raw `whisperer_segments` first — the
// untriggered final segments are the blind spots — and falls back to
// `whisperer_triggers` segment text when the raw table is missing/empty. This
// helper stays pure: the caller (manager route) does the I/O and tags each item
// with its source. A future LLM classifier could refine clustering — see the
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

// One stored unit of text to mine. Day 130: built from a whisperer_triggers row
// (trigger_segment). Day 144: also built from a raw whisperer_segments row
// (raw_segment), which carries a segmentId and whether it was untriggered.
export type DiscoverySource = "raw_segment" | "trigger_segment";

export type DiscoveryItem = {
  text: string;
  sessionId?: string | null;
  detectedAt?: string | null;
  segmentId?: string | null;     // Day 144 — raw whisperer_segments.id
  source?: DiscoverySource;      // Day 144 — defaults to trigger_segment
  untriggered?: boolean;         // Day 144 — true for raw blind-spot segments
};

export type DiscoveryExample = {
  text: string;
  sessionId: string | null;
  detectedAt: string | null;
  segmentId?: string | null;     // Day 144
  source?: DiscoverySource;      // Day 144
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
  // Day 144 — blind-spot provenance (optional; absent on legacy callers).
  source?: DiscoverySource | "mixed";
  untriggered?: boolean;
  exampleSegmentIds?: string[];
  sessionsCount?: number;
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
    segmentId: it.segmentId ?? null,
    source: it.source ?? "trigger_segment",
  }));

  const title = meta ? meta.title : "Recurring phrase";
  const label = meta ? meta.label : "Custom";

  // Day 144 — provenance. A cluster is single-source unless items mix sources.
  const sources = new Set(items.map((it) => it.source ?? "trigger_segment"));
  const source: DiscoverySource | "mixed" = sources.size === 1 ? ([...sources][0] as DiscoverySource) : "mixed";
  const untriggered = items.length > 0 && items.every((it) => it.untriggered === true);
  const exampleSegmentIds = items
    .map((it) => it.segmentId)
    .filter((x): x is string => Boolean(x))
    .slice(0, 10);
  const sessionsCount = new Set(items.map((it) => it.sessionId).filter(Boolean)).size;

  return {
    // Stable id (type + dominant token) so persistent decisions match across
    // discovery runs. One cluster per type per run keeps it unique.
    id: `candidate-${type}-${token}`,
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
      ? untriggered
        ? `Repeated ${token} language in recent sessions that fired no Whisperer trigger — a possible blind spot.`
        : `Repeated ${token} language across recent sessions.`
      : "Phrase recurs across recent sessions without a useful trigger.",
    status: "candidate",
    source,
    untriggered,
    exampleSegmentIds,
    sessionsCount,
  };
}

// ── Main entry ───────────────────────────────────────────────────────────────

// Day 132: an existing custom trigger rule, reduced to what dedupe needs.
export type ExistingTriggerRule = { phrases?: string[] | null; keywords?: string[] | null };

// Suppress candidates that already overlap an enabled custom trigger so a
// manager isn't re-shown something they've already actioned. Read-only, pure.
// A candidate is suppressed when any of its suggested phrases match (exact or
// substring, either direction) an existing match phrase, OR any suggested
// keyword exactly matches an existing match keyword (managers pick keywords
// deliberately, so an exact overlap is a real duplicate).
export function suppressKnownCandidates(
  candidates: TriggerCandidate[],
  existing: ExistingTriggerRule[]
): { kept: TriggerCandidate[]; suppressedCount: number } {
  if (!candidates.length || !existing.length) return { kept: candidates, suppressedCount: 0 };

  const existingPhrases = new Set<string>();
  const existingKeywords = new Set<string>();
  for (const e of existing) {
    for (const p of e.phrases ?? []) { const n = normaliseTranscriptText(String(p)); if (n) existingPhrases.add(n); }
    for (const k of e.keywords ?? []) { const n = normaliseTranscriptText(String(k)); if (n) existingKeywords.add(n); }
  }
  if (existingPhrases.size === 0 && existingKeywords.size === 0) return { kept: candidates, suppressedCount: 0 };

  const kept: TriggerCandidate[] = [];
  let suppressed = 0;
  for (const c of candidates) {
    const phraseHit = c.suggestedPhrases.some((p) => {
      const n = normaliseTranscriptText(p);
      if (!n) return false;
      if (existingPhrases.has(n)) return true;
      for (const ep of existingPhrases) if (n.includes(ep) || ep.includes(n)) return true;
      return false;
    });
    const keywordHit = c.suggestedKeywords.some((k) => existingKeywords.has(normaliseTranscriptText(k)));
    if (phraseHit || keywordHit) { suppressed += 1; continue; }
    kept.push(c);
  }
  return { kept, suppressedCount: suppressed };
}

// Day 145: blend raw blind-spot candidates with triggered-moment candidates so
// managers see both instead of raw-first hiding triggered patterns. Candidates
// share a stable id (`candidate-<type>-<token>`), so a pattern present in both
// sources merges into one "mixed" candidate. Ranking: untriggered blind spots
// first (raw + mixed-with-untriggered), then everything else, each group by
// seenCount desc then confidence desc. Pure — no I/O, no LLM.
export function blendTriggerCandidates(
  rawCandidates: TriggerCandidate[],
  triggerCandidates: TriggerCandidate[]
): TriggerCandidate[] {
  const byId = new Map<string, TriggerCandidate>();
  for (const c of rawCandidates) byId.set(c.id, { ...c });

  for (const t of triggerCandidates) {
    const existing = byId.get(t.id);
    if (!existing) { byId.set(t.id, { ...t }); continue; }
    // Same pattern in both sources → merge into a single "mixed" candidate.
    const examples = [...(existing.examples ?? []), ...(t.examples ?? [])].slice(0, 3);
    const exampleSegmentIds = [...new Set([...(existing.exampleSegmentIds ?? []), ...(t.exampleSegmentIds ?? [])])].slice(0, 10);
    byId.set(t.id, {
      ...existing,
      source: "mixed",
      untriggered: Boolean(existing.untriggered || t.untriggered),
      seenCount: existing.seenCount + t.seenCount,
      confidence: Math.max(existing.confidence, t.confidence),
      examples,
      exampleSegmentIds,
      sessionsCount: (existing.sessionsCount ?? 0) + (t.sessionsCount ?? 0),
      reason: "Recurs in both raw transcript segments (untriggered) and triggered Whisperer moments.",
    });
  }

  const merged = [...byId.values()];
  // Tier: untriggered blind spots first (covers raw + mixed-with-untriggered),
  // then mixed, then trigger-only.
  const tier = (c: TriggerCandidate) => (c.untriggered ? 0 : c.source === "mixed" ? 1 : 2);
  merged.sort((a, b) => (tier(a) - tier(b)) || (b.seenCount - a.seenCount) || (b.confidence - a.confidence));
  return merged;
}

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
