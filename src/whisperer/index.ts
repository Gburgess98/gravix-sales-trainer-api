// src/whisperer/index.ts — Live Whisperer public surface (Tier 2B).
export {
  type WhispererTrigger,
  type WhispererTriggerType,
  type WhispererSuggestion,
  type DetectInput,
  type RecentTrigger,
  detectWhispererTriggers,
  suggestionFor,
  normaliseTranscriptText,
  scoreTriggerIntent,
  detectPriceIntent,
  detectTimingIntent,
  detectAuthorityIntent,
  detectTrustIntent,
  detectCompetitorIntent,
  detectSendInfoIntent,
  TRIGGER_THRESHOLD,
} from "./triggers";

export { whispererTablesAvailable, resetWhispererSchemaProbe } from "./schema";

export {
  type CustomTriggerRow,
  type CustomTriggerRule,
  normaliseCustomTrigger,
  detectCustomWhispererTriggers,
  mergeBuiltInAndCustomTriggers,
} from "./customTriggers";

// Day 130: read-only AI Trigger Discovery (offline candidate mining).
export {
  type DiscoveryItem,
  type DiscoveryExample,
  type TriggerCandidate,
  type DiscoverInput,
  type DiscoveryCandidateType,
  normaliseDiscoveryText,
  classifyDiscoveryCandidate,
  groupSimilarPhrases,
  buildTriggerCandidateFromCluster,
  discoverTriggerCandidates,
} from "./discovery";
