/**
 * validate-whisperer-discovery.ts — Tier 2B Day 130
 * Deterministic assertions for read-only AI Trigger Discovery.
 * Usage: npx tsx scripts/validate-whisperer-discovery.ts
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  discoverTriggerCandidates,
  blendTriggerCandidates,
  classifyDiscoveryCandidate,
  suppressKnownCandidates,
  type DiscoveryItem,
} from "../src/whisperer/discovery";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const item = (text: string, sessionId = "s1"): DiscoveryItem => ({
  text,
  sessionId,
  detectedAt: new Date().toISOString(),
});

// ── Classification ──
check("budget line classifies as price", classifyDiscoveryCandidate("that is outside our budget").type === "price");
check("boss line classifies as authority", classifyDiscoveryCandidate("I need to ask my boss first").type === "authority");
check("competitor line classifies as competitor", classifyDiscoveryCandidate("we already use a competitor").type === "competitor");
check("neutral line classifies as custom", classifyDiscoveryCandidate("good morning lovely weather").type === "custom");

// ── Repeated budget phrases → a price candidate ──
const priceCandidates = discoverTriggerCandidates({
  items: [
    item("that is outside our budget right now"),
    item("the cost is too high for us", "s2"),
    item("honestly the price is too high", "s3"),
  ],
});
check("repeated budget/cost phrases produce a price candidate", priceCandidates.some((c) => c.type === "price"));
const price = priceCandidates.find((c) => c.type === "price");
check("price candidate seenCount >= 2", (price?.seenCount ?? 0) >= 2);
check("price candidate has suggestedKeywords", (price?.suggestedKeywords?.length ?? 0) > 0);
check("price candidate status is candidate", price?.status === "candidate");
check("price candidate includes <=3 examples", (price?.examples?.length ?? 0) <= 3 && (price?.examples?.length ?? 0) >= 1);
check("price candidate has a suggestedResponse", Boolean(price?.suggestedResponse));

// ── Repeated boss/partner phrases → authority candidate ──
const authority = discoverTriggerCandidates({
  items: [item("I have to run it by my boss"), item("my partner needs to decide", "s2")],
}).find((c) => c.type === "authority");
check("repeated boss/partner phrases produce an authority candidate", Boolean(authority));

// ── Repeated competitor phrases → competitor candidate ──
const competitor = discoverTriggerCandidates({
  items: [item("we already use a competitor"), item("we have a provider already", "s2")],
}).find((c) => c.type === "competitor");
check("repeated competitor phrases produce a competitor candidate", Boolean(competitor));

// ── One-off does not appear (minSeenCount default 2) ──
const oneOff = discoverTriggerCandidates({
  items: [item("that is outside our budget"), item("I need to ask my boss", "s2")],
});
check("single one-off phrase per type does not appear", oneOff.length === 0);

// ── Sorting by seenCount desc, then confidence ──
const sorted = discoverTriggerCandidates({
  items: [
    item("that is outside our budget"),
    item("the cost is too high", "s2"),
    item("the price is too high", "s3"),
    item("I need to ask my boss"),
    item("run it by my partner", "s2"),
  ],
});
check("candidates sort by seenCount desc", sorted.length >= 2 && sorted[0].seenCount >= sorted[1].seenCount);

// ── Read-only shape: no activation/enabled flags leaking through ──
check("candidates carry no enabled/active flag", sorted.every((c) => !("enabled" in c) && !("active" in c)));

// ── Day 132: dedupe against existing custom triggers ──
const priceCands = discoverTriggerCandidates({
  items: [item("that is outside our budget"), item("the cost is too high", "s2"), item("the price is too high", "s3")],
});
const supByPhrase = suppressKnownCandidates(priceCands, [{ phrases: ["the price is too high"], keywords: [] }]);
check("phrase overlap suppresses a candidate", supByPhrase.suppressedCount >= 1 && supByPhrase.kept.length < priceCands.length);

const supByKeyword = suppressKnownCandidates(priceCands, [{ phrases: [], keywords: ["budget"] }]);
check("keyword overlap suppresses a candidate", supByKeyword.suppressedCount >= 1);

const supNone = suppressKnownCandidates(priceCands, [{ phrases: ["totally unrelated"], keywords: ["unrelated"] }]);
check("no overlap suppresses nothing", supNone.suppressedCount === 0 && supNone.kept.length === priceCands.length);

const supEmpty = suppressKnownCandidates(priceCands, []);
check("empty library suppresses nothing", supEmpty.suppressedCount === 0 && supEmpty.kept.length === priceCands.length);

// ── Day 144: raw-segment (blind-spot) provenance ──
const rawItem = (text: string, sessionId: string, segmentId: string): DiscoveryItem => ({
  text, sessionId, detectedAt: new Date().toISOString(), segmentId, source: "raw_segment", untriggered: true,
});

const rawCands = discoverTriggerCandidates({
  items: [
    rawItem("we need procurement to approve this", "s1", "seg-1"),
    rawItem("procurement needs to approve this first", "s2", "seg-2"),
    rawItem("our procurement team has to sign this off", "s3", "seg-3"),
  ],
});
const rawAuthority = rawCands.find((c) => c.type === "authority");
check("repeated untriggered raw inputs produce a candidate", Boolean(rawAuthority));
check("raw candidate has source raw_segment", rawAuthority?.source === "raw_segment");
check("raw candidate has untriggered true", rawAuthority?.untriggered === true);
check("raw candidate has exampleSegmentIds", (rawAuthority?.exampleSegmentIds?.length ?? 0) >= 1);
check("raw candidate sessionsCount >= 2", (rawAuthority?.sessionsCount ?? 0) >= 2);
check("raw candidate examples carry segmentId", (rawAuthority?.examples ?? []).every((e) => Boolean(e.segmentId)));

// Trigger-text source items remain trigger_segment / not untriggered (fallback path).
const trigCands = discoverTriggerCandidates({
  items: [
    { text: "the price is too high", sessionId: "s1", source: "trigger_segment", untriggered: false },
    { text: "the cost is too high for us", sessionId: "s2", source: "trigger_segment", untriggered: false },
  ],
});
const trigPrice = trigCands.find((c) => c.type === "price");
check("fallback trigger-text candidate still works", Boolean(trigPrice));
check("trigger candidate source is trigger_segment", trigPrice?.source === "trigger_segment");
check("trigger candidate untriggered is false", trigPrice?.untriggered === false);

// Mixed sources within one cluster → source "mixed".
const mixedCands = discoverTriggerCandidates({
  items: [
    { text: "that is outside our budget", sessionId: "s1", source: "raw_segment", untriggered: true },
    { text: "the price is too high", sessionId: "s2", source: "trigger_segment", untriggered: false },
    { text: "the cost is too high", sessionId: "s3", source: "raw_segment", untriggered: true },
  ],
});
check("mixed-source cluster reports source mixed", mixedCands.find((c) => c.type === "price")?.source === "mixed");

// ── Day 145: blended / ranked discovery ──
const rawAuthorityCands = discoverTriggerCandidates({
  items: [
    rawItem("we need procurement to approve this", "s1", "seg-1"),
    rawItem("procurement needs to approve this first", "s2", "seg-2"),
  ],
});
const trigPriceCands = discoverTriggerCandidates({
  items: [
    { text: "the price is too high", sessionId: "s5", source: "trigger_segment", untriggered: false },
    { text: "the cost is too high for us", sessionId: "s6", source: "trigger_segment", untriggered: false },
  ],
});

// Blend distinct patterns: both survive, raw blind spot ranks first.
const blendedDistinct = blendTriggerCandidates(rawAuthorityCands, trigPriceCands);
check("blend keeps raw candidate", blendedDistinct.some((c) => c.type === "authority" && c.source === "raw_segment"));
check("blend keeps trigger candidate", blendedDistinct.some((c) => c.type === "price" && c.source === "trigger_segment"));
check("raw blind spot ranks before trigger-only", blendedDistinct[0].untriggered === true);
check("trigger-only candidate untriggered false", Boolean(blendedDistinct.find((c) => c.source === "trigger_segment" && c.untriggered === false)));

// Same pattern in both sources (same type + dominant "budget" token → same id)
// → merges into a single "mixed" candidate.
const rawPriceCands = discoverTriggerCandidates({
  items: [rawItem("that is outside our budget", "s1", "seg-a"), rawItem("we don't have budget for this", "s2", "seg-b")],
});
const trigPriceForMix = discoverTriggerCandidates({
  items: [
    { text: "that is outside our budget right now", sessionId: "s3", source: "trigger_segment", untriggered: false },
    { text: "we don't have budget for this", sessionId: "s4", source: "trigger_segment", untriggered: false },
  ],
});
const blendedSame = blendTriggerCandidates(rawPriceCands, trigPriceForMix);
const mixedPrice = blendedSame.find((c) => c.type === "price");
check("same pattern in both → single price candidate", blendedSame.filter((c) => c.type === "price").length === 1);
check("merged candidate source is mixed", mixedPrice?.source === "mixed");
check("mixed candidate untriggered true (raw contributed)", mixedPrice?.untriggered === true);
check("mixed candidate seenCount is summed", (mixedPrice?.seenCount ?? 0) >= 4);

// Blend degenerates correctly: empty raw → trigger-only; empty trigger → raw-only.
check("blend(empty,trigger) = trigger-only", blendTriggerCandidates([], trigPriceCands).every((c) => c.source === "trigger_segment"));
check("blend(raw,empty) = raw-only", blendTriggerCandidates(rawAuthorityCands, []).every((c) => c.source === "raw_segment"));

// No LLM in the discovery helper.
const discSrc = readFileSync(join(__dirname, "..", "src", "whisperer", "discovery.ts"), "utf8");
check("discovery helper has no LLM call", !/openai|anthropic|chat\.completions|responses\.create/i.test(discSrc));

console.log(failures === 0 ? "\nWhisperer discovery validation PASSED" : `\nWhisperer discovery validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
