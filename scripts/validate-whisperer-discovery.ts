/**
 * validate-whisperer-discovery.ts — Tier 2B Day 130
 * Deterministic assertions for read-only AI Trigger Discovery.
 * Usage: npx tsx scripts/validate-whisperer-discovery.ts
 */

import {
  discoverTriggerCandidates,
  classifyDiscoveryCandidate,
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

console.log(failures === 0 ? "\nWhisperer discovery validation PASSED" : `\nWhisperer discovery validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
