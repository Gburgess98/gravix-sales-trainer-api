/**
 * validate-whisperer-triggers.ts — Tier 2B Day 111
 * Deterministic assertions for the Live Whisperer trigger engine.
 * Usage: npx tsx scripts/validate-whisperer-triggers.ts
 */

import { detectWhispererTriggers, suggestionFor } from "../src/whisperer/triggers";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const detect = (text: string, extra: Partial<Parameters<typeof detectWhispererTriggers>[0]> = {}) =>
  detectWhispererTriggers({ sessionId: "s1", text, speaker: "prospect", ...extra });
const topType = (text: string) => detect(text)[0]?.type;

// ── Day 117: semantic (meaning-based) detection — NOT exact phrases ──
check("'the price is too high' → price", topType("the price is too high") === "price");
check("'that's a bit steep' → price", topType("that's a bit steep") === "price");
check("'we can't afford that' → price", topType("we can't afford that") === "price");
check("'that's outside our budget' → price", topType("that's outside our budget") === "price");
check("'we don't have budget' → price", topType("we don't have budget for this") === "price");
check("'hard to justify the cost' → price", topType("it's hard to justify the cost") === "price");
check("'that's more than expected' → price", topType("that's more than we expected") === "price");
check("'we need something cheaper' → price", topType("we need something cheaper") === "price");
check("'costs are tight right now' → price", topType("costs are tight right now") === "price");
check("'we're not ready yet' → timing", topType("we're not ready yet") === "timing");
check("'circle back next month' → timing", topType("let's circle back next month") === "timing");
check("'I need to ask my boss' → authority", topType("I need to ask my boss") === "authority");
check("'my wife needs to decide' → authority", topType("my wife needs to decide on this") === "authority");
check("'do you have reviews' → trust", topType("do you have reviews") === "trust");
check("'how do I know this works' → trust", topType("how do I know this works") === "trust");
check("'we already use someone' → competitor", topType("we already use someone for that") === "competitor");
check("'we have a provider' → competitor", topType("we have a provider already") === "competitor");
check("'send over the details' → send_info", topType("can you send over the details") === "send_info");
check("'email me something' → send_info", topType("just email me something") === "send_info");
check("price phrase from rep → no trigger", detect("the price is too high", { speaker: "rep" }).length === 0);
check("neutral 'high quality product' → no price trigger", !detect("this is a high quality product").some((t) => t.type === "price"));
check("unrelated text → no trigger", detect("good morning, lovely weather today").length === 0);

// ── Type detection ──
check("'too expensive' → price", detect("this is too expensive")[0]?.type === "price");
check("'think about it' → timing", detect("let me think about it")[0]?.type === "timing");
check("'send me info' → send_info", detect("just send me info")[0]?.type === "send_info");
check("'ask my boss' → authority", detect("I need to ask my boss")[0]?.type === "authority");
check("'sounds like a scam' → trust", detect("this sounds like a scam")[0]?.type === "trust");
check("'already have a provider' → competitor", detect("we already have a provider")[0]?.type === "competitor");
check("neutral text → no triggers", detect("good morning, how are you").length === 0);

// ── Confidence tiers (semantic) ──
check("exact phrase → confidence 90", detect("too expensive")[0]?.confidence === 90);
check("strong semantic combo → 80–89", (() => { const c = detect("that's quite pricey")[0]?.confidence ?? 0; return c >= 80 && c < 90; })());
check("semantic matches clear threshold (>=70)", detect("the price is too high")[0]?.confidence >= 70);

// ── Speaker rule ──
check("rep speech never triggers", detect("too expensive", { speaker: "rep" }).length === 0);

// ── Multi-trigger ──
const multi = detect("it's too expensive and we already have a provider");
check("multiple triggers in one segment", multi.length === 2 && multi.map(t => t.type).sort().join() === "competitor,price");

// ── De-dup window ──
const now = new Date();
const recent = [{ type: "price", phrase: "too expensive", detectedAt: new Date(now.getTime() - 10_000).toISOString() }];
check("duplicate type within 30s suppressed", detect("still too expensive", { now, recentTriggers: recent }).length === 0);
const old = [{ type: "price", phrase: "too expensive", detectedAt: new Date(now.getTime() - 40_000).toISOString() }];
check("same type after 30s fires again", detect("still too expensive", { now, recentTriggers: old }).length === 1);

// ── Suggestions ──
const all = ["price", "timing", "authority", "trust", "competitor", "send_info"] as const;
check("every type has a suggestion with title+response", all.every((t) => {
  const s = suggestionFor(t);
  return s.title.length > 0 && s.response.length > 0;
}));
check("price/competitor/trust are high urgency", (["price", "competitor", "trust"] as const).every(t => suggestionFor(t).urgency === "high"));
check("authority/timing/send_info are medium urgency", (["authority", "timing", "send_info"] as const).every(t => suggestionFor(t).urgency === "medium"));
check("confidence clamped 0–100", multi.every(t => t.confidence >= 0 && t.confidence <= 100));

// ── Determinism ──
const a = detect("this is too expensive");
const b = detect("this is too expensive");
check("repeated input gives identical output", JSON.stringify(a) === JSON.stringify(b));

// ── Day 119: custom trigger library matching (DB-independent) ──
import {
  normaliseCustomTrigger,
  detectCustomWhispererTriggers,
  mergeBuiltInAndCustomTriggers,
} from "../src/whisperer/customTriggers";

const acmeRule = normaliseCustomTrigger({
  id: "rule-acme",
  type: "competitor",
  name: "Competitor: Acme",
  match_phrases: ["we use acme"],
  match_keywords: ["acme"],
  suggestion_title: "Position against Acme",
  suggestion_response: "Acknowledge Acme, then ask what they wish was better.",
  urgency: "high",
  emoji: "💥",
  enabled: true,
  priority: 80,
});
check("normaliseCustomTrigger builds a usable rule", acmeRule !== null && acmeRule!.id === "rule-acme");
check("disabled rule normalises to null", normaliseCustomTrigger({ id: "x", enabled: false, suggestion_title: "a", suggestion_response: "b", match_keywords: ["k"] }) === null);
check("rule with no phrases/keywords → null", normaliseCustomTrigger({ id: "x", suggestion_title: "a", suggestion_response: "b" }) === null);

const rules = acmeRule ? [acmeRule] : [];
const customPhrase = detectCustomWhispererTriggers({ text: "we use acme for this", speaker: "prospect" }, rules);
check("custom phrase hit → confidence 90", customPhrase[0]?.confidence === 90 && customPhrase[0]?.type === "competitor");
const customKw = detectCustomWhispererTriggers({ text: "yeah we have acme already", speaker: "prospect" }, rules);
check("custom single keyword → confidence 75", customKw[0]?.confidence === 75);
check("custom carries customTriggerId in meta", (customKw[0] as any)?.meta?.customTriggerId === "rule-acme");
check("custom suggestion title comes from rule", customPhrase[0]?.suggestion.title === "Position against Acme");
check("rep speech suppresses custom triggers", detectCustomWhispererTriggers({ text: "we use acme", speaker: "rep" }, rules).length === 0);
check("custom dedupe within 30s", detectCustomWhispererTriggers({ text: "we use acme", speaker: "prospect", now: new Date(), recentTriggers: [{ type: "competitor", detectedAt: new Date().toISOString() }] }, rules).length === 0);

const merged = mergeBuiltInAndCustomTriggers(
  [{ type: "price", phrase: "p", confidence: 86, suggestion: { title: "t", response: "r", urgency: "high", emoji: null } }],
  customPhrase
);
check("merge sorts highest confidence first (custom 90 > built-in 86)", merged[0]?.confidence === 90);

console.log(failures === 0 ? "\nWhisperer trigger validation PASSED" : `\nWhisperer trigger validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
