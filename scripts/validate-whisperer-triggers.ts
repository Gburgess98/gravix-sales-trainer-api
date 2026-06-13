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

// ── Type detection ──
check("'too expensive' → price", detect("this is too expensive")[0]?.type === "price");
check("'think about it' → timing", detect("let me think about it")[0]?.type === "timing");
check("'send me info' → send_info", detect("just send me info")[0]?.type === "send_info");
check("'ask my boss' → authority", detect("I need to ask my boss")[0]?.type === "authority");
check("'sounds like a scam' → trust", detect("this sounds like a scam")[0]?.type === "trust");
check("'already have a provider' → competitor", detect("we already have a provider")[0]?.type === "competitor");
check("neutral text → no triggers", detect("good morning, how are you").length === 0);

// ── Confidence tiers ──
check("exact phrase → confidence 90", detect("too expensive")[0]?.confidence === 90);
check("loose keyword → confidence 70", detect("that's quite pricey")[0]?.confidence === 70);

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

console.log(failures === 0 ? "\nWhisperer trigger validation PASSED" : `\nWhisperer trigger validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
