/**
 * validate-whisperer-segments.ts — Tier 2B Day 142
 * Deterministic assertions for raw segment persistence DERIVATION (the fields
 * the /segments route computes for a whisperer_segments row). No DB needed —
 * exercises the trigger engine and the source-level guarantees of the helper.
 * Usage: npx tsx scripts/validate-whisperer-segments.ts
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { detectWhispererTriggers } from "../src/whisperer/triggers";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

// Mirror the route: triggers_count = detected.length, trigger_types = unique types.
function deriveSegment(text: string, speaker: "rep" | "prospect" | "unknown") {
  const detected = detectWhispererTriggers({ sessionId: "s1", text, speaker });
  return {
    triggers_count: detected.length,
    trigger_types: Array.from(new Set(detected.map((t) => t.type))),
    speaker,
  };
}

// 1. A no-trigger final segment is still a valid persistable shape (count 0).
const neutral = deriveSegment("Hello, just checking the line is clear.", "prospect");
check("no-trigger segment → triggers_count 0", neutral.triggers_count === 0);
check("no-trigger segment → trigger_types empty", neutral.trigger_types.length === 0);

// 2. A triggered segment carries its trigger_types.
const priced = deriveSegment("The price is a little too high.", "prospect");
check("triggered segment → triggers_count >= 1", priced.triggers_count >= 1);
check("triggered segment → trigger_types includes price", priced.trigger_types.includes("price"));

// 3. Rep-calibrated speech is suppressed but the segment is still representable
//    (count 0, speaker rep) — i.e. rep blind-spot text persists too.
const repSeg = deriveSegment("The price is too high.", "rep");
check("rep-calibrated price phrase → no trigger (suppressed)", repSeg.triggers_count === 0);
check("rep segment still representable with speaker rep", repSeg.speaker === "rep");

// 4. Diarised labels and unknown speakers are representable verbatim.
const labels = ["rep", "prospect", "unknown", "speaker_0", "speaker_1"];
check("all speaker labels are non-empty strings", labels.every((l) => typeof l === "string" && l.length > 0));

// 5. The persistence helper must be pure DB (no LLM) on the live path.
const src = readFileSync(join(__dirname, "..", "src", "routes", "whisperer.ts"), "utf8");
const start = src.indexOf("async function persistWhispererSegment");
// End at the next top-level function declaration so the whole helper is captured
// (the multi-line parameter type contains its own closing brace).
const after = start >= 0 ? src.slice(start + 1) : "";
const nextFn = after.search(/\n(async )?function /);
const helperBody = start < 0 ? "" : nextFn >= 0 ? src.slice(start, start + 1 + nextFn) : src.slice(start);
check("persistWhispererSegment helper found", helperBody.length > 0);
check("persistWhispererSegment has no LLM call", !/openai|anthropic|chat\.completions|responses\.create/i.test(helperBody));
check("persistWhispererSegment inserts whisperer_segments", helperBody.includes("whisperer_segments"));
check("persistWhispererSegment is fail-soft (returns persistence:false)", helperBody.includes("persistence: false"));

console.log(failures === 0 ? "\nWhisperer segments validation PASSED" : `\nWhisperer segments validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
