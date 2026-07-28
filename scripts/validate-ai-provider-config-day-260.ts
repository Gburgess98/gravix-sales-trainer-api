/**
 * validate-ai-provider-config-day-260.ts — cost-safety guard.
 *
 * Statically enforces the cost-safe AI provider invariants documented in
 * AI_PROVIDER_CONFIG.md. Hermetic and NO-COST: it resolves providers and runs
 * only the deterministic stub (no keys, no network, no paid calls).
 *
 * Guards against: accidental Claude-default (which would fail on Anthropic
 * credit), voice code creeping into brain/scoring, the scoring model default
 * drifting off gpt-4o-mini, and the docs either dropping the billing-separation
 * guidance or over-claiming an unimplemented scoring stub.
 *
 * Usage: npx tsx scripts/validate-ai-provider-config-day-260.ts
 */

import { readFileSync, readdirSync } from "fs";
import { join } from "path";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const ROOT = join(__dirname, "..");
function read(rel: string): string {
  return readFileSync(join(ROOT, rel), "utf8");
}

// Prove the no-cost path: no provider keys present for the whole run.
delete process.env.OPENAI_API_KEY;
delete process.env.ANTHROPIC_API_KEY;

function clearBrainEnv() {
  delete process.env.SPARRING_BRAIN_PROVIDER;
  delete process.env.SPARRING_PROVIDER;
}

async function main() {
  const brain = await import("../src/lib/sparringBrain");
  const { generateProspectReply, resolveBrainProvider } = brain;

  // ── Sparring brain: default & safety ───────────────────────────────────────
  clearBrainEnv();
  check("default sparring brain provider resolves to openai", resolveBrainProvider() === "openai");
  check("default provider is NOT claude", resolveBrainProvider() !== "claude");

  process.env.SPARRING_BRAIN_PROVIDER = "not-a-real-provider";
  check("invalid provider does not become claude", resolveBrainProvider() !== "claude");
  check("invalid provider falls back to openai", resolveBrainProvider() === "openai");
  clearBrainEnv();

  // ── Stub runs with NO keys present (no-cost QA) ────────────────────────────
  const reply = await generateProspectReply({
    systemPrompt: "You are a sceptical buyer.",
    history: [{ role: "user", content: "Hello." }],
    providerOverride: "stub",
  });
  check("stub provider runs with OPENAI_API_KEY + ANTHROPIC_API_KEY absent", reply.provider === "stub" && reply.text.trim().length > 0);
  check("stub run reports no fallback (answered directly)", reply.fallbackUsed === false && reply.ok === true);

  // ── No voice provider code in brain/scoring (Whisperer is out of scope) ────
  const brainDir = join(ROOT, "src", "lib", "sparringBrain");
  const brainFiles = readdirSync(brainDir).filter((f) => f.endsWith(".ts")).map((f) => `src/lib/sparringBrain/${f}`);
  const scoringFiles = ["src/lib/scoring.ts", "src/lib/openai.ts", "src/lib/llm.ts"];
  const brainScoringBlob = [...brainFiles, ...scoringFiles].map(read).join("\n").toLowerCase();
  const VOICE_TOKENS = ["livekit", "deepgram", "elevenlabs", "cartesia", "hume evi", "rime"];
  const voiceHit = VOICE_TOKENS.find((t) => brainScoringBlob.includes(t));
  check(`no voice provider imports in brain/scoring code (${voiceHit || "none"})`, voiceHit === undefined);

  // ── Scoring model default remains gpt-4o-mini ──────────────────────────────
  const openaiLib = read("src/lib/openai.ts");
  check("scoring AI_MODEL default is gpt-4o-mini", /AI_MODEL\s*=\s*process\.env\.AI_MODEL\s*\|\|\s*["']gpt-4o-mini["']/.test(openaiLib));

  // ── Scoring-stub reality (Day 261): SCORING_PROVIDER=stub is implemented ────
  const scoringSrc = read("src/lib/scoring.ts");
  const scoringProviderEnvWired = /process\.env\.SCORING_PROVIDER/.test(scoringSrc);
  check("SCORING_PROVIDER scoring switch is wired in code", scoringProviderEnvWired === true);
  check("no SCORING_MODE env crept in (SCORING_PROVIDER is the switch)", /process\.env\.SCORING_MODE\b/.test(scoringSrc) === false);

  // ── Docs: AI_PROVIDER_CONFIG.md content assertions ─────────────────────────
  const doc = read("AI_PROVIDER_CONFIG.md").toLowerCase();

  check("doc explains Claude app vs Anthropic API billing separation",
    (doc.includes("claude app") || doc.includes("claude.ai")) && doc.includes("console") && doc.includes("credit"));
  check("doc includes a no-cost QA mode with stub brain + stub scoring",
    doc.includes("no-cost qa") && doc.includes("sparring_brain_provider=stub") && doc.includes("scoring_provider=stub"));
  check("doc states OpenAI is the default/baseline", doc.includes("openai") && doc.includes("default"));
  check("doc does NOT claim Claude is the default or live-proven",
    !/claude[^\n]*\b(is (the )?default|is now default|proven live|live-proven)\b/.test(doc));

  // Docs and code must agree: if the env is wired, the doc must present stub as a
  // real option, not park it under "future / not implemented".
  if (scoringProviderEnvWired) {
    const docBody = read("AI_PROVIDER_CONFIG.md");
    const futureSectionParksStub = /SCORING_PROVIDER=stub[^\n]*\b(NOT implemented|not wired|Proposed Day)\b/.test(docBody);
    check("doc does NOT still park SCORING_PROVIDER=stub as unimplemented", futureSectionParksStub === false);
  }

  // ── Sanity: default provider export unchanged in code ──────────────────────
  const brainIndex = read("src/lib/sparringBrain/index.ts");
  check("brain module hard default is openai", /DEFAULT_PROVIDER:\s*BrainProviderName\s*=\s*["']openai["']/.test(brainIndex));

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No paid calls made.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("validator crashed:", e?.message || e);
  process.exit(1);
});
