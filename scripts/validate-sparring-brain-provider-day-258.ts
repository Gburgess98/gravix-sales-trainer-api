/**
 * validate-sparring-brain-provider-day-258.ts — Phase 1 (Prospect Brain).
 *
 * Proves the Gravix AI Core `Brain` provider interface behaves safely:
 *  - default provider resolves to openai;
 *  - the legacy SPARRING_PROVIDER env is honoured, SPARRING_BRAIN_PROVIDER wins;
 *  - an invalid provider resolves safely to openai;
 *  - the stub provider is deterministic and makes NO LLM call (runs with no API
 *    keys present);
 *  - the legacy route path (generateBuyerReply) keeps its response shape;
 *  - the Brain result carries provider/model provenance;
 *  - no voice provider (LiveKit/Deepgram/ElevenLabs/…) code was introduced;
 *  - the Brain module does not touch scoring or the database.
 *
 * Hermetic: no network, no DB. Usage: npx tsx scripts/validate-sparring-brain-provider-day-258.ts
 */

import { readFileSync, readdirSync } from "fs";
import { join } from "path";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

// Ensure a clean env baseline for resolution checks.
function clearBrainEnv() {
  delete process.env.SPARRING_BRAIN_PROVIDER;
  delete process.env.SPARRING_PROVIDER;
}

// Prove "no LLM call" by removing every provider key: only the stub can answer.
delete process.env.OPENAI_API_KEY;
delete process.env.ANTHROPIC_API_KEY;

async function main() {
  const brain = await import("../src/lib/sparringBrain");
  const { generateProspectReply, resolveBrainProvider } = brain;
  const { generateBuyerReply } = await import("../src/sparring/providers");
  const { STUB_MODEL } = await import("../src/lib/sparringBrain/stubBrain");

  // ── 1. Default provider resolves openai ────────────────────────────────────
  clearBrainEnv();
  check("default provider resolves to openai", resolveBrainProvider() === "openai");

  // ── 2. Legacy env honoured; canonical env wins ─────────────────────────────
  clearBrainEnv();
  process.env.SPARRING_PROVIDER = "stub";
  check("legacy SPARRING_PROVIDER=stub resolves stub", resolveBrainProvider() === "stub");
  process.env.SPARRING_BRAIN_PROVIDER = "claude";
  check("canonical SPARRING_BRAIN_PROVIDER wins over legacy", resolveBrainProvider() === "claude");

  // ── 3. Invalid provider resolves safely to openai ──────────────────────────
  clearBrainEnv();
  process.env.SPARRING_BRAIN_PROVIDER = "totally-not-a-provider";
  check("invalid provider resolves safely to openai", resolveBrainProvider() === "openai");
  clearBrainEnv();

  // ── 4. Stub is deterministic and needs NO LLM call ─────────────────────────
  const input = {
    systemPrompt: "You are a sceptical buyer.",
    history: [
      { role: "user" as const, content: "Hi, thanks for taking the call." },
      { role: "assistant" as const, content: "Make it quick." },
      { role: "user" as const, content: "We can cut your admin time in half." },
    ],
    providerOverride: "stub" as const,
  };
  const r1 = await generateProspectReply(input);
  const r2 = await generateProspectReply(input);
  check("stub returns a non-empty reply with no API keys set", typeof r1.text === "string" && r1.text.length > 0);
  check("stub reply is deterministic for a fixed input", r1.text === r2.text);
  check("stub provider name reported", r1.provider === "stub");
  check("stub model provenance recorded", r1.model === STUB_MODEL);
  check("stub used no fallback (answered directly)", r1.fallbackUsed === false && r1.ok === true);

  // ── 5. Brain result carries provider + model + latency provenance ──────────
  check("result exposes provider metadata", typeof r1.provider === "string");
  check("result exposes model metadata", typeof r1.model === "string" && r1.model.length > 0);
  check("result exposes numeric latencyMs", typeof r1.latencyMs === "number");
  check("result exposes boolean fallbackUsed", typeof r1.fallbackUsed === "boolean");

  // ── 6. Route path (legacy shim) keeps its response shape via stub ──────────
  clearBrainEnv();
  process.env.SPARRING_BRAIN_PROVIDER = "stub";
  const legacy = await generateBuyerReply({
    systemPrompt: "You are a sceptical buyer.",
    history: [{ role: "user" as const, content: "Hello." }],
  });
  const legacyKeys = Object.keys(legacy).sort().join(",");
  check(
    "route shim returns {ok,text,provider,fallback,latencyMs}",
    legacyKeys === "fallback,latencyMs,ok,provider,text"
  );
  check("route shim text is a non-empty string", typeof legacy.text === "string" && legacy.text.length > 0);
  check("route shim used stub provider", legacy.provider === "stub");
  clearBrainEnv();

  // ── 7. Static guards: no voice / scoring / DB in the Brain module ──────────
  const brainDir = join(__dirname, "..", "src", "lib", "sparringBrain");
  const files = readdirSync(brainDir).filter((f) => f.endsWith(".ts"));
  const combined = files.map((f) => readFileSync(join(brainDir, f), "utf8")).join("\n").toLowerCase();

  const VOICE_TOKENS = ["livekit", "deepgram", "elevenlabs", "cartesia", "hume", "rime"];
  const voiceHit = VOICE_TOKENS.find((t) => combined.includes(t));
  check(`no voice provider code introduced (${voiceHit || "none"})`, voiceHit === undefined);

  // Brain must not own scoring or DB — those stay in the route/engines.
  const SCORING_TOKENS = ["scoresparringturn", "scorerepturnheuristic", "mergeturnscore", "buildsparringsessionsummary"];
  const scoringHit = SCORING_TOKENS.find((t) => combined.includes(t));
  check(`no scoring runtime code in Brain module (${scoringHit || "none"})`, scoringHit === undefined);

  const DB_TOKENS = ["createclient", "@supabase", ".from(", "supabaseurl"];
  const dbHit = DB_TOKENS.find((t) => combined.includes(t));
  check(`no DB/schema code in Brain module (${dbHit || "none"})`, dbHit === undefined);

  // Sanity: the three providers exist and the module surfaces the router.
  check("openai + claude + stub providers all present", files.some((f) => f === "openaiBrain.ts") && files.some((f) => f === "claudeBrain.ts") && files.some((f) => f === "stubBrain.ts"));

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s)`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("validator crashed:", e);
  process.exit(1);
});
