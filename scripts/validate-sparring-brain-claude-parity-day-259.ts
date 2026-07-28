/**
 * validate-sparring-brain-claude-parity-day-259.ts — Phase 1 parity proof.
 *
 * Smoke/parity proof for the Prospect Brain interface: run ONE realistic UFC
 * sparring turn through each available provider (openai / claude / stub) and
 * confirm each behaves like a buyer, in voice, with provenance intact.
 *
 * This is a PARITY/SMOKE proof, not a quality ranking. It does NOT flip the
 * default (OpenAI stays default) and makes no claim that Claude is "better".
 *
 * Env: keys are read from .env via dotenv. A provider with no key is SKIPPED,
 * never failed. Hard gates fail only when a *configured* provider returns an
 * empty reply, meta/assistant language, or provider/fallback provenance that
 * does not match the request. Qualitative rubric fields are reported, not gated
 * (model phrasing is non-deterministic).
 *
 * No secrets and no raw model output are committed — replies are printed to
 * stdout for the operator only. Usage: npx tsx scripts/…-day-259.ts
 */

import "dotenv/config";
import { generateProspectReply } from "../src/lib/sparringBrain";
import type { BrainProvider, BrainProviderName } from "../src/lib/sparringBrain";
import { openaiBrain } from "../src/lib/sparringBrain/openaiBrain";
import { claudeBrain } from "../src/lib/sparringBrain/claudeBrain";
import { stubBrain } from "../src/lib/sparringBrain/stubBrain";
import {
  createInitialSparringState,
  updateSparringState,
  withStateDirectives,
  type SparringState,
} from "../src/sparring";

const PROVIDER_OBJ: Record<BrainProviderName, BrainProvider> = {
  openai: openaiBrain,
  claude: claudeBrain,
  stub: stubBrain,
};

// Keep error reasons safe: cap length, and never echo anything key-shaped.
function safeReason(e: any): string {
  const raw = String(e?.message || e || "unknown error").replace(/\s+/g, " ").trim();
  const scrubbed = raw.replace(/sk-[A-Za-z0-9_-]{8,}/g, "sk-***");
  return scrubbed.length > 200 ? scrubbed.slice(0, 200) + "…" : scrubbed;
}

// ── Realistic UFC sparring turn ─────────────────────────────────────────────
// Persona: UFC Elite buyer — gym owner / manager. Scenario: rep is selling the
// Gravix AI Sales Trainer; buyer has price + trust concerns. Difficulty: hard.
const PERSONA_PROMPT = `You are role-playing a BUYER on a sales call.

WHO YOU ARE:
- You run "UFC Elite", a chain of high-end combat-sports gyms. You are the owner
  and you manage a team of membership sales reps.
- You are commercially sharp, sceptical of sales tools, and protective of budget.
- You have real concerns about (a) price/ROI, (b) trust — you've been burnt by
  "AI" tools that were generic, and (c) proof it actually works for a team like
  yours.

THE SITUATION:
- A rep is selling you "Gravix AI Sales Trainer", a tool that scores sales calls
  and coaches reps on why they lose deals.
- You have not agreed to anything. You are hard to win over on this call.

HOW YOU BEHAVE:
- Stay strictly in character as the buyer. Speak only the buyer's next words.
- Do not instantly agree. Push back with a realistic concern (cost, trust,
  setup effort, or proof) or ask a sceptical follow-up question.
- Be concise and conversational — a couple of sentences, spoken aloud. No lists,
  no markdown, no narration, and never break character or mention being an AI.`;

// A genuine rules-engine state (not a stub literal): hard difficulty, trust
// nudged to medium-low, then advanced by the rep's line so nextBuyerMove is real.
function buildState(): SparringState {
  const init = createInitialSparringState({ personaId: "price_sensitive", difficulty: "hard" });
  const seeded: SparringState = { ...init, trustLevel: 40 }; // medium-low trust
  return updateSparringState(seeded, {
    repText:
      "We help managers see exactly why reps lose deals, not just give a generic AI score.",
    turnScore: 55,
  });
}

const REP_LINE =
  "We help managers see exactly why reps lose deals, not just give a generic AI score.";

function buildInput(state: SparringState) {
  return {
    systemPrompt: withStateDirectives(PERSONA_PROMPT, state),
    history: [
      { role: "user" as const, content: "Alright, you've got a couple of minutes. What is this?" },
      { role: "assistant" as const, content: "I run a busy gym chain — I've seen a dozen of these 'AI' tools. Why are you different?" },
      { role: "user" as const, content: REP_LINE },
    ],
    state,
    personaId: "price_sensitive",
    difficulty: "hard",
  };
}

// ── Language heuristics ─────────────────────────────────────────────────────
const META_PHRASES = [
  "as an ai",
  "as a language model",
  "i am an ai",
  "i'm an ai",
  "language model",
  "as your assistant",
  "i cannot assist",
  "i can't help with that",
  "in this roleplay",
  "as the buyer",
  "as a buyer persona",
  "here is the buyer",
  "here's the buyer",
];

const INSTANT_AGREE = [
  "sounds great",
  "let's do it",
  "sign me up",
  "where do i sign",
  "i'm sold",
  "i'm in",
  "let's get started",
];

const CONCERN_KEYWORDS = [
  "price", "cost", "expensive", "budget", "trust", "proof", "prove", "results",
  "roi", "setup", "set up", "integrat", "worth", "value", "guarantee", "case",
  "reference", "how ", "why ", "what if", "generic", "team", "reps", "data",
];

function findMeta(text: string): string | null {
  const t = text.toLowerCase();
  return META_PHRASES.find((p) => t.includes(p)) ?? null;
}

function isConcise(text: string): boolean {
  if (text.includes("```")) return false;
  if (/^#{1,6}\s/m.test(text)) return false;
  const sentences = text.split(/[.!?]+/).filter((s) => s.trim().length > 0);
  const newlines = (text.match(/\n/g) || []).length;
  return text.length <= 600 && sentences.length <= 5 && newlines <= 4;
}

function looksPersonaRelevant(text: string): boolean {
  const t = text.toLowerCase();
  return t.includes("?") || CONCERN_KEYWORDS.some((k) => t.includes(k));
}

function looksRealisticObjection(text: string): boolean {
  const t = text.toLowerCase();
  const hasObjectionSignal = t.includes("?") || CONCERN_KEYWORDS.some((k) => t.includes(k));
  // "sounds great, but…" / "sounds great — how…" is soften-then-pushback, not
  // agreement. Only count as instant agreement when there is no follow-up
  // objection, question, or contrast conjunction.
  const softened = t.includes(" but ") || t.includes("however") || hasObjectionSignal;
  const instantAgree = INSTANT_AGREE.some((p) => t.includes(p)) && !softened;
  return !instantAgree && hasObjectionSignal;
}

type Row = {
  provider: BrainProviderName;
  status: "RAN" | "SKIPPED" | "UNAVAILABLE";
  model?: string;
  latencyMs?: number;
  replyLen?: number;
  metaHit?: string | null;
  rubric?: Record<string, boolean>;
  reason?: string;
};

let hardFailures = 0;
function gate(label: string, ok: boolean) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}`);
  if (!ok) hardFailures += 1;
}

async function runProvider(name: BrainProviderName, available: boolean): Promise<Row> {
  if (!available) {
    console.log(`\n── ${name.toUpperCase()} — SKIPPED (no API key present) ──`);
    return { provider: name, status: "SKIPPED", reason: "no API key present" };
  }

  const input = buildInput(buildState());

  // Direct provider probe — this is the actual model under test. A throw here
  // (bad key, no credit, outage) means the provider is UNAVAILABLE, not a Brain
  // defect: we report it and then confirm the router still degrades gracefully.
  let reply = "";
  let model = "";
  let latencyMs = 0;
  try {
    const started = Date.now();
    const raw = await PROVIDER_OBJ[name].generateProspectReply(input);
    latencyMs = Date.now() - started;
    reply = raw.text || "";
    model = raw.model;
  } catch (e: any) {
    const reason = safeReason(e);
    console.log(`\n── ${name.toUpperCase()} — UNAVAILABLE (provider could not run) ──`);
    console.log(`  reason: ${reason}`);
    // Resilience: the router must degrade to stub, never throw.
    const degraded = await generateProspectReply({ ...input, providerOverride: name });
    gate(`${name}: router degrades gracefully to stub on provider failure`, degraded.provider === "stub" && degraded.fallbackUsed === true && degraded.text.trim().length > 0);
    return { provider: name, status: "UNAVAILABLE", reason };
  }

  const empty = reply.trim().length === 0;
  const metaHit = findMeta(reply);
  const rubric = {
    buyer_voice: !empty && metaHit === null,
    persona_relevance: looksPersonaRelevant(reply),
    objection_realism: looksRealisticObjection(reply),
    response_concise: isConcise(reply),
    no_meta_language: metaHit === null,
  };

  console.log(`\n── ${name.toUpperCase()} — RAN ──`);
  console.log(`  model=${model}  latency=${latencyMs}ms  len=${reply.length}`);
  console.log(`  reply: ${reply.replace(/\s+/g, " ").trim()}`);
  console.log(`  rubric: ${Object.entries(rubric).map(([k, v]) => `${k}=${v ? "pass" : "fail"}`).join("  ")}`);

  // ── Hard gates (fail the build only when a provider RAN and misbehaved) ──
  gate(`${name}: reply is non-empty`, !empty);
  gate(`${name}: no meta/assistant language (${metaHit || "none"})`, metaHit === null);

  // Router provenance: a healthy provider must be the one the router uses, with
  // no fallback. (stub is the router's own default here, always healthy.)
  const routed = await generateProspectReply({ ...input, providerOverride: name });
  gate(`${name}: router selects it with matching provenance, no fallback`, routed.provider === name && routed.fallbackUsed === false);

  return {
    provider: name,
    status: "RAN",
    model,
    latencyMs,
    replyLen: reply.length,
    metaHit,
    rubric,
  };
}

async function main() {
  const hasOpenAI = !!process.env.OPENAI_API_KEY;
  const hasClaude = !!process.env.ANTHROPIC_API_KEY;

  console.log("Sparring Brain — Claude parity proof (Day 259)");
  console.log(`Provider keys: openai=${hasOpenAI ? "present" : "absent"}  claude=${hasClaude ? "present" : "absent"}  (stub always runs)`);

  const rows: Row[] = [];
  rows.push(await runProvider("openai", hasOpenAI));
  rows.push(await runProvider("claude", hasClaude));
  rows.push(await runProvider("stub", true)); // stub needs no key

  // ── Summary ──
  console.log("\n=== PARITY SUMMARY ===");
  const ran = rows.filter((r) => r.status === "RAN");
  for (const r of rows) {
    if (r.status === "SKIPPED") {
      console.log(`  ${r.provider.padEnd(7)} SKIPPED     (${r.reason})`);
      continue;
    }
    if (r.status === "UNAVAILABLE") {
      console.log(`  ${r.provider.padEnd(7)} UNAVAILABLE (${r.reason})`);
      continue;
    }
    const rubric = Object.entries(r.rubric || {}).map(([k, v]) => `${k}:${v ? "✓" : "✗"}`).join(" ");
    console.log(`  ${r.provider.padEnd(7)} ${String(r.model).padEnd(28)} ${String(r.latencyMs).padStart(5)}ms  ${rubric}`);
  }
  const latencies = ran.map((r) => r.latencyMs!).filter((n) => typeof n === "number");
  if (latencies.length) {
    console.log(`  latency range: ${Math.min(...latencies)}–${Math.max(...latencies)}ms across ${ran.length} live provider(s)`);
  }

  console.log(`\n${hardFailures === 0 ? "PASS" : "FAIL"} — ${hardFailures} hard-gate failure(s). Default provider unchanged (openai).`);
  process.exit(hardFailures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("parity proof crashed:", e?.message || e);
  process.exit(1);
});
