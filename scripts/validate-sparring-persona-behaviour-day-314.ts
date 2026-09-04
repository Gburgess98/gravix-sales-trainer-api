/**
 * Go Live Day 8 — Sparring persona behaviour repair validator.
 *
 * Deterministic, network-free proof that configured persona + difficulty
 * behaviour actually shapes the buyer system prompt (instead of throwing and
 * silently falling back to basic traits).
 *
 * For EVERY persona × supported difficulty it asserts:
 *   1. buildPersonaBehaviourSummary(realPersona, difficulty) returns a
 *      non-empty summary carrying the configured behaviour markers, no throw;
 *   2. persona differentiation — distinct tones across personas;
 *   3. difficulty differentiation — a persona's summary is not identical across
 *      all four difficulties (modifiers are applied);
 *   4. the REAL prompt path (buildPersonaSystemPrompt) embeds that exact summary
 *      and emits NO "falling back to basic traits" warning for valid inputs.
 *
 * Non-vacuity: the pre-fix buggy call (fabricated object cast as any) still
 * throws — proving these assertions distinguish the fixed path from the broken
 * one. No live database or AI provider is used.
 */

// Dummy env MUST be set before importing the route module (it constructs a
// Supabase client + OpenAI client at module load — both lazy, no network).
process.env.SUPABASE_URL ||= "http://localhost:54321";
process.env.SUPABASE_SERVICE_ROLE_KEY ||= "dummy-service-role-key";
process.env.SUPABASE_ANON_KEY ||= "dummy-anon-key";
process.env.OPENAI_API_KEY ||= "dummy-openai-key";

import {
  PERSONAS,
  getPersonaConfig,
  buildPersonaBehaviourSummary,
  type DifficultyLevel,
} from "../src/personas";

const DIFFICULTIES: DifficultyLevel[] = ["easy", "normal", "hard", "nightmare"];
const MARKERS = [
  "Tone:",
  "Reply length:",
  "Objection frequency:",
  "interruption level:",
  "price pressure:",
  "Patience:",
  "Hangup chance",
];

let failures = 0;
const fail = (msg: string) => {
  failures++;
  console.error(`  ✗ ${msg}`);
};
const ok = (msg: string) => console.log(`  ✓ ${msg}`);

/** Run fn while capturing console.warn output. */
function captureWarn<T>(fn: () => T): { value: T; warnings: string[] } {
  const warnings: string[] = [];
  const original = console.warn;
  console.warn = (...args: unknown[]) => {
    warnings.push(args.map((a) => String(a)).join(" "));
  };
  try {
    return { value: fn(), warnings };
  } finally {
    console.warn = original;
  }
}

async function main() {
  const { buildPersonaSystemPrompt } = await import("../src/routes/sparring");

  console.log(
    `Personas: ${PERSONAS.length} (${PERSONAS.map((p) => p.id).join(", ")})`
  );
  console.log(`Difficulties: ${DIFFICULTIES.join(", ")}`);
  console.log(`Matrix combinations: ${PERSONAS.length * DIFFICULTIES.length}\n`);

  const tones = new Set<string>();

  for (const persona of PERSONAS) {
    const perDifficulty: string[] = [];
    for (const difficulty of DIFFICULTIES) {
      const label = `${persona.id} × ${difficulty}`;

      // 1. Summary builds without throwing and carries the configured markers.
      let summary = "";
      try {
        summary = buildPersonaBehaviourSummary(
          getPersonaConfig(persona.id),
          difficulty
        );
      } catch (e) {
        fail(`${label}: buildPersonaBehaviourSummary threw: ${String(e)}`);
        continue;
      }
      if (!summary.trim()) {
        fail(`${label}: empty behaviour summary`);
        continue;
      }
      const missing = MARKERS.filter((m) => !summary.includes(m));
      if (missing.length) {
        fail(`${label}: summary missing markers: ${missing.join(", ")}`);
        continue;
      }
      // Configured tone actually present.
      if (!summary.includes(persona.behaviour.tone)) {
        fail(`${label}: summary omits configured tone "${persona.behaviour.tone}"`);
        continue;
      }
      perDifficulty.push(summary);

      // 4. Real prompt path embeds this exact summary with no fallback warning.
      const { value: prompt, warnings } = captureWarn(() =>
        buildPersonaSystemPrompt({
          personaId: persona.id,
          difficulty,
          mode: "standard",
        })
      );
      if (!prompt.includes(summary)) {
        fail(`${label}: system prompt does NOT embed the behaviour summary`);
        continue;
      }
      const fallbackWarn = warnings.filter((w) =>
        w.includes("buildPersonaBehaviourSummary failed")
      );
      if (fallbackWarn.length) {
        fail(`${label}: prompt path emitted fallback warning: ${fallbackWarn[0]}`);
        continue;
      }

      ok(`${label}: configured behaviour present + embedded in prompt, no fallback`);
    }

    // 3. Difficulty differentiation — not all four summaries identical.
    if (new Set(perDifficulty).size <= 1 && perDifficulty.length > 1) {
      fail(`${persona.id}: behaviour identical across all difficulties (modifiers not applied)`);
    } else if (perDifficulty.length > 1) {
      ok(`${persona.id}: difficulty modifiers differentiate behaviour (${new Set(perDifficulty).size} distinct)`);
    }
    tones.add(persona.behaviour.tone);
  }

  // 2. Persona differentiation — distinct tones.
  if (tones.size !== PERSONAS.length) {
    fail(`persona tones are not all distinct (${tones.size}/${PERSONAS.length})`);
  } else {
    ok(`persona differentiation: ${tones.size} distinct tones across ${PERSONAS.length} personas`);
  }

  // Non-vacuity: the pre-fix buggy call must still throw.
  console.log("\nNon-vacuity (negative control):");
  let threw = false;
  try {
    // The exact pre-fix pattern: a fabricated object cast as any.
    (buildPersonaBehaviourSummary as unknown as (a: unknown, b: unknown) => string)(
      { personaId: "price_sensitive", difficulty: "normal", mode: "standard" },
      "normal"
    );
  } catch {
    threw = true;
  }
  if (threw) {
    ok("pre-fix buggy call (fabricated object) still throws — check is non-vacuous");
  } else {
    fail("pre-fix buggy call did NOT throw — check would be vacuous");
  }

  console.log(
    `\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s) across ${
      PERSONAS.length * DIFFICULTIES.length
    } persona×difficulty combinations`
  );
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("validator crashed:", e);
  process.exit(1);
});
