/**
 * validate-score-cache-provider-isolation-day-262.ts
 *
 * Proves the scoring cache is namespaced by provider so no-cost QA (stub) can
 * neither reuse nor pollute production (openai) cache entries:
 *  - the default/openai key is byte-identical to the pre-Day-262 key (no
 *    provider segment) — the Day 221 guarantee is preserved;
 *  - the stub key differs from the openai key and carries a `provider=stub`
 *    segment;
 *  - context- and scorecard-versioned keys still differentiate (regression);
 *  - an invalid provider resolves to openai and therefore cannot collide with
 *    the stub namespace.
 *
 * Hermetic and no-cost: pure key construction, no DB, no network, no LLM.
 * Usage: npx tsx scripts/validate-score-cache-provider-isolation-day-262.ts
 */

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

async function main() {
  const { buildDeterministicPromptKey, resolveScoringProvider } = await import("../src/lib/scoring");

  // scorecardCacheKey: null == the Gravix default (no scorecard segment), which
  // is exactly the production default path we must keep byte-identical.
  const BASE = {
    callId: "call-abc",
    filename: "demo.mp3",
    sha256: "deadbeef",
    transcript: "Rep: hello. Buyer: how much is it?",
    contextVersion: null as number | null,
    scorecardCacheKey: null as string | null,
  };

  const openaiKey = buildDeterministicPromptKey({ ...BASE, scoringProvider: "openai" }).key;
  const stubKey = buildDeterministicPromptKey({ ...BASE, scoringProvider: "stub" }).key;
  const noProviderKey = buildDeterministicPromptKey({ ...BASE }).key; // legacy caller

  // ── Default / openai namespace stability (Day 221 byte-identity) ───────────
  check("openai key omits any provider segment", !openaiKey.includes("provider="));
  check("openai key == legacy no-provider key (byte-identical)", openaiKey === noProviderKey);

  // ── Stub isolation ─────────────────────────────────────────────────────────
  check("stub key differs from openai key", stubKey !== openaiKey);
  check("stub key carries provider=stub segment", stubKey.includes("provider=stub"));
  check("stub key is the openai key plus the provider segment", stubKey === `${openaiKey}|provider=stub`);

  // ── Version segments still differentiate (regression guard) ────────────────
  const ctxA = buildDeterministicPromptKey({ ...BASE, contextVersion: 1 }).key;
  const ctxB = buildDeterministicPromptKey({ ...BASE, contextVersion: 2 }).key;
  check("context-versioned keys still differ", ctxA !== ctxB && ctxA !== openaiKey);

  const scKey = buildDeterministicPromptKey({ ...BASE, scorecardCacheKey: "scv-custom-123" }).key;
  check("scorecard-versioned key still differs from default", scKey !== openaiKey && scKey.includes("scorecard=scv-custom-123"));

  // A stub run under a custom scorecard must still differ from its openai twin.
  const scOpenai = buildDeterministicPromptKey({ ...BASE, scorecardCacheKey: "scv-custom-123", scoringProvider: "openai" }).key;
  const scStub = buildDeterministicPromptKey({ ...BASE, scorecardCacheKey: "scv-custom-123", scoringProvider: "stub" }).key;
  check("stub vs openai differ even under a custom scorecard", scOpenai !== scStub && scStub.endsWith("|provider=stub"));

  // ── Invalid provider cannot collide with the stub namespace ────────────────
  const savedEnv = process.env.SCORING_PROVIDER;
  process.env.SCORING_PROVIDER = "totally-invalid";
  const resolvedInvalid = resolveScoringProvider();
  check("invalid SCORING_PROVIDER resolves to openai", resolvedInvalid === "openai");
  const invalidKey = buildDeterministicPromptKey({ ...BASE, scoringProvider: resolvedInvalid }).key;
  check("invalid-provider key cannot collide with stub namespace", invalidKey === openaiKey && invalidKey !== stubKey);
  if (savedEnv === undefined) delete process.env.SCORING_PROVIDER;
  else process.env.SCORING_PROVIDER = savedEnv;

  // ── Static wiring: scoreWithLLM threads the provider into the key ──────────
  const { readFileSync } = await import("fs");
  const { join } = await import("path");
  const src = readFileSync(join(__dirname, "..", "src", "lib", "scoring.ts"), "utf8");
  check("scoreWithLLM passes scoringProvider into buildDeterministicPromptKey",
    /buildDeterministicPromptKey\(\{[\s\S]*?scoringProvider,[\s\S]*?\}\)/.test(src));

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No paid calls made.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("validator crashed:", e?.message || e);
  process.exit(1);
});
