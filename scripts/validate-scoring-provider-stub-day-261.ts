/**
 * validate-scoring-provider-stub-day-261.ts — no-cost scoring guard.
 *
 * Proves the Day-261 scoring provider switch is safe and no-cost:
 *  - default provider resolves to openai (production path unchanged);
 *  - an invalid/unset SCORING_PROVIDER never silently disables real scoring;
 *  - SCORING_PROVIDER=stub yields a deterministic score with NO API key and NO
 *    paid model call, keeping the score's top-level shape + fixed four stages;
 *  - the stub result is marked as the stub provider/model;
 *  - the switch only prevents paid calls — SKIP_SCORING_SIDE_EFFECTS remains the
 *    separate side-effect guard, and resolveScoringProvider does not touch it.
 *
 * Hermetic: pure units only, no DB, no network, no paid calls.
 * Usage: npx tsx scripts/validate-scoring-provider-stub-day-261.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

// Prove no-cost: no provider keys present for the whole run.
delete process.env.OPENAI_API_KEY;
delete process.env.ANTHROPIC_API_KEY;

function clearScoringEnv() {
  delete process.env.SCORING_PROVIDER;
}

async function main() {
  const {
    resolveScoringProvider,
    buildStubScore,
    heuristicScoreFallback,
    STUB_SCORING_MODEL,
  } = await import("../src/lib/scoring");

  // ── 1. Default + invalid resolution ────────────────────────────────────────
  clearScoringEnv();
  check("default scoring provider resolves to openai", resolveScoringProvider() === "openai");

  process.env.SCORING_PROVIDER = "not-a-provider";
  check("invalid provider resolves safely to openai", resolveScoringProvider() === "openai");
  check("invalid provider does NOT resolve to stub", resolveScoringProvider() !== "stub");

  process.env.SCORING_PROVIDER = "STUB"; // case-insensitive
  check("SCORING_PROVIDER=stub resolves stub (case-insensitive)", resolveScoringProvider() === "stub");
  clearScoringEnv();

  // ── 2. Stub score is deterministic + no key required ───────────────────────
  const a = buildStubScore();
  const b = buildStubScore();
  check("stub score builds with no API keys present", !!a && typeof a.overall === "number");
  check("stub score is deterministic", JSON.stringify(a) === JSON.stringify(b));
  check("stub model provenance is stub:v1", a.model === STUB_SCORING_MODEL && a.model === "stub:v1");

  // ── 3. Shape parity with the existing score shape + fixed four stages ──────
  const heuristic = heuristicScoreFallback();
  const sameTopLevel =
    JSON.stringify(Object.keys(a).sort()) === JSON.stringify(Object.keys(heuristic).sort());
  check("stub keeps the same top-level score shape", sameTopLevel);

  const stageKeys = Object.keys(a.stages || {}).sort().join(",");
  check("stub preserves the fixed four stages", stageKeys === "close,discovery,intro,objection");
  check("stub carries summary + voice like a normal score", typeof a.summary === "string" && !!(a as any).voice);
  check("stub differs from heuristic only by model tag", a.model !== heuristic.model && a.overall === heuristic.overall);

  // ── 4. Static wiring guards on scoring.ts ──────────────────────────────────
  const src = readFileSync(join(__dirname, "..", "src", "lib", "scoring.ts"), "utf8");

  check("scoreWithLLM consults resolveScoringProvider", /const\s+scoringProvider\s*=\s*resolveScoringProvider\(\)/.test(src));
  check("scoreWithLLM has an explicit stub branch that skips OpenAI", /if\s*\(\s*scoringProvider\s*===\s*["']stub["']\s*\)\s*\{[\s\S]*?buildStubScore\(\)/.test(src));

  // getOpenAI() must live in the else (non-stub) branch, i.e. after the stub
  // branch — proving the stub path makes no paid call.
  const stubIdx = src.indexOf("if (scoringProvider === \"stub\")");
  const scoreFnIdx = src.indexOf("export async function scoreWithLLM");
  const getOpenAiIdx = src.indexOf("const openai = getOpenAI();");
  check("getOpenAI() sits after the stub branch inside scoreWithLLM", stubIdx > scoreFnIdx && getOpenAiIdx > stubIdx);

  check("stub result is marked in rubric._meta.scoring_provider", /_meta\.scoring_provider\s*=\s*scoringProvider/.test(src));

  // ── 5. Side-effect guard stays separate from the provider switch ───────────
  check("SKIP_SCORING_SIDE_EFFECTS remains the side-effect guard", /SKIP_SCORING_SIDE_EFFECTS/.test(src));
  const resolveBody = src.slice(src.indexOf("export function resolveScoringProvider"), src.indexOf("export function buildStubScore"));
  check("provider resolver does NOT read SKIP_SCORING_SIDE_EFFECTS (independent)", !/SKIP_SCORING_SIDE_EFFECTS/.test(resolveBody));
  check("stub branch does not force-skip side effects itself", !/scoringProvider\s*===\s*["']stub["'][\s\S]{0,400}SKIP_SCORING_SIDE_EFFECTS/.test(src));
  check(
    "stub skips embedding-backed knowledge searches",
    /scoringProvider\s*===\s*["']stub["']\s*\?\s*\{\s*playbookText:\s*["']["'],\s*repMemoryText:\s*["']["']\s*\}/.test(src)
  );
  check(
    "side-effect guard blocks automatic critical assignments",
    /ensureCriticalCallAssignment[\s\S]{0,700}if\s*\(SKIP_SCORING_SIDE_EFFECTS\)[\s\S]{0,160}side_effects_disabled/.test(src)
  );
  check(
    "side-effect guard blocks review-flag activity writes",
    (src.match(/!SKIP_SCORING_SIDE_EFFECTS\s*&&\s*reviewFlags\.length\s*>\s*0/g) || []).length === 2
  );

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No paid calls made.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("validator crashed:", e?.message || e);
  process.exit(1);
});
