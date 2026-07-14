/**
 * Day 169 — UFC demo story seed own-checks (Day 135 rhythm, no live DB).
 *
 * Asserts at source level that the seeder exists, targets the UFC demo
 * company, seeds every chapter of the lighthouse story, stays idempotent
 * (deterministic ids + upsert), preserves the manager approval gates (no
 * candidate/decision rows, no auto-anything), and opens no forbidden lanes.
 */
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const seed = readFileSync(join(root, "scripts", "seed-ufc-demo-story.ts"), "utf8");

let fail = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) fail = 1;
}

// ── targets the UFC demo org, resolves ids at runtime ──
check("targets the UFC demo company", /bfb9604e-bc2f-46fa-be97-6461e98e1a19/.test(seed));
check("uses Dana as the demo manager", /dana\.white@ufcelite\.demo/.test(seed));
check("resolves rep/manager/call ids at runtime (no hardcoded auth uuids)", /eq\("email", DANA_EMAIL\)/.test(seed) && /eq\("email", REP_EMAIL\)/.test(seed));

// ── seeds every story chapter ──
check("seeds a Whisperer session", /whisperer_sessions/.test(seed) && /status: "ended"/.test(seed));
check("links the session to a scored demo call", /call_id: call\.id/.test(seed));
check("seeds trigger moments (price/authority/send_info)", /"price"/.test(seed) && /"authority"/.test(seed) && /"send_info"/.test(seed));
check("seeds raw untriggered segments for discovery", /whisperer_segments/.test(seed) && /triggers_count: 0/.test(seed));
check("seeds a custom trigger", /whisperer_trigger_library/.test(seed) && /Partner approval/.test(seed));
check("seeds queue-assigned sparring", /origin_label: "Coaching Queue"/.test(seed) && /source: "manager_dashboard"/.test(seed) && /recommended_drill/.test(seed));
check("seeds completion proof (Day 155 shape)", /completed_via: "sparring_session_match"/.test(seed) && /matched_sparring_session_id/.test(seed) && /completion_score/.test(seed));
check("seeds two proof scores for a trend", /completion_score: 62/.test(seed) && /completion_score: 78/.test(seed));
check("seeds sparring sessions linked by assignment_id", /sparring_sessions/.test(seed) && /assignment_id: assignDone/.test(seed));

// ── idempotency + provenance ──
check("idempotent: deterministic ids", /UFC_STORY::/.test(seed));
check("idempotent: upsert on id", /\.upsert\(rows as any\[\], \{ onConflict: "id" \}\)/.test(seed));
check("rows tagged as demo seed data", /demo_seed/.test(seed) && /"ufc-story"/.test(seed));

// ── Day 217B: enriched hero-call audit evidence ──
check(
  "hero rubric pins the story stage scores (close weakest at 40)",
  /intro:[\s\S]{0,20}score: 57/.test(seed) &&
    /discovery:[\s\S]{0,20}score: 53/.test(seed) &&
    /objection:[\s\S]{0,20}score: 56/.test(seed) &&
    /close:[\s\S]{0,20}score: 40/.test(seed)
);
check(
  "every stage carries non-trivial evidence notes (no \"Demo.\")",
  !/"Demo\."/.test(seed) && (seed.match(/Practise next:/g) || []).length >= 4
);
check(
  "each stage note covers missed behaviour + coaching + next practice",
  (seed.match(/What was missed:/g) || []).length >= 4 &&
    (seed.match(/Coach on:/g) || []).length >= 4
);
check("voice score preserved at 53", /voice_score: 53/.test(seed));
check("review tags flag the weak close", /weak_close: true/.test(seed));
check(
  "enriched rubric mirrored onto call_scores",
  /from\("call_scores"\)/.test(seed) && /eq\("call_id", call\.id\)/.test(seed)
);

// ── approval gates preserved ──
check("does not seed AI candidates or decisions", !/whisperer_trigger_candidate_decisions/.test(seed));

// ── forbidden lanes stayed closed ──
check("no ElevenLabs/TTS/Voice Agent added", !/elevenlabs|text.to.speech|voice.agent/i.test(seed));
check("no LLM on live hot path", !/openai|anthropic|chat\.completions|responses\.create/i.test(seed));

console.log();
if (fail) {
  console.log("UFC demo seed validation FAILED");
  process.exit(1);
}
console.log("UFC demo seed validation PASSED");
