/**
 * Day 224 — UFC Elite Intelligence Layer demo seeder.
 *
 * Seeds the PERSISTENT Intelligence assets the demo needs so the Day 221–223
 * runtime/display work is actually visible in the product:
 *
 *   1. Published UFC company context v1 (+ the draft working copy a manager
 *      would keep editing), compiled deterministically by the real
 *      compileContextBlock.
 *   2. "UFC Sales Scorecard" — an ACTIVE company-default scorecard over the
 *      fixed four stages (intro 20 / discovery 30 / objection 30 / close 20),
 *      one criterion per stage, with the relational weight/criteria rows the
 *      Studio edits AND the immutable activation snapshot the runtime reads.
 *   3. One Day 224 proof call, scored through the REAL scoring runtime so its
 *      rubric._meta carries genuine provenance — this is the call that shows
 *      "Scored with UFC Sales Scorecard v1 · Company context v1 applied" on
 *      /calls/[id].
 *   4. Day 252 — a small set of manager-APPROVED Objection Library items (the
 *      third Intelligence pillar) so /intelligence?tab=objections is not empty
 *      in the demo. Data only: nothing in any runtime reads these.
 *
 * The Nate Diaz hero call is NEVER touched: it stays at 45 with no provenance,
 * which is exactly the calm default state Day 223 renders for pre-Day-221 calls.
 * The demo therefore shows both states side by side.
 *
 * HONESTY — what is real and what is seeded. The proof call's STAGE SCORES are
 * seeded demo values, pinned the same way the hero call's are (no LLM is
 * called: OPENAI_API_KEY is unset here, and a sentinel is placed in score_cache
 * under the key the runtime derives). The PROVENANCE in rubric._meta is not
 * seeded — scoreWithLLM resolves the seeded context/scorecard itself, keys the
 * cache itself, and stamps _meta itself. A cache hit is only possible if that
 * live resolution matched, so the provenance on the call is genuinely the
 * runtime's own output.
 *
 * Idempotent: every row uses a deterministic UUID (uid("UFC_INTEL", key)) and
 * upserts on id; re-running refreshes the same rows and re-scores the same
 * proof call. Nothing is deleted except this seed's own score_cache entry.
 *
 * Run: npx tsx scripts/seed-ufc-intelligence.ts [--dry-run]
 *      npm run seed:ufc-intelligence
 * Prerequisites: sql/20260714_company_context.sql + sql/20260714b_scorecard_studio.sql
 *                applied, and the base demo seed (npm run seed:demo).
 */

import "dotenv/config";

// Rails applied before lib/scoring loads. ESM imports are hoisted and
// lib/scoring reads SKIP_SCORING_SIDE_EFFECTS once at module init, so scoring
// MUST come in via the dynamic import below — a static import captures the flag
// as false and fires real Slack/email/rep-memory side effects at demo people.
delete process.env.OPENAI_API_KEY;
process.env.SKIP_SCORING_SIDE_EFFECTS = "1";

import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

import { compileContextBlock } from "../src/lib/contextEngine";
import { buildVersionSnapshot } from "../src/lib/scorecardStudio";
import { resolveActiveScorecard, resolvePublishedContext } from "../src/lib/intelligenceRuntime";
import { cleanTranscript } from "../src/lib/transcript";

type ScoringModule = typeof import("../src/lib/scoring");
async function loadScoring(): Promise<ScoringModule> {
  return import("../src/lib/scoring");
}

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SERVICE_KEY) {
  console.error("✗  Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const DRY_RUN = process.argv.includes("--dry-run");
const DEMO_COMPANY_ID = process.env.DEMO_COMPANY_ID || "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const DANA_EMAIL = "dana.white@ufcelite.demo";
const SEED_TAG = "ufc-intelligence";

// Deterministic approval timestamp so re-running the seed is byte-identical
// and the whole Intelligence pillar reads as approved on the same demo date
// the context was published ("Published 19 Jul 2026").
export const SEED_APPROVED_AT = "2026-07-19T12:00:00.000Z";

export const SCORECARD_NAME = "UFC Sales Scorecard";

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

export const IDS = {
  ctxDraft: uid("UFC_INTEL", "context-draft"),
  ctxPublished: uid("UFC_INTEL", "context-published"),
  scorecard: uid("UFC_INTEL", "scorecard"),
  version: uid("UFC_INTEL", "scorecard-version-1"),
  proofCall: uid("UFC_INTEL", "proof-call"),
};

// ---------------------------------------------------------------------------
// Context — bounded Day 208 field-spec content describing what UFC Elite sells.
// ---------------------------------------------------------------------------
export const UFC_CONTEXT = {
  profile: {
    about:
      "UFC Elite Sales Team sells an elite sales coaching system to revenue teams: AI call review, sparring practice and manager-led coaching assignments in one platform.",
    sales_motion:
      "Managers review recorded calls, assign coaching from the weak stages, and reps practise objections in sparring before the next live call.",
    icp: "Sales managers, SDR leaders and revenue leaders running teams of 5–50 reps.",
  },
  offering: {
    products_services: [
      { name: "AI call review", description: "Every call scored against the company scorecard with stage-level evidence." },
      { name: "Sparring", description: "Reps rehearse objections against realistic scenarios and are scored on the same rubric." },
      { name: "Coaching assignments", description: "Managers assign drills straight from a weak call and track completion." },
    ],
    pricing_positioning: {
      pricing_notes: "Premium annual platform licence, priced per active seat.",
      positioning_notes:
        "A premium, evidence-led sales coaching platform — bought for repeatable rep improvement, never sold on price.",
    },
  },
  objections: [
    {
      objection: "It's too expensive",
      approved_response:
        "Isolate the concern, then reframe on cost per rep against the revenue of one recovered deal.",
      weak_response: "Discounting immediately or apologising for the price.",
    },
    {
      objection: "I need to think about it",
      approved_response: "Find the real hesitation, then agree a dated next step before the call ends.",
      weak_response: "Agreeing to 'circle back' with no date.",
    },
    {
      objection: "Just send me some info",
      approved_response: "Offer to walk one of their real calls through the platform live rather than sending a deck.",
      weak_response: "Sending a brochure and waiting.",
    },
  ],
  compliance: {
    no_go_language: ["Guaranteed revenue increase", "You will definitely close more deals", "Risk-free"],
    required_disclosures: ["Call recording and AI scoring must be disclosed to the customer's reps."],
  },
  tone: {
    playbook_guidance:
      "Direct and evidence-led. Lead with what the manager will see in the platform, not adjectives.",
    tone_notes: "Manager-first, concrete, never pressure-heavy and never over-promising.",
  },
};

// ---------------------------------------------------------------------------
// Scorecard — fixed four stages only, weights total 100, one criterion each.
// ---------------------------------------------------------------------------
export const STAGE_WEIGHTS = [
  { stage: "intro", weight: 20, guidance: "Judge whether the rep earned the right to run the call." },
  { stage: "discovery", weight: 30, guidance: "Judge the depth of the problem the rep uncovered." },
  { stage: "objection", weight: 30, guidance: "Judge how the price objection was handled against the approved response." },
  { stage: "close", weight: 20, guidance: "Judge whether a real commitment was secured." },
];

export const CRITERIA = [
  {
    stage: "intro",
    label: "Set agenda and establish credibility",
    description: "States the purpose of the call and why UFC Elite is credible for this manager's problem.",
    scoring_guidance: "Full marks only when both an agenda and a credibility anchor land before discovery.",
    emphasis: "standard",
    pass_fail: false,
    critical: false,
    sort_order: 0,
  },
  {
    stage: "discovery",
    label: "Uncover pain, current process and decision route",
    description: "Draws out how calls are reviewed today, what that costs, and who signs off.",
    scoring_guidance: "Full marks only when all three land before any pitch.",
    emphasis: "major",
    pass_fail: false,
    critical: false,
    sort_order: 0,
  },
  {
    stage: "objection",
    label: "Isolate price concern and reframe value",
    description: "Isolates price as the real objection, then reframes on cost per rep against recovered revenue.",
    scoring_guidance: "Immediate discounting scores 0 for this criterion.",
    emphasis: "major",
    pass_fail: false,
    critical: false,
    sort_order: 0,
  },
  {
    stage: "close",
    label: "Secure clear next step and commitment",
    description: "Locks a dated next step with the named decision maker.",
    scoring_guidance: "'I'll follow up' with no date scores 0 for this criterion.",
    emphasis: "major",
    pass_fail: false,
    critical: false,
    sort_order: 0,
  },
];

// ---------------------------------------------------------------------------
// Objection Library — the third Intelligence pillar (Day 236 data layer +
// Day 250/251 WEB MVP). A small, high-quality set of manager-APPROVED
// buyer-pushback guidance so /intelligence?tab=objections is not empty in the
// demo. Categories must be from the API's fixed set (price/timing/authority/
// trust/competitor/fit/logistics/other). Every item is complete enough to be
// approved: label + category + ≥1 buyer phrase + approved response + a
// coaching note. Nothing here couples to scoring, Whisperer or sparring.
// ---------------------------------------------------------------------------
export type SeedObjection = {
  key: string;
  label: string;
  category: string;
  buyer_phrases: string[];
  approved_response: string;
  weak_response_patterns: string[];
  no_go_language: string[];
  coaching_note: string;
};

export const OBJECTIONS: SeedObjection[] = [
  {
    key: "too-expensive",
    label: "Too expensive",
    category: "price",
    buyer_phrases: ["That's too expensive", "Your price is too high", "We don't have the budget"],
    approved_response:
      "Acknowledge the concern, re-anchor to the cost of missed conversions and rep inconsistency, then ask which outcome matters most: lower cost today or higher conversion over the next quarter.",
    weak_response_patterns: ["Discounting immediately", "Defending the price", 'Saying "but it\'s AI"'],
    no_go_language: ["It's cheap compared to hiring", "You can't afford not to"],
    coaching_note: "Coach the rep to connect price back to revenue leakage and manager visibility.",
  },
  {
    key: "need-to-think",
    label: "Need to think about it",
    category: "timing",
    buyer_phrases: ["I need to think about it", "Let me sleep on it", "I'll come back to you"],
    approved_response:
      "Respect the pause, then ask what specifically they need to think through: fit, cost, implementation, or internal buy-in.",
    weak_response_patterns: [
      "Accepting the stall with no next step",
      "Sending a vague follow-up",
      "Pushing harder without diagnosing the concern",
    ],
    no_go_language: ["What's there to think about?", "This offer won't be here forever"],
    coaching_note: "Coach the rep to turn vague delay into a named decision blocker.",
  },
  {
    key: "send-info",
    label: "Send me more information",
    category: "timing",
    buyer_phrases: ["Can you send me some info?", "Just email me the details", "Send over a deck"],
    approved_response:
      "Agree to send a summary, then ask what they want the information to help them decide and book a specific follow-up.",
    weak_response_patterns: [
      "Sending information without a meeting",
      "Ending the call too quickly",
      "Not qualifying the decision criteria",
    ],
    no_go_language: ["Sure, I'll send everything over", "Have a read and let me know"],
    coaching_note: "Coach the rep to keep control of the next step.",
  },
  {
    key: "speak-with-partner",
    label: "Need to speak with my partner",
    category: "authority",
    buyer_phrases: [
      "I need to speak with my partner",
      "I need to run this by my manager",
      "I need to check with the team",
    ],
    approved_response:
      "Acknowledge the need for buy-in, then ask who else is involved and what they will care about most.",
    weak_response_patterns: [
      "Treating it as a final no",
      "Failing to map the decision group",
      "Not offering to join the next conversation",
    ],
    no_go_language: ["You can decide this yourself", "Just convince them"],
    coaching_note: "Coach the rep to identify stakeholders and create a multi-person next step.",
  },
  {
    key: "already-have-training",
    label: "We already have training",
    category: "competitor",
    buyer_phrases: [
      "We already have sales training",
      "We have a manager coaching calls already",
      "We use another tool for this",
    ],
    approved_response:
      "Position Gravix as reinforcement and visibility, not a replacement. Ask how consistently coaching happens today and how managers know it is working.",
    weak_response_patterns: [
      "Attacking the existing solution",
      "Pretending Gravix replaces managers",
      "Ignoring the current process",
    ],
    no_go_language: ["Your current training probably isn't working", "This replaces your manager"],
    coaching_note: "Coach the rep to uncover gaps in consistency, speed, and accountability.",
  },
  {
    key: "not-right-time",
    label: "Not the right time",
    category: "timing",
    buyer_phrases: ["Now isn't the right time", "Maybe next quarter", "We're too busy right now"],
    approved_response:
      "Acknowledge timing, then ask what needs to change before it becomes the right time and whether the current problem is costing them while they wait.",
    weak_response_patterns: [
      "Accepting next quarter with no date",
      "Failing to quantify urgency",
      "Ending without a reactivation plan",
    ],
    no_go_language: ["There's never a perfect time", "You're making excuses"],
    coaching_note: "Coach the rep to create a timed re-entry point and quantify delay cost.",
  },
  {
    key: "distrust-ai",
    label: "I don't trust AI coaching",
    category: "trust",
    buyer_phrases: [
      "I don't trust AI to coach my team",
      "AI feedback can be generic",
      "How do I know it's accurate?",
    ],
    approved_response:
      "Agree that generic AI feedback is not enough, then explain that Gravix uses company context, scorecards, evidence, and manager-controlled standards.",
    weak_response_patterns: ["Overpromising accuracy", "Dismissing the concern", 'Saying "AI is the future"'],
    no_go_language: ["The AI is always right", "Managers won't need to check it"],
    coaching_note: "Coach the rep to position AI as evidence-based support, not unchecked authority.",
  },
  {
    key: "team-fit",
    label: "Not sure it fits our team",
    category: "fit",
    buyer_phrases: [
      "I'm not sure this fits our team",
      "Our sales process is different",
      "We're not a typical sales team",
    ],
    approved_response:
      "Agree that fit matters, then ask what makes their process different and show how Context and Scorecards adapt the review to their standards.",
    weak_response_patterns: ["Forcing a generic pitch", "Ignoring their process", "Claiming it works for everyone"],
    no_go_language: ["Every sales team is basically the same", "It works out of the box for anyone"],
    coaching_note: "Coach the rep to use difference as a discovery path, not an objection to overcome blindly.",
  },
];

// Deterministic id per objection (same uid scheme as the other seeded assets),
// so re-running upserts the same rows and never duplicates.
export function objectionId(key: string): string {
  return uid("UFC_INTEL", `objection-${key}`);
}

// ---------------------------------------------------------------------------
// Proof call — a real-looking demo call that exercises the scorecard.
// ---------------------------------------------------------------------------
// Day 172 demo convention: the human-facing label lives in calls.filename —
// every seeded demo call has title = null and a readable filename, and
// formatCallDisplayTitle falls back to it. The hero call is
// "Nate Diaz — Price Objection Call" under exactly this pattern.
const PROOF_CALL_TITLE = "Nate Diaz — Pricing Follow-up Call";
const PROOF_TRANSCRIPT = [
  "Nate: Thanks for the follow-up, Sarah. I want to cover how your managers review calls today, then agree what happens next.",
  "Prospect: Sounds good. We looked at your pricing and it's a lot for us.",
  "Nate: Understood. Before price — how many calls does a manager actually review in a week right now?",
  "Prospect: Honestly? Maybe three. There's no time for more.",
  "Nate: And what happens to the other calls?",
  "Prospect: Nothing. They just sit there.",
  "Nate: So most of the coaching signal is lost. That's the cost we'd be removing.",
  "Prospect: Maybe, but it's still expensive.",
  "Nate: What are you comparing it against?",
  "Prospect: Nothing specific, it's just a big number.",
  "Nate: Fair. Per rep it works out lower than one lost deal a quarter — I can show you that on your own numbers.",
  "Prospect: That would help. Let me think it over and speak to the team.",
  "Nate: Of course. Shall we put thirty minutes in for Thursday to walk one of your real calls through it?",
  "Prospect: Thursday could work. Send an invite.",
].join("\n");

const PROOF_SHA256 = crypto.createHash("sha256").update(PROOF_TRANSCRIPT).digest("hex");

// Kept within the runtime's 220-char summary clamp (clampText in lib/scoring),
// so the seeded text survives scoring byte-for-byte and displays in full.
const PROOF_SUMMARY =
  "Nate isolated price instead of discounting and exposed the cost of unreviewed calls, but the value reframe came late and the close settled for a soft Thursday slot rather than a confirmed commitment.";

// Seeded demo stage scores — pinned the same way the hero call's are.
// Deliberately mid-range so the demo contrasts with Nate's 45 hero call.
function seededScore(scoringModelVersion: string) {
  return {
    overall: 62,
    summary: PROOF_SUMMARY,
    stages: {
      intro: { score: 70, notes: "Agenda set in the first line and the call had a clear purpose.\nCredibility anchor was thin — no proof point offered before discovery began." },
      discovery: { score: 58, notes: "Strong question exposed that only three calls a week are reviewed, and that the rest are lost.\nNo decision route uncovered — the team who must sign off was never mapped." },
      objection: { score: 55, notes: "Price was isolated with 'what are you comparing it against?' rather than discounted away.\nThe cost-per-rep reframe arrived only after the objection had been repeated twice." },
      close: { score: 65, notes: "A dated slot (Thursday) was proposed rather than a vague follow-up.\nCommitment stayed soft — 'could work' was accepted without confirming the decision maker." },
    },
    moments: [],
    suggestions: [],
    model: scoringModelVersion,
  };
}

async function danaId(): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", DANA_EMAIL).maybeSingle();
  return (data as any)?.id ?? null;
}

async function main() {
  console.log(`\n  Day 224 — UFC Intelligence demo seed${DRY_RUN ? " (dry run)" : ""}`);
  console.log(`  Company: ${DEMO_COMPANY_ID}\n`);

  const { RUBRIC_VERSION, SCORING_MODEL_VERSION, buildDeterministicPromptKey, scoreWithLLM } =
    await loadScoring();

  // Prerequisites.
  const probeCtx = await supa.from("company_context").select("id").limit(1);
  const probeSc = await supa.from("scorecard_versions").select("id").limit(1);
  if (probeCtx.error || probeSc.error) {
    console.error("  ✗ intelligence migrations not applied — run the Day 218/219B SQL first.");
    process.exit(1);
  }

  const dana = await danaId();
  const { data: danaRep } = await supa
    .from("reps").select("org_id, company_id").eq("email", DANA_EMAIL).maybeSingle();
  const orgId = (danaRep as any)?.org_id ?? null;
  if (!dana || !orgId) {
    console.error("  ✗ UFC demo seed not found — run npm run seed:demo first.");
    process.exit(1);
  }
  if (String((danaRep as any)?.company_id) !== DEMO_COMPANY_ID) {
    console.error("  ✗ Dana is not in the demo company — refusing to seed.");
    process.exit(1);
  }

  const compiled = compileContextBlock(UFC_CONTEXT);
  const snapshot = buildVersionSnapshot({ weights: STAGE_WEIGHTS, criteria: CRITERIA });

  if (DRY_RUN) {
    console.log(`  Would publish context v1 (${compiled.length} chars compiled).`);
    console.log(`  Would activate "${SCORECARD_NAME}" v1 with ${CRITERIA.length} criteria.`);
    console.log(`  Would seed + score proof call ${IDS.proofCall} ("${PROOF_CALL_TITLE}").`);
    console.log(`  Would seed ${OBJECTIONS.length} approved objections (${[...new Set(OBJECTIONS.map((o) => o.category))].sort().join(", ")}).`);
    console.log("\n  Dry run — nothing written.");
    return;
  }

  const now = new Date().toISOString();

  // ── 1. Context: draft working copy + published v1 ─────────────────────────
  const { error: ctxErr } = await supa.from("company_context").upsert(
    [
      {
        id: IDS.ctxDraft,
        company_id: DEMO_COMPANY_ID,
        status: "draft",
        version: 0,
        context: UFC_CONTEXT,
        created_by: dana,
        updated_by: dana,
      },
      {
        id: IDS.ctxPublished,
        company_id: DEMO_COMPANY_ID,
        status: "published",
        version: 1,
        context: UFC_CONTEXT,
        compiled_context: compiled,
        created_by: dana,
        updated_by: dana,
        published_by: dana,
        published_at: now,
      },
    ],
    { onConflict: "id" }
  );
  if (ctxErr) throw new Error(`context seed failed: ${ctxErr.message}`);
  console.log(`  ✓ Context published v1 (${compiled.length} chars compiled)`);

  // ── 2. Scorecard: active company default + version 1 ──────────────────────
  const { error: scErr } = await supa.from("scorecards").upsert(
    {
      id: IDS.scorecard,
      company_id: DEMO_COMPANY_ID,
      name: SCORECARD_NAME,
      description: "The scorecard every UFC Elite call is judged against.",
      status: "active",
      is_company_default: true,
      created_by: dana,
      updated_by: dana,
    },
    { onConflict: "id" }
  );
  if (scErr) throw new Error(`scorecard seed failed: ${scErr.message}`);

  const { error: verErr } = await supa.from("scorecard_versions").upsert(
    {
      id: IDS.version,
      scorecard_id: IDS.scorecard,
      company_id: DEMO_COMPANY_ID,
      version: 1,
      status: "active",
      call_types: [],
      origin: "manual",
      snapshot,
      created_by: dana,
      activated_by: dana,
      activated_at: now,
    },
    { onConflict: "id" }
  );
  if (verErr) throw new Error(`scorecard version seed failed: ${verErr.message}`);

  // Relational rows the Studio edits. Replaced wholesale so re-running cannot
  // duplicate them (the unique stage index would reject a second weight row).
  await supa.from("scorecard_criteria").delete().eq("scorecard_version_id", IDS.version);
  await supa.from("scorecard_stage_weights").delete().eq("scorecard_version_id", IDS.version);

  const { error: wErr } = await supa.from("scorecard_stage_weights").insert(
    STAGE_WEIGHTS.map((w) => ({ ...w, scorecard_version_id: IDS.version }))
  );
  if (wErr) throw new Error(`stage weights seed failed: ${wErr.message}`);

  const { error: cErr } = await supa.from("scorecard_criteria").insert(
    CRITERIA.map((c) => ({ ...c, scorecard_version_id: IDS.version }))
  );
  if (cErr) throw new Error(`criteria seed failed: ${cErr.message}`);
  console.log(`  ✓ "${SCORECARD_NAME}" v1 active (company default, ${CRITERIA.length} criteria)`);

  // ── 3. Proof call, scored through the real runtime ────────────────────────
  const { error: callErr } = await supa.from("calls").upsert(
    {
      id: IDS.proofCall,
      user_id: dana,
      org_id: orgId,
      company_id: DEMO_COMPANY_ID,
      filename: PROOF_CALL_TITLE,
      title: PROOF_CALL_TITLE,
      status: "scored",
      kind: "audio",
      mime_type: "audio/mpeg",
      // NOT NULL on calls; this seed is transcript-only, no audio is uploaded.
      storage_path: `ufc-intelligence/${IDS.proofCall}.mp3`,
      audio_path: `ufc-intelligence/${IDS.proofCall}.mp3`,
      size_bytes: 0,
      filesize_bytes: 0,
      sample_rate_hz: 0,
      channels: 1,
      sha256: PROOF_SHA256,
      transcript: PROOF_TRANSCRIPT,
      duration_sec: 512,
      duration_ms: 512_000,
      rep_name: "Nate Diaz",
      tags: [`demo_seed:${SEED_TAG}`],
    },
    { onConflict: "id" }
  );
  if (callErr) throw new Error(`proof call seed failed: ${callErr.message}`);

  // The runtime resolves + keys independently; seeding the cache under the key
  // derived from the just-seeded versions means a HIT only happens if its own
  // resolution agrees. That is what makes the stamped provenance genuine.
  const liveCtx = await resolvePublishedContext(supa, DEMO_COMPANY_ID);
  const liveSc = await resolveActiveScorecard(supa, DEMO_COMPANY_ID, null);
  const key = buildDeterministicPromptKey({
    callId: IDS.proofCall,
    filename: PROOF_CALL_TITLE,
    sha256: PROOF_SHA256,
    transcript: cleanTranscript(PROOF_TRANSCRIPT),
    contextVersion: liveCtx?.context_version ?? null,
    scorecardCacheKey: liveSc.cache_key,
  });
  await supa.from("score_cache").delete().eq("call_sha256", PROOF_SHA256);
  const { error: cacheErr } = await supa.from("score_cache").upsert(
    {
      cache_key: key.key,
      call_sha256: PROOF_SHA256,
      transcript_hash: key.transcriptHash,
      rubric_version: RUBRIC_VERSION,
      prompt_version: "v1",
      model_version: SCORING_MODEL_VERSION,
      result_json: seededScore(SCORING_MODEL_VERSION),
      updated_at: now,
    },
    { onConflict: "cache_key" }
  );
  if (cacheErr) throw new Error(`score cache seed failed: ${cacheErr.message}`);

  const scored = await scoreWithLLM({ supabase: supa, callId: IDS.proofCall });
  if (scored.summary !== PROOF_SUMMARY) {
    throw new Error(
      "proof call did not resolve the seeded assets (cache miss) — provenance would be wrong; seed aborted"
    );
  }

  const { data: proof } = await supa
    .from("calls").select("rubric").eq("id", IDS.proofCall).single();
  const meta = (proof as any)?.rubric?._meta ?? {};
  if (meta.scorecard_name !== SCORECARD_NAME || meta.context_version !== 1) {
    throw new Error(`proof call provenance not stamped as expected: ${JSON.stringify(meta)}`);
  }
  console.log(`  ✓ Proof call scored ${scored.overall}/100 — "${PROOF_CALL_TITLE}"`);
  console.log(`      _meta: ${meta.scorecard_name} v${meta.scorecard_version} · ${meta.scorecard_source} · context v${meta.context_version}`);

  // ── 4. Objection Library — approved buyer-pushback guidance ───────────────
  // Upsert on the deterministic id: re-running refreshes the same rows and
  // never duplicates. Nothing is deleted, so manager-created objections (and
  // the Day 251 archived QA item) are left untouched — the unique live-label
  // index only bites if a manager separately created a LIVE item with one of
  // these labels, which the demo company never does.
  const objectionRows = OBJECTIONS.map((o) => ({
    id: objectionId(o.key),
    company_id: DEMO_COMPANY_ID,
    label: o.label,
    category: o.category,
    status: "approved",
    buyer_phrases: o.buyer_phrases,
    why_it_matters: null,
    approved_response: o.approved_response,
    weak_response_patterns: o.weak_response_patterns,
    no_go_language: o.no_go_language,
    coaching_note: o.coaching_note,
    created_by: dana,
    updated_by: dana,
    approved_by: dana,
    approved_at: SEED_APPROVED_AT,
    updated_at: SEED_APPROVED_AT,
  }));
  const { error: objErr } = await supa
    .from("objection_library_items")
    .upsert(objectionRows, { onConflict: "id" });
  if (objErr) throw new Error(`objection seed failed: ${objErr.message}`);
  const cats = [...new Set(OBJECTIONS.map((o) => o.category))].sort();
  console.log(`  ✓ Objection Library: ${OBJECTIONS.length} approved objections (${cats.join(", ")})`);

  console.log("\n  UFC Intelligence seed complete.");
  console.log(`  Review the proof call at /calls/${IDS.proofCall}`);
  console.log("  The Nate Diaz hero call is untouched (no provenance — the calm default state).");
}

// Only seed when run directly. validate:ufc-intelligence-seed imports the ids
// and payloads from this module to assert against, and importing must never
// trigger a seed run.
const invokedDirectly = (() => {
  const entry = process.argv[1] ?? "";
  return /seed-ufc-intelligence(\.[cm]?ts|\.[cm]?js)?$/.test(entry);
})();

if (invokedDirectly) {
  main().catch((e) => {
    console.error("seed failed:", e?.message || e);
    process.exit(1);
  });
}
