/**
 * Day 169 — UFC Elite demo story seeder.
 *
 * Seeds the minimum viable Whisperer / AI Discovery / sparring-proof story
 * into the UFC Elite demo org so the whole lighthouse demo runs as
 * dana.white@ufcelite.demo without switching logins:
 *
 *   1. One ended Whisperer session linked to Nate Diaz's low-score demo call.
 *   2. Three trigger moments (price / authority / send-info stall).
 *   3. Three raw untriggered "send over some information" segments — AI
 *      Discovery mines these into a candidate naturally; no decision rows are
 *      seeded, so the manager approval gate stays intact.
 *   4. One custom trigger ("Partner approval") stamped as created by Dana —
 *      seed data representing a manager-created rule, not product behaviour.
 *   5. One open queue-assigned sparring drill for Nate (Coaching Queue shape).
 *   6. Two completed sparring drills with Day 155 completion-proof meta,
 *      matched to two seeded sparring sessions (62 → 78 = improving trend).
 *
 * Idempotent: every row uses a deterministic UUID (uid("UFC_STORY", key)) and
 * upsert on id; re-running refreshes the same rows. All meta carries
 * demo_seed: "ufc-story". Complements scripts/seed-demo-org.ts (personas,
 * calls, assignments) — run that first on a fresh environment.
 *
 * Run: npx tsx scripts/seed-ufc-demo-story.ts [--dry-run]
 */
import { createClient } from "@supabase/supabase-js";
import crypto from "crypto";
import "dotenv/config";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SERVICE_KEY) {
  console.error("✗  Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const DEMO_COMPANY_ID = process.env.DEMO_COMPANY_ID || "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const DANA_EMAIL = "dana.white@ufcelite.demo";
const REP_EMAIL = "nate.diaz@ufcelite.demo";
const SEED_TAG = "ufc-story";
// Day 172 — demo-facing identity for the hero call (existing columns only).
const HERO_CALL_TITLE = "Nate Diaz — Price Objection Call";

const dryRun = process.argv.includes("--dry-run");

/** Deterministic v4-format UUID (same scheme as seed-demo-org.ts). */
function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`UFC_STORY::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function daysAgo(n: number, minuteOffset = 0): string {
  return new Date(Date.now() - n * 86_400_000 + minuteOffset * 60_000).toISOString();
}

async function upsert(table: string, rows: Record<string, unknown>[]): Promise<number> {
  if (dryRun) {
    console.log(`  DRY   ${table}: ${rows.length} row(s)`);
    for (const r of rows) console.log(`        id=${r.id}`);
    return rows.length;
  }
  const { error } = await supa.from(table).upsert(rows as any[], { onConflict: "id" });
  if (error) throw new Error(`${table}: ${error.message}`);
  console.log(`  OK    ${table}: ${rows.length} row(s) upserted`);
  return rows.length;
}

async function main() {
  console.log(`\n  UFC demo story seeder${dryRun ? " (dry run)" : ""}`);
  console.log(`  Company: ${DEMO_COMPANY_ID}\n`);

  // ── Resolve live ids (no hardcoded auth uuids) ──
  const { data: dana } = await supa
    .from("users").select("id, company_id").eq("email", DANA_EMAIL).maybeSingle();
  if (!dana?.id) throw new Error(`manager ${DANA_EMAIL} not found — run seed:demo first`);
  if (String(dana.company_id) !== DEMO_COMPANY_ID) throw new Error("manager is not in the demo company");

  const { data: rep } = await supa
    .from("reps").select("id, name, org_id, company_id").eq("email", REP_EMAIL).maybeSingle();
  if (!rep?.id) throw new Error(`rep ${REP_EMAIL} not found — run seed:demo first`);
  if (String(rep.company_id) !== DEMO_COMPANY_ID) throw new Error("rep is not in the demo company");

  const { data: call } = await supa
    .from("calls")
    .select("id, score_overall, created_at")
    .eq("user_id", rep.id)
    .eq("status", "scored")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (!call?.id) throw new Error("no scored demo call for the rep — run seed:demo first");

  console.log(`  Manager: ${DANA_EMAIL} (${dana.id})`);
  console.log(`  Rep    : ${rep.name} (${rep.id})`);
  console.log(`  Call   : ${call.id} (score ${call.score_overall})\n`);

  const tenant = { org_id: null, company_id: DEMO_COMPANY_ID, office_id: null };

  // ── 0. Friendly hero-call identity (Day 172 — replaces demo-call-N.mp3) ──
  if (dryRun) {
    console.log(`  DRY   calls: would title hero call "${HERO_CALL_TITLE}"`);
  } else {
    const { error: titleErr } = await supa
      .from("calls")
      .update({ filename: HERO_CALL_TITLE, rep_name: rep.name })
      .eq("id", call.id);
    if (titleErr) throw new Error(`calls hero title: ${titleErr.message}`);
    console.log(`  OK    calls: hero call titled "${HERO_CALL_TITLE}"`);
  }

  // ── 1. Whisperer session (ended, linked to the demo call) ──
  const sessionId = uid("whisperer-session:1");
  await upsert("whisperer_sessions", [{
    id: sessionId,
    rep_id: rep.id,
    user_id: rep.id,
    ...tenant,
    call_id: call.id,
    status: "ended",
    started_at: daysAgo(3),
    ended_at: daysAgo(3, 14),
    latency_p50_ms: 320,
    latency_p95_ms: 610,
    meta: { source: "live", demo_seed: SEED_TAG },
    created_at: daysAgo(3),
    updated_at: daysAgo(3, 14),
  }]);

  // ── 2. Trigger moments (price / authority / send-info stall) ──
  await upsert("whisperer_triggers", [
    {
      id: uid("trigger:price"),
      session_id: sessionId,
      type: "price",
      phrase: "too expensive",
      segment_text: "Honestly the whole package feels too expensive for what we would actually use — finance will push back hard on this.",
      confidence: 90,
      suggestion: {
        emoji: "💥",
        title: "Handle price objection",
        urgency: "high",
        response: "Acknowledge it, then reframe on value: “Fair enough — if it paid for itself in 60 days, would the price still be the blocker?”",
      },
      latency_ms: 340,
      detected_at: daysAgo(3, 4),
      suggestion_outcome: "used",
      suggestion_outcome_at: daysAgo(3, 5),
      suggestion_outcome_by: rep.id,
      meta: { demo_seed: SEED_TAG },
      created_at: daysAgo(3, 4),
    },
    {
      id: uid("trigger:authority"),
      session_id: sessionId,
      type: "authority",
      phrase: "speak with my partner",
      segment_text: "I like it, but I would need to speak with my partner before we commit to anything at this level.",
      confidence: 86,
      suggestion: {
        emoji: "🤝",
        title: "Bring the partner in",
        urgency: "medium",
        response: "Make the next step easy: “Completely fair — would a 15-minute call with both of you this week help you decide together?”",
      },
      latency_ms: 410,
      detected_at: daysAgo(3, 8),
      meta: { demo_seed: SEED_TAG },
      created_at: daysAgo(3, 8),
    },
    {
      id: uid("trigger:stall"),
      session_id: sessionId,
      type: "send_info",
      phrase: "send over some information",
      segment_text: "Could you just send over some information and we will have a proper look when things calm down?",
      confidence: 82,
      suggestion: {
        emoji: "📬",
        title: "Convert the brush-off",
        urgency: "medium",
        response: "Agree, then anchor a time: “Of course — I’ll send it today. Can we book 10 minutes Thursday so I can walk you through the two pages that matter?”",
      },
      latency_ms: 290,
      detected_at: daysAgo(3, 11),
      meta: { demo_seed: SEED_TAG },
      created_at: daysAgo(3, 11),
    },
  ]);

  // ── 3. Raw untriggered segments → AI Discovery mines a candidate ──
  // Same recurring stall pattern across the session so discovery clusters it
  // (minSeenCount 2). No candidate/decision rows are written — the candidate
  // appears through the normal read-only mining path and stays approval-gated.
  const SEGMENTS = [
    "Can you just send over some information and we will take a look next quarter?",
    "Best thing is to send over some information for the team to review first.",
    "Just send over some information and if it looks right we will set something up.",
  ];
  await upsert("whisperer_segments", SEGMENTS.map((text, i) => ({
    id: uid(`segment:stall:${i}`),
    session_id: sessionId,
    call_id: call.id,
    user_id: rep.id,
    ...tenant,
    speaker: "prospect",
    speaker_role: "prospect",
    text,
    is_final: true,
    source: "live",
    triggers_count: 0,
    created_at: daysAgo(3, 6 + i),
  })));

  // ── 4. Custom trigger (manager-created seed data, created_by Dana) ──
  await upsert("whisperer_trigger_library", [{
    id: uid("library:partner-approval"),
    ...tenant,
    created_by: dana.id,
    name: "Partner approval",
    description: "Prospect defers the decision to a business partner or co-owner.",
    type: "custom",
    match_phrases: ["speak with my partner", "check with my partner", "talk to my business partner"],
    match_keywords: ["partner"],
    suggestion_title: "Partner approval play",
    suggestion_response: "Offer to make the joint decision easy: “Would a short call with both of you help? I can walk through the numbers in 15 minutes.”",
    urgency: "medium",
    emoji: "🤝",
    enabled: true,
    priority: 60,
    meta: { demo_seed: SEED_TAG },
    created_at: daysAgo(2),
    updated_at: daysAgo(2),
  }]);

  // ── 5 + 6. Queue-assigned sparring: one open drill + two completed with proof ──
  const DRILL = "Objection Handling Drill";
  const queueMeta = {
    assignment_origin: "manager_dashboard",
    origin_label: "Coaching Queue",
    flag_section: "objection",
    priority: "high",
    recommended_drill: DRILL,
    source_call_id: call.id,
    demo_seed: SEED_TAG,
  };

  const spar1 = uid("sparring-session:1");
  const spar2 = uid("sparring-session:2");
  const assignOpen = uid("assignment:sparring:open");
  const assignDone1 = uid("assignment:sparring:done:1");
  const assignDone2 = uid("assignment:sparring:done:2");

  await upsert("assignments", [
    {
      id: assignOpen,
      rep_id: rep.id,
      manager_id: dana.id,
      type: "sparring",
      target_id: call.id,
      title: `Recommended drill: ${DRILL}`,
      status: "assigned",
      due_at: daysAgo(-5),
      created_at: daysAgo(2),
      source: "manager_dashboard",
      office_id: tenant.office_id,
      company_id: tenant.company_id,
      meta: queueMeta,
    },
    {
      id: assignDone1,
      rep_id: rep.id,
      manager_id: dana.id,
      type: "sparring",
      target_id: call.id,
      title: `Recommended drill: ${DRILL}`,
      status: "completed",
      due_at: daysAgo(9),
      created_at: daysAgo(14),
      completed_at: daysAgo(10),
      completed_by: "rep",
      source: "manager_dashboard",
      office_id: tenant.office_id,
      company_id: tenant.company_id,
      meta: {
        ...queueMeta,
        completed_via: "sparring_session_match",
        matched_sparring_session_id: spar1,
        completion_score: 62,
        completed_session_at: daysAgo(10),
        completed_from_dashboard: true,
      },
    },
    {
      id: assignDone2,
      rep_id: rep.id,
      manager_id: dana.id,
      type: "sparring",
      target_id: call.id,
      title: `Recommended drill: ${DRILL}`,
      status: "completed",
      due_at: daysAgo(2),
      created_at: daysAgo(7),
      completed_at: daysAgo(3),
      completed_by: "rep",
      source: "manager_dashboard",
      office_id: tenant.office_id,
      company_id: tenant.company_id,
      meta: {
        ...queueMeta,
        completed_via: "sparring_session_match",
        matched_sparring_session_id: spar2,
        completion_score: 78,
        completed_session_at: daysAgo(3),
        completed_from_dashboard: true,
      },
    },
  ]);

  const sessionSummary = (overall: number, summaryText: string) => ({
    session_summary: {
      overall,
      weakestDimension: "objection_handling",
      strongestDimension: "confidence",
      recommendedDrill: DRILL,
      summaryText,
      nextBestAction: "Re-run the drill against a harder persona this week.",
      turnCount: 12,
    },
    ended: true,
    demo_seed: SEED_TAG,
  });

  await upsert("sparring_sessions", [
    {
      id: spar1,
      rep_id: rep.id,
      ...tenant,
      persona_id: "sceptical",
      difficulty: "standard",
      status: "completed",
      assignment_id: assignDone1,
      total_score: 62,
      score: 62,
      duration_ms: 9 * 60_000,
      xp_awarded: 40,
      summary: "Held the frame early but conceded ground on price pushback; closing ask arrived late.",
      completed_at: daysAgo(10),
      created_at: daysAgo(10, -12),
      meta: sessionSummary(62, "Held the frame early but conceded ground on price pushback; closing ask arrived late."),
    },
    {
      id: spar2,
      rep_id: rep.id,
      ...tenant,
      persona_id: "sceptical",
      difficulty: "standard",
      status: "completed",
      assignment_id: assignDone2,
      total_score: 78,
      score: 78,
      duration_ms: 11 * 60_000,
      xp_awarded: 55,
      summary: "Clear improvement — acknowledged the objection, reframed on value and asked for the close twice.",
      completed_at: daysAgo(3),
      created_at: daysAgo(3, -14),
      meta: sessionSummary(78, "Clear improvement — acknowledged the objection, reframed on value and asked for the close twice."),
    },
  ]);

  console.log(`\n  Seeded ids:`);
  console.log(`    whisperer session : ${sessionId}`);
  console.log(`    sparring sessions : ${spar1}, ${spar2}`);
  console.log(`    assignments       : ${assignOpen} (open), ${assignDone1}, ${assignDone2}`);
  console.log(`    custom trigger    : ${uid("library:partner-approval")}`);
  console.log(`\n  ✓  UFC demo story seed ${dryRun ? "dry run complete" : "complete"}.\n`);
}

main().catch((e) => {
  console.error("\n  ✗  Seed failed:", e?.message || e);
  process.exit(1);
});
