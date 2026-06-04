/**
 * seed-demo-org.ts — Gravix Demo Seeder
 *
 * Creates a fully populated demo org (UFC Elite Sales Team) with
 * real Supabase auth accounts, so every persona can log in via
 * email + password.
 *
 * PREREQUISITE — run once in Supabase SQL editor first:
 *   sql/20260603_fix_auth_user_trigger.sql
 *
 * Usage:  npm run seed:demo
 *
 * Safe to re-run — all inserts are idempotent.
 *
 * Env required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY  (or SUPABASE_SERVICE_KEY)
 *   DEFAULT_ORG_ID
 *
 * Demo password: DemoPass123!
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

// ─── bootstrap ───────────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
const ORG_ID = process.env.DEFAULT_ORG_ID!;
const DEMO_PASSWORD = "DemoPass123!";

if (!SUPABASE_URL || !SERVICE_KEY) {
  console.error("✗  Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
if (!ORG_ID) {
  console.error("✗  Missing DEFAULT_ORG_ID");
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ─── helpers ─────────────────────────────────────────────────────────────────

/** Deterministic v4-format UUID. Used for non-auth IDs (calls, contacts, etc.) */
function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/** Old deterministic rep UUID (before auth-first approach). Used for cleanup only. */
function oldUid(role: "manager" | "rep", name: string): string {
  return uid(role === "manager" ? "DEMO_MANAGER" : "DEMO_REP", name);
}

function rng(seed: string, i: number): number {
  const h = crypto.createHash("sha256").update(`${seed}:${i}`).digest("hex");
  return parseInt(h.slice(0, 8), 16) / 0x100000000;
}
function seededInt(seed: string, i: number, min: number, max: number): number {
  return Math.floor(rng(seed, i) * (max - min + 1)) + min;
}
function daysAgo(n: number): string {
  return new Date(Date.now() - n * 86_400_000).toISOString();
}
function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

async function upsert(
  table: string,
  rows: Record<string, unknown>[],
  conflict: string
): Promise<number> {
  if (!rows.length) return 0;
  const { error } = await supa.from(table).upsert(rows as any[], { onConflict: conflict });
  if (error) { console.warn(`  ⚠  ${table}: ${error.message}`); return 0; }
  return rows.length;
}

// ─── personas ────────────────────────────────────────────────────────────────

const MANAGERS = [
  { name: "Dana White",      email: "dana.white@ufcelite.demo"       },
  { name: "Hunter Campbell", email: "hunter.campbell@ufcelite.demo"  },
];

const REPS = [
  { name: "Conor McGregor",      email: "conor.mcgregor@ufcelite.demo",      strength: "opening_confidence",    weakness: "follow_up_consistency" },
  { name: "Nate Diaz",           email: "nate.diaz@ufcelite.demo",           strength: "objection_handling",    weakness: "closing"               },
  { name: "Anderson Silva",      email: "anderson.silva@ufcelite.demo",      strength: "tonality",              weakness: "discovery_depth"       },
  { name: "Georges St-Pierre",   email: "gsp@ufcelite.demo",                 strength: "consistency",           weakness: null                    },
  { name: "Khabib Nurmagomedov", email: "khabib@ufcelite.demo",             strength: "persistence",           weakness: "rapport_building"      },
  { name: "Jon Jones",           email: "jon.jones@ufcelite.demo",           strength: "conversation_control",  weakness: "qualification"         },
  { name: "Demetrious Johnson",  email: "dj@ufcelite.demo",                  strength: "process_adherence",     weakness: "urgency_creation"      },
  { name: "José Aldo",           email: "jose.aldo@ufcelite.demo",           strength: "discovery",             weakness: "objection_handling"    },
  { name: "Michael Bisping",     email: "michael.bisping@ufcelite.demo",     strength: "energy",                weakness: "patience"              },
  { name: "Daniel Cormier",      email: "daniel.cormier@ufcelite.demo",      strength: "relationship_building", weakness: "closing_confidence"    },
];

const ACCOUNTS = [
  { name: "Nike",           domain: "nike.com"          },
  { name: "Monster Energy", domain: "monsterenergy.com" },
  { name: "Reebok",         domain: "reebok.com"        },
  { name: "ESPN",           domain: "espn.com"          },
  { name: "DraftKings",     domain: "draftkings.com"    },
  { name: "Under Armour",   domain: "underarmour.com"   },
  { name: "EA Sports",      domain: "ea.com"            },
  { name: "Crypto.com",     domain: "crypto.com"        },
  { name: "Manscaped",      domain: "manscaped.com"     },
  { name: "Monster Hydro",  domain: "monsterhydro.com"  },
];

const CONTACTS_BY_ACCOUNT: Array<Array<{ first: string; last: string; title: string }>> = [
  [ {first:"Sarah",    last:"Johnson",   title:"Marketing Director"},    {first:"Tom",       last:"Williams",  title:"VP Partnerships"},       {first:"Chris",    last:"Evans",     title:"Commercial Director"},  {first:"Rachel",   last:"Brown",    title:"Brand Manager"},        {first:"James",    last:"Harrison",  title:"Head of Sponsorships"}  ],
  [ {first:"Emily",    last:"Clarke",    title:"Partnerships Lead"},     {first:"Marcus",    last:"Reid",      title:"VP Revenue"},            {first:"Sophie",   last:"Turner",    title:"Brand Strategy Manager"},{first:"David",    last:"Walsh",    title:"Director of Commercial"},{first:"Natalie",  last:"Fox",       title:"Sponsorship Executive"} ],
  [ {first:"Robert",   last:"King",      title:"Category Director"},     {first:"Jessica",   last:"Moore",     title:"Head of Partnerships"},   {first:"Kevin",    last:"Bell",      title:"VP Sales"},             {first:"Laura",    last:"Grant",    title:"Marketing Manager"},    {first:"Andrew",   last:"Shaw",      title:"Commercial Manager"}    ],
  [ {first:"Olivia",   last:"Scott",     title:"Partnerships Director"}, {first:"Nathan",    last:"Hughes",    title:"Head of Sponsorship"},    {first:"Chloe",    last:"Parker",    title:"Brand Lead"},           {first:"Dylan",    last:"Price",    title:"VP Commercial"},        {first:"Abigail",  last:"Carter",    title:"Head of Sales"}         ],
  [ {first:"Ethan",    last:"Cole",      title:"Director of Marketing"}, {first:"Hannah",    last:"Reed",      title:"Partnership Manager"},    {first:"Owen",     last:"Brooks",    title:"Head of Revenue"},      {first:"Mia",      last:"James",    title:"Sponsorship Director"}, {first:"Liam",     last:"Murray",    title:"Commercial Lead"}       ],
  [ {first:"Sophia",   last:"Bailey",    title:"VP Marketing"},          {first:"Lucas",     last:"Cooper",    title:"Head of Commercial"},     {first:"Grace",    last:"Morgan",    title:"Partnership Lead"},     {first:"Mason",    last:"Rivera",   title:"Director of Revenue"},  {first:"Aria",     last:"Ward",      title:"Senior Brand Manager"}  ],
  [ {first:"Logan",    last:"Murphy",    title:"VP Partnerships"},       {first:"Zoe",       last:"Brooks",    title:"Marketing Director"},     {first:"Noah",     last:"Flores",    title:"Sponsorship Manager"},  {first:"Lily",     last:"Sanders",  title:"Commercial Director"},  {first:"Caleb",    last:"Ross",      title:"Head of Partnerships"}  ],
  [ {first:"Emma",     last:"Powell",    title:"Revenue Director"},      {first:"Jacob",     last:"Long",      title:"VP Brand"},              {first:"Ava",      last:"Patterson", title:"Partnerships Manager"},  {first:"William",  last:"Hughes",   title:"Head of Sales"},        {first:"Isabella", last:"Griffin",   title:"Commercial Manager"}    ],
  [ {first:"Benjamin", last:"Diaz",      title:"Director of Partnerships"},{first:"Charlotte",last:"Simmons",  title:"VP Revenue"},            {first:"Elijah",   last:"Foster",    title:"Brand Strategy Lead"},  {first:"Amelia",   last:"Barnes",   title:"Head of Commercial"},   {first:"Daniel",   last:"Alexander", title:"Sponsorship Director"}  ],
  [ {first:"Scarlett", last:"Gonzalez",  title:"VP Partnerships"},       {first:"Henry",     last:"Russell",   title:"Marketing Lead"},        {first:"Victoria", last:"Jenkins",   title:"Head of Sponsorship"},  {first:"Sebastian",last:"Perry",    title:"Commercial Lead"},      {first:"Madison",  last:"Coleman",   title:"Partnership Director"}  ],
];

const SCORE_BANDS = [
  { min: 40, max: 55 }, { min: 55, max: 70 },
  { min: 70, max: 85 }, { min: 85, max: 95 },
];

const SUMMARIES = [
  "Strong opening, struggled to build discovery depth. Objection handling was reactive rather than proactive.",
  "Good energy throughout. Missed key buying signals during discovery. Closing was abrupt.",
  "Excellent rapport building. Discovery was thorough. Objection around pricing not fully resolved.",
  "Confident opener. Discovery questions were surface-level. Follow-up commitment not secured.",
  "Solid overall structure. Voice confidence was high. Close attempt was rushed.",
  "Great tonality. Discovery uncovered real pain points. Pricing objection handled well.",
  "Weak opener, recovered well. Strong objection handling. Failed to create urgency on close.",
  "Consistent structure. Good qualification. Closing confidence could be stronger.",
  "High energy throughout. Discovery was broad but not deep. Objection handling was solid.",
  "Strong relationship-building skills evident. Pricing conversation well-managed. Close was clean.",
];

const INTERVENTION_TYPES = [
  { section: "objection", severity: "critical", label: "Weak objection handling"         },
  { section: "close",     severity: "critical", label: "Low closing confidence"           },
  { section: "discovery", severity: "low",      label: "Discovery depth insufficient"      },
  { section: "objection", severity: "low",      label: "Pricing pushback not addressed"    },
  { section: "intro",     severity: "low",      label: "Poor opening energy"               },
  { section: "close",     severity: "low",      label: "Urgency not created"               },
  { section: "discovery", severity: "critical", label: "Qualification missed"              },
  { section: "objection", severity: "critical", label: "Competitor objection fumbled"      },
  { section: "close",     severity: "low",      label: "Follow-up not secured"             },
  { section: "intro",     severity: "critical", label: "Low confidence opening"            },
];

// ─── auth user resolution ─────────────────────────────────────────────────────

type Persona = { name: string; email: string; role: "Manager" | "SalesRep" };

/**
 * Create a Supabase auth user, or find one if they already exist.
 * Returns the auth.users.id UUID.
 */
async function getOrCreateAuthUser(
  email: string,
  name: string,
  printRawError: boolean
): Promise<string | null> {
  const { data, error } = await supa.auth.admin.createUser({
    email,
    password: DEMO_PASSWORD,
    email_confirm: true,
    user_metadata: { name, demo: true, org_id: ORG_ID },
    app_metadata: { org_id: ORG_ID },
  });

  if (!error && data?.user?.id) return data.user.id;

  if (printRawError && error) {
    console.error("\n  ── RAW SUPABASE ERROR ────────────────────────────────");
    console.error("  email      :", email);
    console.error("  message    :", error.message);
    console.error("  status     :", (error as any).status);
    console.error("  name       :", error.name);
    console.error("  stack      :", (error as any).stack ?? "(no stack)");
    console.error("  full error :", JSON.stringify(error, null, 2));
    console.error("  data       :", JSON.stringify(data, null, 2));
    console.error("  ─────────────────────────────────────────────────────\n");
  }

  // Duplicate — find existing user by email
  if (
    error?.message?.toLowerCase().includes("already") ||
    error?.message?.toLowerCase().includes("duplicate") ||
    (error as any)?.status === 422
  ) {
    let page = 1;
    while (true) {
      const { data: list, error: le } = await supa.auth.admin.listUsers({ page, perPage: 1000 });
      if (le || !list?.users?.length) break;
      const found = list.users.find((u: any) => (u.email as string)?.toLowerCase() === email.toLowerCase());
      if (found) return (found as any).id as string;
      if (list.users.length < 1000) break;
      page++;
    }
  }

  return null;
}

async function resolveAuthUsers(
  personas: Persona[]
): Promise<Map<string, string>> {
  // Returns email → authUuid map
  const map = new Map<string, string>();
  let failed = 0;

  let firstFailure = true;
  for (const p of personas) {
    const authId = await getOrCreateAuthUser(p.email, p.name, firstFailure);
    if (authId) {
      map.set(p.email, authId);
    } else {
      if (firstFailure) firstFailure = false; // only print raw error once
      failed++;
    }
  }

  if (failed > 0) {
    console.error(`  ✗  ${failed}/${personas.length} auth user(s) failed — see raw error above.`);
    if (map.size === 0) process.exit(1);
  }

  return map;
}

// ─── cleanup old deterministic data ──────────────────────────────────────────

/**
 * When we switch from deterministic UUIDs to auth UUIDs, the old seed data
 * (calls, contacts, etc.) still references the old UUID. This function
 * removes the old data so the fresh seed inserts cleanly.
 *
 * Safe to call when there's nothing to clean (all deletes become no-ops).
 */
async function cleanOldDeterministicData(
  authMap: Map<string, string>,
  allPersonas: Persona[]
): Promise<void> {
  const toClean: Array<{ oldId: string; newId: string; name: string }> = [];

  for (const p of allPersonas) {
    const oldId = oldUid(p.role === "Manager" ? "manager" : "rep", p.name);
    const newId = authMap.get(p.email);
    if (newId && oldId !== newId) {
      toClean.push({ oldId, newId, name: p.name });
    }
  }

  if (!toClean.length) return;

  process.stdout.write(`  → Cleaning ${toClean.length} old deterministic records... `);

  for (const { oldId } of toClean) {
    // Old call IDs derived from old rep UUID
    const oldCallIds = Array.from({ length: 10 }, (_, i) =>
      uid("DEMO_CALL", `${oldId}:${i}`)
    );

    // Delete in FK-safe order
    await supa.from("crm_call_links").delete().in("call_id", oldCallIds);
    await supa.from("call_scores").delete().in("call_id", oldCallIds);
    await supa.from("calls").delete().in("id", oldCallIds);
    await supa.from("assignments").delete().eq("rep_id", oldId);
    await supa.from("crm_activities").delete().eq("rep_id", oldId);
    await supa.from("crm_activities").delete().eq("user_id", oldId);
    await supa.from("crm_contacts").delete().eq("user_id", oldId);
    await supa.from("reps").delete().eq("id", oldId);
  }

  console.log("done");
}

// ─── seed functions ───────────────────────────────────────────────────────────

async function seedReps(authMap: Map<string, string>): Promise<void> {
  process.stdout.write("  → Reps & Managers... ");

  const rows: Record<string, unknown>[] = [];

  for (const m of MANAGERS) {
    const id = authMap.get(m.email);
    if (!id) continue;
    rows.push({ id, org_id: ORG_ID, name: m.name, tier: "Manager",  xp: seededInt(m.name, 0, 800, 1500), created_at: daysAgo(seededInt(m.name, 1, 90, 180)) });
  }
  for (const r of REPS) {
    const id = authMap.get(r.email);
    if (!id) continue;
    rows.push({ id, org_id: ORG_ID, name: r.name, tier: "SalesRep", xp: seededInt(r.name, 0, 200, 1200), created_at: daysAgo(seededInt(r.name, 1, 60, 120)) });
  }

  const n = await upsert("reps", rows, "id");
  console.log(`${n} upserted`);
}

async function seedAccounts(): Promise<Record<string, string>> {
  process.stdout.write("  → Accounts... ");

  const rows = ACCOUNTS.map((a) => ({
    id:         uid("DEMO_ACCOUNT", a.name),
    org_id:     ORG_ID,
    name:       a.name,
    domain:     a.domain,
    created_at: daysAgo(seededInt(a.name, 0, 10, 60)),
  }));

  const n = await upsert("crm_accounts", rows, "id");
  console.log(`${n} upserted`);

  return Object.fromEntries(rows.map((r) => [r.name, r.id]));
}

async function seedContacts(authMap: Map<string, string>): Promise<void> {
  process.stdout.write("  → Contacts... ");

  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];
  const rows: Record<string, unknown>[] = [];

  ACCOUNTS.forEach((account, ai) => {
    const repId = repIds[ai % repIds.length];
    CONTACTS_BY_ACCOUNT[ai].forEach((t, ci) => {
      rows.push({
        id:               uid("DEMO_CONTACT", `${account.name}:${t.first}:${t.last}`),
        org_id:           ORG_ID,
        user_id:          repId,
        first_name:       t.first,
        last_name:        t.last,
        email:            `${t.first.toLowerCase()}.${t.last.toLowerCase()}@${account.domain}`,
        company:          account.name,
        last_contacted_at: rng(`lc:${account.name}`, ci) > 0.3
          ? daysAgo(seededInt(`lc_day:${account.name}`, ci, 1, 28))
          : null,
        created_at:       daysAgo(seededInt(`created:${account.name}`, ci, 5, 55)),
      });
    });
  });

  const n = await upsert("crm_contacts", rows, "id");
  console.log(`${n} upserted`);
}

type SeedCall = { id: string; user_id: string; created_at: string };

async function seedCalls(authMap: Map<string, string>): Promise<SeedCall[]> {
  process.stdout.write("  → Calls + Scores... ");

  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];
  const callRows: Record<string, unknown>[] = [];
  const scoreRows: Record<string, unknown>[] = [];
  const result: SeedCall[] = [];

  repIds.forEach((repId) => {
    for (let ci = 0; ci < 10; ci++) {
      const callId    = uid("DEMO_CALL",  `${repId}:${ci}`);
      const scoreId   = uid("DEMO_SCORE", `${repId}:${ci}`);
      const band      = SCORE_BANDS[seededInt(`band:${repId}`, ci, 0, 3)];
      const overall   = seededInt(`overall:${repId}`, ci, band.min, band.max);
      const intro     = clamp(seededInt(`intro:${repId}`, ci, overall - 15, overall + 15), 10, 100);
      const discovery = clamp(seededInt(`disc:${repId}`,  ci, overall - 20, overall + 10), 10, 100);
      const objection = clamp(seededInt(`obj:${repId}`,   ci, overall - 20, overall + 15), 10, 100);
      const close     = clamp(seededInt(`close:${repId}`, ci, overall - 25, overall + 10), 10, 100);
      const voice     = clamp(seededInt(`voice:${repId}`, ci, overall - 10, overall + 10), 10, 100);
      const dayOffset = seededInt(`day:${repId}`, ci, 1, 60);
      const summary   = SUMMARIES[seededInt(`sum:${repId}`, ci, 0, SUMMARIES.length - 1)];
      const path      = `demo/seed/${ORG_ID}/${repId}/call_${ci}.mp3`;

      const rubric = {
        overall, stages: {
          intro:     { score: intro,     notes: "Demo." },
          discovery: { score: discovery, notes: "Demo." },
          objection: { score: objection, notes: "Demo." },
          close:     { score: close,     notes: "Demo." },
        },
        voice_score: voice,
        voice_rubric: { overall: voice, clarity: clamp(seededInt(`clarity:${repId}`, ci, 55, 95), 0, 100), confidence: clamp(seededInt(`conf:${repId}`, ci, 50, 95), 0, 100), pace: clamp(seededInt(`pace:${repId}`, ci, 55, 90), 0, 100) },
        review_tags: { filler_density: parseFloat((rng(`filler:${repId}`, ci) * 0.12).toFixed(3)) },
      };

      callRows.push({ id: callId, user_id: repId, org_id: ORG_ID, storage_path: path, audio_path: path, filename: `demo-call-${ci + 1}.mp3`, status: "scored", score_overall: overall, summary, rubric, created_at: daysAgo(dayOffset) });
      scoreRows.push({ id: scoreId, call_id: callId, user_id: repId, overall, rubric, created_at: daysAgo(dayOffset) });
      result.push({ id: callId, user_id: repId, created_at: daysAgo(dayOffset) });
    }
  });

  const cn = await upsert("calls", callRows, "id");
  const sn = await upsert("call_scores", scoreRows, "id");
  console.log(`${cn} calls, ${sn} scores`);
  return result;
}

async function seedCallLinks(calls: SeedCall[], authMap: Map<string, string>): Promise<void> {
  process.stdout.write("  → Call links... ");

  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];
  const rows: Record<string, unknown>[] = [];

  repIds.forEach((repId, ri) => {
    const accountName = ACCOUNTS[ri % ACCOUNTS.length].name;
    const repCalls = calls.filter((c) => c.user_id === repId).slice(0, 3);
    const templates = CONTACTS_BY_ACCOUNT[ri % CONTACTS_BY_ACCOUNT.length];

    repCalls.forEach((call, ci) => {
      const t = templates[ci % templates.length];
      const contactId = uid("DEMO_CONTACT", `${accountName}:${t.first}:${t.last}`);
      rows.push({
        id:         uid("DEMO_CALL_LINK", `${call.id}:${contactId}`),
        call_id:    call.id,
        contact_id: contactId,
      });
    });
  });

  const n = await upsert("crm_call_links", rows, "id");
  console.log(`${n} upserted`);
}

async function seedAssignments(
  calls: SeedCall[],
  authMap: Map<string, string>
): Promise<void> {
  process.stdout.write("  → Assignments... ");

  const managerId = authMap.get(MANAGERS[0].email)!;
  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];

  const TITLES = [
    "Review and respond to objection handling",
    "Improve closing structure — add urgency",
    "Deepen discovery questioning technique",
    "Work on tone and vocal confidence",
    "Practice price anchoring framework",
    "Complete objection handling drill",
    "Review losing call and identify gaps",
    "Practice 3-step closing sequence",
    "Improve follow-up commitment ritual",
    "Work on qualifying earlier in call",
    "Reduce filler word frequency",
    "Practice competitor differentiation script",
    "Role-play difficult buyer personas",
    "Complete weekly sparring session",
    "Review recorded call and self-score",
    "Submit reflection on last deal lost",
    "Practice cold open without filler",
    "Complete discovery framework review",
    "Run through pricing objection scenarios",
    "Shadow top performer on pricing call",
  ];

  const rows: Record<string, unknown>[] = [];

  repIds.forEach((repId, ri) => {
    for (let ai = 0; ai < 2; ai++) {
      const title      = TITLES[(ri * 2 + ai) % TITLES.length];
      const targetCall = calls.filter((c) => c.user_id === repId)[ai % 10];
      const statusSeed = seededInt(`status:${repId}`, ai, 0, 2);

      let status: string, dueAt: string, completedAt: string | null = null;
      if (statusSeed === 0) {
        status = "assigned"; dueAt = daysAgo(-seededInt(`due_f:${repId}`, ai, 3, 14));
      } else if (statusSeed === 1) {
        status = "completed"; dueAt = daysAgo(seededInt(`due_c:${repId}`, ai, 5, 20));
        completedAt = daysAgo(seededInt(`comp:${repId}`, ai, 1, 4));
      } else {
        status = "assigned"; dueAt = daysAgo(seededInt(`due_o:${repId}`, ai, 3, 10));
      }

      rows.push({
        id:           uid("DEMO_ASSIGN", `${repId}:${ai}`),
        rep_id:       repId,
        manager_id:   managerId,
        type:         "call_review",
        target_id:    targetCall?.id ?? null,
        title, status, due_at: dueAt, completed_at: completedAt,
        source:       "manual",
        created_at:   daysAgo(seededInt(`created:${repId}`, ai, 5, 30)),
      });
    }
  });

  const n = await upsert("assignments", rows, "id");
  console.log(`${n} upserted`);
}

async function seedInterventions(calls: SeedCall[], authMap: Map<string, string>): Promise<void> {
  process.stdout.write("  → Interventions... ");

  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];
  const rows: Record<string, unknown>[] = [];

  for (let i = 0; i < 15; i++) {
    const repId    = repIds[i % repIds.length];
    const repCalls = calls.filter((c) => c.user_id === repId);
    const call     = repCalls[i % Math.max(repCalls.length, 1)];
    const iType    = INTERVENTION_TYPES[i % INTERVENTION_TYPES.length];

    rows.push({
      id: uid("DEMO_INTERVENTION", `${repId}:${i}`),
      org_id: ORG_ID, rep_id: repId, user_id: repId,
      call_id: call?.id ?? null,
      type:   "review_flag",
      title:  iType.label,
      source: "scoring_engine",
      status: "open",
      meta: { flag_section: iType.section, flag_severity: iType.severity, flag_key: `${iType.section}_weakness_${iType.severity}`, score: seededInt(`intervention:${repId}`, i, 25, 55), label: iType.label, demo: true },
      created_at: daysAgo(seededInt(`intervention_day:${repId}`, i, 1, 30)),
    });
  }

  const n = await upsert("crm_activities", rows, "id");
  console.log(`${n} upserted`);
}

async function seedCrmActivities(calls: SeedCall[], authMap: Map<string, string>): Promise<void> {
  process.stdout.write("  → CRM activity log... ");

  const repIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];
  const ACTION_TYPES = [
    { type: "call_logged",    title: "Follow-up call logged"    },
    { type: "email_sent",     title: "Proposal email sent"      },
    { type: "meeting_booked", title: "Demo booked"              },
    { type: "note_added",     title: "Contact note added"       },
    { type: "deal_updated",   title: "Opportunity stage updated" },
  ];

  const rows: Record<string, unknown>[] = [];

  repIds.forEach((repId, ri) => {
    const accountName = ACCOUNTS[ri % ACCOUNTS.length].name;
    const repCalls = calls.filter((c) => c.user_id === repId);

    for (let ai = 0; ai < 3; ai++) {
      const aType = ACTION_TYPES[ai % ACTION_TYPES.length];
      rows.push({
        id: uid("DEMO_CRM_ACT", `${repId}:${ai}`),
        org_id: ORG_ID, rep_id: repId, user_id: repId,
        call_id: repCalls[ai % Math.max(repCalls.length, 1)]?.id ?? null,
        type:   aType.type,
        title:  aType.title,
        status: "open",
        source: "manual",
        meta:   { account_name: accountName, demo: true },
        created_at: daysAgo(seededInt(`crm_act_day:${repId}`, ai, 1, 20)),
      });
    }
  });

  const n = await upsert("crm_activities", rows, "id");
  console.log(`${n} upserted`);
}

// ─── validation + credential table ───────────────────────────────────────────

async function countIn(table: string, col: string, vals: string[]): Promise<number> {
  if (!vals.length) return 0;
  const { count } = await supa.from(table).select("id", { count: "exact", head: true }).in(col, vals);
  return count ?? 0;
}
async function countWhere(table: string, filters: Record<string, unknown>): Promise<number> {
  let q = supa.from(table).select("id", { count: "exact", head: true });
  for (const [k, v] of Object.entries(filters)) q = (q as any).eq(k, v);
  const { count } = await q;
  return count ?? 0;
}

async function printValidation(allPersonas: Persona[], authMap: Map<string, string>): Promise<void> {
  const repIds = allPersonas.map((p) => authMap.get(p.email)).filter(Boolean) as string[];
  const demoRepIds = REPS.map((r) => authMap.get(r.email)).filter(Boolean) as string[];

  const managers      = await countWhere("reps", { org_id: ORG_ID, tier: "Manager"  });
  const reps          = await countWhere("reps", { org_id: ORG_ID, tier: "SalesRep" });
  const accounts      = await countWhere("crm_accounts", { org_id: ORG_ID });
  const contacts      = await countWhere("crm_contacts", { org_id: ORG_ID });
  const calls         = await countWhere("calls", { org_id: ORG_ID });
  const assignments   = await countIn("assignments",   "rep_id", demoRepIds);
  const { count: iv } = await supa.from("crm_activities").select("id", { count: "exact", head: true }).in("rep_id", demoRepIds).eq("type", "review_flag");

  console.log("\n  ── Counts ────────────────────────────────────────");
  console.log(`  Managers:      ${managers}`);
  console.log(`  Reps:          ${reps}`);
  console.log(`  Accounts:      ${accounts}`);
  console.log(`  Contacts:      ${contacts}`);
  console.log(`  Calls:         ${calls}`);
  console.log(`  Assignments:   ${assignments}`);
  console.log(`  Interventions: ${iv ?? 0}  (review_flag)`);

  console.log("\n  ── Demo credentials ──────────────────────────────");
  console.log(`  ${"Email".padEnd(40)} ${"Role".padEnd(12)} UUID`);
  console.log(`  ${"─".repeat(40)} ${"─".repeat(12)} ${"─".repeat(36)}`);

  for (const m of MANAGERS) {
    const id = authMap.get(m.email) ?? "(no auth user)";
    console.log(`  ${m.email.padEnd(40)} ${"Manager".padEnd(12)} ${id}`);
  }
  for (const r of REPS) {
    const id = authMap.get(r.email) ?? "(no auth user)";
    console.log(`  ${r.email.padEnd(40)} ${"SalesRep".padEnd(12)} ${id}`);
  }

  console.log(`\n  Password: ${DEMO_PASSWORD}`);
  console.log("  ──────────────────────────────────────────────────");
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log("\n  Gravix Demo Seeder — UFC Elite Sales Team");
  console.log(`  Org    : ${ORG_ID}`);
  console.log(`  DB     : ${SUPABASE_URL}\n`);

  // Build full persona list with role tags
  const allPersonas: Persona[] = [
    ...MANAGERS.map((m) => ({ ...m, role: "Manager" as const })),
    ...REPS.map((r)     => ({ ...r, role: "SalesRep" as const })),
  ];

  try {
    // Phase 1 — Auth users (must succeed before anything else)
    process.stdout.write("  → Auth users... ");
    const authMap = await resolveAuthUsers(allPersonas);
    console.log(`${authMap.size}/${allPersonas.length} resolved`);

    // Phase 2 — Clean up any old deterministic-UUID data from previous seed runs
    await cleanOldDeterministicData(authMap, allPersonas);

    // Phase 3 — Seed app data using auth UUIDs
    await seedReps(authMap);
    await seedAccounts();
    await seedContacts(authMap);
    const calls = await seedCalls(authMap);
    await seedCallLinks(calls, authMap);
    await seedAssignments(calls, authMap);
    await seedInterventions(calls, authMap);
    await seedCrmActivities(calls, authMap);

    await printValidation(allPersonas, authMap);

    console.log("\n  ✓  Demo seed complete.\n");
    console.log("  NOTE: Email/password login requires the Email provider");
    console.log("  to be enabled in Supabase → Authentication → Providers → Email.\n");
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("\n  ✗  Seed failed:", msg);
    process.exit(1);
  }
}

main();
