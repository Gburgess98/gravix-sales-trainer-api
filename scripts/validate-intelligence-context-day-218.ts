/**
 * validate-intelligence-context-day-218.ts
 *
 * Day 218 — Context Engine data layer (/v1/intelligence/context).
 *
 * Coverage:
 *   ✓ endpoints answer 503 context_engine_not_migrated until the
 *     sql/20260714_company_context.sql migration is applied (fail-soft)
 *   ✓ GET/PUT/publish — no identity rejected, SalesRep → 403
 *   ✓ manager GET empty context → 200 { draft: null, published: null }
 *   ✓ manager PUT saves a draft (status draft, version 0); bad payloads → 400
 *   ✓ publish creates version 1 with published_at + deterministic compiled block
 *   ✓ draft edits after publish never change the published snapshot
 *   ✓ second publish bumps to version 2 and archives version 1 (no deletes)
 *   ✓ cross-company manager sees own (empty) context, never the UFC content
 *   ✓ compiled block contains only the caller's company fields
 *   ✓ publish writes an audit_events row
 *
 * Day 224 — every manager WRITE now lands in a dedicated fixture company
 * (Day218 Primary Co) driven by a fixture manager, not in the UFC demo
 * company. Previously this validator saved drafts and published versions as
 * Dana straight into UFC, which both required UFC to start with zero context
 * rows and silently mutated demo data — once npm run seed:ufc-intelligence
 * seeded a published UFC context, this run archived it and replaced it with
 * validator content. Owning both companies outright also restores absolute
 * assertions (publish → v1, second publish → v2) instead of tolerating
 * whatever happened to already be there. The real UFC identities are still
 * exercised read-only for the SalesRep 403 gates, where nothing is written.
 *
 * Self-cleaning: both fixture companies and all of their context rows are
 * removed at the end. No UFC row is read, written or deleted.
 *
 * Requirements: server running (npm run dev), UFC demo seed applied (for the
 * SalesRep identity), sql/20260714_company_context.sql applied (otherwise
 * reports MIGRATION PENDING after proving the fail-soft behaviour).
 * Usage: npm run validate:intelligence-context
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function hit(method: string, path: string, userId?: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(15_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function repIdByEmail(email: string): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", email).maybeSingle();
  return (data as any)?.id ?? null;
}

// Primary fixture company — every manager write in this run lands here, never
// in the UFC demo company (Day 224).
const PCOMPANY_ID = uid("DAY218", "primary-company");
const PMANAGER_ID = uid("DAY218", "primary-manager");

// Cross-company fixtures — synthetic company whose context must stay invisible.
const XCOMPANY_ID = uid("DAY218", "cross-company");
const XMANAGER_ID = uid("DAY218", "cross-manager");

const UFC_ABOUT = "UFC Gyms runs 12 UK sites selling gym memberships and personal-training packages.";
const UFC_ABOUT_EDITED = "UFC Gyms runs 14 UK sites selling gym memberships, PT packages and classes.";
const X_MARKER = "Day218 cross-company widgets sold exclusively to validators.";

const UFC_CONTEXT = {
  profile: { about: UFC_ABOUT, sales_motion: "Walk-in / field", icp: "Gym-curious 25–45 professionals near a site." },
  offering: {
    products_services: [{ name: "All-Access membership", description: "Every site, all classes included." }],
    pricing_positioning: { pricing_notes: "Standard £34.99/mo, All-Access £49.99/mo.", positioning_notes: "Premium but accessible." },
  },
  objections: [
    { objection: "It's too expensive.", approved_response: "Under £3 a visit including coaching.", weak_response: "Immediately offering a discount." },
    { objection: "I need to think about it.", approved_response: "Book the follow-up tour before they leave." },
  ],
  competitors: [{ name: "PureGym", notes: "Cheaper headline price, no classes.", positioning: "Anchor on included classes and coaching." }],
  compliance: { no_go_language: ["guaranteed results", "cancel anytime"], required_disclosures: ["14-day cooling-off period on 12-month contracts."] },
  tone: { playbook_guidance: "Always book the tour before quoting price.", tone_notes: "Direct, warm, no hard-sell scripts." },
};

const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
const FIXTURE_COMPANY_IDS = [PCOMPANY_ID, XCOMPANY_ID];

/**
 * Two synthetic companies under the demo partner: the primary company this
 * validator writes to, and a second one that must never see its content.
 * Both are owned outright by the validator and dropped by cleanup().
 */
async function seedFixtures(orgId: string) {
  const { error: compErr } = await supa.from("companies").upsert(
    [
      {
        id: PCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day218 Primary Co (validator)",
        slug: "day218-primary-validator",
      },
      {
        id: XCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day218 Cross Company (validator)",
        slug: "day218-cross-company-validator",
      },
    ],
    { onConflict: "id" }
  );
  if (compErr) throw new Error(`fixture company upsert failed: ${compErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: PMANAGER_ID, name: "Day218 P Manager", tier: "Manager", org_id: orgId, company_id: PCOMPANY_ID },
      { id: XMANAGER_ID, name: "Day218 X Manager", tier: "Manager", org_id: orgId, company_id: XCOMPANY_ID },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture reps upsert failed: ${repErr.message}`);

  // A crashed previous run could have left context rows behind; the absolute
  // version assertions below need a genuinely empty start.
  await supa.from("company_context").delete().in("company_id", FIXTURE_COMPANY_IDS);
}

async function cleanup() {
  const { data } = await supa.from("company_context").select("id").in("company_id", FIXTURE_COMPANY_IDS);
  const created = ((data ?? []) as any[]).map((r) => String(r.id));
  if (created.length) await supa.from("company_context").delete().in("id", created);
  await supa.from("reps").delete().in("id", [PMANAGER_ID, XMANAGER_ID]);
  await supa.from("companies").delete().in("id", FIXTURE_COMPANY_IDS);
  return created.length;
}

async function main() {
  console.log("\n  Context Engine Validator — Day 218\n");

  const danaId = await repIdByEmail("dana.white@ufcelite.demo");
  const nateId = await repIdByEmail("nate.diaz@ufcelite.demo");
  if (!danaId || !nateId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }

  // ── Migration preflight ───────────────────────────────────────────────────
  const { error: tableErr } = await supa.from("company_context").select("id").limit(1);
  if (tableErr && /could not find the table/i.test(tableErr.message)) {
    console.log("  ⚠ company_context table missing — proving fail-soft behaviour only.\n");
    const r = await hit("GET", "/v1/intelligence/context", danaId);
    c("GET answers 503 context_engine_not_migrated before migration",
      r.status === 503 && r.data?.error === "context_engine_not_migrated",
      `got ${r.status} ${JSON.stringify(r.data)?.slice(0, 120)}`);
    const asRep = await hit("PUT", "/v1/intelligence/context", nateId, { context: {} });
    c("SalesRep still 403 before migration (gate before table)", asRep.status === 403, `got ${asRep.status}`);
    console.log("\n  MIGRATION PENDING — apply sql/20260714_company_context.sql in the");
    console.log("  Supabase SQL editor, then re-run npm run validate:intelligence-context\n");
    process.exit(checks.every((x) => x.passed) ? 2 : 1);
  }
  if (tableErr) {
    console.error(`  ✗ company_context preflight failed: ${tableErr.message}`);
    process.exit(1);
  }

  const { data: danaRep } = await supa.from("reps").select("org_id, company_id").eq("id", danaId).single();
  const orgId = (danaRep as any).org_id as string;

  await seedFixtures(orgId);

  try {
    // ── Gate checks ──────────────────────────────────────────────────────────
    // Local dev honours DEV_TEST_UID as an identity fallback (never in
    // production), so "no identity" may resolve to the dev user — assert it is
    // rejected either way (401 in prod, 403 forbidden/no scope in dev).
    const noAuth = await hit("GET", "/v1/intelligence/context");
    c("GET without identity rejected", noAuth.status === 401 || noAuth.status === 403, `got ${noAuth.status}`);

    const repGet = await hit("GET", "/v1/intelligence/context", nateId);
    c("GET as SalesRep → 403", repGet.status === 403, `got ${repGet.status}`);
    const repPut = await hit("PUT", "/v1/intelligence/context", nateId, { context: UFC_CONTEXT });
    c("PUT as SalesRep → 403", repPut.status === 403, `got ${repPut.status}`);
    const repPub = await hit("POST", "/v1/intelligence/context/publish", nateId);
    c("publish as SalesRep → 403", repPub.status === 403, `got ${repPub.status}`);

    // ── Empty state ──────────────────────────────────────────────────────────
    // The fixture company is always empty at this point, so these are absolute.
    const empty = await hit("GET", "/v1/intelligence/context", PMANAGER_ID);
    c("manager GET → 200 ok", empty.status === 200 && empty.data?.ok === true, `got ${empty.status}`);
    c("manager GET resolves the fixture company scope, not UFC",
      empty.data?.company_id === PCOMPANY_ID, String(empty.data?.company_id));
    c("empty context → draft null + published null",
      empty.data?.draft === null && empty.data?.published === null,
      JSON.stringify(empty.data)?.slice(0, 160));
    const publishNoDraft = await hit("POST", "/v1/intelligence/context/publish", PMANAGER_ID);
    c("publish with no draft → 400 no_draft_to_publish",
      publishNoDraft.status === 400 && publishNoDraft.data?.error === "no_draft_to_publish",
      `got ${publishNoDraft.status}`);

    // ── Draft validation ─────────────────────────────────────────────────────
    const badKeys = await hit("PUT", "/v1/intelligence/context", PMANAGER_ID, { context: { profile: {}, bogus_section: {} } });
    c("PUT with unknown top-level key → 400 unknown_context_keys",
      badKeys.status === 400 && badKeys.data?.error === "unknown_context_keys",
      `got ${badKeys.status} ${JSON.stringify(badKeys.data)?.slice(0, 120)}`);
    const badShape = await hit("PUT", "/v1/intelligence/context", PMANAGER_ID, { context: ["not", "an", "object"] });
    c("PUT with non-object context → 400", badShape.status === 400, `got ${badShape.status}`);

    // ── Draft save ───────────────────────────────────────────────────────────
    const put = await hit("PUT", "/v1/intelligence/context", PMANAGER_ID, { context: UFC_CONTEXT });
    c("manager PUT saves draft", put.status === 200 && put.data?.draft?.status === "draft", `got ${put.status}`);
    c("draft carries version 0 + saved context",
      put.data?.draft?.version === 0 && put.data?.draft?.context?.profile?.about === UFC_ABOUT,
      JSON.stringify(put.data?.draft)?.slice(0, 160));

    // ── Publish v1 ───────────────────────────────────────────────────────────
    const pub1 = await hit("POST", "/v1/intelligence/context/publish", PMANAGER_ID);
    c("publish → 200 with version 1",
      pub1.status === 200 && pub1.data?.published?.version === 1,
      `got ${pub1.status} v${pub1.data?.published?.version}`);
    c("published row stamped published_at/published_by",
      Boolean(pub1.data?.published?.published_at) && pub1.data?.published?.published_by === PMANAGER_ID,
      JSON.stringify(pub1.data?.published)?.slice(0, 160));
    const compiled1 = String(pub1.data?.published?.compiled_context ?? "");
    c("compiled block contains supplied company fields",
      compiled1.includes(UFC_ABOUT) && compiled1.includes("It's too expensive.") && compiled1.includes("PureGym"),
      compiled1.slice(0, 120));
    c("compiled block contains only supplied content (no cross-company text)",
      !compiled1.includes(X_MARKER));

    // ── Published stays stable after draft edits ────────────────────────────
    const edited = { ...UFC_CONTEXT, profile: { ...UFC_CONTEXT.profile, about: UFC_ABOUT_EDITED } };
    const put2 = await hit("PUT", "/v1/intelligence/context", PMANAGER_ID, { context: edited });
    c("draft edit after publish saves", put2.status === 200 && put2.data?.draft?.context?.profile?.about === UFC_ABOUT_EDITED);

    const after = await hit("GET", "/v1/intelligence/context", PMANAGER_ID);
    c("published snapshot unchanged after draft edit",
      after.data?.published?.version === 1 && after.data?.published?.context?.profile?.about === UFC_ABOUT,
      JSON.stringify(after.data?.published?.context?.profile)?.slice(0, 160));

    const draftCompiled = await hit("GET", "/v1/intelligence/context/compiled?state=draft", PMANAGER_ID);
    const pubCompiled = await hit("GET", "/v1/intelligence/context/compiled?state=published", PMANAGER_ID);
    c("compiled preview: draft reflects edit, published snapshot does not",
      String(draftCompiled.data?.compiled ?? "").includes(UFC_ABOUT_EDITED) &&
      String(pubCompiled.data?.compiled ?? "").includes(UFC_ABOUT) &&
      !String(pubCompiled.data?.compiled ?? "").includes(UFC_ABOUT_EDITED));

    // ── Publish v2 archives v1 ───────────────────────────────────────────────
    const pub2 = await hit("POST", "/v1/intelligence/context/publish", PMANAGER_ID);
    c("second publish bumps to version 2", pub2.status === 200 && pub2.data?.published?.version === 2, `got v${pub2.data?.published?.version}`);
    const { data: archivedRows } = await supa
      .from("company_context")
      .select("id, version, archived_at")
      .eq("company_id", PCOMPANY_ID)
      .eq("status", "archived");
    const v1Archived = ((archivedRows ?? []) as any[]).some((r) => r.version === 1 && r.archived_at);
    c("version 1 archived (history kept, nothing deleted)", v1Archived, JSON.stringify(archivedRows)?.slice(0, 120));

    // ── Cross-company isolation ──────────────────────────────────────────────
    const xGet = await hit("GET", "/v1/intelligence/context", XMANAGER_ID);
    c("cross-company manager GET → 200 own scope", xGet.status === 200 && xGet.data?.company_id === XCOMPANY_ID, `got ${xGet.status} ${xGet.data?.company_id}`);
    c("cross-company manager sees none of the primary company context",
      xGet.data?.draft === null && xGet.data?.published === null,
      JSON.stringify(xGet.data)?.slice(0, 160));

    const xPut = await hit("PUT", "/v1/intelligence/context", XMANAGER_ID, {
      context: { profile: { about: X_MARKER } },
    });
    c("cross-company manager PUT lands in own company",
      xPut.status === 200 && xPut.data?.draft?.context?.profile?.about === X_MARKER);

    const primaryAfterX = await hit("GET", "/v1/intelligence/context", PMANAGER_ID);
    c("primary company context untouched by cross-company writes",
      primaryAfterX.data?.draft?.context?.profile?.about === UFC_ABOUT_EDITED &&
      primaryAfterX.data?.published?.version === 2 &&
      JSON.stringify(primaryAfterX.data).indexOf(X_MARKER) === -1);

    const xCompiled = await hit("GET", "/v1/intelligence/context/compiled?state=draft", XMANAGER_ID);
    c("cross-company compiled block contains only its own fields",
      String(xCompiled.data?.compiled ?? "").includes(X_MARKER) &&
      !String(xCompiled.data?.compiled ?? "").includes(UFC_ABOUT));

    // ── Audit trail ──────────────────────────────────────────────────────────
    const { data: auditRows } = await supa
      .from("audit_events")
      .select("id, action, actor_user_id, metadata")
      .eq("action", "publish_company_context")
      .eq("actor_user_id", PMANAGER_ID)
      .order("created_at", { ascending: false })
      .limit(1);
    const audit = (auditRows ?? [])[0] as any;
    c("publish writes an audit_events row",
      Boolean(audit) && audit?.metadata?.company_id === PCOMPANY_ID,
      JSON.stringify(audit)?.slice(0, 120));
  } finally {
    const removed = await cleanup();
    console.log(`\n  Cleanup: removed ${removed} company_context fixture row(s) + cross-company fixtures.`);
  }

  const failed = checks.filter((x) => !x.passed);
  console.log(`\n  ${checks.length - failed.length}/${checks.length} checks passed`);
  if (failed.length) {
    console.log("  Context Engine validation FAILED\n");
    process.exit(1);
  }
  console.log("  Context Engine validation PASSED\n");
}

main().catch((e) => {
  console.error("\n  ✗ Validator crashed:", e?.message || e);
  process.exit(1);
});
