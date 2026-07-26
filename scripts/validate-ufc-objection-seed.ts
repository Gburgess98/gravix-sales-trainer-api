/**
 * validate-ufc-objection-seed.ts
 *
 * Day 252 — proves the APPROVED Objection Library demo assets seeded by
 * npm run seed:ufc-intelligence are present, correct and demo-safe:
 *
 *   ✓ exactly the seeded UFC objections exist (deterministic ids), no more,
 *     no fewer — re-running the seed did not duplicate them (idempotent)
 *   ✓ every seeded item is APPROVED and carries the pinned approved_at
 *   ✓ categories cover price/timing/authority/competitor/trust/fit
 *   ✓ each has ≥1 buyer phrase, an approved response and a coaching note
 *   ✓ weak-response patterns and no-go language are populated
 *   ✓ no duplicate LIVE label among the UFC company's objections
 *   ✓ the Objection API lists every seeded item as Dana (manager), approved
 *   ✓ a manager in another company sees NONE of the UFC seeded objections
 *   ✓ the Day 251 archived QA objection (if present) is neither required nor
 *     counted as seed — the seed set is exactly the deterministic ids
 *
 * Read-only against the seeded data. Creates one throwaway cross-company
 * manager fixture (company + rep) for the isolation check and removes it.
 *
 * Requirements: server running (npm run dev), npm run seed:demo and
 * npm run seed:ufc-intelligence applied, sql/20260719_objection_library.sql
 * applied.
 * Usage: npm run validate:ufc-objection-seed
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

import { OBJECTIONS, objectionId, SEED_APPROVED_AT } from "./seed-ufc-intelligence";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const UFC_COMPANY_ID = process.env.DEMO_COMPANY_ID || "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
const REQUIRED_CATEGORIES = ["price", "timing", "authority", "competitor", "trust", "fit"];
const BASE = "/v1/intelligence/objections";

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY252::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const XCOMPANY_ID = uid("cross-company");
const XMANAGER_ID = uid("cross-manager");

async function hit(method: string, path: string, userId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    signal: AbortSignal.timeout(15_000),
  });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function repIdByEmail(email: string): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", email).maybeSingle();
  return (data as any)?.id ?? null;
}

async function seedCrossCompanyFixture(orgId: string) {
  const { error: compErr } = await supa.from("companies").upsert(
    {
      id: XCOMPANY_ID,
      tmc_id: DEMO_PARTNER,
      partner_id: DEMO_PARTNER,
      name: "Day252 Cross Company (validator)",
      slug: "day252-cross-company-validator",
    },
    { onConflict: "id" }
  );
  if (compErr) throw new Error(`cross-company fixture failed: ${compErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    { id: XMANAGER_ID, name: "Day252 X Manager", tier: "Manager", org_id: orgId, company_id: XCOMPANY_ID },
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`cross-company manager fixture failed: ${repErr.message}`);
}

async function cleanup() {
  await supa.from("reps").delete().eq("id", XMANAGER_ID);
  await supa.from("companies").delete().eq("id", XCOMPANY_ID);
}

async function main() {
  console.log("\n  Day 252 — UFC Objection Library seed validation\n");

  const probe = await supa.from("objection_library_items").select("id").limit(1);
  if (probe.error && /could not find the table/i.test(probe.error.message)) {
    console.error("  ✗ objection tables missing — apply sql/20260719_objection_library.sql first.");
    process.exit(1);
  }
  if (probe.error) {
    console.error(`  ✗ objection preflight failed: ${probe.error.message}`);
    process.exit(1);
  }

  const danaId = await repIdByEmail("dana.white@ufcelite.demo");
  const nateId = await repIdByEmail("nate.diaz@ufcelite.demo");
  if (!danaId || !nateId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }
  const { data: danaRep } = await supa
    .from("reps").select("org_id, company_id").eq("id", danaId).single();
  const orgId = (danaRep as any).org_id as string;
  if (String((danaRep as any).company_id) !== UFC_COMPANY_ID) {
    console.error("  ✗ Dana is not in the demo company — refusing to validate.");
    process.exit(1);
  }

  const seedIds = OBJECTIONS.map((o) => objectionId(o.key));
  const seedIdSet = new Set(seedIds);

  try {
    // ── Seeded rows in the DB ─────────────────────────────────────────────────
    console.log("— Seeded objection rows");

    const { data: allRows } = await supa
      .from("objection_library_items")
      .select("id, label, category, status, buyer_phrases, approved_response, coaching_note, weak_response_patterns, no_go_language, approved_by, approved_at")
      .eq("company_id", UFC_COMPANY_ID);
    const rows = (allRows ?? []) as any[];
    const seedRows = rows.filter((r) => seedIdSet.has(String(r.id)));

    c("every seeded objection exists (by deterministic id)",
      seedRows.length === OBJECTIONS.length,
      `found ${seedRows.length}/${OBJECTIONS.length} — run npm run seed:ufc-intelligence`);
    c("no duplicate seeded ids (idempotent — re-running did not fork rows)",
      new Set(seedRows.map((r) => String(r.id))).size === seedRows.length);
    c("every seeded objection is approved",
      seedRows.length > 0 && seedRows.every((r) => r.status === "approved"),
      JSON.stringify([...new Set(seedRows.map((r) => r.status))]));
    // Postgres serialises timestamptz as "+00:00" without milliseconds, so
    // compare by instant, not by exact string.
    const pinnedMs = new Date(SEED_APPROVED_AT).getTime();
    c("every seeded objection carries the pinned approved_at (deterministic)",
      seedRows.every((r) => r.approved_at && new Date(r.approved_at).getTime() === pinnedMs),
      JSON.stringify([...new Set(seedRows.map((r) => r.approved_at))]).slice(0, 120));
    c("every seeded objection is approved_by the seeding manager (Dana)",
      seedRows.every((r) => String(r.approved_by) === String(danaId)));

    // Content completeness — mirrors the API approval gate.
    c("every seeded objection has ≥1 buyer phrase",
      seedRows.every((r) => Array.isArray(r.buyer_phrases) && r.buyer_phrases.length > 0));
    c("every seeded objection has an approved response",
      seedRows.every((r) => String(r.approved_response ?? "").trim().length > 0));
    c("every seeded objection has a coaching note",
      seedRows.every((r) => String(r.coaching_note ?? "").trim().length > 0));
    c("every seeded objection has weak-response patterns",
      seedRows.every((r) => Array.isArray(r.weak_response_patterns) && r.weak_response_patterns.length > 0));
    c("every seeded objection has no-go language",
      seedRows.every((r) => Array.isArray(r.no_go_language) && r.no_go_language.length > 0));

    // Content matches the seed payload exactly (labels + categories).
    const rowById = new Map(seedRows.map((r) => [String(r.id), r]));
    c("seeded labels + categories match the seed payload",
      OBJECTIONS.every((o) => {
        const row = rowById.get(objectionId(o.key));
        return row && row.label === o.label && row.category === o.category;
      }));

    const seededCats = new Set(seedRows.map((r) => String(r.category)));
    c(`categories cover ${REQUIRED_CATEGORIES.join("/")}`,
      REQUIRED_CATEGORIES.every((cat) => seededCats.has(cat)),
      `covered: ${[...seededCats].sort().join(", ")}`);

    // ── No duplicate live labels ──────────────────────────────────────────────
    console.log("— Label hygiene");

    const live = rows.filter((r) => r.status !== "archived");
    const liveLabelCounts = new Map<string, number>();
    for (const r of live) {
      const k = String(r.label).toLowerCase();
      liveLabelCounts.set(k, (liveLabelCounts.get(k) ?? 0) + 1);
    }
    const dupLive = [...liveLabelCounts.entries()].filter(([, n]) => n > 1);
    c("no duplicate live label in the UFC objection library",
      dupLive.length === 0, `duplicates: ${JSON.stringify(dupLive)}`);

    // ── Day 251 QA archived object is not counted as seed ─────────────────────
    console.log("— Day 251 QA object is not part of the seed");

    const archived = rows.filter((r) => r.status === "archived");
    c("archived objections are never in the seed id set (QA item not seed)",
      archived.every((r) => !seedIdSet.has(String(r.id))),
      `archived ids in seed set: ${archived.filter((r) => seedIdSet.has(String(r.id))).length}`);
    c("all seeded objections are live (not archived)",
      seedRows.every((r) => r.status !== "archived"));

    // ── API list as Dana ──────────────────────────────────────────────────────
    console.log("— Objection API as Dana (manager)");

    const danaList = await hit("GET", BASE, danaId);
    c("Dana lists the objection library → 200 ok",
      danaList.status === 200 && danaList.data?.ok === true, `got ${danaList.status}`);
    const listIds = new Set(((danaList.data?.items ?? []) as any[]).map((i) => String(i.id)));
    c("API list returns every seeded objection to Dana",
      seedIds.every((id) => listIds.has(id)),
      `missing: ${seedIds.filter((id) => !listIds.has(id)).length}`);
    const listSeedItems = ((danaList.data?.items ?? []) as any[]).filter((i) => seedIdSet.has(String(i.id)));
    c("API reports the seeded items as approved",
      listSeedItems.length === OBJECTIONS.length && listSeedItems.every((i) => i.status === "approved"));

    // ── Cross-company isolation ───────────────────────────────────────────────
    console.log("— Cross-company isolation");

    await seedCrossCompanyFixture(orgId);
    const xList = await hit("GET", BASE, XMANAGER_ID);
    c("cross-company manager lists → 200 ok", xList.status === 200, `got ${xList.status}`);
    const xIds = new Set(((xList.data?.items ?? []) as any[]).map((i) => String(i.id)));
    c("cross-company manager sees NONE of the UFC seeded objections",
      seedIds.every((id) => !xIds.has(id)), `leaked: ${seedIds.filter((id) => xIds.has(id)).length}`);
    const xGet = await hit("GET", `${BASE}/${seedIds[0]}`, XMANAGER_ID);
    c("cross-company GET of a seeded objection by id → 404 (no existence leak)",
      xGet.status === 404, `got ${xGet.status}`);

    // A rep still cannot read the library at all.
    const repList = await hit("GET", BASE, nateId);
    c("a SalesRep cannot list the objection library → 403", repList.status === 403, `got ${repList.status}`);
  } finally {
    await cleanup();
  }

  const passed = checks.filter((x) => x.passed).length;
  console.log(`\n  ${passed}/${checks.length} checks passed`);
  if (passed !== checks.length) {
    console.log("  UFC Objection seed validation FAILED\n");
    process.exit(1);
  }
  console.log("  UFC Objection seed validation PASSED");
  console.log(`\n  Demo: /intelligence?tab=objections shows ${OBJECTIONS.length} approved objections.\n`);
}

main().catch(async (e) => {
  console.error("\n  ✗ Validator crashed:", e?.message || e);
  try { await cleanup(); } catch { /* best effort */ }
  process.exit(1);
});
