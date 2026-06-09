/**
 * validate-permissions.ts
 *
 * Verifies that the PartnerAdmin / SuperAdmin middleware enforces
 * the correct access rules against the live server.
 *
 * Permission matrix:
 *   Endpoint                        SalesRep  Manager  PartnerAdmin  SuperAdmin
 *   GET /v1/admin/partner/health    403       403      200          200
 *   GET /v1/admin/super/health      403       403      403          200
 *
 * Usage: npm run validate:permissions
 * Requires: server running (npm run dev) + reps with each tier in the DB.
 *
 * To create test reps (in Supabase SQL editor):
 *   INSERT INTO reps (id, org_id, company_id, name, tier)
 *   VALUES
 *     ('aaaaaaaa-0000-4000-8000-000000000001', ..., 'PartnerAdmin'),
 *     ('aaaaaaaa-0000-4000-8000-000000000002', ..., 'SuperAdmin');
 *
 * Or set TEST_PARTNER_ADMIN_ID / TEST_SUPER_ADMIN_ID env vars to existing rep UUIDs.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const PARTNER_COMPANY_ID = "bfb9604e-bc2f-46fa-be97-6461e98e1a19"; // UFC Demo Company

// Deterministic test UUIDs — same pattern as the seed script
import crypto from "node:crypto";
function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${v}${h.slice(17,20)}-${h.slice(20,32)}`;
}

type FixtureRep = { id: string; name: string; tier: string };

async function upsertFixtureRep(rep: FixtureRep & { org_id: string; company_id: string }): Promise<void> {
  await supa.from("reps").upsert(
    { id: rep.id, name: rep.name, tier: rep.tier, org_id: rep.org_id, company_id: rep.company_id },
    { onConflict: "id" }
  );
}

async function cleanupFixtureReps(ids: string[]): Promise<void> {
  await supa.from("reps").delete().in("id", ids);
}

async function hit(path: string, userId: string): Promise<{ status: number; body: any }> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "x-user-id": userId, "content-type": "application/json" },
    signal: AbortSignal.timeout(8_000),
  });
  let body: any = null;
  try { body = await res.json(); } catch { /* ignore */ }
  return { status: res.status, body };
}

type Check = { label: string; passed: boolean; detail?: string };
function c(label: string, passed: boolean, detail?: string): Check { return { label, passed, detail }; }

async function main() {
  console.log("\n  Permission Matrix Validator\n");

  // ── Connectivity check ───────────────────────────────────────────────────
  try {
    const ping = await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
    if (!ping.ok) throw new Error("health check failed");
  } catch (e: any) {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`);
    process.exit(2);
  }

  // ── Fixture setup ────────────────────────────────────────────────────────
  const { data: orgRow } = await supa.from("reps").select("org_id").limit(1).single();
  const orgId = (orgRow as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";

  const fixtures: FixtureRep[] = [
    { id: uid("TEST_PERM", "SalesRep"),     name: "Test SalesRep (perm)",     tier: "SalesRep"     },
    { id: uid("TEST_PERM", "Manager"),      name: "Test Manager (perm)",      tier: "Manager"      },
    { id: uid("TEST_PERM", "PartnerAdmin"), name: "Test PartnerAdmin (perm)", tier: "PartnerAdmin" },
    { id: uid("TEST_PERM", "SuperAdmin"),   name: "Test SuperAdmin (perm)",   tier: "SuperAdmin"   },
  ];

  process.stdout.write("  Setting up fixture reps... ");
  for (const f of fixtures) {
    await upsertFixtureRep({ ...f, org_id: orgId, company_id: PARTNER_COMPANY_ID });
  }
  console.log("done");

  const results: Check[] = [];

  try {
    const repById = Object.fromEntries(fixtures.map(f => [f.tier, f]));

    // ── partner/health matrix ─────────────────────────────────────────────

    const partnerPath = "/v1/admin/partner/health";

    for (const [tier, expected] of [
      ["SalesRep",     403],
      ["Manager",      403],
      ["PartnerAdmin", 200],
      ["SuperAdmin",   200],
    ] as [string, number][]) {
      const r = await hit(partnerPath, repById[tier].id);
      results.push(c(
        `${partnerPath} — ${tier} → ${expected}`,
        r.status === expected,
        `actual: ${r.status}${r.status !== expected ? " error: " + (r.body?.error ?? "?") : ""}`
      ));
    }

    // ── super/health matrix ───────────────────────────────────────────────

    const superPath = "/v1/admin/super/health";

    for (const [tier, expected] of [
      ["SalesRep",     403],
      ["Manager",      403],
      ["PartnerAdmin", 403],
      ["SuperAdmin",   200],
    ] as [string, number][]) {
      const r = await hit(superPath, repById[tier].id);
      results.push(c(
        `${superPath} — ${tier} → ${expected}`,
        r.status === expected,
        `actual: ${r.status}${r.status !== expected ? " error: " + (r.body?.error ?? "?") : ""}`
      ));
    }

    // ── response shape checks (for passing tiers) ─────────────────────────

    const pa = await hit(partnerPath, repById["PartnerAdmin"].id);
    results.push(c(
      "partner/health body.scope = 'partner'",
      pa.body?.scope === "partner",
      `actual: ${pa.body?.scope}`
    ));
    results.push(c(
      "partner/health body.companies is array",
      Array.isArray(pa.body?.companies),
      `actual: ${typeof pa.body?.companies}`
    ));

    const sa = await hit(superPath, repById["SuperAdmin"].id);
    results.push(c(
      "super/health body.scope = 'super'",
      sa.body?.scope === "super",
      `actual: ${sa.body?.scope}`
    ));
    results.push(c(
      "super/health body.companies is array",
      Array.isArray(sa.body?.companies),
      `actual: ${typeof sa.body?.companies}`
    ));

    // ── no-auth check ─────────────────────────────────────────────────────

    for (const path of [partnerPath, superPath]) {
      const r = await fetch(`${API_BASE}${path}`, { signal: AbortSignal.timeout(4_000) });
      results.push(c(`${path} — no auth → 401`, r.status === 401, `actual: ${r.status}`));
    }

  } finally {
    // ── Cleanup ───────────────────────────────────────────────────────────
    process.stdout.write("\n  Cleaning up fixture reps... ");
    await cleanupFixtureReps(fixtures.map(f => f.id));
    console.log("done");
  }

  // ── Report ────────────────────────────────────────────────────────────────

  console.log("\n  ── Permission Matrix ────────────────────────────────");
  console.log(`  ${"Endpoint".padEnd(32)} ${"Tier".padEnd(14)} Expected  Actual`);
  console.log(`  ${"─".repeat(32)} ${"─".repeat(14)} ──────── ──────`);

  const matrixRows = results.filter(r => r.label.includes("→"));
  for (const r of matrixRows) {
    const [path, rest] = r.label.split(" — ");
    const [tier, expected] = rest.split(" → ");
    const actual = r.detail?.match(/actual: (\d+)/)?.[1] ?? "?";
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon} ${path.padEnd(32)} ${tier.padEnd(14)} ${expected.padEnd(9)} ${actual}`);
  }

  console.log("\n  ── All checks ───────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }

  console.log(`\n  ${passed} passed  ${failed} failed\n`);
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
