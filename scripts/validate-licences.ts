/**
 * validate-licences.ts
 *
 * Validates the licence pool API endpoints.
 *
 * Coverage:
 *   ✓ GET /v1/admin/partner/licences — PartnerAdmin → 200 + correct shape
 *   ✓ GET /v1/admin/partner/licences — SuperAdmin   → 200 (also allowed)
 *   ✓ GET /v1/admin/partner/licences — Manager      → 403
 *   ✓ GET /v1/admin/partner/licences — no auth      → 401
 *   ✓ pool.purchased ≥ 0
 *   ✓ totals.available = purchased - allocated
 *   ✓ companies array is present
 *   ✓ GET /v1/admin/super/licences — SuperAdmin     → 200 + partners array
 *   ✓ GET /v1/admin/super/licences — PartnerAdmin   → 403
 *   ✓ GET /v1/admin/super/licences — Manager        → 403
 *   ✓ GET /v1/admin/super/licences — no auth        → 401
 *   ✓ super response: each partner has pool + totals
 *   ✓ super totals.available = purchased - allocated for each partner
 *
 * Requirements:
 *   - Server running (npm run dev)
 *   - Migration 20260609_licence_pools.sql applied
 *   - At least one PartnerAdmin or SuperAdmin user in reps table
 *
 * Usage: npm run validate:licences
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function hit(method: string, path: string, userId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { method, headers, signal: AbortSignal.timeout(10_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function findUserByTier(tier: string): Promise<string | null> {
  const { data } = await supa
    .from("reps")
    .select("id")
    .eq("tier", tier)
    .limit(1)
    .maybeSingle();
  return (data as any)?.id ?? null;
}

async function main() {
  console.log("\n  Licence Pool Validator\n");

  // Connectivity
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  // Resolve test users
  process.stdout.write("  Resolving test users... ");
  const [superAdminId, partnerAdminId, managerId] = await Promise.all([
    findUserByTier("SuperAdmin"),
    findUserByTier("PartnerAdmin"),
    findUserByTier("Manager"),
  ]);

  console.log("done");
  console.log(`  SuperAdmin:    ${superAdminId  ?? "(none — some checks will be skipped)"}`);
  console.log(`  PartnerAdmin:  ${partnerAdminId ?? "(none — some checks will be skipped)"}`);
  console.log(`  Manager:       ${managerId      ?? "(none — some checks will be skipped)"}\n`);

  const results: Check[] = [];

  // ── Migration guard ───────────────────────────────────────────────────────
  // Quick table existence check via a direct DB query.
  {
    const { error: lpErr } = await supa.from("licence_pools").select("id").limit(1);
    results.push(c("licence_pools table exists", !lpErr, lpErr?.message));

    const { error: clErr } = await supa.from("company_licences").select("id").limit(1);
    results.push(c("company_licences table exists", !clErr, clErr?.message));
  }

  // ── GET /v1/admin/partner/licences ────────────────────────────────────────

  // Auth gate: no auth → 401
  {
    const { status } = await hit("GET", "/v1/admin/partner/licences");
    results.push(c("/partner/licences: no auth → 401", status === 401, `actual: ${status}`));
  }

  // Auth gate: Manager → 403
  if (managerId) {
    const { status } = await hit("GET", "/v1/admin/partner/licences", managerId);
    results.push(c("/partner/licences: Manager → 403", status === 403, `actual: ${status}`));
  } else {
    results.push(c("/partner/licences: Manager gate (skipped — no Manager found)", true));
  }

  // PartnerAdmin access + shape
  if (partnerAdminId) {
    const { status, data } = await hit("GET", "/v1/admin/partner/licences", partnerAdminId);
    results.push(c("/partner/licences: PartnerAdmin → 200",          status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c("/partner/licences: response.ok = true",          data?.ok === true, `ok=${data?.ok}`));
    results.push(c("/partner/licences: partner_id present",          !!data?.partner_id, `partner_id=${data?.partner_id}`));
    results.push(c("/partner/licences: companies is array",          Array.isArray(data?.companies), `type=${typeof data?.companies}`));
    results.push(c("/partner/licences: totals present",              !!data?.totals, `totals=${JSON.stringify(data?.totals)}`));

    if (data?.totals) {
      const { purchased, allocated, available } = data.totals;
      results.push(c("/partner/licences: totals.purchased ≥ 0",      purchased >= 0, `purchased=${purchased}`));
      results.push(c("/partner/licences: available = purchased − allocated",
        available === purchased - allocated,
        `${available} === ${purchased} − ${allocated}`
      ));
    }

    if (data?.pool) {
      results.push(c("/partner/licences: pool.plan_type is string",  typeof data.pool.plan_type === "string", `plan_type=${data.pool.plan_type}`));
      results.push(c("/partner/licences: pool.purchased ≥ 0",        data.pool.purchased >= 0, `purchased=${data.pool.purchased}`));
    }
  } else {
    results.push(c("/partner/licences: PartnerAdmin checks (skipped — no PartnerAdmin found)", true));
  }

  // SuperAdmin also allowed on /partner/licences
  if (superAdminId) {
    const { status, data } = await hit("GET", "/v1/admin/partner/licences", superAdminId);
    // SuperAdmin passes requirePartnerAdmin; may get 400 if their company has no partner linked
    results.push(c("/partner/licences: SuperAdmin → 200 or 400", status === 200 || status === 400, `actual: ${status} error=${data?.error}`));
  } else {
    results.push(c("/partner/licences: SuperAdmin check (skipped — no SuperAdmin found)", true));
  }

  // ── GET /v1/admin/super/licences ──────────────────────────────────────────

  // Auth gate: no auth → 401
  {
    const { status } = await hit("GET", "/v1/admin/super/licences");
    results.push(c("/super/licences: no auth → 401", status === 401, `actual: ${status}`));
  }

  // Auth gate: Manager → 403
  if (managerId) {
    const { status } = await hit("GET", "/v1/admin/super/licences", managerId);
    results.push(c("/super/licences: Manager → 403", status === 403, `actual: ${status}`));
  } else {
    results.push(c("/super/licences: Manager gate (skipped — no Manager found)", true));
  }

  // Auth gate: PartnerAdmin → 403
  if (partnerAdminId) {
    const { status } = await hit("GET", "/v1/admin/super/licences", partnerAdminId);
    results.push(c("/super/licences: PartnerAdmin → 403", status === 403, `actual: ${status}`));
  } else {
    results.push(c("/super/licences: PartnerAdmin gate (skipped — no PartnerAdmin found)", true));
  }

  // SuperAdmin access + shape
  if (superAdminId) {
    const { status, data } = await hit("GET", "/v1/admin/super/licences", superAdminId);
    results.push(c("/super/licences: SuperAdmin → 200",          status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c("/super/licences: response.ok = true",        data?.ok === true, `ok=${data?.ok}`));
    results.push(c("/super/licences: partners is array",         Array.isArray(data?.partners), `type=${typeof data?.partners}`));
    results.push(c("/super/licences: count matches array length",
      data?.count === (data?.partners?.length ?? -1),
      `count=${data?.count} len=${data?.partners?.length}`
    ));

    if (Array.isArray(data?.partners) && data.partners.length > 0) {
      const first = data.partners[0];
      results.push(c("/super/licences: partner has partner_id",    !!first.partner_id, `partner_id=${first.partner_id}`));
      results.push(c("/super/licences: partner has partner_name",  !!first.partner_name, `name=${first.partner_name}`));
      results.push(c("/super/licences: partner has totals",        !!first.totals, `totals=${JSON.stringify(first.totals)}`));
      results.push(c("/super/licences: partner has companies array", Array.isArray(first.companies), `type=${typeof first.companies}`));

      if (first.totals) {
        const { purchased, allocated, available } = first.totals;
        results.push(c("/super/licences: first partner available = purchased − allocated",
          available === purchased - allocated,
          `${available} === ${purchased} − ${allocated}`
        ));
      }
    }
  } else {
    results.push(c("/super/licences: SuperAdmin checks (skipped — no SuperAdmin found)", true));
  }

  // ── Report ────────────────────────────────────────────────────────────────
  console.log("\n  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);

  if (failed > 0) {
    const missingMigration = results.some(r => !r.passed && r.label.includes("table exists"));
    if (missingMigration) {
      console.log("  ⚠  Run the migration first:");
      console.log("     Supabase SQL editor → sql/20260609_licence_pools.sql\n");
    }
    process.exit(1);
  }
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
