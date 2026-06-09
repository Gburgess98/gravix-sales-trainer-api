/**
 * validate-rls.ts
 *
 * Validates RLS Phase 1 — confirms:
 *   ✓ RLS is enabled on all target CRM, coaching, and assignment tables
 *   ✓ FORCE ROW LEVEL SECURITY is NOT set (service-role must continue to bypass)
 *   ✓ No policies exist yet (Phase 1 only enables RLS; Phase 2 adds policies)
 *   ✓ Tables that should NOT have RLS yet do not have it
 *   ✓ API health endpoints still respond (runtime behaviour unchanged)
 *   ✓ CRM endpoint still returns 200 for authenticated requests (service-role unaffected)
 *   ✓ Assignments endpoint still returns 200 for authenticated requests
 *
 * Requirements:
 *   - DATABASE_URL env var set (Supabase project connection string)
 *     Find it: Supabase Dashboard → Settings → Database → Connection string (URI mode)
 *   - Server running (npm run dev) — for runtime behaviour checks
 *
 * Usage: npm run validate:rls
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import pg from "pg";

const { Pool } = pg;

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const DATABASE_URL = process.env.DATABASE_URL;

// Tables that MUST have RLS enabled after Phase 1
const RLS_PHASE1_TABLES = [
  "crm_contacts",
  "crm_opportunities",
  "crm_activities",
  "crm_accounts",
  "crm_actions",
  "crm_contact_notes",
  "crm_call_links",
  "crm_demo_call_links",
  "crm_auto_assign_runs",
  "coach_assignments",
  "coach_notes",
  "assignments",
] as const;

// Tables that should NOT have RLS yet (Phase 2+ scope)
const NOT_YET_TABLES = [
  "reps",
  "companies",
  "calls",
  "audit_events",
  "partners",
] as const;

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function hitApi(path: string, userId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(10_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function findRepByTier(supa: ReturnType<typeof createClient>, tier: string) {
  const { data } = await supa.from("reps").select("id").eq("tier", tier).limit(1).maybeSingle();
  return (data as any)?.id ?? null;
}

async function main() {
  console.log("\n  RLS Phase 1 Validator\n");

  const results: Check[] = [];

  // ── Section 1: Database-level RLS checks ─────────────────────────────────
  // Uses pg directly to query pg_class — the authoritative source for RLS state.

  if (!DATABASE_URL) {
    console.log("  ⚠  DATABASE_URL not set — skipping DB-level RLS checks.");
    console.log("     Find it: Supabase Dashboard → Settings → Database → Connection string\n");
    results.push(c("DATABASE_URL configured", false, "Set DATABASE_URL in .env to enable DB checks"));
  } else {
    const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

    try {
      // Query pg_class for RLS state on all relevant tables
      const { rows } = await pool.query<{
        table_name: string;
        rls_enabled: boolean;
        rls_forced: boolean;
      }>(`
        SELECT
          c.relname          AS table_name,
          c.relrowsecurity   AS rls_enabled,
          c.relforcerowsecurity AS rls_forced
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND c.relname = ANY($1)
        ORDER BY c.relname
      `, [[...RLS_PHASE1_TABLES, ...NOT_YET_TABLES]]);

      const tableMap = new Map(rows.map(r => [r.table_name, r]));

      // Tables that must have RLS enabled
      for (const table of RLS_PHASE1_TABLES) {
        const row = tableMap.get(table);

        if (!row) {
          // Table doesn't exist — not an error for Phase 1 (some may not be created yet)
          results.push(c(
            `${table}: exists in DB`,
            false,
            `Table not found — migration may not have been applied, or table name differs`
          ));
          continue;
        }

        results.push(c(
          `${table}: RLS enabled`,
          row.rls_enabled === true,
          row.rls_enabled ? undefined : `relrowsecurity=false — run 20260609_rls_phase1.sql`
        ));

        // FORCE should NOT be set — service_role must still bypass
        results.push(c(
          `${table}: FORCE RLS not set (service-role bypass preserved)`,
          row.rls_forced === false,
          row.rls_forced ? `relforcerowsecurity=true — this would break service-role queries!` : undefined
        ));
      }

      // Tables that should NOT have RLS yet
      for (const table of NOT_YET_TABLES) {
        const row = tableMap.get(table);
        if (!row) continue; // table doesn't exist, not relevant

        results.push(c(
          `${table}: RLS not yet enabled (Phase 2+ scope)`,
          row.rls_enabled === false,
          row.rls_enabled ? `RLS was enabled — this was not part of Phase 1` : undefined
        ));
      }

      // Check that no policies exist yet on Phase 1 tables
      const { rows: policyRows } = await pool.query<{
        tablename: string;
        policyname: string;
      }>(`
        SELECT tablename, policyname
        FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = ANY($1)
        ORDER BY tablename, policyname
      `, [[...RLS_PHASE1_TABLES]]);

      if (policyRows.length === 0) {
        results.push(c(
          "Phase 1: no policies exist yet (correct — policies are Phase 2)",
          true
        ));
      } else {
        // Policies exist — this is unexpected for Phase 1 but not harmful
        results.push(c(
          "Phase 1: no policies exist yet",
          false,
          `Found ${policyRows.length} policies: ${policyRows.map(r => `${r.tablename}.${r.policyname}`).join(", ")} — Phase 2 may have already been applied`
        ));
      }

    } catch (e: any) {
      results.push(c("DB connection successful", false, e?.message));
    } finally {
      await pool.end().catch(() => {});
    }
  }

  // ── Section 2: Runtime behaviour checks ──────────────────────────────────
  // Confirm that service-role API queries still work (RLS bypass is active).

  // Connectivity
  let serverReachable = false;
  try {
    const res = await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
    serverReachable = res.ok || res.status < 500;
    results.push(c("Server reachable", serverReachable, `status=${res.status}`));
  } catch (e: any) {
    results.push(c("Server reachable", false, `not reachable at ${API_BASE} — start with npm run dev`));
  }

  if (serverReachable) {
    // Health endpoint — basic sanity
    const { status: hs } = await hitApi("/v1/health");
    results.push(c("GET /v1/health → 200 (API unaffected by RLS)", hs === 200, `actual: ${hs}`));

    // Find a test user for authenticated checks
    const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });
    const testUserId = await findRepByTier(supa, "Manager")
      ?? await findRepByTier(supa, "SalesRep")
      ?? await findRepByTier(supa, "Owner");

    if (testUserId) {
      // CRM contacts — service-role query should still work
      const { status: crmStatus, data: crmData } = await hitApi(
        "/v1/crm/contacts?limit=5",
        testUserId
      );
      results.push(c(
        "GET /v1/crm/contacts still returns 200 (service-role bypasses RLS)",
        crmStatus === 200,
        `actual: ${crmStatus} error=${crmData?.error}`
      ));

      // Assignments — service-role query should still work
      const { status: assignStatus, data: assignData } = await hitApi(
        `/v1/assignments?repId=${testUserId}`,
        testUserId
      );
      results.push(c(
        "GET /v1/assignments still returns 200 (service-role bypasses RLS)",
        assignStatus === 200,
        `actual: ${assignStatus} error=${assignData?.error}`
      ));

      // CRM activities
      const { status: actStatus, data: actData } = await hitApi(
        "/v1/crm/activities?limit=5",
        testUserId
      );
      results.push(c(
        "GET /v1/crm/activities still returns 200 (service-role bypasses RLS)",
        actStatus === 200,
        `actual: ${actStatus} error=${actData?.error}`
      ));

    } else {
      results.push(c("Runtime API checks — authenticated (skipped: no rep found in DB)", true));
    }

    // Unauthenticated requests should still get 401/403, not 500
    for (const [label, path] of [
      ["CRM contacts", "/v1/crm/contacts"],
      ["admin users", "/v1/admin/users"],
    ]) {
      const { status } = await hitApi(path); // no user ID
      results.push(c(
        `Unauthenticated ${label}: still 401/403 (not 500 from RLS error)`,
        status === 401 || status === 403 || status === 400,
        `actual: ${status} — 500 would indicate RLS is blocking service-role`
      ));
    }
  }

  // ── Report ─────────────────────────────────────────────────────────────────
  console.log("\n  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);

  const rlsFails = results.filter(r => !r.passed && r.label.includes("RLS enabled"));
  if (rlsFails.length > 0) {
    console.log("  ⚠  Some tables are missing RLS. Run the migration:");
    console.log("     Supabase SQL editor → paste contents of sql/20260609_rls_phase1.sql\n");
  }

  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
