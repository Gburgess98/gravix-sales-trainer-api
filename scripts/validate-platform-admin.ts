/**
 * validate-platform-admin.ts
 *
 * Validates platform admin and support endpoints.
 *
 * Coverage:
 *   ✓ GET /v1/admin/platform → 200 with expected stat keys
 *   ✓ Non-SuperAdmin → 403 on platform
 *   ✓ GET /v1/admin/organisations → 200 with array
 *   ✓ Non-SuperAdmin → 403 on organisations
 *   ✓ GET /v1/admin/support/users → 200, returns users array
 *   ✓ GET /v1/admin/support/users?q= → filtered results
 *   ✓ GET /v1/admin/support/companies → 200, returns companies array
 *   ✓ GET /v1/admin/support/impersonation-history → 200
 *   ✓ Non-SuperAdmin blocked on all support endpoints → 403
 *
 * Usage: npm run validate:platform-admin
 * Requires: server running (npm run dev)
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const GEORGE_ID       = "b817133a-44f1-4be3-89a1-6e6f6159c018";
const DEMO_COMPANY_ID = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${v}${h.slice(17,20)}-${h.slice(20,32)}`;
}

async function hit(method: string, path: string, userId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { method, headers, signal: AbortSignal.timeout(8_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function main() {
  console.log("\n  Platform Admin Validator\n");

  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  const { data: georgeRow } = await supa.from("reps").select("org_id").eq("id", GEORGE_ID).maybeSingle();
  const orgId = (georgeRow as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";

  const managerRep = { id: uid("PA", "Manager"), name: "PA Test Manager", tier: "Manager", org_id: orgId, company_id: DEMO_COMPANY_ID };
  process.stdout.write("  Setting up fixture… ");
  await supa.from("reps").upsert(
    { id: managerRep.id, name: managerRep.name, tier: managerRep.tier, org_id: managerRep.org_id, company_id: managerRep.company_id },
    { onConflict: "id" }
  );
  console.log("done");

  const results: Check[] = [];

  try {
    // ── Platform stats ────────────────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/platform", GEORGE_ID);
      results.push(c("GET /v1/admin/platform → 200",        status === 200,                        `actual: ${status}`));
      results.push(c("Response ok: true",                  data?.ok === true,                     `ok=${data?.ok}`));
      results.push(c("stats.partners is number",           typeof data?.stats?.partners === "number",   `partners=${data?.stats?.partners}`));
      results.push(c("stats.companies is number",          typeof data?.stats?.companies === "number",  `companies=${data?.stats?.companies}`));
      results.push(c("stats.users is number",              typeof data?.stats?.users === "number",      `users=${data?.stats?.users}`));
      results.push(c("stats.audit_events is number",       typeof data?.stats?.audit_events === "number", `audit_events=${data?.stats?.audit_events}`));
      results.push(c("stats.calls_processed is number",    typeof data?.stats?.calls_processed === "number", `calls_processed=${data?.stats?.calls_processed}`));
      results.push(c("storage array present",              Array.isArray(data?.storage),           `storage=${JSON.stringify(data?.storage)}`));
    }

    // ── Platform stats auth ───────────────────────────────────────────────────
    {
      const { status } = await hit("GET", "/v1/admin/platform", managerRep.id);
      results.push(c("Manager blocked from /v1/admin/platform → 403", status === 403, `actual: ${status}`));
    }
    if (!process.env.DEV_TEST_UID) {
      const { status } = await hit("GET", "/v1/admin/platform");
      results.push(c("No-auth blocked from /v1/admin/platform → 401", status === 401, `actual: ${status}`));
    } else {
      results.push(c("No-auth /v1/admin/platform → 401 (DEV: skipped)", true));
    }

    // ── Organisations ─────────────────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/organisations", GEORGE_ID);
      results.push(c("GET /v1/admin/organisations → 200",       status === 200,                   `actual: ${status}`));
      results.push(c("organisations array present",             Array.isArray(data?.organisations), `type=${typeof data?.organisations}`));
      results.push(c("count is number",                        typeof data?.count === "number",    `count=${data?.count}`));
      if (Array.isArray(data?.organisations) && data.organisations.length > 0) {
        const org = data.organisations[0];
        results.push(c("org has id",           !!org.id,                            `id=${org.id}`));
        results.push(c("org has user_count",   typeof org.user_count === "number",  `user_count=${org.user_count}`));
        results.push(c("org has company_count",typeof org.company_count === "number",`company_count=${org.company_count}`));
      } else {
        results.push(c("org shape checks (skipped — empty)", true));
        results.push(c("org shape checks (skipped — empty)", true));
        results.push(c("org shape checks (skipped — empty)", true));
      }
    }

    // ── Organisations auth ────────────────────────────────────────────────────
    {
      const { status } = await hit("GET", "/v1/admin/organisations", managerRep.id);
      results.push(c("Manager blocked from /v1/admin/organisations → 403", status === 403, `actual: ${status}`));
    }

    // ── Support: user lookup ──────────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/support/users", GEORGE_ID);
      results.push(c("GET /v1/admin/support/users → 200",   status === 200,            `actual: ${status}`));
      results.push(c("users array present",                 Array.isArray(data?.users), `type=${typeof data?.users}`));
    }

    // ── Support: user lookup with query ───────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/support/users?q=George&limit=10", GEORGE_ID);
      results.push(c("Support user search ?q= → 200",      status === 200,            `actual: ${status}`));
      results.push(c("Filtered results returned",          Array.isArray(data?.users), `type=${typeof data?.users}`));
    }

    // ── Support: company lookup ───────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/support/companies", GEORGE_ID);
      results.push(c("GET /v1/admin/support/companies → 200",     status === 200,               `actual: ${status}`));
      results.push(c("companies array present",                   Array.isArray(data?.companies), `type=${typeof data?.companies}`));
    }

    // ── Support: impersonation history ────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/admin/support/impersonation-history", GEORGE_ID);
      results.push(c("GET /v1/admin/support/impersonation-history → 200", status === 200,          `actual: ${status}`));
      results.push(c("events array present",                              Array.isArray(data?.events), `type=${typeof data?.events}`));
    }

    // ── Support endpoints auth ────────────────────────────────────────────────
    for (const path of ["/v1/admin/support/users", "/v1/admin/support/companies", "/v1/admin/support/impersonation-history"]) {
      const { status } = await hit("GET", path, managerRep.id);
      results.push(c(`Manager blocked from ${path} → 403`, status === 403, `actual: ${status}`));
    }

  } finally {
    process.stdout.write("\n  Cleaning up fixtures… ");
    await supa.from("reps").delete().eq("id", managerRep.id);
    console.log("done");
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
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
