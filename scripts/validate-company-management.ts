/**
 * validate-company-management.ts
 *
 * Validates company management endpoints against the live server.
 *
 * Coverage:
 *   ✓ GET /v1/admin/companies/:id returns correct shape
 *   ✓ PATCH /v1/admin/companies/:id persists name, website, industry, phone, address
 *   ✓ active toggle persists
 *   ✓ audit row written (update_company action)
 *   ✓ Manager cannot edit company outside their own
 *   ✓ SuperAdmin can edit any company
 *
 * Usage: npm run validate:company-management
 * Requires: server running (npm run dev)
 *           sql/20260605d_user_company_profile_fields.sql applied
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${v}${h.slice(17,20)}-${h.slice(20,32)}`;
}

async function hit(method: string, path: string, userId: string, body?: object) {
  const init: RequestInit = {
    method,
    headers: { "x-user-id": userId, "content-type": "application/json" },
    signal: AbortSignal.timeout(8_000),
  };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

const DEMO_COMPANY_ID = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const GEORGE_ID       = "b817133a-44f1-4be3-89a1-6e6f6159c018";

async function getOrgId(): Promise<string> {
  const { data } = await supa.from("reps").select("org_id").eq("id", GEORGE_ID).single();
  return (data as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
}

async function main() {
  console.log("\n  Company Management Validator\n");

  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  const orgId = await getOrgId();

  // Fixture: a Manager scoped to DEMO_COMPANY_ID, and an outsider
  const managerRep  = { id: uid("CM", "Manager"),  name: "CM Test Manager",  tier: "Manager",  org_id: orgId, company_id: DEMO_COMPANY_ID };
  const outsiderRep = { id: uid("CM", "Outsider"), name: "CM Test Outsider", tier: "Manager",  org_id: orgId, company_id: "00000000-0000-4000-8000-000000000099" };
  const fixtures    = [managerRep, outsiderRep];

  process.stdout.write("  Setting up fixture reps… ");
  for (const f of fixtures) {
    await supa.from("reps").upsert(
      { id: f.id, name: f.name, tier: f.tier, org_id: f.org_id, company_id: f.company_id },
      { onConflict: "id" }
    );
  }
  console.log("done");

  const results: Check[] = [];

  // Snapshot original values so we can restore
  const { data: orig } = await supa
    .from("companies")
    .select("name, website, industry, phone_number, address, is_active")
    .eq("id", DEMO_COMPANY_ID)
    .maybeSingle();

  try {
    // ── 1. GET /v1/admin/companies/:id ────────────────────────────────────────
    {
      const { status, data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("GET /v1/admin/companies/:id → 200",      status === 200,                    `actual: ${status}`));
      results.push(c("Response has company.id",               !!data?.company?.id,               `id=${data?.company?.id}`));
      results.push(c("Response has company.name",             !!data?.company?.name,             `name=${data?.company?.name}`));
      results.push(c("Response has user_count (number)",      typeof data?.user_count === "number", `user_count=${data?.user_count}`));
      results.push(c("Response has actor_tier",               !!data?.actor_tier,               `actor_tier=${data?.actor_tier}`));
    }

    // ── 2. GET — Manager can read own company ─────────────────────────────────
    {
      const { status } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, managerRep.id);
      results.push(c("Manager GET own company → 200", status === 200, `actual: ${status}`));
    }

    // ── 3. GET — Outsider cannot read company ─────────────────────────────────
    {
      const { status } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, outsiderRep.id);
      results.push(c("Outsider GET company → 403", status === 403, `actual: ${status}`));
    }

    // ── 4. PATCH — update website + industry ─────────────────────────────────
    const testWebsite  = `https://test-${Date.now()}.example.com`;
    const testIndustry = "FinTech";
    {
      const { status, data } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, {
        website:  testWebsite,
        industry: testIndustry,
      });
      results.push(c("PATCH /v1/admin/companies/:id → 200", status === 200, `actual: ${status} err=${data?.error}`));
      results.push(c("PATCH body.ok",                       data?.ok === true, `ok=${data?.ok}`));
    }

    // ── 5. Persistence — website and industry ────────────────────────────────
    {
      const { data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("website persists after PATCH",  data?.company?.website  === testWebsite,  `got=${data?.company?.website}`));
      results.push(c("industry persists after PATCH", data?.company?.industry === testIndustry, `got=${data?.company?.industry}`));
    }

    // ── 6. PATCH — phone + address ────────────────────────────────────────────
    const testPhone   = "+44207946" + Math.floor(Math.random() * 10000).toString().padStart(4, "0");
    const testAddress = "1 Test Street, London";
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, {
        phone:   testPhone,
        address: testAddress,
      });
      results.push(c("PATCH phone + address → 200", status === 200, `actual: ${status}`));
    }

    // Verify persistence
    {
      const { data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("phone persists",   data?.company?.phone   === testPhone,   `got=${data?.company?.phone}`));
      results.push(c("address persists", data?.company?.address === testAddress, `got=${data?.company?.address}`));
    }

    // ── 7. PATCH — active flag ────────────────────────────────────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, { active: false });
      results.push(c("PATCH active=false → 200", status === 200, `actual: ${status}`));
      const { data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("active=false persists", data?.company?.active === false, `got=${data?.company?.active}`));

      // Restore active
      await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, { active: true });
    }

    // ── 8. Empty body → 400 ───────────────────────────────────────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, {});
      results.push(c("PATCH empty body → 400", status === 400, `actual: ${status}`));
    }

    // ── 9. Manager can PATCH own company ──────────────────────────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, managerRep.id, { industry: "Sales" });
      results.push(c("Manager PATCH own company → 200", status === 200, `actual: ${status}`));
    }

    // ── 10. Outsider cannot PATCH company ────────────────────────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, outsiderRep.id, { industry: "Hacked" });
      results.push(c("Outsider PATCH company → 403", status === 403, `actual: ${status}`));
    }

    // ── 11. Audit row written ─────────────────────────────────────────────────
    {
      await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, { industry: "Audit Test" });
      const { data: audit } = await hit("GET", "/v1/admin/super/audit?limit=5&action=update_company", GEORGE_ID);
      results.push(c("Audit row written for company update", (audit?.count ?? 0) >= 1, `count=${audit?.count}`));
    }

  } finally {
    process.stdout.write("\n  Restoring company + cleaning fixtures… ");
    if (orig) {
      await supa.from("companies").update({
        website:      (orig as any).website      ?? null,
        industry:     (orig as any).industry     ?? null,
        phone_number: (orig as any).phone_number ?? null,
        address:      (orig as any).address      ?? null,
        is_active:    (orig as any).is_active    ?? true,
      }).eq("id", DEMO_COMPANY_ID);
    }
    await supa.from("reps").delete().in("id", fixtures.map(f => f.id));
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
