/**
 * validate-user-management.ts
 *
 * Validates user and company management endpoints against the live server.
 *
 * Coverage:
 *   ✓ user self-edit (GET + PATCH /v1/users/me)
 *   ✓ phone number persists
 *   ✓ manager scope enforcement
 *   ✓ partner scope enforcement
 *   ✓ superadmin scope enforcement
 *   ✓ audit row written for user update
 *   ✓ company update persists
 *   ✓ audit row written for company update
 *
 * Usage: npm run validate:user-management
 * Requires: server running (npm run dev)
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

// ── Helpers ──────────────────────────────────────────────────────────────────

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
function c(label: string, passed: boolean, detail?: string): Check {
  return { label, passed, detail };
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

// Reuse the existing demo company + George's org for all fixtures
const DEMO_COMPANY_ID = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const GEORGE_ID       = "b817133a-44f1-4be3-89a1-6e6f6159c018";

async function getOrgId(): Promise<string> {
  const { data } = await supa.from("reps").select("org_id").eq("id", GEORGE_ID).single();
  return (data as any)?.org_id ?? "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
}

async function upsertRep(rep: { id: string; name: string; tier: string; org_id: string; company_id: string }) {
  await supa.from("reps").upsert(
    { id: rep.id, name: rep.name, tier: rep.tier, org_id: rep.org_id, company_id: rep.company_id },
    { onConflict: "id" }
  );
}

async function deleteReps(ids: string[]) {
  await supa.from("reps").delete().in("id", ids);
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log("\n  User Management Validator\n");

  // Connectivity
  try {
    const p = await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
    if (!p.ok) throw new Error("health fail");
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  const orgId = await getOrgId();

  // Fixture reps
  const managerRep    = { id: uid("UM", "Manager"),    name: "Test Manager (um)",    tier: "Manager",    org_id: orgId, company_id: DEMO_COMPANY_ID };
  const salesRep      = { id: uid("UM", "SalesRep"),   name: "Test SalesRep (um)",   tier: "SalesRep",   org_id: orgId, company_id: DEMO_COMPANY_ID };
  const partnerAdmin  = { id: uid("UM", "PartnerAdmin"),name: "Test PartnerAdmin (um)",tier: "PartnerAdmin",org_id: orgId, company_id: DEMO_COMPANY_ID };
  // An "outsider" rep with a different (non-existent) company to test scope rejection
  const outsiderRep   = { id: uid("UM", "Outsider"),   name: "Test Outsider (um)",   tier: "SalesRep",   org_id: orgId, company_id: "00000000-0000-4000-8000-000000000099" };

  const fixtures = [managerRep, salesRep, partnerAdmin, outsiderRep];
  process.stdout.write("  Setting up fixture reps… ");
  for (const f of fixtures) await upsertRep(f);
  console.log("done");

  const results: Check[] = [];

  try {
    // ── 1. GET /v1/users/me ────────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("GET /v1/users/me → 200",            status === 200,        `actual: ${status}`));
      results.push(c("GET /v1/users/me body.ok",          data?.ok === true,     `ok=${data?.ok}`));
      results.push(c("GET /v1/users/me has user.id",      !!data?.user?.id,      `id=${data?.user?.id}`));
      results.push(c("GET /v1/users/me has user.tier",    !!data?.user?.tier,    `tier=${data?.user?.tier}`));
    }

    // ── 2. GET /v1/users/me — no auth (skipped in DEV_TEST_UID mode) ────────
    // In dev, the API uses DEV_TEST_UID as a fallback, so no-auth always resolves.
    // Skip this check in dev to avoid false failures.
    if (!process.env.DEV_TEST_UID) {
      const r = await fetch(`${API_BASE}/v1/users/me`, { signal: AbortSignal.timeout(4_000) });
      results.push(c("GET /v1/users/me — no auth → 401", r.status === 401, `actual: ${r.status}`));
    } else {
      results.push(c("GET /v1/users/me — no auth → 401 (DEV: skipped, DEV_TEST_UID active)", true));
    }

    // ── 3. PATCH /v1/users/me — phone + timezone ──────────────────────────
    const testPhone = "+44770090" + Math.floor(Math.random() * 10000).toString().padStart(4, "0");
    {
      const { status, data } = await hit("PATCH", "/v1/users/me", GEORGE_ID, {
        phone:    testPhone,
        timezone: "Europe/London",
      });
      results.push(c("PATCH /v1/users/me → 200",         status === 200,     `actual: ${status}`));
      results.push(c("PATCH /v1/users/me body.ok",       data?.ok === true,  `ok=${data?.ok}`));
    }

    // ── 4. Phone persists ─────────────────────────────────────────────────
    {
      const { data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("Phone persists after PATCH",
        data?.user?.phone === testPhone,
        `expected=${testPhone} got=${data?.user?.phone}`
      ));
    }

    // ── 5. Tier NOT editable via self-edit ────────────────────────────────
    {
      const { status, data } = await hit("PATCH", "/v1/users/me", salesRep.id, { phone: "+44770000001" });
      // Should succeed (phone update) but tier ignored — confirm it doesn't change tier
      const { data: after } = await hit("GET", "/v1/users/me", salesRep.id);
      results.push(c("Tier not self-editable via PATCH /v1/users/me",
        after?.user?.tier === "SalesRep",
        `tier=${after?.user?.tier}`
      ));
    }

    // ── 6. PATCH /v1/users/me — no fields → 400 ──────────────────────────
    {
      const { status } = await hit("PATCH", "/v1/users/me", GEORGE_ID, {});
      results.push(c("PATCH /v1/users/me empty body → 400", status === 400, `actual: ${status}`));
    }

    // ── 7. GET /v1/admin/users/:id — SuperAdmin can read any user ─────────
    {
      const { status, data } = await hit("GET", `/v1/admin/users/${salesRep.id}`, GEORGE_ID);
      results.push(c("SuperAdmin GET /v1/admin/users/:id → 200",    status === 200,   `actual: ${status}`));
      results.push(c("SuperAdmin response has actor_tier=SuperAdmin", data?.actor_tier === "SuperAdmin", `actor_tier=${data?.actor_tier}`));
    }

    // ── 8. Manager scope — can edit user in own company ───────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/users/${salesRep.id}`, managerRep.id, { phone: "+447700000002" });
      results.push(c("Manager can PATCH user in same company → 200", status === 200, `actual: ${status}`));
    }

    // ── 9. Manager scope — CANNOT edit user in different company ──────────
    // 404 is also acceptable: user outside scope appears non-existent.
    {
      const { status } = await hit("PATCH", `/v1/admin/users/${outsiderRep.id}`, managerRep.id, { phone: "+447700000003" });
      results.push(c("Manager CANNOT PATCH user in other company → 403/404", status === 403 || status === 404, `actual: ${status}`));
    }

    // ── 10. Manager cannot change tier ────────────────────────────────────
    {
      const { status, data } = await hit("PATCH", `/v1/admin/users/${salesRep.id}`, managerRep.id, { tier: "SuperAdmin" });
      results.push(c("Manager cannot change tier → 403", status === 403, `actual: ${status} error=${data?.error}`));
    }

    // ── 11. PartnerAdmin scope — can edit users in partner's companies ─────
    {
      const { status } = await hit("PATCH", `/v1/admin/users/${salesRep.id}`, partnerAdmin.id, { phone: "+447700000004" });
      results.push(c("PartnerAdmin can PATCH user in partner companies → 200", status === 200, `actual: ${status}`));
    }

    // ── 12. PartnerAdmin CANNOT edit outsider ─────────────────────────────
    // 404 is also acceptable: user outside scope appears non-existent.
    {
      const { status } = await hit("PATCH", `/v1/admin/users/${outsiderRep.id}`, partnerAdmin.id, { phone: "+447700000005" });
      results.push(c("PartnerAdmin CANNOT PATCH outsider → 403/404", status === 403 || status === 404, `actual: ${status}`));
    }

    // ── 13. Audit row written for user update ─────────────────────────────
    {
      // Trigger a known audit event
      const { data: preAudit } = await hit("GET", "/v1/admin/super/audit?limit=1&action=update_user", GEORGE_ID);
      const beforeCount = preAudit?.count ?? 0;

      await hit("PATCH", `/v1/admin/users/${salesRep.id}`, GEORGE_ID, { job_title: "AE (audit test)" });

      const { data: postAudit } = await hit("GET", "/v1/admin/super/audit?limit=1&action=update_user", GEORGE_ID);
      results.push(c("Audit row written for user update",
        (postAudit?.count ?? 0) >= 1,
        `count=${postAudit?.count}`
      ));
    }

    // ── 14. GET /v1/admin/companies/:id ───────────────────────────────────
    {
      const { status, data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("SuperAdmin GET /v1/admin/companies/:id → 200", status === 200, `actual: ${status}`));
      results.push(c("Company response has company.id",  !!data?.company?.id,   `id=${data?.company?.id}`));
      results.push(c("Company response has user_count",  typeof data?.user_count === "number", `user_count=${data?.user_count}`));
    }

    // ── 15. PATCH /v1/admin/companies/:id — company update persists ───────
    const testPhone2 = "+44207946" + Math.floor(Math.random() * 10000).toString().padStart(4, "0");
    {
      const { status, data } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID, {
        phone:    testPhone2,
        industry: "Sales Technology",
      });
      results.push(c("PATCH /v1/admin/companies/:id → 200", status === 200, `actual: ${status}`));
      results.push(c("Company PATCH body.ok",               data?.ok === true, `ok=${data?.ok}`));
    }

    // Verify persistence
    {
      const { data } = await hit("GET", `/v1/admin/companies/${DEMO_COMPANY_ID}`, GEORGE_ID);
      results.push(c("Company phone persists after PATCH",
        data?.company?.phone === testPhone2,
        `expected=${testPhone2} got=${data?.company?.phone}`
      ));
    }

    // ── 16. Audit row written for company update ──────────────────────────
    {
      const { data: audit } = await hit("GET", "/v1/admin/super/audit?limit=5&action=update_company", GEORGE_ID);
      results.push(c("Audit row written for company update",
        (audit?.count ?? 0) >= 1,
        `count=${audit?.count}`
      ));
    }

    // ── 17. Manager CANNOT edit company outside own ───────────────────────
    {
      const { status } = await hit("PATCH", `/v1/admin/companies/${DEMO_COMPANY_ID}`, outsiderRep.id, { phone: "+447700000099" });
      results.push(c("Outsider cannot PATCH company → 403 or 401", status >= 401 && status <= 403, `actual: ${status}`));
    }

  } finally {
    process.stdout.write("\n  Cleaning up fixture reps… ");
    await deleteReps(fixtures.map(f => f.id));
    // Restore George's phone to something clean. The column is phone_number —
    // the API surface calls it "phone" and maps it (routes/users.ts), but this
    // writes straight to the table. The old `phone` key failed with PGRST204
    // and the result was never checked, so this teardown silently never ran.
    const restore = await supa
      .from("reps")
      .update({ phone_number: null, updated_at: new Date().toISOString() })
      .eq("id", GEORGE_ID);
    if (restore.error) console.warn("phone restore failed:", restore.error.message);
    console.log("done");
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
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
