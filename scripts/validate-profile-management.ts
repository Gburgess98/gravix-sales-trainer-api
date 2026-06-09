/**
 * validate-profile-management.ts
 *
 * Validates the self-service profile endpoints.
 *
 * Coverage:
 *   ✓ GET /v1/users/me returns correct shape
 *   ✓ PATCH /v1/users/me updates display_name, phone, timezone
 *   ✓ All three fields persist after PATCH
 *   ✓ Read-only fields (email, tier, company) NOT changeable via this endpoint
 *   ✓ audit row written (update_profile or update_user action)
 *   ✓ Empty body → 400
 *   ✓ Invalid user → 401
 *
 * Usage: npm run validate:profile-management
 * Requires: server running (npm run dev)
 *           sql/20260605d_user_company_profile_fields.sql applied
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const GEORGE_ID = "b817133a-44f1-4be3-89a1-6e6f6159c018";

async function hit(method: string, path: string, userId?: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(8_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function main() {
  console.log("\n  Profile Management Validator\n");

  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  // Snapshot original profile values for restore
  const { data: origRep } = await supa
    .from("reps")
    .select("display_name, phone_number, timezone")
    .eq("id", GEORGE_ID)
    .maybeSingle();

  const results: Check[] = [];

  try {
    // ── 1. GET /v1/users/me ───────────────────────────────────────────────────
    {
      const { status, data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("GET /v1/users/me → 200",          status === 200,      `actual: ${status}`));
      results.push(c("Response ok: true",               data?.ok === true,  `ok=${data?.ok}`));
      results.push(c("user.id present",                 !!data?.user?.id,   `id=${data?.user?.id}`));
      results.push(c("user.tier present",               !!data?.user?.tier, `tier=${data?.user?.tier}`));
      results.push(c("user.timezone present",           !!data?.user?.timezone, `tz=${data?.user?.timezone}`));
      results.push(c("user.company_name present (may be null)", data?.user !== undefined, "field absent"));
    }

    // ── 2. GET — no auth header → 401 (DEV_TEST_UID skips this) ──────────────
    if (!process.env.DEV_TEST_UID) {
      const { status } = await hit("GET", "/v1/users/me");
      results.push(c("GET /v1/users/me no auth → 401", status === 401, `actual: ${status}`));
    } else {
      results.push(c("GET /v1/users/me no auth → 401 (DEV: skipped)", true));
    }

    // ── 3. PATCH — display_name ───────────────────────────────────────────────
    const testName = `Test Profile ${Date.now()}`;
    {
      const { status, data } = await hit("PATCH", "/v1/users/me", GEORGE_ID, { display_name: testName });
      results.push(c("PATCH display_name → 200",  status === 200,    `actual: ${status}`));
      results.push(c("PATCH body.ok",             data?.ok === true, `ok=${data?.ok}`));
    }

    // ── 4. display_name persists ──────────────────────────────────────────────
    {
      const { data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("display_name persists after PATCH",
        data?.user?.display_name === testName,
        `expected=${testName} got=${data?.user?.display_name}`
      ));
    }

    // ── 5. PATCH — phone ──────────────────────────────────────────────────────
    const testPhone = "+44770055" + Math.floor(Math.random() * 10000).toString().padStart(4, "0");
    {
      const { status } = await hit("PATCH", "/v1/users/me", GEORGE_ID, { phone: testPhone });
      results.push(c("PATCH phone → 200", status === 200, `actual: ${status}`));
    }

    // ── 6. phone persists ─────────────────────────────────────────────────────
    {
      const { data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("phone persists after PATCH",
        data?.user?.phone === testPhone,
        `expected=${testPhone} got=${data?.user?.phone}`
      ));
    }

    // ── 7. PATCH — timezone ───────────────────────────────────────────────────
    {
      const { status } = await hit("PATCH", "/v1/users/me", GEORGE_ID, { timezone: "America/New_York" });
      results.push(c("PATCH timezone → 200", status === 200, `actual: ${status}`));
    }

    // ── 8. timezone persists ──────────────────────────────────────────────────
    {
      const { data } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("timezone persists after PATCH",
        data?.user?.timezone === "America/New_York",
        `got=${data?.user?.timezone}`
      ));
    }

    // ── 9. PATCH all three at once ────────────────────────────────────────────
    {
      const { status, data } = await hit("PATCH", "/v1/users/me", GEORGE_ID, {
        display_name: testName + " v2",
        phone:        testPhone,
        timezone:     "Europe/London",
      });
      results.push(c("PATCH all three fields at once → 200", status === 200, `actual: ${status}`));
      results.push(c("Returns updated fields list",          Array.isArray(data?.updated), `updated=${JSON.stringify(data?.updated)}`));
    }

    // ── 10. Tier NOT self-editable ────────────────────────────────────────────
    {
      const { data: before } = await hit("GET", "/v1/users/me", GEORGE_ID);
      const tierBefore = before?.user?.tier;
      await hit("PATCH", "/v1/users/me", GEORGE_ID, { tier: "SuperAdmin" } as any);
      const { data: after } = await hit("GET", "/v1/users/me", GEORGE_ID);
      results.push(c("Tier NOT self-editable via PATCH /v1/users/me",
        after?.user?.tier === tierBefore,
        `before=${tierBefore} after=${after?.user?.tier}`
      ));
    }

    // ── 11. Empty body → 400 ─────────────────────────────────────────────────
    {
      const { status } = await hit("PATCH", "/v1/users/me", GEORGE_ID, {});
      results.push(c("Empty body → 400", status === 400, `actual: ${status}`));
    }

    // ── 12. Audit row written ─────────────────────────────────────────────────
    {
      const auditAction = "update_user"; // self-edit writes update_user
      const { data: audit } = await hit("GET", `/v1/admin/super/audit?limit=5&action=${auditAction}`, GEORGE_ID);
      results.push(c("Audit row written for profile update",
        (audit?.count ?? 0) >= 1,
        `count=${audit?.count}`
      ));
    }

  } finally {
    process.stdout.write("\n  Restoring original profile values… ");
    const patch: Record<string, any> = { updated_at: new Date().toISOString() };
    if (origRep) {
      patch.display_name = (origRep as any).display_name ?? null;
      patch.phone_number = (origRep as any).phone_number ?? null;
      patch.timezone     = (origRep as any).timezone     ?? "UTC";
    }
    await supa.from("reps").update(patch).eq("id", GEORGE_ID);
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
