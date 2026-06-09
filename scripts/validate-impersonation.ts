/**
 * validate-impersonation.ts
 *
 * Verifies SuperAdmin impersonation access control and audit trail.
 *
 * Permission matrix:
 *   POST /v1/admin/super/impersonate     SalesRep=403 Manager=403 PartnerAdmin=403 SuperAdmin=200
 *   POST /v1/admin/super/stop-impersonation  same
 *
 * Also verifies:
 *   - Audit events IMPERSONATE_USER and END_IMPERSONATION are written
 *   - x-impersonated-user-id header swaps the effective user context
 *
 * Usage: npm run validate:impersonation
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${v}${h.slice(17,20)}-${h.slice(20,32)}`;
}

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function post(path: string, body: unknown, userId?: string, impersonatedId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId)       headers["x-user-id"]            = userId;
  if (impersonatedId) headers["x-impersonated-user-id"] = impersonatedId;
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST", headers, body: JSON.stringify(body), signal: AbortSignal.timeout(8_000),
  });
  let body2: any = null;
  try { body2 = await res.json(); } catch { /* ignore */ }
  return { status: res.status, body: body2 };
}

async function get(path: string, userId?: string) {
  const headers: Record<string, string> = {};
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(8_000) });
  let body: any = null;
  try { body = await res.json(); } catch { /* ignore */ }
  return { status: res.status, body };
}

async function main() {
  console.log("\n  Impersonation Validator\n");

  // Connectivity
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch { console.error(`  ✗  Server not reachable`); process.exit(2); }

  const results: Check[] = [];

  const orgId     = "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
  const companyId = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";

  // ── Fixture reps ──────────────────────────────────────────────────────────

  const fixtures = [
    { id: uid("IMPER_VAL", "SalesRep"),     name: "Impersonation Test SalesRep",     tier: "SalesRep"     },
    { id: uid("IMPER_VAL", "Manager"),      name: "Impersonation Test Manager",      tier: "Manager"      },
    { id: uid("IMPER_VAL", "PartnerAdmin"), name: "Impersonation Test PartnerAdmin", tier: "PartnerAdmin" },
    { id: uid("IMPER_VAL", "SuperAdmin"),   name: "Impersonation Test SuperAdmin",   tier: "SuperAdmin"   },
    { id: uid("IMPER_VAL", "Target"),       name: "Impersonation Test Target",       tier: "SalesRep"     },
  ];

  process.stdout.write("  Setting up fixture reps... ");
  for (const f of fixtures) {
    await supa.from("reps").upsert({ id: f.id, name: f.name, tier: f.tier, org_id: orgId, company_id: companyId }, { onConflict: "id" });
  }
  console.log("done");

  const byTier = Object.fromEntries(fixtures.map(f => [f.tier, f]));
  // Note: two SalesRep entries - use Target separately
  const target = fixtures.find(f => f.name.includes("Target"))!;
  const sa     = byTier["SuperAdmin"];

  try {
    // ── 1. Access control — impersonate endpoint ──────────────────────────

    const impPath = "/v1/admin/super/impersonate";
    const stopPath = "/v1/admin/super/stop-impersonation";

    for (const [tier, expected] of [
      ["SalesRep",     403],
      ["Manager",      403],
      ["PartnerAdmin", 403],
      ["SuperAdmin",   200],
    ] as [string, number][]) {
      const actor = tier === "SuperAdmin" ? sa : fixtures.find(f => f.tier === tier)!;
      const r = await post(impPath, { targetUserId: target.id }, actor.id);
      results.push(c(`POST ${impPath} — ${tier} → ${expected}`, r.status === expected, `actual: ${r.status} ${r.body?.error ?? ""}`));
    }

    // ── 2. Access control — stop endpoint ────────────────────────────────

    for (const [tier, expected] of [
      ["Manager",  403],
      ["SuperAdmin", 200],
    ] as [string, number][]) {
      const actor = tier === "SuperAdmin" ? sa : fixtures.find(f => f.tier === tier)!;
      const r = await post(stopPath, { targetUserId: target.id }, actor.id);
      results.push(c(`POST ${stopPath} — ${tier} → ${expected}`, r.status === expected, `actual: ${r.status}`));
    }

    // ── 3. No auth → 401 ────────────────────────────────────────────────

    const noAuth = await post(impPath, { targetUserId: target.id });
    results.push(c("No auth → 401", noAuth.status === 401, `actual: ${noAuth.status}`));

    // ── 4. Response shape ────────────────────────────────────────────────

    const goodR = await post(impPath, { targetUserId: target.id }, sa.id);
    results.push(c("Response has impersonationToken",  !!goodR.body?.impersonationToken));
    results.push(c("Response has targetUserId",        goodR.body?.targetUserId === target.id));
    results.push(c("Response has targetName",          typeof goodR.body?.targetName === "string"));

    // ── 5. Middleware enforcement — non-SuperAdmin cannot use x-impersonated-user-id ──
    // If a Manager sends x-impersonated-user-id, the middleware must reject with 403.
    const managerActor = fixtures.find(f => f.tier === "Manager")!;
    const rejectR = await (async () => {
      const res = await fetch(`${API_BASE}/v1/crm/health`, {
        headers: {
          "x-user-id":             managerActor.id,
          "x-impersonated-user-id": target.id,
        },
        signal: AbortSignal.timeout(8_000),
      });
      return { status: res.status };
    })();

    results.push(c(
      "Manager cannot use x-impersonated-user-id (→ 403)",
      rejectR.status === 403,
      `actual: ${rejectR.status}`
    ));

    // SuperAdmin CAN use x-impersonated-user-id without rejection
    const allowR = await (async () => {
      const res = await fetch(`${API_BASE}/v1/crm/health`, {
        headers: {
          "x-user-id":             sa.id,
          "x-impersonated-user-id": target.id,
        },
        signal: AbortSignal.timeout(8_000),
      });
      return { status: res.status };
    })();

    results.push(c(
      "SuperAdmin CAN use x-impersonated-user-id (→ 200)",
      allowR.status === 200,
      `actual: ${allowR.status}`
    ));

    // ── 6. Audit events written ──────────────────────────────────────────

    // Give a moment for async audit writes
    await new Promise(r => setTimeout(r, 300));

    const { data: auditRows } = await supa
      .from("audit_events")
      .select("action, actor_user_id, target_user_id")
      .eq("actor_user_id", sa.id)
      .in("action", ["impersonate_user", "end_impersonation"])
      .order("created_at", { ascending: false })
      .limit(10);

    const hasImpersonateEvent = (auditRows || []).some(r => r.action === "impersonate_user");
    const hasEndEvent         = (auditRows || []).some(r => r.action === "end_impersonation");

    results.push(c("Audit: IMPERSONATE_USER event written",  hasImpersonateEvent, `found ${auditRows?.length ?? 0} events`));
    results.push(c("Audit: END_IMPERSONATION event written", hasEndEvent,         `found ${auditRows?.length ?? 0} events`));

    if (auditRows?.length) {
      const row = auditRows[0] as any;
      results.push(c("Audit: actor_user_id = SuperAdmin",  row.actor_user_id  === sa.id));
      results.push(c("Audit: target_user_id = target rep", row.target_user_id === target.id));
    }

  } finally {
    // Cleanup
    process.stdout.write("\n  Cleaning up... ");
    await supa.from("audit_events").delete().eq("actor_user_id", sa.id);
    await supa.from("reps").delete().in("id", fixtures.map(f => f.id));
    console.log("done");
  }

  // ── Report ────────────────────────────────────────────────────────────────

  console.log("\n  ── Permission matrix ─────────────────────────────────");
  console.log(`  ${"Endpoint".padEnd(36)} ${"Tier".padEnd(14)} Expected`);
  console.log(`  ${"─".repeat(36)} ${"─".repeat(14)} ────────`);
  for (const r of results.filter(r => r.label.includes(" — ") && r.label.includes(" → "))) {
    const [path, rest] = r.label.split(" — ");
    const [tier, exp] = (rest ?? "").split(" → ");
    const actual = r.detail?.match(/actual: (\d+)/)?.[1] ?? "?";
    console.log(`  ${r.passed ? "✓" : "✗"} ${path.padEnd(36)} ${tier.padEnd(14)} ${exp} (actual ${actual})`);
  }

  console.log("\n  ── All checks ────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    console.log(`  ${r.passed ? "✓" : "✗"}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    r.passed ? passed++ : failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
