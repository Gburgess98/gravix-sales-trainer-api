/**
 * validate-lighthouse.ts
 *
 * End-to-end access verification for the 4 key demo personas.
 *
 * Lighthouse users:
 *   Dana White        — Manager
 *   Hunter Campbell   — Manager
 *   Conor McGregor    — SalesRep
 *   Anderson Silva    — SalesRep
 *
 * Checks:
 *   ✓ CRM access    — can read contacts + accounts
 *   ✓ Dashboard     — leaderboard + rep overview accessible
 *   ✓ Assignments   — can read own assignments
 *   ✓ Profile       — GET /v1/users/me returns correct tier
 *   ✓ Company iso.  — SalesRep cannot reach admin user-management endpoints (403)
 *   ✓ Company iso.  — Manager can reach admin endpoints for own company
 *   ✓ Company iso.  — SalesRep cannot GET another rep's admin profile (403)
 *
 * Requirements:
 *   - Server running (npm run dev)
 *   - Demo seed applied (npm run seed:demo)
 *
 * Usage: npm run validate:lighthouse
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const DEFAULT_ORG = process.env.DEFAULT_ORG_ID ?? "";

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

// ── Known personas ────────────────────────────────────────────────────────────

const PERSONAS = {
  danaWhite:      { name: "Dana White",      email: "dana.white@ufcelite.demo",      expectedTier: "Manager"  },
  hunterCampbell: { name: "Hunter Campbell", email: "hunter.campbell@ufcelite.demo", expectedTier: "Manager"  },
  conorMcGregor:  { name: "Conor McGregor",  email: "conor.mcgregor@ufcelite.demo",  expectedTier: "SalesRep" },
  andersonSilva:  { name: "Anderson Silva",  email: "anderson.silva@ufcelite.demo",  expectedTier: "SalesRep" },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function hit(method: string, path: string, userId: string, extraHeaders?: Record<string, string>) {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-user-id":    userId,
    "x-org-id":     DEFAULT_ORG,
    ...extraHeaders,
  };
  const res = await fetch(`${API_BASE}${path}`, { method, headers, signal: AbortSignal.timeout(10_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function lookupUserId(email: string): Promise<string | null> {
  // Try reps.email first (added in sprint-3 migration)
  const { data: rep } = await supa.from("reps").select("id").eq("email", email.toLowerCase()).maybeSingle();
  if ((rep as any)?.id) return (rep as any).id;

  // Fallback: name match within the org
  const name = Object.values(PERSONAS).find(p => p.email === email)?.name ?? "";
  if (name) {
    const { data: byName } = await supa.from("reps").select("id").eq("name", name).maybeSingle();
    if ((byName as any)?.id) return (byName as any).id;
  }

  // Last resort: auth.users list
  try {
    let page = 1;
    while (true) {
      const { data: list, error } = await supa.auth.admin.listUsers({ page, perPage: 1000 });
      if (error || !list?.users?.length) break;
      const found = list.users.find((u: any) => u.email?.toLowerCase() === email.toLowerCase());
      if (found) return (found as any).id;
      if (list.users.length < 1000) break;
      page++;
    }
  } catch { /* ignore */ }

  return null;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log("\n  Lighthouse Validator — UFC Elite Demo Personas\n");

  // Connectivity
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  // ── Resolve user IDs ──────────────────────────────────────────────────────────
  process.stdout.write("  Resolving persona IDs... ");
  const ids: Record<keyof typeof PERSONAS, string | null> = {
    danaWhite:      await lookupUserId(PERSONAS.danaWhite.email),
    hunterCampbell: await lookupUserId(PERSONAS.hunterCampbell.email),
    conorMcGregor:  await lookupUserId(PERSONAS.conorMcGregor.email),
    andersonSilva:  await lookupUserId(PERSONAS.andersonSilva.email),
  };

  const missing = Object.entries(ids).filter(([, v]) => !v).map(([k]) => k);
  if (missing.length > 0) {
    console.log(`\n  ✗  Cannot find IDs for: ${missing.join(", ")}`);
    console.log("     Run:  npm run seed:demo   then retry.\n");
    process.exit(2);
  }
  console.log("done");
  console.log(`  Dana White:      ${ids.danaWhite}`);
  console.log(`  Hunter Campbell: ${ids.hunterCampbell}`);
  console.log(`  Conor McGregor:  ${ids.conorMcGregor}`);
  console.log(`  Anderson Silva:  ${ids.andersonSilva}\n`);

  const dw  = ids.danaWhite!;
  const hc  = ids.hunterCampbell!;
  const cmg = ids.conorMcGregor!;
  const as_ = ids.andersonSilva!;

  const results: Check[] = [];

  // ── PROFILE / IDENTITY ────────────────────────────────────────────────────────

  for (const [key, userId] of Object.entries({ danaWhite: dw, hunterCampbell: hc, conorMcGregor: cmg, andersonSilva: as_ })) {
    const p = PERSONAS[key as keyof typeof PERSONAS];
    const { status, data } = await hit("GET", "/v1/users/me", userId);
    results.push(c(`${p.name}: GET /v1/users/me → 200`,       status === 200, `actual: ${status}`));
    results.push(c(`${p.name}: tier = ${p.expectedTier}`,     data?.user?.tier === p.expectedTier, `actual: ${data?.user?.tier}`));
  }

  // ── DASHBOARD ─────────────────────────────────────────────────────────────────

  // Leaderboard is org-scoped — accessible to any logged-in user
  for (const [name, userId] of [["Dana White", dw], ["Conor McGregor", cmg]]) {
    const { status, data } = await hit("GET", "/v1/dashboard/leaderboard?limit=5", userId);
    results.push(c(`${name}: dashboard leaderboard → 200`, status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c(`${name}: leaderboard has items array`,  Array.isArray(data?.items), `type=${typeof data?.items}`));
  }

  // Rep overview — each rep can see their own data
  for (const [name, userId] of [["Conor McGregor", cmg], ["Anderson Silva", as_]]) {
    const { status, data } = await hit("GET", `/v1/reps/${userId}/overview`, userId);
    results.push(c(`${name}: rep overview → 200 or 404`, status === 200 || status === 404, `actual: ${status} error=${data?.error}`));
    // 404 is acceptable if the reps table row exists but overview query finds nothing (no calls yet)
  }

  // ── CRM ACCESS ────────────────────────────────────────────────────────────────

  // Contacts list — all authenticated reps can list their CRM contacts
  for (const [name, userId] of [["Dana White", dw], ["Conor McGregor", cmg], ["Anderson Silva", as_]]) {
    const { status, data } = await hit("GET", "/v1/crm/contacts?limit=5", userId);
    results.push(c(`${name}: CRM contacts → 200`, status === 200, `actual: ${status} error=${data?.error}`));
  }

  // CRM activities list — GET /v1/crm/activities exists and is accessible to all authenticated users
  for (const [name, userId] of [["Hunter Campbell", hc], ["Anderson Silva", as_]]) {
    const { status, data } = await hit("GET", "/v1/crm/activities?limit=5", userId);
    results.push(c(`${name}: CRM activities → 200`, status === 200, `actual: ${status} error=${data?.error}`));
  }

  // ── ASSIGNMENTS ───────────────────────────────────────────────────────────────

  // Each rep can read their own assignments
  for (const [name, userId] of [["Conor McGregor", cmg], ["Anderson Silva", as_]]) {
    const { status, data } = await hit("GET", `/v1/assignments?repId=${userId}`, userId);
    // Accept 200 (has assignments) or 200 with empty list — just not 4xx
    results.push(c(`${name}: assignments list → 200`, status === 200, `actual: ${status} error=${data?.error}`));
  }

  // Managers can query assignments for their reps
  for (const [manName, manId, repId] of [["Dana White", dw, cmg], ["Hunter Campbell", hc, as_]]) {
    const { status } = await hit("GET", `/v1/assignments?repId=${repId}`, manId);
    results.push(c(`${manName} (Manager): can query rep assignments → 200`, status === 200, `actual: ${status}`));
  }

  // ── COMPANY ISOLATION ─────────────────────────────────────────────────────────

  // SalesRep cannot reach admin user-list (requireManager gate → 403)
  for (const [name, userId] of [["Conor McGregor", cmg], ["Anderson Silva", as_]]) {
    const { status } = await hit("GET", "/v1/admin/users", userId);
    results.push(c(`${name} (SalesRep): admin users list → 403`, status === 403, `actual: ${status}`));
  }

  // SalesRep cannot access partner/companies endpoint (requirePartnerAdmin → 403)
  for (const [name, userId] of [["Conor McGregor", cmg], ["Anderson Silva", as_]]) {
    const { status } = await hit("GET", "/v1/admin/partner/companies", userId);
    results.push(c(`${name} (SalesRep): partner companies → 403`, status === 403, `actual: ${status}`));
  }

  // SalesRep cannot GET admin user profile for another rep
  // (assertUserEditScope returns 403 for non-manager tiers)
  {
    const { status } = await hit("GET", `/v1/admin/users/${as_}`, cmg);
    results.push(c("Conor cannot GET Anderson's admin profile → 403", status === 403, `actual: ${status}`));
  }

  // Manager CAN get admin user profile for a rep in same company
  // (assertUserEditScope: Manager → own company OK)
  {
    const { status } = await hit("GET", `/v1/admin/users/${cmg}`, dw);
    results.push(c("Dana White (Manager) can GET Conor's admin profile → 200", status === 200, `actual: ${status}`));
  }

  // SalesRep cannot reach SuperAdmin-only platform endpoint
  {
    const { status } = await hit("GET", "/v1/admin/platform", cmg);
    results.push(c("Conor (SalesRep): /admin/platform → 403", status === 403, `actual: ${status}`));
  }

  // Manager cannot reach SuperAdmin-only platform endpoint either
  {
    const { status } = await hit("GET", "/v1/admin/platform", dw);
    results.push(c("Dana White (Manager): /admin/platform → 403", status === 403, `actual: ${status}`));
  }

  // ── REPORT ────────────────────────────────────────────────────────────────────
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
