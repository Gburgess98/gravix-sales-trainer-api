/**
 * validate-audit.ts
 *
 * Verifies the audit_events table and logAuditEvent() infrastructure:
 *   - table exists with correct columns
 *   - insert works
 *   - read works (GET /v1/admin/super/audit)
 *   - pagination works (cursor)
 *   - wrong tier blocked (403)
 *   - no auth blocked (401)
 *
 * Usage: npm run validate:audit
 * Requires: server running + sql/20260605c_audit_events.sql applied
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

async function hit(path: string, userId?: string): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(8_000) });
  let body: any = null;
  try { body = await res.json(); } catch { /* ignore */ }
  return { status: res.status, body };
}

async function main() {
  console.log("\n  Audit Infrastructure Validator\n");

  // Connectivity
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  const results: Check[] = [];

  // ── 1. Table exists ────────────────────────────────────────────────────────

  const { data: probe, error: probeErr } = await supa.from("audit_events").select("id").limit(1);

  if (probeErr) {
    results.push(c("audit_events table exists", false, probeErr.message));
    console.error("\n  ✗  Run sql/20260605c_audit_events.sql in Supabase SQL editor.\n");
    report(results); process.exit(1);
  }

  results.push(c("audit_events table exists", true));

  // Correct columns
  const testInsertId = uid("AUDIT_VAL", "schema_probe");
  const { data: inserted, error: insErr } = await supa
    .from("audit_events")
    .insert({
      id:             testInsertId,
      actor_user_id:  uid("AUDIT_VAL", "actor"),
      target_user_id: uid("AUDIT_VAL", "target"),
      action:         "create_account",
      entity_type:    "account",
      entity_id:      uid("AUDIT_VAL", "entity"),
      metadata:       { test: true },
    })
    .select("id, actor_user_id, action, entity_type, entity_id, metadata, created_at")
    .single();

  results.push(c("audit_events insert works", !insErr, insErr?.message));

  if (!insErr && inserted) {
    const row = inserted as any;
    results.push(c("row.action correct",      row.action      === "create_account"));
    results.push(c("row.entity_type correct", row.entity_type === "account"));
    results.push(c("row.metadata is object",  typeof row.metadata === "object"));
    results.push(c("row.created_at present",  !!row.created_at));

    // Print a sample row
    console.log("  Sample audit row:");
    console.log("  " + JSON.stringify({
      id:           row.id,
      action:       row.action,
      entity_type:  row.entity_type,
      entity_id:    row.entity_id,
      metadata:     row.metadata,
      created_at:   row.created_at,
    }, null, 2).replace(/\n/g, "\n  "));
    console.log();
  }

  // ── 2. Fixture SuperAdmin for API tests ────────────────────────────────────

  const orgId     = "89f61a54-dc76-4ce8-b408-500afd5bdcdb";
  const companyId = "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
  const saId      = uid("AUDIT_VAL", "super_admin");
  const mgId      = uid("AUDIT_VAL", "manager");

  await supa.from("reps").upsert([
    { id: saId, name: "Audit Test SuperAdmin", tier: "SuperAdmin", org_id: orgId, company_id: companyId },
    { id: mgId, name: "Audit Test Manager",    tier: "Manager",    org_id: orgId, company_id: companyId },
  ], { onConflict: "id" });

  // ── 3. API — auth checks ──────────────────────────────────────────────────

  const noAuth  = await hit("/v1/admin/super/audit");
  const wrongTier = await hit("/v1/admin/super/audit", mgId);
  results.push(c("GET /super/audit — no auth → 401",    noAuth.status    === 401, `actual: ${noAuth.status}`));
  results.push(c("GET /super/audit — Manager → 403",    wrongTier.status === 403, `actual: ${wrongTier.status}`));

  // ── 4. API — SuperAdmin can read ──────────────────────────────────────────

  const r = await hit("/v1/admin/super/audit", saId);
  results.push(c("GET /super/audit — SuperAdmin → 200",       r.status === 200,               `actual: ${r.status}`));
  results.push(c("response has ok: true",                    r.body?.ok === true));
  results.push(c("response has events array",                Array.isArray(r.body?.events)));
  results.push(c("response has count",                       typeof r.body?.count === "number"));

  // ── 5. Pagination — cursor ────────────────────────────────────────────────

  // Insert two more rows with known timestamps
  const ts1 = new Date(Date.now() - 2000).toISOString();
  const ts2 = new Date(Date.now() - 1000).toISOString();
  await supa.from("audit_events").insert([
    { id: uid("AUDIT_VAL", "page1"), actor_user_id: saId, action: "login", created_at: ts1 },
    { id: uid("AUDIT_VAL", "page2"), actor_user_id: saId, action: "logout", created_at: ts2 },
  ]);

  const page1 = await hit(`/v1/admin/super/audit?limit=1&actorId=${saId}`, saId);
  const hasNextCursor = page1.body?.nextCursor !== undefined;
  results.push(c("pagination: limit=1 returns nextCursor field",   hasNextCursor,                      `nextCursor: ${page1.body?.nextCursor}`));
  results.push(c("pagination: limit=1 returns 1 event",           page1.body?.events?.length === 1, `count: ${page1.body?.events?.length}`));

  if (page1.body?.nextCursor) {
    const page2 = await hit(`/v1/admin/super/audit?limit=1&actorId=${saId}&cursor=${encodeURIComponent(page1.body.nextCursor)}`, saId);
    results.push(c("pagination: page 2 returns different events",
      Array.isArray(page2.body?.events) && page2.body.events.length > 0 &&
      page2.body.events[0]?.id !== page1.body?.events?.[0]?.id,
      `page2 first id: ${page2.body?.events?.[0]?.id}`
    ));
  }

  // ── Cleanup ────────────────────────────────────────────────────────────────

  await supa.from("audit_events").delete().eq("actor_user_id", saId);
  await supa.from("audit_events").delete().eq("id", testInsertId);
  await supa.from("reps").delete().in("id", [saId, mgId]);

  // ── AUDIT_ACTIONS constants spot-check ────────────────────────────────────

  const { AUDIT_ACTIONS } = await import("../src/lib/audit.ts");
  results.push(c("AUDIT_ACTIONS.CREATE_ACCOUNT = 'create_account'",  AUDIT_ACTIONS.CREATE_ACCOUNT  === "create_account"));
  results.push(c("AUDIT_ACTIONS.IMPERSONATE_USER defined",           !!AUDIT_ACTIONS.IMPERSONATE_USER));
  results.push(c("AUDIT_ACTIONS.END_IMPERSONATION defined",          !!AUDIT_ACTIONS.END_IMPERSONATION));

  report(results);
}

function report(results: Check[]) {
  console.log("  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    console.log(`  ${r.passed ? "✓" : "✗"}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
