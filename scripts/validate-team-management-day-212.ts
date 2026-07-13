/**
 * validate-team-management-day-212.ts
 *
 * Day 212 — Team management scope foundation + rep_missing_office resolution.
 *
 * Coverage:
 *   ✓ GET /v1/team/members — no auth            → 401
 *   ✓ GET /v1/team/members — SalesRep (Nate)    → 403 (no manager bypass)
 *   ✓ GET /v1/team/members — Manager (Dana)     → 200 + company-scoped list
 *   ✓ members include Nate + Michael with scope "company" (null office)
 *   ✓ seat summary from company_licences (canonical) with used/available
 *   ✓ cross-company manager sees NO UFC members (isolation)
 *   ✓ POST /v1/assignments Dana → Nate succeeds (rep_missing_office resolved)
 *   ✓ created assignment stamped with rep company_id, office_id null
 *   ✓ POST /v1/assignments Dana → cross-company rep → 403 rep_out_of_scope
 *   ✓ POST /v1/assignments as SalesRep → 403 (manager gate intact)
 *
 * Requirements: server running (npm run dev), UFC demo seed applied.
 * Usage: npm run validate:team-management
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function hit(method: string, path: string, userId?: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(15_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function repIdByEmail(email: string): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", email).maybeSingle();
  return (data as any)?.id ?? null;
}

// Cross-company fixtures — synthetic company that must stay invisible to Dana.
const XCOMPANY_ID = uid("DAY212", "cross-company");
const XMANAGER_ID = uid("DAY212", "cross-manager");
const XREP_ID     = uid("DAY212", "cross-rep");

async function seedCrossCompanyFixtures(orgId: string) {
  // reps.company_id has an FK to companies — the synthetic company must exist.
  const { error: compErr } = await supa
    .from("companies")
    .upsert(
      {
        id: XCOMPANY_ID,
        tmc_id: "5055e1b6-fb33-45c0-959a-d7dd45f98a13", // demo partner (same TMC, different company)
        partner_id: "5055e1b6-fb33-45c0-959a-d7dd45f98a13",
        name: "Day212 Cross Company (validator)",
        slug: "day212-cross-company-validator",
      },
      { onConflict: "id" }
    );
  if (compErr) throw new Error(`fixture company upsert failed: ${compErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: XMANAGER_ID, name: "Day212 X Manager", tier: "Manager",  org_id: orgId, company_id: XCOMPANY_ID },
      { id: XREP_ID,     name: "Day212 X Rep",     tier: "SalesRep", org_id: orgId, company_id: XCOMPANY_ID },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture reps upsert failed: ${repErr.message}`);
}

async function cleanup(assignmentIds: string[]) {
  if (assignmentIds.length) {
    await supa.from("assignments").delete().in("id", assignmentIds);
    for (const id of assignmentIds) {
      await supa.from("crm_activities").delete().eq("type", "drill_memory").contains("meta", { assignment_id: id });
    }
  }
  await supa.from("reps").delete().in("id", [XMANAGER_ID, XREP_ID]);
  await supa.from("companies").delete().eq("id", XCOMPANY_ID);
}

async function main() {
  console.log("\n  Team Management Validator — Day 212\n");

  const danaId    = await repIdByEmail("dana.white@ufcelite.demo");
  const nateId    = await repIdByEmail("nate.diaz@ufcelite.demo");
  const michaelId = await repIdByEmail("michael.bisping@ufcelite.demo");
  if (!danaId || !nateId || !michaelId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }

  const { data: danaRep } = await supa.from("reps").select("org_id, company_id").eq("id", danaId).single();
  const orgId = (danaRep as any).org_id as string;

  await seedCrossCompanyFixtures(orgId);
  const createdAssignments: string[] = [];

  try {
    // ── Gate checks ─────────────────────────────────────────────────────────
    // Local dev honours DEV_TEST_UID as an identity fallback (never in
    // production), so "no identity" may resolve to the dev user — assert it is
    // rejected either way (401 in prod, 403 no_company_scope/forbidden in dev).
    const noAuth = await hit("GET", "/v1/team/members");
    c("GET /v1/team/members without identity rejected", noAuth.status === 401 || noAuth.status === 403, `got ${noAuth.status}`);

    const unknown = await hit("GET", "/v1/team/members", uid("DAY212", "unknown-user"));
    c("GET /v1/team/members with unknown identity → 403", unknown.status === 403, `got ${unknown.status}`);

    const asRep = await hit("GET", "/v1/team/members", nateId);
    c("GET /v1/team/members as SalesRep → 403", asRep.status === 403, `got ${asRep.status}`);

    // ── Manager view ────────────────────────────────────────────────────────
    const asDana = await hit("GET", "/v1/team/members", danaId);
    c("GET /v1/team/members as Dana → 200 ok", asDana.status === 200 && asDana.data?.ok === true, `got ${asDana.status}`);

    const items: any[] = asDana.data?.items ?? [];
    const ids = new Set(items.map((m) => m.id));
    c("team list includes Nate + Michael", ids.has(nateId) && ids.has(michaelId), `got ${items.length} members`);
    c("team list excludes cross-company members", !ids.has(XMANAGER_ID) && !ids.has(XREP_ID));

    const nate = items.find((m) => m.id === nateId);
    c('null-office rep reports scope "company"', nate?.scope === "company", `got ${nate?.scope}`);
    c("office status visible per member", nate ? "office_id" in nate && "warnings" in nate : false);

    const seats = asDana.data?.seats;
    c("seat summary from company_licences", seats?.source === "company_licences", `got ${seats?.source}`);
    c("seat summary has allocated/used/available",
      typeof seats?.allocated === "number" && typeof seats?.used === "number" && typeof seats?.available === "number",
      JSON.stringify(seats));

    // ── Cross-company isolation on the read side ────────────────────────────
    const asXManager = await hit("GET", "/v1/team/members", XMANAGER_ID);
    const xIds = new Set(((asXManager.data?.items ?? []) as any[]).map((m) => m.id));
    c("cross-company manager sees no UFC members",
      asXManager.status === 200 && !xIds.has(danaId) && !xIds.has(nateId),
      `got ${asXManager.status}, ${xIds.size} members`);

    // ── rep_missing_office resolution ───────────────────────────────────────
    const assign = await hit("POST", "/v1/assignments", danaId, {
      rep_id: nateId,
      type: "custom",
      title: "Day 212 validation assignment",
      meta: { flag_section: "day212_validation" },
    });
    const assignOk = assign.status === 200 && assign.data?.ok === true && !assign.data?.skipped;
    c("Dana can assign to null-office Nate (no rep_missing_office)", assignOk,
      `got ${assign.status} ${JSON.stringify(assign.data)?.slice(0, 120)}`);

    if (assign.data?.item?.id) createdAssignments.push(String(assign.data.item.id));
    if (assignOk && assign.data?.item?.id) {
      const { data: row } = await supa
        .from("assignments")
        .select("id, company_id, office_id")
        .eq("id", assign.data.item.id)
        .maybeSingle();
      c("assignment stamped with rep company_id, office_id null",
        (row as any)?.company_id != null && (row as any)?.office_id == null,
        JSON.stringify(row));
    }

    // ── Cross-company write guard (new, tighter than before) ────────────────
    const crossAssign = await hit("POST", "/v1/assignments", danaId, {
      rep_id: XREP_ID,
      type: "custom",
      title: "Day 212 cross-company probe",
    });
    if (crossAssign.data?.item?.id) createdAssignments.push(String(crossAssign.data.item.id));
    c("Dana → cross-company rep rejected (rep_out_of_scope)",
      crossAssign.status === 403 && crossAssign.data?.error === "rep_out_of_scope",
      `got ${crossAssign.status} ${crossAssign.data?.error}`);

    // ── Manager gate on writes intact ───────────────────────────────────────
    const repAssign = await hit("POST", "/v1/assignments", nateId, {
      rep_id: michaelId,
      type: "custom",
      title: "Day 212 rep-as-manager probe",
    });
    if (repAssign.data?.item?.id) createdAssignments.push(String(repAssign.data.item.id));
    c("SalesRep cannot create assignments (403)", repAssign.status === 403, `got ${repAssign.status}`);
  } finally {
    await cleanup(createdAssignments);
  }

  const failed = checks.filter((x) => !x.passed);
  console.log(`\n  ${checks.length - failed.length}/${checks.length} checks passed\n`);
  if (failed.length) process.exit(1);
}

main().catch((e) => {
  console.error("  validator crashed:", e?.message || e);
  process.exit(1);
});
