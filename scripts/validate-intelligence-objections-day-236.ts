/**
 * validate-intelligence-objections-day-236.ts
 *
 * Day 236 — Objection Library data layer (/v1/intelligence/objections).
 *
 * Coverage:
 *   ✓ endpoints answer 503 objection_library_not_migrated until
 *     sql/20260719_objection_library.sql is applied (fail-soft)
 *   ✓ gates: no identity rejected, SalesRep → 403 on read and write
 *   ✓ manager lists empty library; create incomplete draft (label only)
 *   ✓ create without label / invalid category / bad list shape → 400
 *   ✓ duplicate live label (case-insensitive) → 409
 *   ✓ manager updates draft (partial patch)
 *   ✓ approve blocks incomplete items (400 names every missing field)
 *   ✓ approve completes with required fields; approved_by/at stamped
 *   ✓ approved item immutable → 409 immutable_approved; re-approve → 409
 *   ✓ archive preserves the row + evidence; archived item rejects
 *     edit/approve/evidence with 409; label freed for a new live item
 *   ✓ manual evidence: phrase-only works, own-company call attaches
 *     rep_id, cross-company/unknown call → 404, empty body → 400
 *   ✓ cross-company manager: list excludes, GET/PUT/approve/archive/
 *     evidence by id → 404 (no existence leak)
 *   ✓ no hard-delete endpoint (DELETE → 404)
 *   ✓ audit rows for create/update/approve/archive/evidence
 *
 * Self-cleaning: cross-company fixtures and every objection row created
 * during the run are removed at the end (child rows first — the only
 * delete path anywhere is this validator's own fixtures).
 *
 * Requirements: server running (npm run dev), UFC demo seed applied,
 * sql/20260719_objection_library.sql applied (otherwise reports MIGRATION
 * PENDING after proving the fail-soft behaviour).
 * Usage: npm run validate:intelligence-objections
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

const BASE = "/v1/intelligence/objections";

// Primary fixture company — every manager write in this run lands here, never
// in the UFC demo company (Day 224 rule). The cross company must see none of it.
const PCOMPANY_ID = uid("DAY236", "primary-company");
const PMANAGER_ID = uid("DAY236", "primary-manager");

const XCOMPANY_ID = uid("DAY236", "cross-company");
const XMANAGER_ID = uid("DAY236", "cross-manager");

const RUN_TAG = `Day236 ${new Date().toISOString().slice(0, 10)}`;

const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
const FIXTURE_COMPANY_IDS = [PCOMPANY_ID, XCOMPANY_ID];

/**
 * Two synthetic companies under the demo partner: the primary company this
 * validator writes objections in, and a second one that must never see them.
 * Both are owned outright by the validator and dropped by cleanup().
 */
async function seedFixtures(orgId: string) {
  const { error: compErr } = await supa.from("companies").upsert(
    [
      {
        id: PCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day236 Primary Co (validator)",
        slug: "day236-primary-validator",
      },
      {
        id: XCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day236 Cross Company (validator)",
        slug: "day236-cross-company-validator",
      },
    ],
    { onConflict: "id" }
  );
  if (compErr) throw new Error(`fixture company upsert failed: ${compErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: PMANAGER_ID, name: "Day236 P Manager", tier: "Manager", org_id: orgId, company_id: PCOMPANY_ID },
      { id: XMANAGER_ID, name: "Day236 X Manager", tier: "Manager", org_id: orgId, company_id: XCOMPANY_ID },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture reps upsert failed: ${repErr.message}`);

  // A crashed previous run could have left items behind; the empty-list
  // assertion below needs a genuinely empty start.
  await purgeObjections(FIXTURE_COMPANY_IDS);
}

async function purgeObjections(companyIds: string[]) {
  const { data } = await supa
    .from("objection_library_items").select("id").in("company_id", companyIds);
  const ids = ((data ?? []) as any[]).map((r) => String(r.id));
  await supa.from("objection_suggestion_decisions").delete().in("company_id", companyIds);
  if (ids.length) {
    await supa.from("objection_evidence").delete().in("objection_id", ids);
    await supa.from("objection_library_items").delete().in("id", ids);
  }
  return ids.length;
}

async function cleanup() {
  const removed = await purgeObjections(FIXTURE_COMPANY_IDS);
  await supa.from("reps").delete().in("id", [PMANAGER_ID, XMANAGER_ID]);
  await supa.from("companies").delete().in("id", FIXTURE_COMPANY_IDS);
  return removed;
}

async function main() {
  console.log("\n  Objection Library Validator — Day 236\n");

  const danaId = await repIdByEmail("dana.white@ufcelite.demo");
  const nateId = await repIdByEmail("nate.diaz@ufcelite.demo");
  if (!danaId || !nateId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }

  // ── Migration preflight ───────────────────────────────────────────────────
  const { error: tableErr } = await supa.from("objection_library_items").select("id").limit(1);
  if (tableErr && /could not find the table/i.test(tableErr.message)) {
    console.log("  ⚠ objection tables missing — proving fail-soft behaviour only.\n");
    const r = await hit("GET", BASE, danaId);
    c("GET answers 503 objection_library_not_migrated before migration",
      r.status === 503 && r.data?.error === "objection_library_not_migrated",
      `got ${r.status} ${JSON.stringify(r.data)?.slice(0, 120)}`);
    const asRep = await hit("POST", BASE, nateId, { label: "x" });
    c("SalesRep still 403 before migration (gate before table)", asRep.status === 403, `got ${asRep.status}`);
    console.log("\n  MIGRATION PENDING — apply sql/20260719_objection_library.sql in the");
    console.log("  Supabase SQL editor, then re-run npm run validate:intelligence-objections\n");
    process.exit(checks.every((x) => x.passed) ? 2 : 1);
  }
  if (tableErr) {
    console.error(`  ✗ objection_library_items preflight failed: ${tableErr.message}`);
    process.exit(1);
  }

  const { data: danaRep } = await supa.from("reps").select("org_id, company_id").eq("id", danaId).single();
  const orgId = (danaRep as any).org_id as string;
  const ufcCompanyId = String((danaRep as any).company_id);

  await seedFixtures(orgId);

  try {
    // ── Gate checks ──────────────────────────────────────────────────────────
    const noAuth = await hit("GET", BASE);
    c("GET without identity rejected", noAuth.status === 401 || noAuth.status === 403, `got ${noAuth.status}`);
    const repGet = await hit("GET", BASE, nateId);
    c("GET as SalesRep → 403", repGet.status === 403, `got ${repGet.status}`);
    const repPost = await hit("POST", BASE, nateId, { label: "Rep objection" });
    c("POST as SalesRep → 403", repPost.status === 403, `got ${repPost.status}`);

    // ── List (empty) ─────────────────────────────────────────────────────────
    const emptyList = await hit("GET", BASE, PMANAGER_ID);
    c("manager GET list → 200 ok", emptyList.status === 200 && emptyList.data?.ok === true, `got ${emptyList.status}`);
    c("empty company lists no objections",
      Array.isArray(emptyList.data?.items) && emptyList.data.items.length === 0,
      `got ${emptyList.data?.items?.length}`);

    // ── Create draft ─────────────────────────────────────────────────────────
    const label = `It's too expensive — ${RUN_TAG}`;
    const created = await hit("POST", BASE, PMANAGER_ID, { label, category: "price" });
    c("manager POST creates draft item", created.status === 201 && created.data?.item?.status === "draft",
      `got ${created.status} ${JSON.stringify(created.data)?.slice(0, 120)}`);
    const itemId = String(created.data?.item?.id ?? "");
    c("draft may be incomplete (no response/phrases required to save)",
      created.data?.item?.approved_response === null &&
      (created.data?.item?.buyer_phrases ?? []).length === 0,
      JSON.stringify(created.data?.item)?.slice(0, 120));

    const noLabel = await hit("POST", BASE, PMANAGER_ID, { category: "price" });
    c("create without label → 400 label_required",
      noLabel.status === 400 && noLabel.data?.error === "label_required", `got ${noLabel.status}`);
    const badCategory = await hit("POST", BASE, PMANAGER_ID, { label: `Bad cat — ${RUN_TAG}`, category: "stalling" });
    c("invalid category rejected → 400 invalid_category",
      badCategory.status === 400 && badCategory.data?.error === "invalid_category",
      `got ${badCategory.status} ${badCategory.data?.error}`);
    const badList = await hit("POST", BASE, PMANAGER_ID, { label: `Bad list — ${RUN_TAG}`, buyer_phrases: "not-a-list" });
    c("non-array list field rejected → 400 invalid_list",
      badList.status === 400 && badList.data?.error === "invalid_list", `got ${badList.status}`);
    const dupe = await hit("POST", BASE, PMANAGER_ID, { label: label.toUpperCase() });
    c("duplicate live label (case-insensitive) → 409 objection_label_taken",
      dupe.status === 409 && dupe.data?.error === "objection_label_taken", `got ${dupe.status}`);

    // ── Update draft ─────────────────────────────────────────────────────────
    const draftUpdate = await hit("PUT", `${BASE}/${itemId}`, PMANAGER_ID, {
      buyer_phrases: ["That's a lot per month", "I can't justify that right now"],
      weak_response_patterns: ["Offering the discount immediately"],
      no_go_language: ["guaranteed results"],
      why_it_matters: "Price pushback on tours is usually value doubt, not budget.",
    });
    c("manager updates draft (partial patch)",
      draftUpdate.status === 200 && (draftUpdate.data?.item?.buyer_phrases ?? []).length === 2 &&
      draftUpdate.data?.item?.category === "price",
      `got ${draftUpdate.status} ${JSON.stringify(draftUpdate.data)?.slice(0, 140)}`);
    const repPut = await hit("PUT", `${BASE}/${itemId}`, nateId, { label: "hijack" });
    c("PUT as SalesRep → 403", repPut.status === 403, `got ${repPut.status}`);

    // ── Approval gate ────────────────────────────────────────────────────────
    const approveIncomplete = await hit("POST", `${BASE}/${itemId}/approve`, PMANAGER_ID);
    c("approve without approved_response → 400 with field list",
      approveIncomplete.status === 400 &&
      JSON.stringify(approveIncomplete.data).includes("approved_response_required"),
      `got ${approveIncomplete.status} ${JSON.stringify(approveIncomplete.data)?.slice(0, 140)}`);

    await hit("PUT", `${BASE}/${itemId}`, PMANAGER_ID, {
      approved_response: "Compared to what you'd pay per class elsewhere, it's under £3 a visit — including coaching.",
      coaching_note: "Don't defend the price. Anchor their goal first.",
    });
    const repApprove = await hit("POST", `${BASE}/${itemId}/approve`, nateId);
    c("approve as SalesRep → 403", repApprove.status === 403, `got ${repApprove.status}`);
    const approve = await hit("POST", `${BASE}/${itemId}/approve`, PMANAGER_ID);
    c("manager approves complete item → 200 approved",
      approve.status === 200 && approve.data?.item?.status === "approved",
      `got ${approve.status} ${JSON.stringify(approve.data)?.slice(0, 140)}`);
    c("approval stamps approved_by/approved_at",
      approve.data?.item?.approved_by === PMANAGER_ID && Boolean(approve.data?.item?.approved_at),
      JSON.stringify(approve.data?.item)?.slice(0, 140));

    const editApproved = await hit("PUT", `${BASE}/${itemId}`, PMANAGER_ID, { label: "Edited after approval" });
    c("approved item is immutable → 409 immutable_approved",
      editApproved.status === 409 && editApproved.data?.error === "immutable_approved",
      `got ${editApproved.status} ${editApproved.data?.error}`);
    const approveAgain = await hit("POST", `${BASE}/${itemId}/approve`, PMANAGER_ID);
    c("approving twice → 409 already_approved",
      approveAgain.status === 409 && approveAgain.data?.error === "already_approved",
      `got ${approveAgain.status}`);

    // ── Evidence ─────────────────────────────────────────────────────────────
    const emptyEvidence = await hit("POST", `${BASE}/${itemId}/evidence`, PMANAGER_ID, {});
    c("evidence without call or phrase → 400",
      emptyEvidence.status === 400 && emptyEvidence.data?.error === "call_id_or_phrase_required",
      `got ${emptyEvidence.status}`);
    const phraseEvidence = await hit("POST", `${BASE}/${itemId}/evidence`, PMANAGER_ID, {
      phrase: "PureGym is half that",
    });
    c("manager adds phrase-only evidence → 201 manual",
      phraseEvidence.status === 201 && phraseEvidence.data?.evidence?.source === "manual",
      `got ${phraseEvidence.status} ${JSON.stringify(phraseEvidence.data)?.slice(0, 120)}`);

    // Cross-company call id must not attach (UFC's calls are foreign here).
    const { data: ufcCall } = await supa
      .from("calls").select("id").eq("company_id", ufcCompanyId).limit(1).maybeSingle();
    if (ufcCall) {
      const foreignCall = await hit("POST", `${BASE}/${itemId}/evidence`, PMANAGER_ID, {
        call_id: String((ufcCall as any).id),
      });
      c("evidence with another company's call id → 404 call_not_found",
        foreignCall.status === 404 && foreignCall.data?.error === "call_not_found",
        `got ${foreignCall.status}`);
    } else {
      c("evidence with another company's call id → 404 call_not_found", false, "no UFC call found to test with");
    }
    const ghostCall = await hit("POST", `${BASE}/${itemId}/evidence`, PMANAGER_ID, {
      call_id: uid("DAY236", "ghost-call"),
    });
    c("evidence with unknown call id → 404", ghostCall.status === 404, `got ${ghostCall.status}`);
    const repEvidence = await hit("POST", `${BASE}/${itemId}/evidence`, nateId, { phrase: "x" });
    c("evidence as SalesRep → 403", repEvidence.status === 403, `got ${repEvidence.status}`);

    const detail = await hit("GET", `${BASE}/${itemId}`, PMANAGER_ID);
    c("detail returns item + evidence",
      detail.status === 200 && detail.data?.item?.id === itemId &&
      (detail.data?.evidence ?? []).length === 1 &&
      detail.data?.evidence?.[0]?.phrase === "PureGym is half that",
      `got ${detail.status} ${(detail.data?.evidence ?? []).length} evidence rows`);

    // ── Archive ──────────────────────────────────────────────────────────────
    const repArchive = await hit("POST", `${BASE}/${itemId}/archive`, nateId);
    c("archive as SalesRep → 403", repArchive.status === 403, `got ${repArchive.status}`);
    const archive = await hit("POST", `${BASE}/${itemId}/archive`, PMANAGER_ID);
    c("manager archives item → 200 archived + archived_at",
      archive.status === 200 && archive.data?.item?.status === "archived" &&
      Boolean(archive.data?.item?.archived_at),
      `got ${archive.status} ${archive.data?.item?.status}`);

    const { data: archivedRow } = await supa
      .from("objection_library_items").select("id, status, approved_response").eq("id", itemId).maybeSingle();
    const { data: archivedEvidence } = await supa
      .from("objection_evidence").select("id").eq("objection_id", itemId);
    c("archive preserves the item + evidence (nothing deleted)",
      (archivedRow as any)?.status === "archived" &&
      Boolean((archivedRow as any)?.approved_response) &&
      (archivedEvidence ?? []).length === 1,
      JSON.stringify({ row: (archivedRow as any)?.status, evidence: (archivedEvidence ?? []).length }));

    const editArchived = await hit("PUT", `${BASE}/${itemId}`, PMANAGER_ID, { label: "Edited after archive" });
    c("archived item rejects edits → 409 objection_archived",
      editArchived.status === 409 && editArchived.data?.error === "objection_archived",
      `got ${editArchived.status}`);
    const approveArchived = await hit("POST", `${BASE}/${itemId}/approve`, PMANAGER_ID);
    c("archived item rejects approval → 409", approveArchived.status === 409, `got ${approveArchived.status}`);
    const evidenceArchived = await hit("POST", `${BASE}/${itemId}/evidence`, PMANAGER_ID, { phrase: "late" });
    c("archived item rejects new evidence → 409", evidenceArchived.status === 409, `got ${evidenceArchived.status}`);
    const archiveAgain = await hit("POST", `${BASE}/${itemId}/archive`, PMANAGER_ID);
    c("archiving twice → 409", archiveAgain.status === 409, `got ${archiveAgain.status}`);
    const archivedReadable = await hit("GET", `${BASE}/${itemId}`, PMANAGER_ID);
    c("archived item stays readable (history)", archivedReadable.status === 200, `got ${archivedReadable.status}`);

    const relabel = await hit("POST", BASE, PMANAGER_ID, { label, category: "price" });
    c("archiving frees the label for a new live item",
      relabel.status === 201 && relabel.data?.item?.status === "draft",
      `got ${relabel.status} ${relabel.data?.error ?? ""}`);
    const relabelId = String(relabel.data?.item?.id ?? "");

    // ── No hard delete ───────────────────────────────────────────────────────
    const del = await hit("DELETE", `${BASE}/${relabelId}`, PMANAGER_ID);
    c("no hard-delete endpoint (DELETE → 404)", del.status === 404, `got ${del.status}`);
    const { data: stillThere } = await supa
      .from("objection_library_items").select("id").eq("id", relabelId).maybeSingle();
    c("item survives the DELETE attempt", Boolean(stillThere), JSON.stringify(stillThere));

    // ── Cross-company isolation ──────────────────────────────────────────────
    const xList = await hit("GET", BASE, XMANAGER_ID);
    const xIds = new Set(((xList.data?.items ?? []) as any[]).map((i) => String(i.id)));
    c("cross-company manager list excludes the primary company items",
      xList.status === 200 && !xIds.has(itemId) && !xIds.has(relabelId),
      `got ${xList.status}, ${xIds.size} items`);
    const xGet = await hit("GET", `${BASE}/${relabelId}`, XMANAGER_ID);
    c("cross-company GET by id → 404 (no existence leak)", xGet.status === 404, `got ${xGet.status}`);
    const xPut = await hit("PUT", `${BASE}/${relabelId}`, XMANAGER_ID, { label: "Hijacked" });
    c("cross-company PUT by id → 404", xPut.status === 404, `got ${xPut.status}`);
    const xApprove = await hit("POST", `${BASE}/${relabelId}/approve`, XMANAGER_ID);
    c("cross-company approve by id → 404", xApprove.status === 404, `got ${xApprove.status}`);
    const xArchive = await hit("POST", `${BASE}/${relabelId}/archive`, XMANAGER_ID);
    c("cross-company archive by id → 404", xArchive.status === 404, `got ${xArchive.status}`);
    const xEvidence = await hit("POST", `${BASE}/${relabelId}/evidence`, XMANAGER_ID, { phrase: "x" });
    c("cross-company evidence by id → 404", xEvidence.status === 404, `got ${xEvidence.status}`);

    const { data: primaryItem } = await supa
      .from("objection_library_items").select("label, status").eq("id", relabelId).single();
    c("primary company item untouched by cross-company attempts",
      (primaryItem as any)?.label === label && (primaryItem as any)?.status === "draft",
      JSON.stringify(primaryItem));

    // ── Audit trail ──────────────────────────────────────────────────────────
    for (const action of [
      "create_objection",
      "update_objection_draft",
      "approve_objection",
      "archive_objection",
      "add_objection_evidence",
    ]) {
      const { data: rows } = await supa
        .from("audit_events")
        .select("id, metadata")
        .eq("action", action)
        .eq("actor_user_id", PMANAGER_ID)
        .order("created_at", { ascending: false })
        .limit(1);
      const row = (rows ?? [])[0] as any;
      c(`audit row exists for ${action}`,
        Boolean(row) && row?.metadata?.company_id === PCOMPANY_ID,
        JSON.stringify(row)?.slice(0, 120));
    }
  } finally {
    const removed = await cleanup();
    console.log(`\n  Cleanup: removed ${removed} objection fixture(s) + cross-company fixtures.`);
  }

  const failed = checks.filter((x) => !x.passed);
  console.log(`\n  ${checks.length - failed.length}/${checks.length} checks passed`);
  if (failed.length) {
    console.log("  Objection Library validation FAILED\n");
    process.exit(1);
  }
  console.log("  Objection Library validation PASSED\n");
}

main().catch((e) => {
  console.error("\n  ✗ Validator crashed:", e?.message || e);
  process.exit(1);
});
