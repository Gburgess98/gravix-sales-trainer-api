import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";
import { requireManager } from "../middleware/requireManager";
import { logAuditEvent } from "../lib/audit";

// Intelligence Layer — Day 236: Objection Library data layer.
//
// Third Intelligence pillar (OBJECTION_LIBRARY_BLUEPRINT.md +
// OBJECTION_LIBRARY_FIELD_SPEC.md, WEB repo docs): manager-approved
// guidance for how the company handles buyer pushback. Day 236 is
// CRUD + lifecycle only — no WEB UI, no suggestion mining, and nothing
// in the scoring/Whisperer/sparring runtimes reads these tables yet.
//
// Lifecycle: draft → approved → archived.
// - Drafts may be incomplete; approval is the completeness gate (label,
//   category, ≥1 buyer phrase, approved response, and a coaching note or
//   why-it-matters).
// - Approved items are immutable in Day 236 — editing answers 409
//   immutable_approved (a fork/revision model is a later lane, mirroring
//   Scorecard Studio's Day 220 shape).
// - Archive marks, never deletes; evidence and decisions survive. No
//   hard-delete endpoint exists.
//
// Scope rules (same shape as routes/intelligence.ts, Day 218):
// - every endpoint requireManager-gated; reps 403, no identity 401.
//   (Rep read-only guidance surfaces ship with their WEB consumers,
//   not before — same call as the Context Engine.)
// - company resolved from the requester via the users→reps identity
//   bridge, never from the request — cross-company ids answer 404;
// - no destructive deletes anywhere.

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

function requesterIdFromHeaders(req: any): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

// Same users→reps identity bridge as routes/team.ts / intelligence.ts.
async function resolveCompanyId(userId: string): Promise<string | null> {
  if (!userId) return null;

  const { data: userRow } = await supa
    .from("users")
    .select("company_id")
    .eq("id", userId)
    .maybeSingle();
  if (userRow?.company_id) return String(userRow.company_id);

  const { data: repRow } = await supa
    .from("reps")
    .select("company_id")
    .eq("id", userId)
    .maybeSingle();
  return (repRow as any)?.company_id ? String((repRow as any).company_id) : null;
}

// Fail-soft until sql/20260719_objection_library.sql is applied.
function isMissingTableError(message: string | undefined): boolean {
  return /could not find the table/i.test(String(message ?? ""));
}
function tableError(res: any, message: string | undefined) {
  if (isMissingTableError(message)) {
    return res.status(503).json({
      ok: false,
      error: "objection_library_not_migrated",
      detail: "Apply sql/20260719_objection_library.sql",
    });
  }
  return res.status(500).json({ ok: false, error: message || "server_error" });
}

// Postgres rejects a malformed uuid with 22P02 before any row match, so
// ill-formed call ids are screened here and answer the same 404 as an
// unknown one — no shape oracle, no 500.
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const STATUSES = ["draft", "approved", "archived"] as const;
const CATEGORIES = [
  "price", "timing", "authority", "trust",
  "competitor", "fit", "logistics", "other",
] as const;

const MAX_LABEL_CHARS = 200;
const MAX_TEXT_CHARS = 4_000;
const MAX_LIST_ENTRIES = 40;
const MAX_LIST_ENTRY_CHARS = 500;

function serialiseItem(row: Record<string, any>) {
  return {
    id: String(row.id),
    label: String(row.label),
    category: String(row.category),
    status: String(row.status),
    buyer_phrases: Array.isArray(row.buyer_phrases) ? row.buyer_phrases : [],
    why_it_matters: row.why_it_matters ?? null,
    approved_response: row.approved_response ?? null,
    weak_response_patterns: Array.isArray(row.weak_response_patterns)
      ? row.weak_response_patterns
      : [],
    no_go_language: Array.isArray(row.no_go_language) ? row.no_go_language : [],
    coaching_note: row.coaching_note ?? null,
    linked_scorecard_criterion_id: row.linked_scorecard_criterion_id ?? null,
    linked_trigger_key: row.linked_trigger_key ?? null,
    created_by: row.created_by ?? null,
    updated_by: row.updated_by ?? null,
    approved_by: row.approved_by ?? null,
    approved_at: row.approved_at ?? null,
    archived_at: row.archived_at ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
  };
}

function serialiseEvidence(row: Record<string, any>) {
  return {
    id: String(row.id),
    objection_id: String(row.objection_id),
    call_id: row.call_id ?? null,
    rep_id: row.rep_id ?? null,
    phrase: row.phrase ?? null,
    source: String(row.source ?? "manual"),
    confidence: row.confidence === null || row.confidence === undefined
      ? null
      : Number(row.confidence),
    occurred_at: row.occurred_at ?? null,
    created_at: row.created_at ?? null,
  };
}

async function fetchItem(companyId: string, id: string) {
  return supa
    .from("objection_library_items")
    .select("*")
    .eq("company_id", companyId)
    .eq("id", id)
    .maybeSingle();
}

type FieldError = { error: string; field?: string };

function cleanText(value: unknown): string | null {
  return typeof value === "string" && value.trim()
    ? value.trim().slice(0, MAX_TEXT_CHARS)
    : null;
}

// text[] fields: must arrive as an array of non-empty strings.
function cleanList(value: unknown, field: string): { list?: string[]; error?: FieldError } {
  if (!Array.isArray(value)) return { error: { error: "invalid_list", field } };
  if (value.length > MAX_LIST_ENTRIES) return { error: { error: "list_too_long", field } };
  const list: string[] = [];
  for (const entry of value) {
    if (typeof entry !== "string") return { error: { error: "invalid_list_entry", field } };
    const s = entry.trim();
    if (!s) continue;
    list.push(s.slice(0, MAX_LIST_ENTRY_CHARS));
  }
  return { list };
}

// Shared body parser for create + draft update. Only touches keys present
// in the body, so PUT stays a partial update.
function parsePatch(body: Record<string, any>): { patch?: Record<string, unknown>; error?: FieldError } {
  const patch: Record<string, unknown> = {};

  if (body.label !== undefined) {
    const label = typeof body.label === "string" ? body.label.trim() : "";
    if (!label) return { error: { error: "label_required", field: "label" } };
    if (label.length > MAX_LABEL_CHARS) return { error: { error: "label_too_long", field: "label" } };
    patch.label = label;
  }
  if (body.category !== undefined) {
    const category = typeof body.category === "string" ? body.category.trim() : "";
    if (!CATEGORIES.includes(category as any)) {
      return { error: { error: "invalid_category", field: "category" } };
    }
    patch.category = category;
  }
  for (const field of ["why_it_matters", "approved_response", "coaching_note"] as const) {
    if (body[field] !== undefined) patch[field] = cleanText(body[field]);
  }
  for (const field of ["buyer_phrases", "weak_response_patterns", "no_go_language"] as const) {
    if (body[field] !== undefined) {
      const { list, error } = cleanList(body[field], field);
      if (error) return { error };
      patch[field] = list;
    }
  }
  if (body.linked_scorecard_criterion_id !== undefined) {
    patch.linked_scorecard_criterion_id =
      typeof body.linked_scorecard_criterion_id === "string" && body.linked_scorecard_criterion_id.trim()
        ? body.linked_scorecard_criterion_id.trim()
        : null;
  }
  if (body.linked_trigger_key !== undefined) {
    patch.linked_trigger_key = cleanText(body.linked_trigger_key);
  }
  return { patch };
}

function duplicateLabelError(res: any, message: string): boolean {
  if (/duplicate key|uq_objection_items_company_label_live/i.test(message)) {
    res.status(409).json({ ok: false, error: "objection_label_taken" });
    return true;
  }
  return false;
}

// Approval completeness gate (field spec §5 + Day 236 rules): drafts may be
// incomplete, approval may not.
function approvalErrors(item: Record<string, any>): FieldError[] {
  const errors: FieldError[] = [];
  if (!String(item.label ?? "").trim()) errors.push({ error: "label_required", field: "label" });
  if (!CATEGORIES.includes(String(item.category ?? "") as any)) {
    errors.push({ error: "invalid_category", field: "category" });
  }
  if (!Array.isArray(item.buyer_phrases) || item.buyer_phrases.length === 0) {
    errors.push({ error: "buyer_phrase_required", field: "buyer_phrases" });
  }
  if (!String(item.approved_response ?? "").trim()) {
    errors.push({ error: "approved_response_required", field: "approved_response" });
  }
  if (!String(item.coaching_note ?? "").trim() && !String(item.why_it_matters ?? "").trim()) {
    errors.push({ error: "coaching_note_or_why_it_matters_required", field: "coaching_note" });
  }
  return errors;
}

// ----- GET /v1/intelligence/objections ----------------------------------------
// Company items, newest first; optional ?status= / ?category= filters.
router.get("/", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    let query = supa
      .from("objection_library_items")
      .select("*")
      .eq("company_id", companyId)
      .order("created_at", { ascending: false });

    const status = typeof req.query.status === "string" ? req.query.status : "";
    if (status) {
      if (!STATUSES.includes(status as any)) {
        return res.status(400).json({ ok: false, error: "invalid_status" });
      }
      query = query.eq("status", status);
    }
    const category = typeof req.query.category === "string" ? req.query.category : "";
    if (category) {
      if (!CATEGORIES.includes(category as any)) {
        return res.status(400).json({ ok: false, error: "invalid_category" });
      }
      query = query.eq("category", category);
    }

    const { data, error } = await query;
    if (error) return tableError(res, error.message);

    return res.json({
      ok: true,
      company_id: companyId,
      items: ((data ?? []) as any[]).map(serialiseItem),
      categories: CATEGORIES,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/objections ---------------------------------------
// Create a draft item. Only the label is required to save — drafts may be
// incomplete (approval is the completeness gate).
router.post("/", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const body = (req.body ?? {}) as Record<string, any>;
    if (body.label === undefined) {
      return res.status(400).json({ ok: false, error: "label_required", field: "label" });
    }
    const parsed = parsePatch(body);
    if (parsed.error) {
      return res.status(400).json({ ok: false, ...parsed.error });
    }

    const { data: item, error: insErr } = await supa
      .from("objection_library_items")
      .insert({
        company_id: companyId,
        category: "other",
        ...parsed.patch,
        status: "draft",
        created_by: requesterId,
        updated_by: requesterId,
      })
      .select("*")
      .single();
    if (insErr) {
      if (duplicateLabelError(res, insErr.message)) return;
      return tableError(res, insErr.message);
    }

    await logAuditEvent({
      actorUserId: requesterId,
      action: "create_objection",
      entityType: "objection_library_item",
      entityId: String(item.id),
      metadata: { company_id: companyId, label: String(item.label), category: String(item.category) },
    });

    return res.status(201).json({ ok: true, item: serialiseItem(item) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- GET /v1/intelligence/objections/:id ------------------------------------
// Item detail + its evidence rows (archived items stay readable — history).
router.get("/:id", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: item, error: itemErr } = await fetchItem(companyId, String(req.params.id));
    if (itemErr) return tableError(res, itemErr.message);
    if (!item) return res.status(404).json({ ok: false, error: "not_found" });

    const { data: evidence, error: evErr } = await supa
      .from("objection_evidence")
      .select("*")
      .eq("company_id", companyId)
      .eq("objection_id", item.id)
      .order("created_at", { ascending: false });
    if (evErr) return tableError(res, evErr.message);

    return res.json({
      ok: true,
      item: serialiseItem(item),
      evidence: ((evidence ?? []) as any[]).map(serialiseEvidence),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- PUT /v1/intelligence/objections/:id ------------------------------------
// Update a DRAFT item only. Approved items are immutable in Day 236
// (409 immutable_approved — revision model is a later lane); archived
// items are read-only history.
router.put("/:id", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: item, error: itemErr } = await fetchItem(companyId, String(req.params.id));
    if (itemErr) return tableError(res, itemErr.message);
    if (!item) return res.status(404).json({ ok: false, error: "not_found" });
    if (item.status === "archived") {
      return res.status(409).json({ ok: false, error: "objection_archived" });
    }
    if (item.status === "approved") {
      return res.status(409).json({ ok: false, error: "immutable_approved" });
    }

    const parsed = parsePatch((req.body ?? {}) as Record<string, any>);
    if (parsed.error) {
      return res.status(400).json({ ok: false, ...parsed.error });
    }

    const { data: updated, error: updErr } = await supa
      .from("objection_library_items")
      .update({
        ...parsed.patch,
        updated_by: requesterId,
        updated_at: new Date().toISOString(),
      })
      .eq("id", item.id)
      .eq("company_id", companyId)
      .select("*")
      .single();
    if (updErr) {
      if (duplicateLabelError(res, updErr.message)) return;
      return tableError(res, updErr.message);
    }

    await logAuditEvent({
      actorUserId: requesterId,
      action: "update_objection_draft",
      entityType: "objection_library_item",
      entityId: String(item.id),
      metadata: { company_id: companyId, fields: Object.keys(parsed.patch ?? {}) },
    });

    return res.json({ ok: true, item: serialiseItem(updated) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/objections/:id/approve ---------------------------
// Explicit manager action, never automatic. The completeness gate lives
// here, not on save: label + category + ≥1 buyer phrase + approved
// response + (coaching note or why-it-matters).
router.post("/:id/approve", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: item, error: itemErr } = await fetchItem(companyId, String(req.params.id));
    if (itemErr) return tableError(res, itemErr.message);
    if (!item) return res.status(404).json({ ok: false, error: "not_found" });
    if (item.status === "archived") {
      return res.status(409).json({ ok: false, error: "objection_archived" });
    }
    if (item.status === "approved") {
      return res.status(409).json({ ok: false, error: "already_approved" });
    }

    const errors = approvalErrors(item);
    if (errors.length) {
      return res.status(400).json({ ok: false, error: errors[0].error, errors });
    }

    const now = new Date().toISOString();
    const { data: approved, error: updErr } = await supa
      .from("objection_library_items")
      .update({
        status: "approved",
        approved_by: requesterId,
        approved_at: now,
        updated_by: requesterId,
        updated_at: now,
      })
      .eq("id", item.id)
      .eq("company_id", companyId)
      .select("*")
      .single();
    if (updErr) return tableError(res, updErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "approve_objection",
      entityType: "objection_library_item",
      entityId: String(item.id),
      metadata: { company_id: companyId, label: String(item.label), category: String(item.category) },
    });

    return res.json({ ok: true, item: serialiseItem(approved) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/objections/:id/archive ---------------------------
// Archive marks, never deletes — evidence, decisions and the item itself
// survive as read-only history. Works from draft or approved.
router.post("/:id/archive", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: item, error: itemErr } = await fetchItem(companyId, String(req.params.id));
    if (itemErr) return tableError(res, itemErr.message);
    if (!item) return res.status(404).json({ ok: false, error: "not_found" });
    if (item.status === "archived") {
      return res.status(409).json({ ok: false, error: "objection_archived" });
    }

    const now = new Date().toISOString();
    const { data: archived, error: updErr } = await supa
      .from("objection_library_items")
      .update({
        status: "archived",
        archived_at: now,
        updated_by: requesterId,
        updated_at: now,
      })
      .eq("id", item.id)
      .eq("company_id", companyId)
      .select("*")
      .single();
    if (updErr) return tableError(res, updErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "archive_objection",
      entityType: "objection_library_item",
      entityId: String(item.id),
      metadata: { company_id: companyId, label: String(item.label), previous_status: String(item.status) },
    });

    return res.json({ ok: true, item: serialiseItem(archived) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/objections/:id/evidence --------------------------
// Manual evidence link (field spec §4 path 3): a call and/or a quoted
// phrase. Suggestion-sourced evidence arrives with the mining lane, not
// here — source is always 'manual' on this endpoint.
router.post("/:id/evidence", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: item, error: itemErr } = await fetchItem(companyId, String(req.params.id));
    if (itemErr) return tableError(res, itemErr.message);
    if (!item) return res.status(404).json({ ok: false, error: "not_found" });
    if (item.status === "archived") {
      return res.status(409).json({ ok: false, error: "objection_archived" });
    }

    const body = (req.body ?? {}) as Record<string, any>;
    const callId =
      typeof body.call_id === "string" && body.call_id.trim() ? body.call_id.trim() : null;
    const phrase = cleanText(body.phrase);
    if (!callId && !phrase) {
      return res.status(400).json({ ok: false, error: "call_id_or_phrase_required" });
    }

    // A linked call must be the company's own — cross-company call ids
    // answer 404 like every other foreign id, and so does anything we
    // cannot positively resolve. The evidence row's rep_id comes from the
    // call's owning user (calls.user_id — the calls table has no rep_id).
    let repId: string | null = null;
    if (callId) {
      if (!UUID_RE.test(callId)) {
        return res.status(404).json({ ok: false, error: "call_not_found" });
      }
      const { data: call, error: callErr } = await supa
        .from("calls")
        .select("id, company_id, user_id")
        .eq("id", callId)
        .maybeSingle();
      // A failed lookup is not proof the call exists, so it answers 404
      // rather than 500 — but it is logged, because a silent 404 is how a
      // schema drift here hides (Day 237A: the old select named a column
      // that does not exist and every lookup 500'd).
      if (callErr) {
        console.warn("[objections] evidence call lookup failed:", callErr.message);
        return res.status(404).json({ ok: false, error: "call_not_found" });
      }
      if (!call || String((call as any).company_id ?? "") !== companyId) {
        return res.status(404).json({ ok: false, error: "call_not_found" });
      }
      repId = (call as any).user_id ? String((call as any).user_id) : null;
    }

    const { data: evidence, error: insErr } = await supa
      .from("objection_evidence")
      .insert({
        company_id: companyId,
        objection_id: item.id,
        call_id: callId,
        rep_id: repId,
        phrase,
        source: "manual",
        occurred_at: typeof body.occurred_at === "string" && body.occurred_at.trim()
          ? body.occurred_at.trim()
          : null,
      })
      .select("*")
      .single();
    if (insErr) return tableError(res, insErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "add_objection_evidence",
      entityType: "objection_evidence",
      entityId: String(evidence.id),
      metadata: { company_id: companyId, objection_id: String(item.id), call_id: callId },
    });

    return res.status(201).json({ ok: true, evidence: serialiseEvidence(evidence) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

export default router;
