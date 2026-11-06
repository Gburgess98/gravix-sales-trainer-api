// Coach-assignment object-level authorisation (security fix; see COACH_ASSIGNMENT_AUTHZ.md).
//
// Before: DELETE and PATCH /v1/coach/assignments/:id only required *an* identity, so any signed-in user could
// delete or complete/reopen any assignment in any tenant by guessing/knowing its id.
//
// Rules (all derived from the VERIFIED caller id; nothing from headers, query or body is trusted for scope):
//   DELETE  — caller must be a manager tier (reps.tier) AND the assignment must target a rep inside the caller's
//             verified tenant (canAccessRep: same company/org key). Reps can never delete, not even their own.
//   PATCH   — (complete / reopen) the assignee themself, or a manager tier with the same tenant scope.
//   Unknown, malformed-as-unknown and out-of-scope ids are indistinguishable (404 not_found) so the endpoint is not an
//   existence oracle. A reps-only caller on DELETE is refused (403) before any lookup, so that reveals nothing either.
//
// Pure + injectable (no network) so scripts/validate-coach-assignment-authz.ts can prove it against an in-memory store.

import { accessibleRepIds, canAccessRep, isManagerTier, tenantKey, type RepLookups, type RepScope } from "./repAccess";

export type AssignmentRow = {
  id: string;
  assignee_user_id: string | null;
  org_id: string | null;
  [k: string]: unknown;
};

export type AssignmentStore = {
  /** null = not found. Throws on a database error. */
  getAssignment: (id: string) => Promise<AssignmentRow | null>;
  deleteAssignment: (id: string) => Promise<void>;
  /** Returns the updated row, or null when nothing matched. */
  updateAssignment: (id: string, patch: Record<string, unknown>) => Promise<AssignmentRow | null>;
};

export type Deps = { store: AssignmentStore; lookups: RepLookups };
export type Result = { status: number; body: Record<string, unknown> };

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isUuid = (v: unknown): v is string => typeof v === "string" && UUID_RE.test(v);

const NOT_FOUND: Result = { status: 404, body: { ok: false, error: "not_found" } };
const FAILED = (error: string): Result => ({ status: 500, body: { ok: false, error } });

/**
 * Can this manager act on this assignment? Requires a manager tier plus the assignee inside the caller's tenant.
 * An assignment with no assignee is only reachable through its own org_id matching the manager's org (never by id alone).
 * If both sides carry an org_id and they differ, the answer is no (defence in depth against mismatched rows).
 */
export async function managerCanActOn(me: RepScope | null, row: AssignmentRow, lookups: RepLookups): Promise<boolean> {
  if (!me || !isManagerTier(me.tier) || !tenantKey(me)) return false;
  if (row.org_id && me.org_id && row.org_id !== me.org_id) return false;
  if (row.assignee_user_id) return canAccessRep(me.id, row.assignee_user_id, lookups);
  return !!row.org_id && !!me.org_id && row.org_id === me.org_id;
}

export async function deleteCoachAssignment(deps: Deps, requesterId: string | null | undefined, rawId: unknown): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  if (!isUuid(rawId)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  try {
    const me = await deps.lookups.getRep(requester);
    // Not a manager (reps, unknown callers, callers without a tenant): refused before any lookup of the assignment.
    if (!me || !isManagerTier(me.tier)) return { status: 403, body: { ok: false, error: "forbidden_not_manager" } };
    const row = await deps.store.getAssignment(rawId);
    if (!row || !(await managerCanActOn(me, row, deps.lookups))) return NOT_FOUND;
    await deps.store.deleteAssignment(rawId);
    return { status: 200, body: { ok: true } };
  } catch {
    return FAILED("delete_failed");
  }
}

export async function updateCoachAssignmentStatus(
  deps: Deps,
  requesterId: string | null | undefined,
  rawId: unknown,
  status: unknown
): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  if (!isUuid(rawId)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  if (status !== "open" && status !== "completed") {
    return { status: 400, body: { ok: false, error: "status must be 'open' or 'completed'" } };
  }
  try {
    const row = await deps.store.getAssignment(rawId);
    if (!row) return NOT_FOUND;
    const isAssignee = !!row.assignee_user_id && row.assignee_user_id === requester;
    if (!isAssignee) {
      const me = await deps.lookups.getRep(requester);
      if (!(await managerCanActOn(me, row, deps.lookups))) return NOT_FOUND;
    }
    const patch: Record<string, unknown> = { status };
    patch.completed_at = status === "completed" ? new Date().toISOString() : null;
    const updated = await deps.store.updateAssignment(rawId, patch);
    if (!updated) return NOT_FOUND;
    return { status: 200, body: { ok: true, assignment: updated } };
  } catch {
    return FAILED("patch_failed");
  }
}

/** POST /v1/coach/assign: the assignee must be someone the caller may act for (self, or a rep in a manager's tenant). */
export async function canAssignTo(requesterId: string, assigneeUserId: string, lookups: RepLookups): Promise<boolean> {
  return canAccessRep(requesterId, assigneeUserId, lookups);
}

// ── Reads and notes ────────────────────────────────────────────────────────────────────────────────────────
//
// Assignment reads: a row is visible only when its ASSIGNEE is inside the caller's verified scope
// (accessibleRepIds: the caller alone for reps, every rep of their tenant for manager tiers). Client-supplied
// orgId / x-org-id / role / rep-id values are never used to widen that scope; a requested assignee outside it simply
// returns nothing, exactly like an assignee with no assignments. Unknown and foreign call/account/contact ids therefore
// answer an empty list, not a 403/404, so nothing is learnt by probing ids.
//
// Notes (coach_notes: one free-text note per call, no author column): a note belongs to its call. Read and write need the
// same access as acting for the call's OWNER rep: the owner themself, or a manager tier in the owner's verified tenant.
// Another rep (even in the same tenant), another tenant, and unknown calls all get the same 404.

export type CallRef = { id: string; user_id: string | null; org_id?: string | null };
export type NoteRow = { id: string; call_id: string; notes: string | null; updated_at: string | null };

export type CoachStore = AssignmentStore & {
  /** Assignments for any of `callIds` (when given) or for one assignee; newest first; `max` rows. */
  listAssignments: (q: { callIds?: string[]; assignee?: string; status?: "open" | "completed"; max: number }) => Promise<AssignmentRow[]>;
  /** Call ids (newest first, bounded) that reference the account or contact. */
  callIdsForEntity: (e: { accountId?: string; contactId?: string }) => Promise<string[]>;
  /** Calls by id, only the fields needed for scope and labels. */
  getCalls: (ids: string[]) => Promise<(CallRef & { filename?: string | null })[]>;
  getNote: (callId: string) => Promise<NoteRow | null>;
  upsertNote: (callId: string, notes: string | null) => Promise<NoteRow | null>;
};
export type CoachDeps = { store: CoachStore; lookups: RepLookups };

const asSet = (ids: string[]) => new Set(ids);

async function scopeOf(deps: CoachDeps, requester: string): Promise<Set<string>> {
  return asSet(await accessibleRepIds(requester, deps.lookups));
}

type ListQuery = { callId?: unknown; accountId?: unknown; contactId?: unknown; assigneeUserId?: unknown; repId?: unknown; status?: unknown; limit?: unknown };
const str = (v: unknown) => (typeof v === "string" && v.trim() ? v.trim() : null);

/** GET /v1/coach/assignments (one of callId | accountId | contactId | assigneeUserId|repId). Extra params such as orgId are ignored. */
export async function listCoachAssignments(deps: CoachDeps, requesterId: string | null | undefined, q: ListQuery): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  const callId = str(q.callId), accountId = str(q.accountId), contactId = str(q.contactId);
  const assignee = str(q.assigneeUserId) ?? str(q.repId);
  const status = q.status === "open" || q.status === "completed" ? q.status : undefined;
  const limit = Math.max(1, Math.min(Number(q.limit || 50) || 50, 100));
  if (!callId && !accountId && !contactId && !assignee) {
    return { status: 400, body: { ok: false, error: "one of callId, accountId, contactId, assigneeUserId|repId required" } };
  }
  for (const v of [callId, accountId, contactId, assignee]) if (v && !isUuid(v)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  try {
    const scope = await scopeOf(deps, requester);
    let rows: AssignmentRow[];
    if (callId || accountId || contactId) {
      const callIds = callId ? [callId] : await deps.store.callIdsForEntity({ accountId: accountId ?? undefined, contactId: contactId ?? undefined });
      rows = callIds.length ? await deps.store.listAssignments({ callIds, status, max: 500 }) : [];
    } else {
      if (!scope.has(assignee!)) return { status: 200, body: { ok: true, items: [] } }; // out of scope looks like "no assignments"
      rows = await deps.store.listAssignments({ assignee: assignee!, status, max: limit });
    }
    const visible = rows.filter((r) => !!r.assignee_user_id && scope.has(String(r.assignee_user_id))).slice(0, limit);
    const labels = await callLabels(deps, visible, scope);
    const items = visible.map((x: any) => ({
      id: x.id,
      call_id: x.call_id,
      call_label: labels.get(String(x.call_id)) || x.call_id,
      assignee_user_id: x.assignee_user_id,
      drill_id: x.drill_id,
      notes: x.notes,
      status: x.status,
      created_at: x.created_at,
      completed_at: x.completed_at || null,
    }));
    return { status: 200, body: { ok: true, items } };
  } catch {
    return FAILED("assignments_list_failed");
  }
}

/** GET /v1/coach/assignments/by-entity (accountId | contactId): same visibility rule, original (slimmer) item shape. */
export async function listAssignmentsByEntity(deps: CoachDeps, requesterId: string | null | undefined, q: ListQuery): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  const accountId = str(q.accountId), contactId = str(q.contactId);
  const limit = Math.max(1, Math.min(Number(q.limit || 20) || 20, 50));
  if (!accountId && !contactId) return { status: 400, body: { ok: false, error: "accountId or contactId required" } };
  for (const v of [accountId, contactId]) if (v && !isUuid(v)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  try {
    const scope = await scopeOf(deps, requester);
    const callIds = await deps.store.callIdsForEntity({ accountId: accountId ?? undefined, contactId: accountId ? undefined : contactId ?? undefined });
    const rows = callIds.length ? await deps.store.listAssignments({ callIds, max: 500 }) : [];
    const visible = rows.filter((r) => !!r.assignee_user_id && scope.has(String(r.assignee_user_id))).slice(0, limit);
    const labels = await callLabels(deps, visible, scope);
    const items = visible.map((x: any) => ({
      id: x.id,
      call_id: x.call_id,
      call_label: labels.get(String(x.call_id)) || x.call_id,
      drill_id: x.drill_id,
      notes: x.notes,
      created_at: x.created_at,
    }));
    return { status: 200, body: { ok: true, items } };
  } catch {
    return FAILED("assignments_aggregate_failed");
  }
}

// A call's filename is shown only when the call belongs to a rep inside the caller's scope; otherwise the id stands in (as before).
async function callLabels(deps: CoachDeps, rows: AssignmentRow[], scope: Set<string>): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  const ids = Array.from(new Set(rows.map((r) => String((r as any).call_id || "")).filter(Boolean)));
  if (!ids.length) return out;
  try {
    for (const c of await deps.store.getCalls(ids)) {
      if (c.user_id && scope.has(String(c.user_id))) out.set(String(c.id), String(c.filename || c.id));
    }
  } catch { /* labels are best-effort */ }
  return out;
}

async function canUseCallNotes(deps: CoachDeps, requester: string, callId: string): Promise<boolean> {
  const [call] = await deps.store.getCalls([callId]);
  if (!call || !call.user_id) return false;
  return canAccessRep(requester, call.user_id, deps.lookups);
}

export async function getCoachNote(deps: CoachDeps, requesterId: string | null | undefined, callId: unknown): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  if (!callId) return { status: 400, body: { ok: false, error: "callId required" } };
  if (!isUuid(callId)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  try {
    if (!(await canUseCallNotes(deps, requester, callId))) return NOT_FOUND;
    const note = await deps.store.getNote(callId);
    return { status: 200, body: { ok: true, note: note ?? null } };
  } catch {
    return FAILED("fetch_failed");
  }
}

export async function saveCoachNote(deps: CoachDeps, requesterId: string | null | undefined, callId: unknown, notes: unknown): Promise<Result> {
  const requester = String(requesterId || "").trim();
  if (!requester) return { status: 401, body: { ok: false, error: "missing_user_identity" } };
  if (!callId) return { status: 400, body: { ok: false, error: "callId required" } };
  if (!isUuid(callId)) return { status: 400, body: { ok: false, error: "invalid_id" } };
  if (notes !== undefined && notes !== null && typeof notes !== "string") return { status: 400, body: { ok: false, error: "notes must be text" } };
  try {
    if (!(await canUseCallNotes(deps, requester, callId))) return NOT_FOUND;
    const note = await deps.store.upsertNote(callId, (notes as string | null | undefined) ?? null);
    return { status: 200, body: { ok: true, note } };
  } catch {
    return FAILED("save_failed");
  }
}
