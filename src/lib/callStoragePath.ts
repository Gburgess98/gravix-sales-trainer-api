// Go Live Day 26 — ownership rules for POST /v1/calls.
//
// Contract (Day 26 investigation): calls are CALLER-OWNED. The only WEB upload
// flow (/v1/upload/signed -> storage PUT -> /v1/upload/finalize) always writes
// user_id = the authenticated caller and requires the path to start with
// `${callerId}/`; the upload page's "Rep" picker only sets a text label
// (profileLabel). POST /v1/calls has no WEB caller. So this route follows the
// same model: path in the caller's folder AND row owned by the caller — no
// on-behalf rows.
//
// The handler used to upsert on a caller-supplied `storage_path`, so a caller
// could pass another tenant's path and silently re-assign that existing call
// row (user_id / company_id) to themselves. These pure rules are applied
// before any write and are tested network-free
// (scripts/validate-authz-remediation-day-26.ts).

export type ExistingCallRow = {
  id: string;
  user_id: string | null;
  company_id: string | null;
} | null;

export type CallWriteDecision =
  | { ok: true; mode: "insert" }
  | { ok: true; mode: "update"; id: string }
  | { ok: false; status: 403 | 409; error: string };

/**
 * A storage path belongs to `ownerId` only when it sits inside that user's
 * folder (the same `${userId}/...` convention /v1/upload/finalize enforces)
 * and cannot escape it.
 */
export function storagePathOwnedBy(ownerId: string, storagePath: string): boolean {
  const owner = String(ownerId || "").trim();
  const p = String(storagePath || "");
  if (!owner || !p) return false;
  if (p.includes("\\") || p.includes("\0")) return false;
  if (!p.startsWith(`${owner}/`)) return false;
  const rest = p.slice(owner.length + 1);
  if (!rest) return false;
  return rest.split("/").every((seg) => seg !== "" && seg !== "." && seg !== "..");
}

export function decideCallWrite(args: {
  callerId: string;
  targetUserId: string;
  targetCompanyId: string | null;
  storagePath: string;
  existing: ExistingCallRow;
}): CallWriteDecision {  if (!storagePathOwnedBy(args.callerId, args.storagePath)) {
    return { ok: false, status: 403, error: "storage_path_not_owned" };
  }
  if (args.targetUserId !== args.callerId) {
    return { ok: false, status: 403, error: "on_behalf_not_supported" };
  }
  const existing = args.existing;
  if (!existing) return { ok: true, mode: "insert" };
  if (existing.user_id !== args.targetUserId) {
    return { ok: false, status: 409, error: "storage_path_conflict" };
  }
  if (existing.company_id && args.targetCompanyId && existing.company_id !== args.targetCompanyId) {
    return { ok: false, status: 409, error: "storage_path_conflict" };
  }
  return { ok: true, mode: "update", id: existing.id };
}
