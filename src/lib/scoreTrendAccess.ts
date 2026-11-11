// Access rules for GET /v1/crm/analytics/score-trend (pure + injectable: network-free tests in validate-insights-gate.ts).
//
// Before: any identity could read the organisation-wide trend, or another rep's trend by passing a raw repId, and the
// organisation came from a client-supplied x-org-id header. Now everything is derived from the VERIFIED caller (reps row):
//   no / malformed identity                           → 401 missing_user
//   unknown caller, or no organisation on the caller  → 403 forbidden          (fail closed)
//   malformed repId                                   → 400 invalid_rep_id     (never silently widened to "the whole team")
//   manager tier (Manager/Owner/PartnerAdmin/SuperAdmin):
//        no repId   → organisation/team trend of THEIR organisation
//        repId      → only a rep in their own tenant (their own id included); anyone else → 403 forbidden
//   everyone else (SalesRep, unknown tiers):
//        repId must be the caller's own id → self-only trend; no repId (= org-wide) or another rep → 403 forbidden
// Every refusal is the same body, so it says nothing about whether the other rep / tenant exists. The organisation used for the
// query is the caller's own reps.org_id — a raw repId or x-org-id value is never trusted.

import { canAccessRep, isManagerTier, type RepLookups } from "./repAccess";

const UUID_LOOSE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type ScopeResult =
  | { ok: true; orgId: string; repId: string | null }
  | { ok: false; status: number; body: Record<string, unknown> };

const FORBIDDEN: ScopeResult = { ok: false, status: 403, body: { ok: false, error: "forbidden" } };

export async function resolveScoreTrendScope(lookups: RepLookups, requesterRaw: unknown, repIdRaw: unknown): Promise<ScopeResult> {
  const requester = String(requesterRaw ?? "").trim();
  if (!UUID_LOOSE.test(requester)) return { ok: false, status: 401, body: { ok: false, error: "missing_user" } };

  const wanted = String(repIdRaw ?? "").trim();
  if (wanted && !UUID_LOOSE.test(wanted)) return { ok: false, status: 400, body: { ok: false, error: "invalid_rep_id" } };
  const repId = wanted ? wanted.toLowerCase() : null;

  const me = await lookups.getRep(requester);
  if (!me || !me.org_id) return FORBIDDEN;

  if (isManagerTier(me.tier)) {
    if (!repId) return { ok: true, orgId: me.org_id, repId: null };
    if (repId === requester.toLowerCase()) return { ok: true, orgId: me.org_id, repId };
    const target = await lookups.getRep(repId);
    // same tenant key (company, falling back to org) AND the same organisation the data is read from
    if (!target || target.org_id !== me.org_id || !(await canAccessRep(requester, repId, lookups))) return FORBIDDEN;
    return { ok: true, orgId: me.org_id, repId };
  }

  // SalesRep and anything that is not a manager tier: self only.
  if (repId && repId === requester.toLowerCase()) return { ok: true, orgId: me.org_id, repId };
  return FORBIDDEN;
}
