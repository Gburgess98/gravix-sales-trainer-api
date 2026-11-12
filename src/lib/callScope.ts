// Who may see which calls in Insights — resolved from the VERIFIED caller, identically for the score trend and the command centre
// so the two figures describe the same set of calls.
//
//   organisation      always the caller's own reps.org_id (never a header); none on record → no scope → the caller is refused
//   office / company  the caller's existing hierarchy (users.role office_manager / company_manager); a manager tier with no users
//                     row is scoped to reps.company_id instead of being left unscoped

import type { RepLookups } from "./repAccess";
import { isManagerTier } from "./repAccess";

export type CallScope = { orgId: string; companyId: string | null; officeId: string | null };

export async function resolveCallScope(db: any, userId: string, lookups: RepLookups): Promise<CallScope | null> {
  const me = await lookups.getRep(userId);
  if (!me || !me.org_id) return null;

  const { data: u } = await db.from("users").select("id, role, office_id, company_id").eq("id", userId).maybeSingle();
  if (u) {
    if (u.role === "office_manager") {
      if (u.office_id) return { orgId: me.org_id, companyId: null, officeId: String(u.office_id) };
      if (u.company_id) return { orgId: me.org_id, companyId: String(u.company_id), officeId: null };
    } else if (u.role === "company_manager" && u.company_id) {
      return { orgId: me.org_id, companyId: String(u.company_id), officeId: null };
    }
    return { orgId: me.org_id, companyId: null, officeId: null };
  }
  if (isManagerTier(me.tier) && me.company_id) return { orgId: me.org_id, companyId: me.company_id, officeId: null };
  return { orgId: me.org_id, companyId: null, officeId: null };
}

export function applyCallScope<T extends { eq: (col: string, v: unknown) => T }>(query: T, scope: CallScope): T {
  let q = query.eq("org_id", scope.orgId);
  if (scope.officeId) q = q.eq("office_id", scope.officeId);
  else if (scope.companyId) q = q.eq("company_id", scope.companyId);
  return q;
}
