// Go Live Day 26 (G2) — shared rep/tenant access rules for routes that
// previously had no identity or tenant scoping (sparring, dashboard, rewards,
// calls create, team profile).
//
// Rules (tenant key = reps.company_id, falling back to reps.org_id):
//   - a caller can always access their own rep id;
//   - manager tiers (Manager/Owner/PartnerAdmin/SuperAdmin) can access reps
//     with the same tenant key;
//   - everything else is denied (unknown caller, unknown target, no tenant).
//
// The DB lookups are injectable so the rules are testable without a network
// (scripts/validate-route-authz-day-26.ts).

import { supaAdmin } from "./supa";

export const MANAGER_TIERS = new Set(["Manager", "Owner", "PartnerAdmin", "SuperAdmin"]);

export type RepScope = {
  id: string;
  tier: string | null;
  company_id: string | null;
  org_id: string | null;
};

export type RepLookups = {
  getRep: (id: string) => Promise<RepScope | null>;
  listTenantRepIds: (scope: RepScope) => Promise<string[]>;
};

export function tenantKey(r: RepScope | null): string | null {
  if (!r) return null;
  return (r.company_id || r.org_id || null) as string | null;
}

export function isManagerTier(tier: string | null | undefined): boolean {
  return MANAGER_TIERS.has(String(tier || ""));
}

const dbLookups: RepLookups = {
  async getRep(id) {
    if (!id) return null;
    const { data, error } = await supaAdmin
      .from("reps")
      .select("id, tier, company_id, org_id")
      .eq("id", id)
      .maybeSingle();
    if (error || !data) return null;
    return data as RepScope;
  },
  async listTenantRepIds(scope) {
    const col = scope.company_id ? "company_id" : "org_id";
    const val = scope.company_id || scope.org_id;
    if (!val) return [];
    const { data, error } = await supaAdmin.from("reps").select("id").eq(col, val);
    if (error || !data) return [];
    return (data as Array<{ id: string }>).map((r) => r.id);
  },
};

/** Can `requesterId` read/act on data owned by `targetRepId`? */
export async function canAccessRep(
  requesterId: string | null | undefined,
  targetRepId: string | null | undefined,
  lookups: RepLookups = dbLookups
): Promise<boolean> {
  const me = String(requesterId || "").trim();
  const target = String(targetRepId || "").trim();
  if (!me || !target) return false;
  if (me === target) return true;
  const [meRow, targetRow] = await Promise.all([lookups.getRep(me), lookups.getRep(target)]);
  if (!meRow || !targetRow || !isManagerTier(meRow.tier)) return false;
  const k = tenantKey(meRow);
  return !!k && k === tenantKey(targetRow);
}

/**
 * Rep ids whose data the caller may see in aggregate views: every rep in the
 * caller's tenant for manager tiers, otherwise only the caller.
 */
export async function accessibleRepIds(
  requesterId: string | null | undefined,
  lookups: RepLookups = dbLookups
): Promise<string[]> {
  const me = String(requesterId || "").trim();
  if (!me) return [];
  const meRow = await lookups.getRep(me);
  if (!meRow || !isManagerTier(meRow.tier) || !tenantKey(meRow)) return [me];
  const ids = await lookups.listTenantRepIds(meRow);
  return Array.from(new Set([me, ...ids]));
}

export function requesterIdOf(req: any): string {
  return String(req?.userId || "").trim();
}
