/**
 * partnerAccess.ts
 *
 * Shared helpers for partner-scoped data access.
 * Exported so routes and middleware can import without circular deps.
 */

import { createClient } from "@supabase/supabase-js";

let _supa: ReturnType<typeof createClient> | null = null;
function getSupa() {
  if (_supa) return _supa;
  _supa = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!,
    { auth: { persistSession: false, autoRefreshToken: false } }
  );
  return _supa;
}

export type CompanyRow = {
  id: string;
  name: string;
  partner_id: string | null;
};

/**
 * Returns the companies visible to a user based on their reps.tier.
 *
 * SalesRep / Manager  → own company only
 * PartnerAdmin        → all companies under their partner
 * SuperAdmin          → all companies
 *
 * Returns an empty array on error (fail-soft — callers must guard on empty).
 */
export async function getVisibleCompanies(userId: string): Promise<CompanyRow[]> {
  const supa = getSupa();

  try {
    // 1. Resolve the user's tier + company_id from reps
    const { data: rep, error: repErr } = await supa
      .from("reps")
      .select("tier, company_id")
      .eq("id", userId)
      .maybeSingle();

    if (repErr || !rep) return [];

    const tier       = String((rep as any).tier ?? "");
    const companyId  = (rep as any).company_id ?? null;

    // 2. SuperAdmin — all companies
    if (tier === "SuperAdmin") {
      const { data } = await supa
        .from("companies")
        .select("id, name, partner_id")
        .order("name");
      return (data as CompanyRow[]) ?? [];
    }

    // 3. PartnerAdmin — all companies under their partner
    if (tier === "PartnerAdmin") {
      if (!companyId) return [];

      // Resolve the partner_id via the user's company
      const { data: company } = await supa
        .from("companies")
        .select("partner_id")
        .eq("id", companyId)
        .maybeSingle();

      const partnerId = (company as any)?.partner_id ?? null;
      if (!partnerId) return [];

      const { data } = await supa
        .from("companies")
        .select("id, name, partner_id")
        .eq("partner_id", partnerId)
        .order("name");
      return (data as CompanyRow[]) ?? [];
    }

    // 4. Manager / SalesRep — own company only
    if (!companyId) return [];

    const { data } = await supa
      .from("companies")
      .select("id, name, partner_id")
      .eq("id", companyId);
    return (data as CompanyRow[]) ?? [];
  } catch {
    return [];
  }
}
