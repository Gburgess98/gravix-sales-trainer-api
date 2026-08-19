// 🔥 ORG CONFIG (Day 66)

import { createClient } from "@supabase/supabase-js";

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

export type CallVisibility = "everyone" | "managers" | "disabled";

/**
 * Resolve an org's company-call visibility policy.
 *
 * Day 291 — this is an ENFORCEMENT read (used by /v1/calls/paged?scope=company
 * and canAccessCall), so it must fail CLOSED. A genuine transport/schema error
 * (e.g. the org_settings table missing, or the DB unreachable) is re-thrown so
 * the caller's try/catch denies the request — it must NEVER be swallowed into an
 * "everyone" default, which would silently broaden company-call access on a
 * failure. Only a genuine ABSENT ROW (table present, no policy row for the org)
 * resolves to the explicit historical default of "everyone".
 */
export async function getOrgCallVisibility(orgId: string): Promise<CallVisibility> {
  const { data, error } = await supa
    .from("org_settings")
    .select("call_visibility")
    .eq("org_id", orgId)
    .maybeSingle();

  if (error) throw error; // fail-closed: never default to "everyone" on error

  return (data?.call_visibility as CallVisibility) || "everyone"; // explicit absent-row default
}