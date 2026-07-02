// Day 171 — shared org-scoped call read access.
// Extracted from routes/calls.ts so routes/pins.ts can apply the same
// visibility rule to pin reads without a route-to-route import (stale
// compiled .js siblings shadow extensionless imports between routes).

import { createClient } from "@supabase/supabase-js";
import { getOrgCallVisibility } from "./adminConfig";

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

export async function getRequesterOrgId(requester: string): Promise<string | null> {
  const { data, error } = await supa
    .from("calls")
    .select("org_id")
    .eq("user_id", requester)
    .not("org_id", "is", null)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (data?.org_id) return data.org_id;

  // Day 167 — managers who have never recorded/uploaded a call have no
  // call-derived org, which blocked them from opening any rep call (403).
  // Fall back to their reps-table membership; org visibility rules still apply.
  const { data: repRow, error: repError } = await supa
    .from("reps")
    .select("org_id")
    .eq("id", requester)
    .not("org_id", "is", null)
    .maybeSingle();

  if (repError) return null;
  return repRow?.org_id ?? null;
}

export async function canAccessCall(
  requester: string,
  callUserId: string,
  callOrgId: string | null
) {
  if (callUserId === requester) return true;
  if (!callOrgId) return false;

  const requesterOrgId = await getRequesterOrgId(requester);
  if (!requesterOrgId || requesterOrgId !== callOrgId) return false;

  const visibility = await getOrgCallVisibility(callOrgId);

  if (visibility === "disabled") return false;

  if (visibility === "everyone") return true;

  if (visibility === "managers") {
    // TEMP: treat users with assignments as "managers"
    const { data } = await supa
      .from("assignments")
      .select("id")
      .eq("manager_id", requester)
      .limit(1);

    return (data?.length ?? 0) > 0;
  }

  return false;
}
