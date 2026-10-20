// Go Live Day 39 (F-38-1) — canonical upload hierarchy for an authenticated caller.
//
// Problem: `/v1/upload/finalize` stamped office_id/company_id from the legacy `users`
// table only, and `/v1/upload` stamped neither; both used the global DEFAULT_ORG_ID.
// An auth-first rep (canonical `reps` row, no `users` row) therefore produced a call
// with company_id NULL, which company/office manager views (`.eq("company_id", …)`)
// can never match.
//
// Contract (follows REPS_FIRST_ARCHITECTURE.md: `reps` is the canonical identity table,
// `users` a compatibility layer; same bridge as assignments.ts Day 168):
//   - Identity = the VERIFIED caller id (never anything from the request body).
//   - Each field is read from the caller's `reps` row first, then (only if that field
//     is missing) from the legacy `users` row.
//   - REQUIRED: company_id (manager scoping key) and org_id (calls.org_id is NOT NULL;
//     reps/users org_id, else the explicit transitional DEFAULT_ORG_ID the caller of
//     this function passes in). If either is missing the upload is REFUSED — a call is
//     never created unscoped.
//   - OPTIONAL: office_id (some roles/orgs legitimately have none; manager views fall
//     back to company scope).
//   - A lookup ERROR is distinct from "missing": it returns hierarchy_lookup_failed.
// The client can never supply or override any of these fields.

import type { SupabaseClient } from "@supabase/supabase-js";

export type CallerHierarchy = { org_id: string; company_id: string; office_id: string | null };
export type HierarchyResult =
  | { ok: true; hierarchy: CallerHierarchy }
  | { ok: false; error: "caller_hierarchy_incomplete" | "hierarchy_lookup_failed"; missing?: Array<"company_id" | "org_id"> };

const id = (v: unknown): string | null => {
  const s = typeof v === "string" ? v.trim() : "";
  return s ? s : null;
};

export async function resolveCallerHierarchy(
  client: SupabaseClient,
  callerId: string,
  opts: { defaultOrgId?: string | null } = {}
): Promise<HierarchyResult> {
  const caller = String(callerId || "").trim();
  if (!caller) return { ok: false, error: "caller_hierarchy_incomplete", missing: ["company_id", "org_id"] };

  const rep = await client.from("reps").select("org_id, company_id, office_id").eq("id", caller).maybeSingle();
  if (rep.error) return { ok: false, error: "hierarchy_lookup_failed" };

  const r = (rep.data || {}) as Record<string, unknown>;
  let company = id(r.company_id), office = id(r.office_id), org = id(r.org_id);

  if (!company || !office || !org) {
    const usr = await client.from("users").select("org_id, company_id, office_id").eq("id", caller).maybeSingle();
    if (usr.error) return { ok: false, error: "hierarchy_lookup_failed" };
    const u = (usr.data || {}) as Record<string, unknown>;
    company = company ?? id(u.company_id);
    office = office ?? id(u.office_id);
    org = org ?? id(u.org_id);
  }
  org = org ?? id(opts.defaultOrgId);

  const missing: Array<"company_id" | "org_id"> = [];
  if (!company) missing.push("company_id");
  if (!org) missing.push("org_id");
  if (missing.length) return { ok: false, error: "caller_hierarchy_incomplete", missing };
  return { ok: true, hierarchy: { org_id: org!, company_id: company!, office_id: office } };
}
