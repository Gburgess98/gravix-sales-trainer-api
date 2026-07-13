import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import 'dotenv/config';
import { requireManager } from "../middleware/requireManager";

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

// ----- Users list (tenant-scoped, Day 168) ----------------------------------
// Previously this endpoint listed the `profiles` table with no tenant filter —
// every profile in the DB was returned to any caller, which leaked
// wrong-company reps into the Upload Call picker. It now resolves the
// requester's company (users first, reps identity bridge second — same rule
// as accounts.ts resolveCompanyId) and only returns members of that company,
// sourced from `reps` (named) with a `users` fallback (email-only rows).
// No requester or no company → empty list, never the whole DB.

function requesterIdFromHeaders(req: any): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

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

// GET /v1/team/users?q=alex&limit=100
router.get("/users", async (req, res) => {
  try {
    const q = (req.query.q as string | undefined)?.trim().toLowerCase() || "";
    const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));

    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.json({ ok: true, items: [] });

    type Item = { id: string; name: string; email: string | null; role: string | null; manager_id: string | null };
    const byId = new Map<string, Item>();

    const { data: repRows, error: repErr } = await supa
      .from("reps")
      .select("id, name, display_name, email, tier, manager_id")
      .eq("company_id", companyId)
      .limit(500);
    if (repErr) return res.status(500).json({ ok: false, error: repErr.message });
    for (const r of (repRows ?? []) as any[]) {
      const id = String(r.id);
      const email = typeof r.email === "string" ? r.email : null;
      const name = r.display_name || r.name || email || id;
      byId.set(id, { id, name: String(name), email, role: r.tier ? String(r.tier) : null, manager_id: r.manager_id ?? null });
    }

    // Company members with a users row but no reps row (legacy hierarchy) —
    // email-only display, never overrides a named reps entry.
    const { data: userRows, error: userErr } = await supa
      .from("users")
      .select("id, email, role, manager_id")
      .eq("company_id", companyId)
      .limit(500);
    if (userErr && byId.size === 0) return res.status(500).json({ ok: false, error: userErr.message });
    for (const u of (userRows ?? []) as any[]) {
      const id = String(u.id);
      if (byId.has(id)) continue;
      const email = typeof u.email === "string" ? u.email : null;
      byId.set(id, { id, name: String(email || id), email, role: u.role ? String(u.role) : null, manager_id: u.manager_id ?? null });
    }

    const items = Array.from(byId.values())
      .filter((it) => !q || it.name.toLowerCase().includes(q) || (it.email || "").toLowerCase().includes(q))
      .sort((a, b) => (a.name || "").localeCompare(b.name || ""))
      .slice(0, limit);

    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- Team members (manager-gated, Day 212) --------------------------------
// Foundation for the future /team surface (MANAGER_TEAM_MANAGEMENT_ROUTE_PLAN).
// Returns the requester's company team with per-member office/scope status and
// a seat summary. Read-only: no mutating team endpoints ship in this subset.
//
// Scope rules:
// - requireManager gate — reps get 403, unauthenticated gets 401.
// - Company-scoped via the same users→reps identity bridge as GET /users.
// - scope per member: "office" when an office is assigned, else "company"
//   (matching applyOrgScope/applyHierarchyFilters since Day 166/168).
// - Missing office is a warning, not an error — assignment creation now falls
//   back to company scope (Day 212 assignments.ts fix), so it is visible and
//   fixable here rather than a dead-end rep_missing_office.
//
// Seats: company_licences.allocated is canonical; org_limits.max_users is the
// legacy fallback (resolved via the requester's reps.org_id). Managers can see
// usage but nothing here lets them alter the purchased allocation.

// GET /v1/team/members
router.get("/members", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) {
      return res.status(403).json({ ok: false, error: "no_company_scope" });
    }

    type Member = {
      id: string;
      name: string;
      email: string | null;
      role: string | null;
      manager_id: string | null;
      office_id: string | null;
      office_name: string | null;
      scope: "office" | "company";
      identity: "rep" | "user_only";
      warnings: string[];
    };
    const byId = new Map<string, Member>();

    const { data: repRows, error: repErr } = await supa
      .from("reps")
      .select("id, name, display_name, email, tier, manager_id, office_id")
      .eq("company_id", companyId)
      .limit(500);
    if (repErr) return res.status(500).json({ ok: false, error: repErr.message });
    for (const r of (repRows ?? []) as any[]) {
      const id = String(r.id);
      const email = typeof r.email === "string" ? r.email : null;
      const name = r.display_name || r.name || email || id;
      byId.set(id, {
        id,
        name: String(name),
        email,
        role: r.tier ? String(r.tier) : null,
        manager_id: r.manager_id ?? null,
        office_id: r.office_id ?? null,
        office_name: null,
        scope: r.office_id ? "office" : "company",
        identity: "rep",
        warnings: [],
      });
    }

    // Legacy users-only rows (no reps identity) — same fallback as GET /users.
    const { data: userRows, error: userErr } = await supa
      .from("users")
      .select("id, email, role, manager_id, office_id")
      .eq("company_id", companyId)
      .limit(500);
    if (userErr && byId.size === 0) {
      return res.status(500).json({ ok: false, error: userErr.message });
    }
    for (const u of (userRows ?? []) as any[]) {
      const id = String(u.id);
      if (byId.has(id)) {
        // A users row can carry office scope the reps row lacks.
        const existing = byId.get(id)!;
        if (!existing.office_id && u.office_id) {
          existing.office_id = String(u.office_id);
          existing.scope = "office";
        }
        continue;
      }
      const email = typeof u.email === "string" ? u.email : null;
      byId.set(id, {
        id,
        name: String(email || id),
        email,
        role: u.role ? String(u.role) : null,
        manager_id: u.manager_id ?? null,
        office_id: u.office_id ?? null,
        office_name: null,
        scope: u.office_id ? "office" : "company",
        identity: "user_only",
        warnings: [],
      });
    }

    // Office names + per-member warnings.
    const { data: officeRows } = await supa
      .from("offices")
      .select("id, name")
      .eq("company_id", companyId)
      .limit(200);
    const officeNames = new Map<string, string>(
      ((officeRows ?? []) as any[]).map((o) => [String(o.id), String(o.name ?? "")])
    );
    const companyHasOffices = officeNames.size > 0;

    for (const m of byId.values()) {
      if (m.office_id) {
        m.office_name = officeNames.get(m.office_id) ?? null;
        if (!m.office_name) m.warnings.push("office_not_in_company");
      } else if (companyHasOffices) {
        // Only a warning when the company actually uses offices; a fully
        // office-less company operating at company scope is normal.
        m.warnings.push("no_office_assigned");
      }
      if (m.identity === "user_only") m.warnings.push("no_rep_identity");
    }

    const items = Array.from(byId.values()).sort((a, b) =>
      (a.name || "").localeCompare(b.name || "")
    );

    // Seat summary — company_licences canonical, org_limits legacy fallback.
    let allocated: number | null = null;
    let seatSource: "company_licences" | "org_limits" | null = null;
    const { data: licRows } = await supa
      .from("company_licences")
      .select("allocated")
      .eq("company_id", companyId);
    if (licRows?.length) {
      allocated = (licRows as any[]).reduce(
        (sum, row) => sum + (Number(row.allocated) || 0),
        0
      );
      seatSource = "company_licences";
    } else {
      const { data: requesterRep } = await supa
        .from("reps")
        .select("org_id")
        .eq("id", requesterId)
        .maybeSingle();
      if ((requesterRep as any)?.org_id) {
        const { data: limitRow } = await supa
          .from("org_limits")
          .select("max_users")
          .eq("org_id", (requesterRep as any).org_id)
          .maybeSingle();
        if (limitRow && limitRow.max_users != null) {
          allocated = Number(limitRow.max_users) || 0;
          seatSource = "org_limits";
        }
      }
    }

    const used = items.length;
    const seats = {
      allocated,
      used,
      available: allocated == null ? null : Math.max(0, allocated - used),
      source: seatSource,
    };

    const warnings: string[] = [];
    if (items.some((m) => m.warnings.includes("no_office_assigned"))) {
      warnings.push("members_missing_office");
    }
    if (allocated != null && used > allocated) warnings.push("over_seat_allocation");
    if (seatSource === null) warnings.push("no_seat_allocation_found");

    return res.json({ ok: true, company_id: companyId, items, seats, warnings });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- Ensure profile exists (schema-agnostic upsert) ----------------------
router.post("/ensure-profile", async (req, res) => {
  try {
    const { id, name, email } = (req.body || {}) as { id?: string; name?: string; email?: string };
    if (!id || typeof id !== "string") return res.status(400).json({ ok: false, error: "missing id" });

    const table = supa.from("profiles");

    // 1) Minimal shape first: only user_id
    let r = await table.upsert({ user_id: id }, { onConflict: "user_id" }).select().single();

    // 2) If that fails, progressively try richer shapes
    if (r.error) {
      const candidates: any[] = [
        { user_id: id, name: name ?? null },
        { user_id: id, full_name: name ?? null },
        { user_id: id, name: name ?? null, email: email ?? null },
        { user_id: id, full_name: name ?? null, email: email ?? null },
      ];
      let success = false;
      for (const candidate of candidates) {
        r = await table.upsert(candidate, { onConflict: "user_id" }).select().single();
        if (!r.error) { success = true; break; }
      }
      if (!success && r.error) return res.status(500).json({ ok: false, error: r.error.message });
    }

    return res.json({ ok: true, item: r.data });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "ensure_failed" });
  }
});

export default router;