# RLS Roadmap — Gravix Sales Trainer API

**Date:** 2026-06-09

---

## Current Access Model

**All database queries use `SUPABASE_SERVICE_ROLE_KEY`.** The service role bypasses every Postgres Row Level Security policy unconditionally. This means:

- Enabling RLS today would not protect any data — the service role ignores it.
- All tenant isolation is enforced exclusively in the application layer (Express middleware + per-route `.eq()` filters).
- A compromised service-role key = full unrestricted database access.

---

## Why RLS Still Matters

Enabling RLS is the defence-in-depth layer:

1. **Compromised service key** — if the key leaks, an attacker using the anon client with a stolen JWT would still hit RLS, limiting blast radius.
2. **Direct Supabase Dashboard access** — when a developer queries via the Supabase table editor, RLS policies govern what they see (unless they hold the service role).
3. **Future client-side queries** — if the architecture ever uses `supabase-js` with user JWTs on the client, RLS is the only backend safety net.
4. **Compliance** — SOC2 / ISO27001 auditors expect defence-in-depth at the data layer.

---

## Tables Assessed

### High-value tables (PII / business data)

| Table | Current isolation | RLS needed |
|---|---|---|
| `reps` | App-level (tier check) | Yes |
| `companies` | App-level (company_id) | Yes |
| `calls` | App-level (user_id + org_id) | Yes |
| `crm_contacts` | App-level (org_id) | Yes |
| `crm_opportunities` | App-level (org_id) | Yes |
| `crm_activities` | App-level (user_id) | Yes |
| `assignments` | App-level (company_id) | Yes |
| `audit_events` | App-level (SuperAdmin only) | Yes |
| `licence_pools` | App-level (PartnerAdmin+) | Yes |
| `company_licences` | App-level (PartnerAdmin+) | Yes |

### Low-risk tables (config / shared)

| Table | Current isolation | RLS needed |
|---|---|---|
| `partners` | App-level | Low — read-only for most |
| `admin_config` | App-level (requireManager) | Medium |
| `jobs` | App-level (user_id) | Medium |

---

## Required Architecture Change

RLS only provides protection when queries run as the authenticated user, not as the service role. Two approaches:

### Option A — RLS with `auth.uid()` (Recommended, but requires auth refactor)

Each request creates a Supabase client using the user's JWT:

```ts
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  global: { headers: { Authorization: `Bearer ${userJwt}` } }
});
```

Postgres policies then use `auth.uid()` to scope rows. This is the intended Supabase architecture.

**Impact:** Significant refactor. Every route would need the user's verified JWT token, not just their UUID. Auth middleware would need to forward the full token, not just extract the sub.

### Option B — RLS with service role + explicit `set local` (Advanced)

The service role is used for connection, but each query runs under an explicit role claim:

```sql
SET LOCAL role = 'authenticated';
SET LOCAL request.jwt.claims = '{"sub": "<uuid>", "role": "authenticated"}';
```

This is complex and brittle; not recommended.

### Option C — Enable RLS, use service role, add policies as belt-and-suspenders

Enable RLS but keep using the service role. Write policies anyway. They will be ignored by service-role queries but will protect any anon-client queries (e.g., from Supabase Dashboard, direct client usage, future architecture changes).

**This is the safest short-term migration.** Zero risk of breaking existing API. Adds protection for non-service-role access paths.

---

## Recommended Policy Designs

### `reps` table

```sql
-- Users can read their own row
CREATE POLICY "reps_select_own"
  ON public.reps FOR SELECT
  USING (id = auth.uid());

-- Managers can read reps in their company
CREATE POLICY "reps_select_manager"
  ON public.reps FOR SELECT
  USING (
    EXISTS (
      SELECT 1 FROM public.reps AS caller
      WHERE caller.id = auth.uid()
        AND caller.company_id = reps.company_id
        AND caller.tier IN ('Manager', 'Owner', 'PartnerAdmin', 'SuperAdmin')
    )
  );

-- No self-updates of tier via direct SQL
CREATE POLICY "reps_update_own_profile"
  ON public.reps FOR UPDATE
  USING (id = auth.uid())
  WITH CHECK (tier = (SELECT tier FROM public.reps WHERE id = auth.uid()));
```

### `calls` table

```sql
CREATE POLICY "calls_select_owner"
  ON public.calls FOR SELECT
  USING (user_id = auth.uid());

CREATE POLICY "calls_select_manager"
  ON public.calls FOR SELECT
  USING (
    EXISTS (
      SELECT 1 FROM public.reps AS caller
      JOIN public.reps AS target ON target.id = calls.user_id
      WHERE caller.id = auth.uid()
        AND caller.company_id = target.company_id
        AND caller.tier IN ('Manager', 'Owner', 'PartnerAdmin', 'SuperAdmin')
    )
  );

CREATE POLICY "calls_insert_own"
  ON public.calls FOR INSERT
  WITH CHECK (user_id = auth.uid());
```

### `crm_contacts` table

```sql
CREATE POLICY "contacts_select_org"
  ON public.crm_contacts FOR SELECT
  USING (
    EXISTS (
      SELECT 1 FROM public.reps
      WHERE reps.id = auth.uid()
        AND reps.org_id = crm_contacts.org_id
    )
  );

CREATE POLICY "contacts_insert_org"
  ON public.crm_contacts FOR INSERT
  WITH CHECK (
    EXISTS (
      SELECT 1 FROM public.reps
      WHERE reps.id = auth.uid()
        AND reps.org_id = NEW.org_id
    )
  );

CREATE POLICY "contacts_update_org"
  ON public.crm_contacts FOR UPDATE
  USING (
    EXISTS (
      SELECT 1 FROM public.reps
      WHERE reps.id = auth.uid()
        AND reps.org_id = crm_contacts.org_id
    )
  );
```

### `audit_events` table

```sql
-- Only SuperAdmin can read audit events
CREATE POLICY "audit_select_superadmin"
  ON public.audit_events FOR SELECT
  USING (
    (SELECT tier FROM public.reps WHERE id = auth.uid()) = 'SuperAdmin'
  );

-- Service role handles inserts; authenticated users cannot insert directly
CREATE POLICY "audit_insert_deny"
  ON public.audit_events FOR INSERT
  WITH CHECK (false);
```

### `licence_pools` / `company_licences` tables

```sql
CREATE POLICY "licence_pools_select_partner"
  ON public.licence_pools FOR SELECT
  USING (
    (SELECT tier FROM public.reps WHERE id = auth.uid()) = 'SuperAdmin'
    OR partner_id = (
      SELECT companies.partner_id FROM public.reps
      JOIN public.companies ON companies.id = reps.company_id
      WHERE reps.id = auth.uid()
        AND reps.tier IN ('PartnerAdmin', 'SuperAdmin')
    )
  );
```

---

## Migration Plan

### Phase 1 — Enable RLS (no policy = deny all for anon key) — **SAFE NOW**

```sql
-- Enable RLS on all tables but service role still bypasses.
-- No policies = deny for anon-key clients; existing API unaffected.
ALTER TABLE public.reps              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.calls             ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.companies         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.crm_contacts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.crm_opportunities ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.crm_activities    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.assignments       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_events      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.licence_pools     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.company_licences  ENABLE ROW LEVEL SECURITY;
```

**Risk:** None. Service role queries are unaffected. Anon-key queries (Dashboard, direct client) are now denied by default.

### Phase 2 — Add belt-and-suspenders policies (Option C above)

Add the policies documented above. Service role still bypasses them, but they're ready for Phase 3.

**Risk:** None to API. Some Dashboard queries may start returning empty results — expected.

### Phase 3 — Architecture refactor: user-JWT queries (Option A)

Refactor auth middleware to forward the full JWT. Switch per-user queries to anon-client with JWT. Service role retained only for admin/background operations.

**Risk:** High. Requires thorough testing. Roll out route-by-route in a feature branch.

---

## Decision

**Recommendation: Implement Phase 1 + Phase 2 now.**

Phase 1 (enable RLS with no policies) is safe and takes 10 minutes in the Supabase SQL editor. Phase 2 adds the correct policies without changing any API behaviour. Phase 3 is a sprint-level architectural work item.

The SQL for Phase 1 + 2 is in `sql/20260609_rls_phase1_enable.sql` and `sql/20260609_rls_phase2_policies.sql` (to be created when approved).

---

## Migration Risk Assessment

| Phase | API risk | Dashboard risk | Effort |
|---|---|---|---|
| Phase 1 — Enable RLS | None | Low (denies anon reads) | 30 min |
| Phase 2 — Add policies | None | Low | 2 hours |
| Phase 3 — JWT queries | **High** | None | 2-3 sprints |
