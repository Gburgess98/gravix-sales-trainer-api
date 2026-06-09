# Identity Consolidation Roadmap

**Date:** 2026-06-09  
**Goal:** Single canonical identity table (`reps`), no `users`/`profiles` queries in application code

---

## Current position

The `reps`-first architecture is half-implemented:
- ✅ All middleware reads `reps`
- ✅ `GET /v1/users/me` + `PATCH /v1/users/me` read/write `reps`
- ✅ `reps.company_id` populated for all reps
- ✅ `companies.partner_id` in place
- ❌ `POST /v1/admin/users` writes `users`, not `reps`
- ❌ 12+ route files still read from `users` or `profiles` for name hydration and user lists
- ❌ CRM scoping uses `org_id` (not `company_id`)
- ❌ Seat limits checked against `org_limits` (not `licence_pools`)

---

## Phase 1: Backfill + Shadow Write

**Scope:** Ensure every user that exists in `users` has a corresponding `reps` row. Add shadow-write to user creation. No read-path changes yet.

**Duration estimate:** 1 sprint (2–3 days)

### Tasks

1. **Backfill SQL** (`sql/202606xx_backfill_reps_from_users.sql`)
   - INSERT INTO reps (SELECT from users WHERE id NOT IN (SELECT id FROM reps))
   - Map `users.role` → `reps.tier` (`'rep'` → `'SalesRep'`, `'manager'` → `'Manager'`)
   - Set `reps.email` from `users.email`
   - Set `reps.display_name` from `users.full_name` where `reps.display_name IS NULL`
   - Assign `reps.company_id` from `users.org_id` mapping (via orgs → companies lookup)

2. **Orphan detection** (run before and after backfill)
   ```sql
   SELECT id FROM users WHERE id NOT IN (SELECT id FROM reps);
   SELECT id FROM calls WHERE user_id NOT IN (SELECT id FROM reps) AND user_id IS NOT NULL;
   SELECT id FROM assignments WHERE user_id NOT IN (SELECT id FROM reps) AND user_id IS NOT NULL;
   ```

3. **Shadow-write in `POST /v1/admin/users`**
   - After inserting into `users`, also insert into `reps`
   - Map fields as above
   - If `reps` insert fails, log warning but do not fail the request (fail-soft during transition)

4. **FK additions** (safe after backfill, zero orphans confirmed)
   - `calls.user_id → reps.id ON DELETE SET NULL`
   - `assignments.user_id → reps.id ON DELETE SET NULL`

### Deliverable gate
- `npm run validate:identity-architecture` passing on all Phase 1 checks
- Zero rows in orphan detection queries
- Shadow-write verified in dev: create user → appears in both `users` and `reps`

---

## Phase 2: Read-path Migration

**Scope:** Move all read paths off `users` and `profiles` onto `reps`. No schema drops yet — both tables remain populated.

**Duration estimate:** 1–2 sprints (1 week)

### Tasks

1. **Name hydration (server.ts)**
   - Replace `profiles → users → "Rep"` chain with `reps.display_name`
   - Files: `server.ts:323–381`, `server.ts:1174–1178`

2. **Dashboard name hydration (dashboard.ts)**
   - Replace `profiles.display_name → users.full_name` in `dashboard.ts:890–913`
   - Use `reps.display_name`

3. **Scoring context (scoring.ts)**
   - Replace `profiles` lookups in `scoring.ts:1299,1340–1392`
   - Use `reps.display_name` + `reps.email`

4. **Team route (team.ts)**
   - Migrate `team.ts:28,73` from `profiles` → `reps`
   - Filter by `reps.company_id` instead of (currently no company filter)

5. **GET /v1/admin/users migration**
   - Switch from `users` → `reps` query
   - Replace `org_id` scope with `company_id` scope
   - The response shape must be preserved (backward compatible)

6. **POST /v1/admin/users: write `reps` only**
   - Once shadow-write (Phase 1) is running and all readers are on `reps`, drop the `users` insert
   - Seat limit check migrates from `org_limits.max_users` to `licence_pools` calculation

7. **CRM org_id → company_id**
   - Execute OWNERSHIP_MIGRATION_PLAN.md Phase 2 SQL
   - Update `crm.ts` to use `company_id` filter instead of `org_id`
   - `x-org-id` header value becomes `company_id` UUID (same header name, updated semantics)

8. **calls.ts reads reps**
   - `calls.ts:74,87,100` — replace `users` lookups with `reps` lookups

9. **assignments.ts reads reps**
   - `assignments.ts:203,217` — replace `users` lookups with `reps` lookups

10. **sparring.ts reads reps**
    - `sparring.ts:2508` — replace `users` lookup

11. **internal.ts reads reps**
    - `internal.ts:152` — replace `users` lookup

### Deliverable gate
- `grep -r '\.from("users")' src/` returns 0 results
- `grep -r '\.from("profiles")' src/` returns 0 results
- All regression and auth/lighthouse validation suites pass

---

## Phase 3: Cleanup

**Scope:** Archive legacy tables, drop redundant columns, remove compatibility shims.

**Duration estimate:** 1 sprint (half-day of migrations)

**Prerequisite:** Phase 2 fully deployed and verified in production for ≥ 2 weeks

### Tasks

1. **Archive `users` table**
   ```sql
   ALTER TABLE public.users RENAME TO _users_archive;
   -- do not DROP — keep as read-only audit trail
   ```

2. **Archive `profiles` table**
   ```sql
   ALTER TABLE public.profiles RENAME TO _profiles_archive;
   ```

3. **Drop `reps.org_id`** (after CRM migration and all readers confirmed gone)
   ```sql
   ALTER TABLE public.reps DROP COLUMN IF EXISTS org_id;
   ```

4. **Drop `companies.tmc_id`** (after confirming `partner_id` fully supersedes it)
   ```sql
   ALTER TABLE public.companies DROP COLUMN IF EXISTS tmc_id;
   ```

5. **Archive `org_limits` table** (after `licence_pools` seat enforcement live)
   ```sql
   ALTER TABLE public.org_limits RENAME TO _org_limits_archive;
   ```

6. **Drop Supabase trigger** that creates rows in `users` on new auth.users signup (if one exists) — or redirect it to create `reps` rows instead

7. **Update `validate:identity-architecture`** to verify zero references to archived tables

### Deliverable gate
- `grep -rn "_archive\|org_limits\|\.tmc_id" src/` returns 0 results
- `users` and `profiles` tables are renamed with `_archive` prefix
- All tests pass

---

## Phase summary

| Phase | Effort | Risk | API impact |
|---|---|---|---|
| Phase 1 — Backfill + shadow write | Low | Low | None |
| Phase 2 — Read-path migration | Medium | Medium (x-org-id semantic change) | Additive; backward compatible if header value remains valid |
| Phase 3 — Cleanup | Low | Low (archives, not drops) | None |
