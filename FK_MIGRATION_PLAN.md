# FK Migration Plan

**Date:** 2026-06-09  
**Status:** Pre-implementation — no schema changes in this document

---

## Scope

This plan covers foreign key relationships that need to change as the application moves from the `users`-centric model to the `reps`-first model. It does not cover additive FK additions (those are safe to run at any time).

---

## Current FK landscape (relevant tables)

```
reps.id ─────────────────────────── no FK to auth.users (same UUID convention, not enforced)
reps.company_id ─────────────────── companies.id  ON DELETE SET NULL  ✅ already in place
reps.office_id ──────────────────── offices.id    ON DELETE SET NULL  ✅ already in place
reps.manager_id ─────────────────── (no FK — self-referential, intentionally unconstrained)

companies.partner_id ────────────── partners.id   ON DELETE SET NULL  ✅ added in 20260605
companies.org_id ────────────────── orgs.id (legacy, nullable)

calls.user_id ───────────────────── users.id or reps.id? (ambiguous — needs audit)
assignments.user_id ─────────────── users.id (legacy)
crm_contacts.org_id ─────────────── orgs.id (via x-org-id header)
audit_events.actor_id ───────────── (no FK — intentional for immutability)
audit_events.target_id ──────────── (no FK — intentional)
licence_pools.partner_id ────────── partners.id   ✅ added in 20260609
company_licences.company_id ──────── companies.id  ✅ added in 20260609
```

---

## Tables affected by the migration

### HIGH PRIORITY (block new feature development)

#### `calls.user_id`
- **Current:** assumed to reference `users.id` by original design; the leaderboard and scoring code resolves names by looking up `profiles` and `users` using the same UUID
- **Target:** `calls.user_id` should be a FK to `reps.id` with `ON DELETE SET NULL`
- **Risk:** If any `calls.user_id` values exist that have a `users` row but no `reps` row, adding the FK will fail until backfill runs
- **Migration order:** backfill `reps` from `users` → verify zero orphans → `ALTER TABLE calls ADD CONSTRAINT calls_user_id_fkey FOREIGN KEY (user_id) REFERENCES reps(id) ON DELETE SET NULL`

#### `assignments.user_id`
- **Current:** reads from `users` (admin.ts:203,217)
- **Target:** references `reps.id`
- **Risk:** Same orphan risk as `calls.user_id`
- **Migration order:** same backfill → verify → add FK

### MEDIUM PRIORITY (compatibility layer still active)

#### `crm_contacts.org_id`, `crm_activities.org_id`, `crm_opportunities.org_id`
- **Current:** references `orgs.id` (passed via `x-org-id` header)
- **Target:** should reference `companies.id` after org_id → company_id migration completes
- **Dependency:** `OWNERSHIP_MIGRATION_PLAN.md` Phase 2 must complete first
- **Risk:** High — changing the FK target requires all existing `org_id` values to have a corresponding `companies.id` (one-to-one mapping must be confirmed)

#### `coach_assignments.rep_id`, `coach_notes.rep_id`
- **Current:** likely references `reps.id` already (added post-reps era)
- **Action:** confirm FK exists; add if missing

### LOW PRIORITY (no active migration needed)

#### `reps.manager_id`
- Intentionally unconstrained (self-referential FKs can cause delete cascade issues)
- Will remain application-enforced, not DB-enforced

---

## Migration order

The order matters because of FK dependency chains. Violating it requires disabling FK checks.

```
Phase 1 (safe, additive)
  1. Backfill reps from users (no FK changes)
  2. Verify: SELECT id FROM users WHERE id NOT IN (SELECT id FROM reps) → 0 rows
  3. Add: calls.user_id → reps.id FK (DEFERRABLE INITIALLY DEFERRED for safety)
  4. Add: assignments.user_id → reps.id FK

Phase 2 (requires org migration complete)
  1. Create companies ↔ orgs mapping table OR confirm orgs.id = companies.id (check live data)
  2. Migrate crm_contacts.org_id values to companies.id if schema differs
  3. Add FK: crm_contacts.org_id → companies.id (only after value migration)
  4. Repeat for crm_activities, crm_opportunities

Phase 3 (cleanup — no functional dependency)
  1. Drop calls.user_id → users.id FK if it exists (after Phase 1 FK added)
  2. Archive users table (rename to _users_archive, keep data)
  3. Archive profiles table (rename to _profiles_archive)
```

---

## Rollback strategy

### Phase 1 rollback

All Phase 1 FKs are additive (`ADD CONSTRAINT`). Rollback is:
```sql
ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_user_id_fkey;
ALTER TABLE assignments DROP CONSTRAINT IF EXISTS assignments_user_id_fkey;
```
No data is modified; no data loss possible.

### Phase 2 rollback

Phase 2 involves value migration (changing `org_id` values to `company_id` values). Before any value changes:
1. Always `BEGIN; ... ROLLBACK;` first run to verify row counts
2. Take a named snapshot: `CREATE TABLE _crm_contacts_pre_migration AS SELECT * FROM crm_contacts`
3. Only `COMMIT` after verification

Rollback procedure:
```sql
BEGIN;
UPDATE crm_contacts SET org_id = _bak.org_id FROM _crm_contacts_pre_migration _bak WHERE crm_contacts.id = _bak.id;
-- verify counts match
COMMIT;
```

### Phase 3 rollback

Archive tables (`_users_archive`, `_profiles_archive`) are never dropped. Restore by:
```sql
ALTER TABLE _users_archive RENAME TO users;
```
No data loss possible as long as the archive exists.

---

## Orphan detection query (run before any FK constraint add)

```sql
-- Calls with user_id not in reps
SELECT COUNT(*) FROM calls c
WHERE c.user_id IS NOT NULL
  AND c.user_id NOT IN (SELECT id FROM reps);

-- Assignments with user_id not in reps
SELECT COUNT(*) FROM assignments a
WHERE a.user_id IS NOT NULL
  AND a.user_id NOT IN (SELECT id FROM reps);
```

These must return 0 before adding the FK. If non-zero, include the orphan user IDs in the Phase 1 backfill SQL.

---

## FK additions that are safe NOW (no migration risk)

These can be added at any time because the referenced tables already exist and the values are already valid:

```sql
-- coach assignments (if not already present)
ALTER TABLE coach_assignments
  ADD CONSTRAINT IF NOT EXISTS coach_assignments_rep_id_fkey
  FOREIGN KEY (rep_id) REFERENCES reps(id) ON DELETE CASCADE;

-- coach notes
ALTER TABLE coach_notes
  ADD CONSTRAINT IF NOT EXISTS coach_notes_rep_id_fkey
  FOREIGN KEY (rep_id) REFERENCES reps(id) ON DELETE CASCADE;
```
