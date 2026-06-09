# Users Compatibility Layer

**Date:** 2026-06-09  
**Status:** Active — compatibility layer in place, deprecation pending

---

## What the compatibility layer is

The `users` and `profiles` tables are legacy stores that pre-date the `reps`-first architecture. Rather than a hard cut-over (which would break running code), a compatibility strategy maintains both while new writes land in `reps` and old callers are migrated route-by-route.

---

## Current state: dual-write / dual-read

### POST /v1/admin/users (admin.ts:206)
Writes to **`users`**, not `reps`. Creates a user in the legacy table. This is the biggest gap — the create path is not on the new model.

Route behaviour today:
1. Reads `users.org_id` to check the seat count against `org_limits.max_users`
2. Validates `manager_id` from `users`
3. Inserts into `users`

This means users created via this endpoint:
- Do not appear in middleware tier lookups (which read `reps`)
- Do not appear in `GET /v1/users/me` (which reads `reps`)
- Are invisible to `requireManager`, `requirePartnerAdmin`, `requireSuperAdmin`

### GET /v1/admin/users (admin.ts:350)
Reads from **`users`**. Returns `org_id`-scoped user list.

### GET /v1/admin/partner/users (admin.ts:1254)
Reads from **`reps`** via `getVisibleCompanies`. Correctly uses the new model.

### POST /v1/admin/users/:id (admin.ts:1506)
Reads from and writes to **`reps`** exclusively. Uses `assertUserEditScope`. Already on the new model.

### Leaderboard name hydration (server.ts:323–381)
Falls back through `profiles.display_name → users.full_name → "Rep"`. Neither table is `reps`.

### Team route (team.ts:28,73)
Reads from **`profiles`** only.

### Scoring / call context (scoring.ts:1299, 1340–1392)
Reads `profiles.display_name` with fallback to `profiles.email`.

---

## Compatibility rules (in effect now)

1. **All new routes MUST read from `reps` only.** Do not add new `.from("users")` or `.from("profiles")` calls.
2. **All middleware reads `reps` only.** This is already the case for all three gates.
3. **Profile reads go to `reps`.** `GET /v1/users/me` and `PATCH /v1/users/me` already use `reps`.
4. **Existing `users` callers are frozen.** No new features are built on top of them. They get migrated, not extended.

---

## Deprecation plan

### Step 1 — Shadow-write to `reps` on POST /v1/admin/users (Phase 1)

When `POST /v1/admin/users` creates a user, also insert a corresponding row in `reps` with `tier` mapped from the old `role` field:

| users.role | reps.tier |
|---|---|
| `rep` | `SalesRep` |
| `manager` | `Manager` |
| `owner` | `Owner` |

The response continues to include the `users` row until callers are migrated. This unblocks the auth flow — the new user becomes visible to middleware immediately.

Seat limit check: migrate to `reps` count after shadow-write is running and verified.

### Step 2 — Migrate name hydration (Phase 2)

Replace the `profiles → users → "Rep"` fallback chain in:
- `server.ts:323–381` (leaderboard)
- `server.ts:1174–1178` (rep name batch)
- `scoring.ts:1299,1340–1392` (call context)
- `dashboard.ts:890–913` (dashboard name hydration)

With: `.from("reps").select("id, display_name, email")`

Backfill `reps.display_name` from `profiles.display_name` where not already set (a one-time SQL UPDATE, already partially done in `20260605d_user_company_profile_fields.sql`).

### Step 3 — Migrate GET /v1/admin/users and POST /v1/admin/users to reps (Phase 2)

Switch GET /admin/users to read from `reps` with `company_id` scope (replacing `org_id` scope).  
Switch POST /admin/users to write `reps` only once the shadow-write has been running and all consumers verified.

Seat limit check migrates to `reps` seat counting at the same time.

### Step 4 — team.ts reads `reps` (Phase 2)

`team.ts` currently reads `profiles` directly. Migrate to `reps`. Drop `profiles` dependency.

### Step 5 — Drop `profiles` reads (Phase 3)

After all call sites are off `profiles`, the `profiles` table can be archived (not dropped — kept as read-only audit trail until end of retention period).

### Step 6 — Drop `users` reads (Phase 3)

After all call sites are off `users`, the `users` table can be archived. The Supabase auth trigger that creates rows in `users` on new signups (if one exists) can be removed or redirected to `reps`.

---

## Deprecated fields reference

| Legacy location | Replacement |
|---|---|
| `users.role` | `reps.tier` |
| `users.org_id` | `reps.company_id` → `companies.id` |
| `users.manager_id` | `reps.manager_id` |
| `users.office_id` | `reps.office_id` |
| `users.full_name` | `reps.display_name` |
| `profiles.display_name` | `reps.display_name` |
| `profiles.manager_id` | `reps.manager_id` |
| `profiles.email` | `reps.email` |
| `org_limits.max_users` | `licence_pools.purchased` / `company_licences.allocated` |

---

## Risk: users created before full migration

Users created via the old `POST /v1/admin/users` path before shadow-write lands:
- Exist in `users` but not in `reps`
- Cannot authenticate via any `requireManager`-gated route
- Appear in `GET /v1/admin/users` (old route) but not in partner user lists

**Mitigation:** Run a one-time backfill SQL that creates `reps` rows from all `users` rows that have no corresponding `reps` row. Include in Phase 1 migration SQL.
