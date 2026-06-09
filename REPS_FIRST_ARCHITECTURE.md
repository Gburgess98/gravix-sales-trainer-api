# Reps-First Architecture

**Date:** 2026-06-09  
**Status:** Target model — partially implemented

---

## Problem statement

The application currently has three overlapping identity tables:

| Table | Role | State |
|---|---|---|
| `auth.users` | GoTrue auth (Supabase managed) | Authoritative for authentication |
| `users` | Legacy application identity | Still used by 12+ routes in admin.ts, calls.ts, assignments.ts, dashboard.ts, sparring.ts, internal.ts |
| `profiles` | Mid-layer display-name store | Used by team.ts, scoring.ts, server.ts leaderboard hydration |
| `reps` | Emerging canonical identity | Used by all middleware + newer routes |

Every identity query has a fallback chain — `profiles → users → "Rep"` in the leaderboard, `profiles.display_name → users.full_name` in server.ts — that exists purely because `reps` didn't always carry the full profile. This fallback chain is the debt being repaid.

---

## Target identity model

```
auth.users (GoTrue — Supabase managed)
    │  id (UUID) — primary auth identity
    │  email
    │  encrypted_password
    │  created_at / last_sign_in_at
    │
    └──► reps
             id          UUID PK  (= auth.users.id — same UUID)
             email       TEXT     (denormalised from auth.users for query convenience)
             display_name TEXT
             first_name  TEXT
             last_name   TEXT
             phone_number TEXT
             avatar_url  TEXT
             job_title   TEXT
             department  TEXT
             timezone    TEXT NOT NULL DEFAULT 'UTC'
             is_active   BOOLEAN NOT NULL DEFAULT true
             tier        TEXT  CHECK(tier IN ('SalesRep','TeamLead','Manager','Owner','PartnerAdmin','SuperAdmin'))
             xp          INT
             manager_id  UUID  → reps.id (self-referential, no FK constraint)
             org_id      UUID  → orgs.id (legacy — being migrated to company_id)
             company_id  UUID  → companies.id
             office_id   UUID  → offices.id
             created_at  TIMESTAMPTZ
             updated_at  TIMESTAMPTZ
             │
             └──► companies
                      id          UUID PK
                      name        TEXT
                      partner_id  UUID  → partners.id
                      org_id      UUID  (legacy — see OWNERSHIP_MIGRATION_PLAN.md)
                      is_active   BOOLEAN
                      created_at  TIMESTAMPTZ
                      updated_at  TIMESTAMPTZ
                      │
                      └──► partners
                               id         UUID PK
                               name       TEXT
                               slug       TEXT UNIQUE
                               status     TEXT DEFAULT 'active'
                               created_at TIMESTAMPTZ
                               updated_at TIMESTAMPTZ
```

---

## Table roles in the target model

### `auth.users` (Supabase GoTrue — immutable from API)
- Source of truth for authentication: password hashing, JWT signing, session management
- Modified only via `supabase.auth.admin.*` calls (service role) or Supabase Dashboard
- The API never directly queries `auth.users` in application code except for one best-effort name hydration in `crm.ts:4394`
- `reps.id = auth.users.id` — the UUID is set at signup time and shared

### `reps` (application identity — target canonical table)
- One row per user; row created on first signup via Supabase trigger or explicit API call
- `tier` replaces the `users.role` field (`rep`→`SalesRep`, `manager`→`Manager`)
- Carries all profile data: display name, contact info, locale, status
- All middleware (`requireManager`, `requirePartnerAdmin`, `requireSuperAdmin`) read only from `reps`
- `GET /v1/users/me` and `PATCH /v1/users/me` already read/write `reps` exclusively
- `reps.email` is denormalised from `auth.users.email` for join-free queries; write-through on auth email change (not yet implemented — see Phase 2)

### `companies`
- Represents one customer organisation
- Has `partner_id` → `partners.id` (added in `20260605_partner_foundation.sql`)
- `org_id` column is legacy (see OWNERSHIP_MIGRATION_PLAN.md)
- Carries profile: website, industry, phone_number, address, is_active (added in `20260605d`)

### `partners`
- Represents a reseller / channel partner
- Created in `20260605_partner_foundation.sql` using the existing `tmc_id` UUID as PK
- One `licence_pool` per partner (from `20260609_licence_pools.sql`)
- Admins for a partner have `reps.tier = 'PartnerAdmin'` and their `reps.company_id` points to a company under that partner

---

## Tables being retired (do not add new queries against these)

### `users` — retire in Phase 2
Current columns known to be in use: `id`, `email`, `role`, `org_id`, `manager_id`, `office_id`  
Current callers: `admin.ts:262,284,316,369,550,896,987`, `calls.ts:74,87,100`, `assignments.ts:203,217`, `dashboard.ts:34,907`, `sparring.ts:2508`, `internal.ts:152`

### `profiles` — retire in Phase 3
Current columns in use: `id`, `display_name`, `manager_id`, `email`  
Current callers: `server.ts:325,376,1174,1178`, `scoring.ts:1299,1351,1392`, `team.ts:28,73`, `calls.ts:438`, `dashboard.ts:896`

---

## Current state vs target

| Feature | Current | Target |
|---|---|---|
| Auth | auth.users (GoTrue) | Same |
| Application identity | Split: users + reps | reps only |
| Display names | profiles.display_name + users.full_name fallback | reps.display_name |
| Tier/role | users.role ('rep'/'manager') + reps.tier | reps.tier only |
| Company scope | users.org_id → orgs + reps.company_id → companies | reps.company_id → companies |
| Partner scope | companies.tmc_id (denormalised) | companies.partner_id → partners |
| Middleware | Reads from reps ✅ | Same |
| User create (admin) | Writes to users ❌ | Write to reps only |
| Leaderboard name hydration | profiles → users → "Rep" fallback ❌ | reps.display_name direct |
