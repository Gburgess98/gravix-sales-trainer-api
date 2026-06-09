# Ownership Migration Plan

**Date:** 2026-06-09  
**Status:** Phase 0 complete (company_id / partner_id added); org_id retirement pending

---

## The three ownership axes

| Field | Lives on | Current use | Target use |
|---|---|---|---|
| `org_id` | reps, crm_contacts, crm_activities, crm_opportunities, users, calls (some) | CRM + call scoping via `x-org-id` header | Retired; replaced by company_id |
| `company_id` | reps, companies (self), company_licences | Company membership for reps; partner→company FK | Sole company ownership field |
| `partner_id` | companies, licence_pools, company_licences | Partner-level grouping | Retained as-is |

---

## Phase 0: what is already done

### `reps.company_id` → `companies.id` ✅
Added in `20260604_reps_company_office_bridge.sql`. Backfilled from `users.company_id` where available, then assigned remaining reps to the Gravix Test Company. All middleware now uses `reps.company_id` for company-scope lookups.

### `companies.partner_id` → `partners.id` ✅
Added in `20260605_partner_foundation.sql`. Backfilled from `companies.tmc_id` using the existing UUID. `getVisibleCompanies()` correctly traverses `reps.company_id → companies.partner_id`.

### `licence_pools.partner_id` → `partners.id` ✅
Created in `20260609_licence_pools.sql` with correct FK from the start.

---

## Phase 1: `org_id` audit

### Where `org_id` is still active

**CRM routes (crm.ts):**
- `getOrgIdHeader()` reads `x-org-id` header, falls back to `DEFAULT_ORG_ID` env var
- All CRM contact/opportunity/activity reads filter `.eq("org_id", orgId)`
- All CRM records created with `org_id` stamped from the resolved header value

**reps table:**
- `reps.org_id` column exists (legacy); `GET /v1/users/me` returns it
- Not used by any middleware
- Not used by `getVisibleCompanies()`

**users table (legacy):**
- `users.org_id` still used by `POST /v1/admin/users` for seat limit checks
- Scoped against `org_limits.max_users`

### Relationship between `org_id` and `company_id`

The `org_id` is a UUID passed by the frontend via the `x-org-id` header. Based on the migration history, there is a 1:1 relationship:

```
orgs.id ←──── crm tables (via x-org-id header)
companies.id ←──── reps.company_id, company_licences
```

Whether `orgs.id === companies.id` for all rows must be verified before any migration. The `20260605_partner_foundation.sql` notes that `tmc_id` was used as the proto-partner UUID, suggesting the IDs may differ from `companies.id`.

**Action required (before Phase 2):** Run this query to determine if they match:
```sql
SELECT
  c.id AS company_id,
  c.name,
  c.org_id AS company_org_id,  -- if this column exists
  COUNT(DISTINCT cr.org_id) AS distinct_crm_org_ids
FROM companies c
LEFT JOIN crm_contacts cr ON cr.org_id IS NOT NULL
GROUP BY c.id, c.name
ORDER BY c.name;
```

---

## Phase 2: migrate CRM `org_id` → `company_id`

### Goal
Replace `crm_contacts.org_id`, `crm_activities.org_id`, `crm_opportunities.org_id` with `company_id` references. After this, `x-org-id` header value becomes `company_id` and the `orgs` table can be retired.

### Pre-conditions
1. Phase 0 complete ✅
2. `org_id` values audited — confirmed to map 1:1 to `companies.id`
3. All `reps` have `company_id` set (zero NULLs) ✅

### Migration steps

```sql
-- Step 1: add company_id columns
ALTER TABLE public.crm_contacts       ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id) ON DELETE SET NULL;
ALTER TABLE public.crm_activities     ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id) ON DELETE SET NULL;
ALTER TABLE public.crm_opportunities  ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id) ON DELETE SET NULL;

-- Step 2: backfill using the org_id → company mapping
-- (Assumes org_id values in crm tables match companies.id — verify first)
UPDATE public.crm_contacts      SET company_id = org_id WHERE company_id IS NULL;
UPDATE public.crm_activities    SET company_id = org_id WHERE company_id IS NULL;
UPDATE public.crm_opportunities SET company_id = org_id WHERE company_id IS NULL;

-- Step 3: verify zero NULLs before making non-nullable
SELECT COUNT(*) FROM crm_contacts      WHERE company_id IS NULL;
SELECT COUNT(*) FROM crm_activities    WHERE company_id IS NULL;
SELECT COUNT(*) FROM crm_opportunities WHERE company_id IS NULL;
-- All must be 0

-- Step 4: add NOT NULL constraint
ALTER TABLE public.crm_contacts      ALTER COLUMN company_id SET NOT NULL;
ALTER TABLE public.crm_activities    ALTER COLUMN company_id SET NOT NULL;
ALTER TABLE public.crm_opportunities ALTER COLUMN company_id SET NOT NULL;
```

### API change
After column migration, update `crm.ts`:
- `getOrgIdHeader()` resolves to `company_id` value (same header, different semantics)
- Filter changes from `.eq("org_id", orgId)` to `.eq("company_id", companyId)`
- The `x-org-id` header name can be kept for backward compatibility but its value must be a `company_id` UUID going forward

---

## Phase 3: retire `org_id` fields

### `reps.org_id` retirement
- Once Phase 2 CRM migration is complete and all consumers use `company_id`
- Drop `reps.org_id` from `GET /v1/users/me` response (emit `null` or omit)
- Add deprecation warning to API response if `reps.org_id` is still being set

### `users.org_id` retirement
- Dependent on `POST /v1/admin/users` being migrated to write `reps` (see USERS_COMPATIBILITY_LAYER.md)
- Once `users` table is archived, `users.org_id` goes with it

### `org_limits` table retirement
- Seat limits will be tracked via `licence_pools` (purchased) / `company_licences` (allocated)
- `org_limits.max_users` check in `admin.ts:262–272` migrates to: `licence_pools.purchased - SUM(company_licences.allocated) > 0`
- After migration, `org_limits` table can be archived

---

## partner_id: no migration required

`companies.partner_id → partners.id` is correctly in place. No changes needed.

The `companies.tmc_id` column is legacy (pre-partners era). It should be:
1. Left in place (for now — do not break any code that still reads it)
2. Confirmed to equal `companies.partner_id` after the `20260605` backfill
3. Dropped as part of Phase 3 cleanup after confirmation

---

## Summary of field ownership in target state

| Table | Ownership field | Value | FK target |
|---|---|---|---|
| `reps` | `company_id` | Company UUID | `companies.id` |
| `reps` | `org_id` | **Retired** | — |
| `companies` | `partner_id` | Partner UUID | `partners.id` |
| `crm_contacts` | `company_id` (Phase 2) | Company UUID | `companies.id` |
| `crm_contacts` | `org_id` | **Retired after Phase 2** | — |
| `licence_pools` | `partner_id` | Partner UUID | `partners.id` |
| `company_licences` | `company_id` | Company UUID | `companies.id` |
