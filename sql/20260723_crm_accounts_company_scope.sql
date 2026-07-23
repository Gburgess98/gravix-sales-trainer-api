-- 20260723_crm_accounts_company_scope.sql
--
-- Day 248 — make CRM accounts company-scoped.
--
-- Background: crm_accounts was (id, org_id, name, domain, created_at). Day 247
-- moved account ownership off a non-existent user_id column onto org_id, which
-- revived account creation. But org_id is too coarse: both demo companies share
-- one org (89f61a54…), so org scoping does not isolate companies within an org.
--
-- Product decision: company_id is the tenant boundary for CRM accounts.
-- Managers see only their company's accounts. org_id is KEPT for legacy /
-- backwards compatibility and is still written, but is no longer the isolation
-- key once this migration is applied.
--
-- Nullable, not NOT NULL, and NO data backfill here — deliberately:
--   * Every existing crm_accounts row predates this column.
--   * There is no deterministic SQL path from an account to exactly one company:
--     the account has only org_id, that org contains two companies, and no
--     contact/call/opportunity currently links back to these rows. Guessing a
--     company would be inventing ownership, which the Day 247 line explicitly
--     refused to do.
--   * So existing rows keep company_id = NULL and are EXCLUDED from
--     company-scoped reads until repaired. The demo rows are repaired
--     deterministically by re-running seed:demo (seed-demo-org.ts now stamps
--     company_id = DEMO_COMPANY_ID), which is the only source that knows their
--     intended company. Genuinely orphaned rows can be repaired manually.
--   * New rows written through the API always carry company_id (resolved from
--     the requester's rep/company identity), so the column trends to complete.
--
-- Promote to NOT NULL only in a later migration, once every row is backfilled.

alter table public.crm_accounts
  add column if not exists company_id uuid null;

-- Company is the read/scoping key, so index it.
create index if not exists idx_crm_accounts_company
  on public.crm_accounts (company_id);

-- Name is unique per company (case-insensitive), matching the API dedup which
-- resolves-or-creates by (company, name). Partial: only backfilled rows are
-- constrained, so the NULL-company legacy rows never collide, and two different
-- companies may each hold an account of the same name.
create unique index if not exists uq_crm_accounts_company_name
  on public.crm_accounts (company_id, lower(name))
  where company_id is not null;

-- Domain likewise unique per company where present.
create unique index if not exists uq_crm_accounts_company_domain
  on public.crm_accounts (company_id, lower(domain))
  where company_id is not null and domain is not null;
