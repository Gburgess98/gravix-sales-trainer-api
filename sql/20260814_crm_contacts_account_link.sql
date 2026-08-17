-- 20260814_crm_contacts_account_link.sql
--
-- Day 285 — CRM contact ↔ account link (accepted decision: one CRM contact
-- belongs to at most one account).
--
-- Background: the account-detail page links/unlinks contacts and lists an
-- account's contacts, but `crm_contacts` (the canonical store the WEB search and
-- "Add Contact" use) had no account relationship, and the disconnected legacy
-- `contacts` table was never populated by that flow (see
-- Decision - Account Contact-Link Data Model). This adds the smallest model that
-- matches the product: a single nullable account foreign key on `crm_contacts`.
--
--   * Nullable — a contact may have zero or one account; existing rows keep NULL.
--   * FK → accounts(id) ON DELETE SET NULL — deleting an account detaches its
--     contacts, it does NOT delete them (contacts are owned by a rep/user, not the
--     account).
--   * Indexed — the account-detail read path filters crm_contacts by account_id.
--   * If contacts ever need to span accounts, this nullable FK migrates cleanly
--     into a join table without losing existing links.
--
-- Idempotent: safe to run more than once. Additive only; no backfill.

alter table public.crm_contacts
  add column if not exists account_id uuid null;

-- Foreign key (added separately so re-runs don't attempt a duplicate constraint).
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'crm_contacts_account_id_fkey'
  ) then
    alter table public.crm_contacts
      add constraint crm_contacts_account_id_fkey
      foreign key (account_id) references public.accounts (id) on delete set null;
  end if;
end $$;

-- The account-detail read path filters by account_id, so index it.
create index if not exists idx_crm_contacts_account
  on public.crm_contacts (account_id);
