-- 20260819_org_settings.sql
--
-- Day 291 — org-scoped Call Library visibility policy (`org_settings`).
--
-- Background: the admin Call-Library "company calls" policy read/write
-- (GET/PATCH /v1/admin/org-settings) and the backend enforcement
-- (getOrgCallVisibility, used by /v1/calls/paged?scope=company and canAccessCall)
-- both target public.org_settings, but no migration ever created the table — so a
-- resolved manager's policy read 500s and, before this lane, enforcement fell back
-- to "everyone" on the resulting error. This creates the smallest contract the
-- code already assumes.
--
--   * One row per org — org_id is the primary key.
--   * FK → orgs(id) ON DELETE CASCADE — settings belong to the org (reps.org_id
--     already FKs orgs(id)); dropping an org drops its settings.
--   * call_visibility constrained to the three values the API/UI already use,
--     with an explicit default of 'everyone' (the historical absent-row default;
--     absent rows keep resolving to 'everyone' in code, errors never do).
--   * created_at/updated_at timestamptz, matching existing table conventions.
--
-- Idempotent: safe to run more than once. Additive only; no backfill. Never apply
-- to production from here — staging DDL is an approved operator step.

create table if not exists public.org_settings (
  org_id uuid primary key,
  call_visibility text not null default 'everyone',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Constrain call_visibility to the allowed policy values (added separately so
-- re-runs don't attempt a duplicate constraint).
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'org_settings_call_visibility_check'
  ) then
    alter table public.org_settings
      add constraint org_settings_call_visibility_check
      check (call_visibility in ('everyone', 'managers', 'disabled'));
  end if;
end $$;

-- Foreign key → orgs(id) (added separately for idempotent re-runs).
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'org_settings_org_id_fkey'
  ) then
    alter table public.org_settings
      add constraint org_settings_org_id_fkey
      foreign key (org_id) references public.orgs (id) on delete cascade;
  end if;
end $$;
