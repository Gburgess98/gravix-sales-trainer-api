-- 20260610_call_manager_reviews.sql
-- SPRINT 4 — Day 91: manager call review history.
--
-- Records that a manager reviewed a call (who/when/optional note) so the
-- review queue can be cleared and ROI can count real manager reviews.
-- Product choice (MVP): unique(call_id) — one manager review clears the
-- call for the whole team.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists public.call_manager_reviews;

create table if not exists public.call_manager_reviews (
  id uuid primary key default gen_random_uuid(),

  call_id uuid not null references public.calls(id) on delete cascade,
  manager_id uuid not null,

  org_id uuid null,
  company_id uuid null,
  office_id uuid null,

  status text not null default 'reviewed',
  note text null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint call_manager_reviews_call_id_unique unique (call_id)
);

create index if not exists idx_cmr_call_id on public.call_manager_reviews(call_id);
create index if not exists idx_cmr_manager_id on public.call_manager_reviews(manager_id);
create index if not exists idx_cmr_org_created on public.call_manager_reviews(org_id, created_at desc);
create index if not exists idx_cmr_company_created on public.call_manager_reviews(company_id, created_at desc);
create index if not exists idx_cmr_office_created on public.call_manager_reviews(office_id, created_at desc);
