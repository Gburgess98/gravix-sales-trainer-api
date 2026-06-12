-- 20260612_sparring_data_model_hardening.sql
-- TIER 2A — Day 106: first-class sparring columns (Day 100 data model plan).
--
-- Adds tenant/assignment/lifecycle columns to sparring_sessions and per-turn
-- score/state columns to sparring_turns so manager queries can use direct
-- scoped DB filters instead of in-memory meta parsing. Meta fields are kept
-- for backwards compatibility; `npm run db:backfill-sparring` lifts existing
-- values out of meta after this runs.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- NOTE: no FK on assignment_id — legacy meta.assignment_id values may point at
-- deleted assignments (e.g. cleaned-up test drills), so an FK would break the
-- backfill. The backfill validates linkage against live assignments instead.

alter table public.sparring_sessions
  add column if not exists assignment_id uuid null,
  add column if not exists org_id uuid null,
  add column if not exists company_id uuid null,
  add column if not exists office_id uuid null,
  add column if not exists status text null,
  add column if not exists completed_at timestamptz null,
  add column if not exists state jsonb null;

alter table public.sparring_turns
  add column if not exists turn_score jsonb null,
  add column if not exists state_snapshot jsonb null,
  add column if not exists meta jsonb null;

create index if not exists idx_ss_assignment_id on public.sparring_sessions(assignment_id);
create index if not exists idx_ss_org_completed on public.sparring_sessions(org_id, completed_at desc);
create index if not exists idx_ss_company_completed on public.sparring_sessions(company_id, completed_at desc);
create index if not exists idx_ss_office_completed on public.sparring_sessions(office_id, completed_at desc);
create index if not exists idx_ss_status_completed on public.sparring_sessions(status, completed_at desc);
create index if not exists idx_st_session_id on public.sparring_turns(session_id);
create index if not exists idx_st_session_created on public.sparring_turns(session_id, created_at);

-- ── Rollback (run manually if ever needed) ──────────────────────────────────
-- drop index if exists idx_ss_assignment_id;
-- drop index if exists idx_ss_org_completed;
-- drop index if exists idx_ss_company_completed;
-- drop index if exists idx_ss_office_completed;
-- drop index if exists idx_ss_status_completed;
-- drop index if exists idx_st_session_id;
-- drop index if exists idx_st_session_created;
-- alter table public.sparring_sessions
--   drop column if exists assignment_id,
--   drop column if exists org_id,
--   drop column if exists company_id,
--   drop column if exists office_id,
--   drop column if exists status,
--   drop column if exists completed_at,
--   drop column if exists state;
-- alter table public.sparring_turns
--   drop column if exists turn_score,
--   drop column if exists state_snapshot,
--   drop column if exists meta;
