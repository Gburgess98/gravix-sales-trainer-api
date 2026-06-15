-- 20260615_whisperer_trigger_library.sql
-- TIER 2B — Day 119: manager-configurable Whisperer triggers.
--
-- Teams define their own objection phrases/keywords + suggested responses,
-- tenant-scoped, loaded by the trigger engine at session time and merged with
-- the built-in semantic triggers. No code change needed to tune Whisperer.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists whisperer_trigger_library;

create table if not exists public.whisperer_trigger_library (
  id uuid primary key default gen_random_uuid(),

  org_id uuid null,
  company_id uuid null,
  office_id uuid null,
  created_by uuid null,

  name text not null,
  description text null,
  type text not null default 'custom',

  match_phrases text[] not null default '{}',
  match_keywords text[] not null default '{}',

  suggestion_title text not null,
  suggestion_response text not null,
  urgency text not null default 'medium',
  emoji text null,

  enabled boolean not null default true,
  priority integer not null default 50,

  meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint whisperer_trigger_library_urgency_chk
    check (urgency in ('low', 'medium', 'high'))
);

create index if not exists idx_wtl_org_enabled on public.whisperer_trigger_library(org_id, enabled, created_at desc);
create index if not exists idx_wtl_company_enabled on public.whisperer_trigger_library(company_id, enabled, created_at desc);
create index if not exists idx_wtl_office_enabled on public.whisperer_trigger_library(office_id, enabled, created_at desc);
create index if not exists idx_wtl_type_enabled on public.whisperer_trigger_library(type, enabled);
create index if not exists idx_wtl_creator on public.whisperer_trigger_library(created_by, created_at desc);
