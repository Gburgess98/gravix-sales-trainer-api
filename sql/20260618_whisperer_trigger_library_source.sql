-- 20260618_whisperer_trigger_library_source.sql
-- TIER 2B — Day 139: approved candidate → source link.
--
-- When a manager saves a custom trigger from an AI Trigger Discovery candidate,
-- record which candidate produced it plus a small source snapshot, so the
-- trigger can be traced back to the evidence. Additive + nullable — existing
-- rows and the normal manual create path are unaffected.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   alter table public.whisperer_trigger_library
--     drop column if exists source_candidate_id,
--     drop column if exists source_meta;

alter table public.whisperer_trigger_library
  add column if not exists source_candidate_id text null,
  add column if not exists source_meta jsonb not null default '{}'::jsonb;

create index if not exists idx_wtl_source_candidate
  on public.whisperer_trigger_library(source_candidate_id);
