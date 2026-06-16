-- 20260616_whisperer_suggestion_outcome.sql
-- TIER 2B — Day 122: Whisperer suggestion outcome tracking.
--
-- Reps/managers mark each Whisperer suggestion as used / ignored / not_relevant
-- so managers can see which suggestions land, which are noise, and which custom
-- triggers need editing. Additive only — no behaviour change to detection.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS / guarded constraint throughout.
--
-- Rollback:
--   alter table public.whisperer_triggers drop constraint if exists whisperer_triggers_suggestion_outcome_chk;
--   drop index if exists idx_wt_outcome_created;
--   alter table public.whisperer_triggers
--     drop column if exists suggestion_outcome,
--     drop column if exists suggestion_outcome_at,
--     drop column if exists suggestion_outcome_by,
--     drop column if exists suggestion_feedback;

alter table public.whisperer_triggers
  add column if not exists suggestion_outcome text null,
  add column if not exists suggestion_outcome_at timestamptz null,
  add column if not exists suggestion_outcome_by uuid null,
  add column if not exists suggestion_feedback text null;

-- Restrict to the three known outcomes (NULL allowed = unrated).
do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'whisperer_triggers_suggestion_outcome_chk'
  ) then
    alter table public.whisperer_triggers
      add constraint whisperer_triggers_suggestion_outcome_chk
      check (suggestion_outcome is null
             or suggestion_outcome in ('used', 'ignored', 'not_relevant'));
  end if;
end $$;

create index if not exists idx_wt_outcome_created
  on public.whisperer_triggers(suggestion_outcome, created_at desc);
