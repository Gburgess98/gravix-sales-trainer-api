-- 20260617_whisperer_trigger_candidate_decisions.sql
-- TIER 2B — Day 133: persistent lifecycle for AI Trigger Discovery candidates.
--
-- Managers can dismiss / reject / approve discovery candidates and those
-- decisions survive refreshes (closing the Day 132 "reappear after refresh"
-- caveat). A decision NEVER creates or enables a trigger — activation still
-- happens through the Custom Trigger Library save flow. Read paths stay
-- fail-soft so the candidate endpoint keeps working until this is applied.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists whisperer_trigger_candidate_decisions;

create table if not exists public.whisperer_trigger_candidate_decisions (
  id uuid primary key default gen_random_uuid(),

  org_id uuid null,
  company_id uuid null,
  office_id uuid null,

  candidate_id text not null,
  candidate_type text null,

  decision text not null,
  decided_by uuid null,
  note text null,

  source jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint whisperer_tcd_decision_chk
    check (decision in ('approved', 'dismissed', 'rejected'))
);

-- One live decision per candidate per tenant scope. NULLS NOT DISTINCT treats
-- null scope columns as equal so the unique key holds for partially-stamped
-- rows; code upserts on this key to keep the latest decision.
create unique index if not exists uq_wtcd_scope_candidate
  on public.whisperer_trigger_candidate_decisions (org_id, company_id, office_id, candidate_id)
  nulls not distinct;

create index if not exists idx_wtcd_org_candidate on public.whisperer_trigger_candidate_decisions(org_id, candidate_id);
create index if not exists idx_wtcd_company_candidate on public.whisperer_trigger_candidate_decisions(company_id, candidate_id);
create index if not exists idx_wtcd_office_candidate on public.whisperer_trigger_candidate_decisions(office_id, candidate_id);
create index if not exists idx_wtcd_decision_created on public.whisperer_trigger_candidate_decisions(decision, created_at desc);
create index if not exists idx_wtcd_decided_by on public.whisperer_trigger_candidate_decisions(decided_by, created_at desc);
