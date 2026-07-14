-- 20260714b_scorecard_studio.sql
-- Intelligence Layer — Day 219B: Scorecard Studio data foundation.
--
-- Managers define what a good call looks like per call type, WITHIN the
-- fixed four-stage scoring frame (intro/discovery/objection/close) that the
-- scoring runtime and every WEB consumer render — per SCORECARD_STUDIO_SPEC.md
-- (WEB repo docs). Custom stage structures are phase 2, deliberately.
--
-- Shape:
--   scorecards            named container per company (draft/active/archived)
--   scorecard_versions    numbered versions; draft is editable, an activated
--                         version is IMMUTABLE and carries a deterministic
--                         jsonb snapshot; the previous active → superseded
--   scorecard_stage_weights / scorecard_criteria
--                         relational working rows per version (draft editing
--                         + phase-2 per-criterion analytics); the activation
--                         snapshot is what the runtime will read later
--
-- Weights sum-to-100, one-active-per-call-type and one-company-default are
-- enforced at API activation time (SQL keeps the structural rules only).
-- Nothing in the scoring runtime reads these tables yet — Day 219B is data
-- layer only; scoring integration is a later, explicit step.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists public.scorecard_criteria;
--   drop table if exists public.scorecard_stage_weights;
--   drop table if exists public.scorecard_versions;
--   drop table if exists public.scorecards;

create table if not exists public.scorecards (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null,

  name text not null,
  description text null,

  status text not null default 'draft',
  is_company_default boolean not null default false,

  created_by uuid null,
  updated_by uuid null,
  archived_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint scorecards_status_chk
    check (status in ('draft', 'active', 'archived'))
);

-- Name unique per company (case-insensitive) — field spec rule 1.
create unique index if not exists uq_scorecards_company_name
  on public.scorecards (company_id, lower(name));

-- At most one ACTIVE company default (field spec rule 5).
create unique index if not exists uq_scorecards_company_default
  on public.scorecards (company_id)
  where is_company_default and status = 'active';

create index if not exists idx_scorecards_company_status
  on public.scorecards (company_id, status);

create table if not exists public.scorecard_versions (
  id uuid primary key default gen_random_uuid(),
  scorecard_id uuid not null references public.scorecards(id),
  company_id uuid not null,

  version integer not null,
  status text not null default 'draft',

  -- Fixed call-type keys (SCORECARD_STUDIO_FIELD_SPEC.md §1); empty array =
  -- relies on the scorecard-level company-default toggle.
  call_types text[] not null default '{}',

  origin text not null default 'manual',
  activation_note text null,
  activated_by uuid null,
  activated_at timestamptz null,

  -- Deterministic immutable snapshot, stamped at activation (empty on drafts).
  snapshot jsonb not null default '{}'::jsonb,

  created_by uuid null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint scorecard_versions_status_chk
    check (status in ('draft', 'active', 'superseded')),
  constraint scorecard_versions_origin_chk
    check (origin in ('manual', 'ai_draft', 'import', 'duplicate', 'default'))
);

create unique index if not exists uq_scorecard_versions_number
  on public.scorecard_versions (scorecard_id, version);

-- One editable draft and one active version per scorecard.
create unique index if not exists uq_scorecard_versions_one_draft
  on public.scorecard_versions (scorecard_id) where status = 'draft';
create unique index if not exists uq_scorecard_versions_one_active
  on public.scorecard_versions (scorecard_id) where status = 'active';

create index if not exists idx_scorecard_versions_company_status
  on public.scorecard_versions (company_id, status);

create table if not exists public.scorecard_stage_weights (
  id uuid primary key default gen_random_uuid(),
  scorecard_version_id uuid not null references public.scorecard_versions(id),

  stage text not null,
  weight integer not null,
  guidance text null,

  created_at timestamptz not null default now(),

  constraint scorecard_stage_weights_stage_chk
    check (stage in ('intro', 'discovery', 'objection', 'close')),
  constraint scorecard_stage_weights_range_chk
    check (weight >= 0 and weight <= 100)
);

create unique index if not exists uq_scorecard_stage_weights_stage
  on public.scorecard_stage_weights (scorecard_version_id, stage);

create table if not exists public.scorecard_criteria (
  id uuid primary key default gen_random_uuid(),
  scorecard_version_id uuid not null references public.scorecard_versions(id),

  stage text not null,
  label text not null,
  description text null,
  scoring_guidance text null,
  good_example text null,
  weak_example text null,
  coaching_prompt text null,

  pass_fail boolean not null default false,
  critical boolean not null default false,
  emphasis text not null default 'standard',
  sort_order integer not null default 0,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint scorecard_criteria_stage_chk
    check (stage in ('intro', 'discovery', 'objection', 'close')),
  constraint scorecard_criteria_emphasis_chk
    check (emphasis in ('minor', 'standard', 'major')),
  -- Critical is a pass/fail refinement — field spec rule 6.
  constraint scorecard_criteria_critical_chk
    check (not critical or pass_fail)
);

create index if not exists idx_scorecard_criteria_version_stage
  on public.scorecard_criteria (scorecard_version_id, stage, sort_order);
