-- 20260714_company_context.sql
-- Intelligence Layer — Day 218: Context Engine data foundation.
--
-- Manager-controlled company memory ("teach Gravix how your company sells"),
-- per CONTEXT_ENGINE_SPEC.md / CONTEXT_ENGINE_FIELD_SPEC.md (WEB repo docs).
-- Row-per-lifecycle-state model:
--   - at most ONE draft row per company (the manager's working copy);
--   - at most ONE published row per company (the only version runtime may
--     ever read — a half-finished draft can never leak into scoring);
--   - publishing archives the previous published row (history preserved,
--     nothing destructive) and inserts a new published snapshot with a
--     monotonically increasing version and a deterministic compiled block.
--
-- Scoring/runtime integration is deliberately NOT part of this migration —
-- nothing reads this table at score time yet (Day 218 is data layer only).
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists public.company_context;

create table if not exists public.company_context (
  id uuid primary key default gen_random_uuid(),

  company_id uuid not null,

  -- draft = manager working copy · published = live snapshot ·
  -- archived = superseded published version (kept for history)
  status text not null default 'draft',

  -- 0 on draft rows; publish stamps the next version (1, 2, 3…)
  version integer not null default 0,

  -- Section keys per CONTEXT_ENGINE_FIELD_SPEC.md:
  -- profile / offering / objections / competitors / compliance / tone
  context jsonb not null default '{}'::jsonb,

  -- Deterministic compiled text block, stamped at publish time so the
  -- published snapshot is stable even if the compiler evolves later.
  compiled_context text null,

  created_by uuid null,
  updated_by uuid null,
  published_by uuid null,
  published_at timestamptz null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  archived_at timestamptz null,

  constraint company_context_status_chk
    check (status in ('draft', 'published', 'archived'))
);

-- One working draft and one live published version per company.
create unique index if not exists uq_company_context_one_draft
  on public.company_context (company_id) where status = 'draft';
create unique index if not exists uq_company_context_one_published
  on public.company_context (company_id) where status = 'published';

create index if not exists idx_company_context_company_version
  on public.company_context (company_id, version desc);
create index if not exists idx_company_context_status
  on public.company_context (status, updated_at desc);
