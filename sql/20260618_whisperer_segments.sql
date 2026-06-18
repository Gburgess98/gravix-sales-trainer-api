-- 20260618_whisperer_segments.sql
-- TIER 2B — Day 142: raw Whisperer transcript segment storage.
--
-- Today only segments that FIRE a trigger persist (whisperer_triggers.segment_text),
-- so discovery can't see blind spots — missed objections, untriggered competitor
-- mentions, new buyer language. This table stores EVERY final transcript segment
-- (triggered or not) so Day 143 discovery can mine untriggered buyer language.
--
-- Privacy/retention: stores transcript TEXT ONLY — never audio, never audio
-- features. Tenant-scoped (org/company/office). Reuse the planned
-- WHISPERER_RETENTION_DAYS (default 90) purge window later. No LLM touches this
-- table — it is a plain insert on the live path.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists public.whisperer_segments;

create table if not exists public.whisperer_segments (
  id uuid primary key default gen_random_uuid(),

  session_id uuid not null references public.whisperer_sessions(id) on delete cascade,
  call_id uuid null,
  user_id uuid null,
  org_id uuid null,
  company_id uuid null,
  office_id uuid null,

  speaker text not null default 'unknown',
  speaker_original text null,
  speaker_role text null,

  text text not null,
  text_normalised text null,

  confidence numeric null,
  is_final boolean not null default true,
  source text not null default 'live',

  started_at_ms integer null,
  ended_at_ms integer null,
  client_sent_at timestamptz null,
  received_at timestamptz not null default now(),
  processed_at timestamptz null,

  triggers_count integer not null default 0,
  trigger_types text[] not null default '{}',

  meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),

  constraint whisperer_segments_source_chk
    check (source in ('live', 'manual', 'simulator')),
  constraint whisperer_segments_text_chk
    check (length(btrim(text)) > 0)
);

create index if not exists idx_wseg_session_created on public.whisperer_segments(session_id, created_at desc);
create index if not exists idx_wseg_call_created on public.whisperer_segments(call_id, created_at desc);
create index if not exists idx_wseg_user_created on public.whisperer_segments(user_id, created_at desc);
create index if not exists idx_wseg_org_created on public.whisperer_segments(org_id, created_at desc);
create index if not exists idx_wseg_company_created on public.whisperer_segments(company_id, created_at desc);
create index if not exists idx_wseg_office_created on public.whisperer_segments(office_id, created_at desc);
create index if not exists idx_wseg_speaker_created on public.whisperer_segments(speaker, created_at desc);
create index if not exists idx_wseg_trigger_types on public.whisperer_segments using gin (trigger_types);
