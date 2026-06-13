-- 20260612_whisperer_stub_loop.sql
-- TIER 2B — Day 111: Live Whisperer foundation (transcript stub loop).
--
-- Two tables: sessions + triggers. Tenant columns from day one (Tier 2A
-- lesson). Segments stay in the response/UI for the stub phase; a segments
-- table arrives with Deepgram (Day 112/113) when volume justifies it.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Privacy/retention: segment_text on triggers contains live-call speech.
-- Plan: configurable purge window (WHISPERER_RETENTION_DAYS, default 90)
-- nulls segment_text/phrase while keeping counts — job lands later in Tier 2B.
--
-- Rollback:
--   drop table if exists whisperer_triggers;
--   drop table if exists whisperer_sessions;

create table if not exists public.whisperer_sessions (
  id uuid primary key default gen_random_uuid(),

  rep_id uuid null,
  user_id uuid null,
  org_id uuid null,
  company_id uuid null,
  office_id uuid null,
  call_id uuid null,

  status text not null default 'active',
  started_at timestamptz not null default now(),
  ended_at timestamptz null,

  latency_p50_ms integer null,
  latency_p95_ms integer null,

  meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_ws_user_created on public.whisperer_sessions(user_id, created_at desc);
create index if not exists idx_ws_rep_created on public.whisperer_sessions(rep_id, created_at desc);
create index if not exists idx_ws_org_created on public.whisperer_sessions(org_id, created_at desc);
create index if not exists idx_ws_company_created on public.whisperer_sessions(company_id, created_at desc);
create index if not exists idx_ws_office_created on public.whisperer_sessions(office_id, created_at desc);
create index if not exists idx_ws_status_created on public.whisperer_sessions(status, created_at desc);
create index if not exists idx_ws_call_id on public.whisperer_sessions(call_id);

create table if not exists public.whisperer_triggers (
  id uuid primary key default gen_random_uuid(),

  session_id uuid not null references public.whisperer_sessions(id) on delete cascade,
  segment_text text not null,

  type text not null,
  phrase text null,
  confidence integer not null default 80,
  suggestion jsonb not null default '{}'::jsonb,

  latency_ms integer null,
  detected_at timestamptz not null default now(),

  meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_wt_session_created on public.whisperer_triggers(session_id, created_at desc);
create index if not exists idx_wt_type_created on public.whisperer_triggers(type, created_at desc);
