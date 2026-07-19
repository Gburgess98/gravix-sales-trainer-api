-- 20260719_objection_library.sql
-- Intelligence Layer — Day 236: Objection Library data foundation.
--
-- Third Intelligence pillar (OBJECTION_LIBRARY_BLUEPRINT.md +
-- OBJECTION_LIBRARY_FIELD_SPEC.md, WEB repo docs): manager-approved
-- guidance for how the company handles buyer pushback — the buyer's
-- phrases, the approved response, the weak patterns to coach away, and
-- the no-go language. Day 236 is data layer + CRUD/lifecycle API only:
-- no WEB UI, no scoring/Whisperer/sparring runtime integration, no
-- suggestion mining. Nothing in any runtime reads these tables yet.
--
-- Shape:
--   objection_library_items          the library entries (draft/approved/archived)
--   objection_evidence               call/phrase evidence linked to an item
--   objection_suggestion_decisions   persisted manager decisions on mined
--                                    suggestions (the trigger-candidate
--                                    decisions pattern; mining endpoint is a
--                                    later lane — table ships now so the
--                                    decision model is fixed early)
--
-- Lifecycle rules (enforced at the API):
--   draft → approved (manager, requires the approval field set) → archived.
--   Approved items are immutable in Day 236 (409 immutable_approved — a
--   revision model is a later lane). Archive marks, never deletes; there is
--   no hard-delete path anywhere.
--
-- Apply: paste into the Supabase SQL editor (standard workflow for this repo).
-- Safe to re-run — IF NOT EXISTS guards throughout.
--
-- Rollback:
--   drop table if exists public.objection_suggestion_decisions;
--   drop table if exists public.objection_evidence;
--   drop table if exists public.objection_library_items;

create table if not exists public.objection_library_items (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null,

  label text not null,
  category text not null default 'other',
  status text not null default 'draft',

  buyer_phrases text[] not null default '{}',
  why_it_matters text null,
  approved_response text null,
  weak_response_patterns text[] not null default '{}',
  no_go_language text[] not null default '{}',
  coaching_note text null,

  -- Reference-only links (no behavioural coupling in MVP).
  linked_scorecard_criterion_id uuid null,
  linked_trigger_key text null,

  created_by uuid null,
  updated_by uuid null,
  approved_by uuid null,
  approved_at timestamptz null,
  archived_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint objection_items_status_chk
    check (status in ('draft', 'approved', 'archived')),
  constraint objection_items_category_chk
    check (category in (
      'price', 'timing', 'authority', 'trust',
      'competitor', 'fit', 'logistics', 'other'
    ))
);

-- Label unique per company (case-insensitive) among LIVE items only —
-- archiving frees the label for a fresh item without losing history.
create unique index if not exists uq_objection_items_company_label_live
  on public.objection_library_items (company_id, lower(label))
  where status <> 'archived';

create index if not exists idx_objection_items_company_status
  on public.objection_library_items (company_id, status);

create table if not exists public.objection_evidence (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null,
  objection_id uuid not null references public.objection_library_items(id),

  call_id uuid null,
  rep_id uuid null,
  phrase text null,
  source text not null default 'manual',
  confidence numeric null,
  occurred_at timestamptz null,
  created_at timestamptz not null default now(),

  constraint objection_evidence_source_chk
    check (source in ('manual', 'suggestion', 'moment_match'))
);

create index if not exists idx_objection_evidence_company_item
  on public.objection_evidence (company_id, objection_id);

create table if not exists public.objection_suggestion_decisions (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null,

  suggestion_key text not null,
  decision text not null,
  objection_id uuid null references public.objection_library_items(id),
  decided_by uuid null,
  decided_at timestamptz not null default now(),
  reason text null,

  constraint objection_suggestion_decisions_chk
    check (decision in ('approved', 'merged', 'dismissed'))
);

create index if not exists idx_objection_decisions_company_key
  on public.objection_suggestion_decisions (company_id, suggestion_key);
