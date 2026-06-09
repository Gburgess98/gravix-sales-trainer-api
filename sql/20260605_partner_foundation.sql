-- 20260605_partner_foundation.sql
-- Phase 1: Partner layer foundation.
-- Creates the partners table, links it to companies, backfills existing data.
--
-- Design notes:
--   - Both existing companies share tmc_id = 5055e1b6-fb33-45c0-959a-d7dd45f98a13
--     which is the proto-partner concept sketched in the original schema.
--     We formalise it as the "Gravix" partner and use that UUID as partner.id
--     so no FK references need updating.
--   - companies.partner_id is nullable for forward-compat (new companies start
--     without a partner until explicitly assigned).
--   - Safe to run multiple times (IF NOT EXISTS / ON CONFLICT DO NOTHING).
--
-- Run in Supabase SQL editor. No API downtime required.

-- ── 1. Create partners table ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.partners (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'active',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_partners_slug ON public.partners(slug);
CREATE INDEX        IF NOT EXISTS idx_partners_status ON public.partners(status);

-- ── 2. Add partner_id to companies ──────────────────────────────────────────

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS partner_id UUID REFERENCES public.partners(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_companies_partner_id ON public.companies(partner_id);

-- ── 3. Backfill: create "Gravix" partner from the existing tmc_id ────────────
-- tmc_id 5055e1b6-fb33-45c0-959a-d7dd45f98a13 is already on both companies;
-- using it as the partner.id keeps the FK plumbing clean with no data migration.

INSERT INTO public.partners (id, name, slug, status)
VALUES (
  '5055e1b6-fb33-45c0-959a-d7dd45f98a13',
  'Gravix',
  'gravix',
  'active'
)
ON CONFLICT (id) DO NOTHING;

-- ── 4. Attach existing companies to Gravix ───────────────────────────────────

UPDATE public.companies
SET partner_id = tmc_id
WHERE tmc_id IS NOT NULL
  AND partner_id IS NULL;

-- ── 5. Extend reps tier CHECK constraint ────────────────────────────────────
-- Current allowed values: SalesRep, Manager, Owner, TeamLead
-- Adding: PartnerAdmin, SuperAdmin
-- Must drop and recreate — Postgres does not support ALTER CONSTRAINT.

ALTER TABLE public.reps DROP CONSTRAINT IF EXISTS reps_tier_check;

ALTER TABLE public.reps
  ADD CONSTRAINT reps_tier_check
  CHECK (tier IN ('SalesRep', 'TeamLead', 'Manager', 'Owner', 'PartnerAdmin', 'SuperAdmin'));
