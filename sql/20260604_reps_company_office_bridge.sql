-- 20260604_reps_company_office_bridge.sql
-- Phase 1 identity bridge: add company_id + office_id to reps so reps becomes
-- the canonical identity table and company-level scoping no longer requires users.
--
-- Safe to run multiple times (IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- Run in Supabase SQL editor before running: npm run db:backfill

-- 1. Extend reps schema
ALTER TABLE public.reps
  ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES public.companies(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS office_id  UUID REFERENCES public.offices(id)   ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_reps_company_id ON public.reps(company_id);
CREATE INDEX IF NOT EXISTS idx_reps_office_id  ON public.reps(office_id);

-- 2. Backfill from users where IDs overlap
UPDATE public.reps r
SET
  company_id = u.company_id,
  office_id  = u.office_id
FROM public.users u
WHERE r.id = u.id
  AND u.company_id IS NOT NULL
  AND r.company_id IS NULL;

-- 3. Any rep still without a company_id → assign to Gravix Test Company.
--    Covers all platform/dev reps with no matching users row (Rep, You, etc.).
--    No rep should leave this migration with a NULL company_id.
UPDATE public.reps
SET company_id = 'c1c17223-aa46-4998-8995-de6bf25a23e6'
WHERE company_id IS NULL;
