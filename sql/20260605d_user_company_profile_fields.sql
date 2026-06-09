-- =============================================================================
-- 20260605d_user_company_profile_fields.sql
-- Sprint 3 — User & Company Management Foundation
--
-- Adds profile fields to reps and companies tables.
-- Required by: /settings/profile, /admin/users/[id], /admin/companies/[id]
--
-- Safe: all statements use IF NOT EXISTS / default guards.
-- Idempotent: safe to run more than once.
-- No data loss: additive only.
-- Run in: Supabase SQL editor (service role).
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. REPS — profile fields
-- ─────────────────────────────────────────────────────────────────────────────

-- Identity
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS display_name TEXT;
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS first_name   TEXT;
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS last_name    TEXT;
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS email        TEXT;
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS avatar_url   TEXT;

-- Contact
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS phone_number TEXT;

-- Role / org structure
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS job_title  TEXT;
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS department TEXT;

-- Self-referential manager link.
-- No FK constraint — avoids circular dependency issues with IF NOT EXISTS.
-- Referential integrity enforced at the application layer.
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS manager_id UUID;

-- Localisation
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC';

-- Status
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true;

-- Audit timestamp
ALTER TABLE public.reps ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. REPS — backfill from existing data
-- ─────────────────────────────────────────────────────────────────────────────

-- Carry existing name into display_name so the profile page is never blank.
UPDATE public.reps
SET    display_name = name
WHERE  display_name IS NULL
  AND  name IS NOT NULL;

-- Pull email from profiles where available.
UPDATE public.reps r
SET    email = p.email
FROM   public.profiles p
WHERE  p.user_id = r.id
  AND  r.email IS NULL
  AND  p.email IS NOT NULL;

-- Seed manager_id from profiles.manager_id where available.
UPDATE public.reps r
SET    manager_id = p.manager_id
FROM   public.profiles p
WHERE  p.user_id      = r.id
  AND  r.manager_id   IS NULL
  AND  p.manager_id   IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. REPS — indexes
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_reps_manager_id      ON public.reps(manager_id);
CREATE INDEX IF NOT EXISTS idx_reps_is_active       ON public.reps(is_active);
CREATE INDEX IF NOT EXISTS idx_reps_company_active  ON public.reps(company_id, is_active);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. COMPANIES — profile fields
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS website      TEXT;
ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS industry     TEXT;
ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS phone_number TEXT;
ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS address      TEXT;
ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS is_active    BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE public.companies ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ DEFAULT now();

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. COMPANIES — indexes
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_companies_is_active ON public.companies(is_active);

-- ─────────────────────────────────────────────────────────────────────────────
-- Verification query (run after migration to confirm columns exist)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- SELECT column_name, data_type, column_default
-- FROM   information_schema.columns
-- WHERE  table_schema = 'public'
--   AND  table_name   IN ('reps', 'companies')
--   AND  column_name  IN (
--          'display_name','first_name','last_name','email','phone_number',
--          'job_title','department','manager_id','timezone','is_active',
--          'avatar_url','updated_at',
--          'website','industry','address'
--        )
-- ORDER BY table_name, column_name;
