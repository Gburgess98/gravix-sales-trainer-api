-- api/sql/20251106_add_total_score.sql
-- Purpose: add total_score to sparring_sessions and a useful index.
-- Safe to run multiple times.

BEGIN;

-- 1) Add column if missing
ALTER TABLE IF EXISTS public.sparring_sessions
  ADD COLUMN IF NOT EXISTS total_score numeric;

-- 2) (Optional) Index to speed up recent-by-rep lookups
CREATE INDEX IF NOT EXISTS idx_sparring_sessions_rep_created
  ON public.sparring_sessions (rep_id, created_at DESC);

COMMIT;
