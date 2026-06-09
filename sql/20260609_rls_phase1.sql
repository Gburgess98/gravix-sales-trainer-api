-- 20260609_rls_phase1.sql
-- RLS Phase 1: Enable Row Level Security on CRM, coaching, and assignment tables.
--
-- WHAT THIS DOES
-- ==============
-- Runs ALTER TABLE ... ENABLE ROW LEVEL SECURITY on each target table.
-- No policies are added (Phase 2 task).
--
-- WHAT IT DOES NOT DO
-- ===================
-- Does NOT add FORCE ROW LEVEL SECURITY — the service_role bypasses RLS by
-- design, so all existing API queries are completely unaffected.
-- Does NOT change any application code.
-- Does NOT restrict the service_role client in any way.
--
-- EFFECT
-- ======
-- Anon-key clients (Supabase Dashboard table editor, direct client-side queries)
-- will now receive 0 rows / permission denied on these tables because no policy
-- allows access. Service-role clients (the entire API) are unaffected.
--
-- Safe to run multiple times (IF NOT EXISTS equivalent via ENABLE is idempotent).
--
-- Run in: Supabase SQL editor (project dashboard → SQL Editor → New query)

-- ── CRM tables ────────────────────────────────────────────────────────────────

ALTER TABLE IF EXISTS public.crm_contacts         ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_opportunities    ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_activities       ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_accounts         ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_actions          ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_contact_notes    ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_call_links       ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_demo_call_links  ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.crm_auto_assign_runs ENABLE ROW LEVEL SECURITY;

-- ── Coaching tables ───────────────────────────────────────────────────────────

ALTER TABLE IF EXISTS public.coach_assignments    ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.coach_notes          ENABLE ROW LEVEL SECURITY;

-- ── Assignment tables ─────────────────────────────────────────────────────────

ALTER TABLE IF EXISTS public.assignments          ENABLE ROW LEVEL SECURITY;

-- ── Verification query ────────────────────────────────────────────────────────
-- Run this after applying the migration to confirm all tables have RLS enabled.
-- Expected: every row has relrowsecurity = true.

SELECT
  c.relname   AS table_name,
  c.relrowsecurity   AS rls_enabled,
  c.relforcerowsecurity AS rls_forced
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND c.relname IN (
    'crm_contacts',
    'crm_opportunities',
    'crm_activities',
    'crm_accounts',
    'crm_actions',
    'crm_contact_notes',
    'crm_call_links',
    'crm_demo_call_links',
    'crm_auto_assign_runs',
    'coach_assignments',
    'coach_notes',
    'assignments'
  )
ORDER BY c.relname;
