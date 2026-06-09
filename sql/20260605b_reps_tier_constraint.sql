-- 20260605b_reps_tier_constraint.sql
-- Extend reps.tier CHECK constraint to include PartnerAdmin and SuperAdmin.
-- Run in Supabase SQL editor (standalone — does not require the partner table).

ALTER TABLE public.reps DROP CONSTRAINT IF EXISTS reps_tier_check;

ALTER TABLE public.reps
  ADD CONSTRAINT reps_tier_check
  CHECK (tier IN ('SalesRep', 'TeamLead', 'Manager', 'Owner', 'PartnerAdmin', 'SuperAdmin'));
