-- 20260603_fix_auth_user_trigger.sql
--
-- PROBLEM
-- auth.admin.createUser() fails with "Database error saving new user" because
-- the handle_new_user trigger on auth.users attempts to INSERT into public.users,
-- which has NOT NULL constraints on office_id and org_id that the trigger cannot
-- satisfy for programmatically-created users.
--
-- FIX
-- Make office_id and org_id nullable on public.users.
-- Existing rows are unaffected. New auth-created users start with null values
-- for these fields and can be updated later via the admin API.
--
-- Run this once in the Supabase SQL editor before running: npm run seed:demo

ALTER TABLE public.users
  ALTER COLUMN office_id DROP NOT NULL,
  ALTER COLUMN org_id    DROP NOT NULL;
