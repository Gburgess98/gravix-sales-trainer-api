-- 20260526_assignments_add_meta.sql
-- Add meta jsonb column omitted from original 20251223_assignments migration.
-- Safe to re-run — IF NOT EXISTS guards the add.

alter table public.assignments
  add column if not exists meta jsonb;
