-- DAY 79.5 — Add flag columns to crm_activities
--
-- scoring.ts writes flag_key / flag_section / flag_severity / flag_category
-- as top-level columns (see scoring.ts ~line 2000 direct insert).
-- These columns were never migrated, so those inserts were silently discarded.
-- Existing rows already carry this data inside the meta JSONB field
-- (written by logReviewFlag in crmActivities.ts).
--
-- Safe to run multiple times (IF NOT EXISTS).

ALTER TABLE crm_activities
  ADD COLUMN IF NOT EXISTS flag_key      text,
  ADD COLUMN IF NOT EXISTS flag_section  text,
  ADD COLUMN IF NOT EXISTS flag_severity text,
  ADD COLUMN IF NOT EXISTS flag_category text;
