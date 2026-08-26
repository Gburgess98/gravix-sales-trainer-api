-- 20260826_admin_config_score_thresholds.sql
--
-- Day 297 — persist the low/critical score thresholds on the admin_config
-- singleton. The scoring runtime (getScoringThresholds) and PATCH /v1/admin/config
-- already reference `low_score_threshold` and `critical_score_threshold`, but the
-- columns never existed: reads silently fell back to hard-coded {low:65,
-- critical:45} and PATCH values were dropped (responses reported nulls). This adds
-- the two columns as a truthful persisted contract.
--
--   * integer, NOT NULL, defaults matching the runtime fallback (low=65,
--     critical=45) so existing rows and future inserts keep today's behaviour.
--   * range checks 0..100 on each.
--   * invariant: critical_score_threshold <= low_score_threshold.
--
-- Additive + idempotent. No destructive statements; no unrelated table changes;
-- existing singleton values are preserved (the new columns fill with their
-- defaults, and 45 <= 65 satisfies the invariant for the pre-existing row).

alter table public.admin_config
  add column if not exists low_score_threshold integer not null default 65;

alter table public.admin_config
  add column if not exists critical_score_threshold integer not null default 45;

-- Range + ordering constraints, added separately + guarded so re-runs don't
-- attempt duplicate constraints.
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'admin_config_low_score_threshold_range'
  ) then
    alter table public.admin_config
      add constraint admin_config_low_score_threshold_range
      check (low_score_threshold between 0 and 100);
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'admin_config_critical_score_threshold_range'
  ) then
    alter table public.admin_config
      add constraint admin_config_critical_score_threshold_range
      check (critical_score_threshold between 0 and 100);
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'admin_config_threshold_order'
  ) then
    alter table public.admin_config
      add constraint admin_config_threshold_order
      check (critical_score_threshold <= low_score_threshold);
  end if;
end $$;
