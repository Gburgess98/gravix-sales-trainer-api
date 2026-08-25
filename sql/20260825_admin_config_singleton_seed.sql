-- Day 294 — restore the canonical admin_config singleton row.
--
-- The table is defined in sql/20251218_admin_config.sql as a boolean-PK singleton
-- (id=true) with database defaults for streak_threshold / xp_multiplier /
-- comeback_bonus / updated_at, and that migration also seeds id=true. A staging
-- environment ended up with the table present but the seed row absent (0 rows),
-- so GET admin_config `.eq("id", true).single()` returns "no rows" and the
-- scoring-settings section reports "Couldn't load scoring settings."
--
-- This migration idempotently restores exactly one canonical row using database
-- defaults. `create table if not exists` makes it self-sufficient on a partially
-- migrated environment; `on conflict (id) do nothing` guarantees it NEVER
-- overwrites existing values. It touches no other table and no production data.

create table if not exists public.admin_config (
  id boolean primary key default true,
  streak_threshold integer not null default 3,
  xp_multiplier numeric not null default 1.0,
  comeback_bonus integer not null default 50,
  updated_at timestamptz not null default now()
);

insert into public.admin_config (id)
values (true)
on conflict (id) do nothing;
