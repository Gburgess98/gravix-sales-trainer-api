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

create or replace function public.set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_admin_config_updated_at on public.admin_config;
create trigger trg_admin_config_updated_at
before update on public.admin_config
for each row execute function public.set_updated_at();