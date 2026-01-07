-- 20251223_assignments.sql
-- Manager → Rep assignments (MVP)

create table if not exists public.assignments (
  id uuid primary key default gen_random_uuid(),

  rep_id uuid not null,
  manager_id uuid not null,

  type text not null check (type in ('call_review','sparring','custom')),
  target_id uuid null,

  title text not null default '',
  status text not null default 'assigned' check (status in ('assigned','completed','missed')),

  due_at timestamptz null,
  created_at timestamptz not null default now(),
  completed_at timestamptz null
);

create index if not exists idx_assignments_rep_id on public.assignments(rep_id);
create index if not exists idx_assignments_manager_id on public.assignments(manager_id);
create index if not exists idx_assignments_status on public.assignments(status);
create index if not exists idx_assignments_due_at on public.assignments(due_at);