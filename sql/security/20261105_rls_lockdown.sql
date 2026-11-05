-- 20261105_rls_lockdown.sql — close direct database access for the public Supabase roles.
--
-- WHY: every public table had full privileges (select/insert/update/delete/truncate) for `anon` and `authenticated`
-- (Supabase default privileges) and ~49 of 72 tables had RLS disabled, so anyone holding the public anon key could read
-- (and write) tenant data through PostgREST/GraphQL. The API uses the service role (BYPASSRLS), and the web app queries
-- exactly one table directly: the proxy's own-row `reps` read to find the signed-in user's organisation.
--
-- WHAT:
--   1. Enable RLS on every public table (not FORCED: service_role/owner paths and SECURITY DEFINER functions are unchanged).
--   2. Revoke ALL privileges on public tables, sequences and functions from anon, authenticated and PUBLIC.
--   3. Re-assert service_role privileges so the backend keeps working.
--   4. Default privileges: new public tables/sequences/functions are no longer open to anon/authenticated/PUBLIC.
--   5. The ONE allowed direct read: authenticated may SELECT (id, org_id, company_id) of their OWN reps row
--      (policy id = auth.uid()). No insert/update/delete anywhere → a client cannot choose or change its organisation.
--      A user with no reps row sees nothing.
--   6. Event trigger: any new public table is created with RLS on and no public grants (best effort; needs the privilege).
--
-- Idempotent (safe to run repeatedly), one transaction, no data changes. Rollback: the *_rollback_<env>.sql files.
-- Does NOT change the API's authorisation or its DEFAULT_ORG_ID fallback. Apply to staging first; production only after approval.

begin;
set local lock_timeout = '15s';

-- 1. RLS on every public table ---------------------------------------------------------------------------------------
do $$
declare r record;
begin
  for r in select c.relname from pg_class c where c.relnamespace = 'public'::regnamespace and c.relkind in ('r', 'p') loop
    execute format('alter table public.%I enable row level security', r.relname);
  end loop;
end $$;

-- 2. Nothing for anon / authenticated / PUBLIC -----------------------------------------------------------------------
revoke all on all tables    in schema public from anon, authenticated, public;
revoke all on all sequences in schema public from anon, authenticated, public;
revoke all on all functions in schema public from anon, authenticated, public;

-- 3. The backend (service role) keeps full access ---------------------------------------------------------------------
grant usage on schema public to service_role;
grant all on all tables    in schema public to service_role;
grant all on all sequences in schema public to service_role;
grant all on all functions in schema public to service_role;

-- 4. New objects are born closed ------------------------------------------------------------------------------------
alter default privileges for role postgres in schema public revoke all on tables    from anon, authenticated, public;
alter default privileges for role postgres in schema public revoke all on sequences from anon, authenticated, public;
alter default privileges for role postgres in schema public revoke all on functions from anon, authenticated, public;
do $$
begin
  -- Supabase also seeds defaults for objects created by supabase_admin (dashboard). Needs that role's privilege.
  alter default privileges for role supabase_admin in schema public revoke all on tables    from anon, authenticated, public;
  alter default privileges for role supabase_admin in schema public revoke all on sequences from anon, authenticated, public;
  alter default privileges for role supabase_admin in schema public revoke all on functions from anon, authenticated, public;
exception when insufficient_privilege then
  raise notice 'could not alter supabase_admin default privileges (insufficient privilege); the event trigger / posture check cover new tables';
end $$;
grant all on all tables in schema public to service_role;  -- default-ACL revokes above never touch service_role; explicit for clarity

-- 5. The single permitted direct read: your own membership row -------------------------------------------------------
grant select (id, org_id, company_id) on public.reps to authenticated;
drop policy if exists reps_select_self on public.reps;
create policy reps_select_self on public.reps for select to authenticated using (id = auth.uid());

-- 6. Guard future tables ---------------------------------------------------------------------------------------------
create or replace function public.gravix_secure_new_public_table() returns event_trigger
language plpgsql security definer set search_path = pg_catalog as $fn$
declare r record;
begin
  for r in select * from pg_event_trigger_ddl_commands() where object_type = 'table' and schema_name = 'public' loop
    execute format('alter table %s enable row level security', r.object_identity);
    execute format('revoke all on table %s from anon, authenticated, public', r.object_identity);
  end loop;
end
$fn$;
revoke all on function public.gravix_secure_new_public_table() from anon, authenticated, public;
do $$
begin
  drop event trigger if exists gravix_secure_new_public_table;
  create event trigger gravix_secure_new_public_table on ddl_command_end
    when tag in ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
    execute function public.gravix_secure_new_public_table();
exception when insufficient_privilege then
  raise notice 'event trigger not created (insufficient privilege); rely on scripts/check-rls-posture.ts';
end $$;

-- 7. Post-conditions (abort the whole transaction if the posture is not as intended) --------------------------------
do $$
declare bad text;
begin
  select string_agg(c.relname, ', ') into bad from pg_class c where c.relnamespace = 'public'::regnamespace and c.relkind in ('r','p') and not c.relrowsecurity;
  if bad is not null then raise exception 'post-check failed: RLS still off on %', bad; end if;

  select string_agg(c.relname || ':' || r.rolname, ', ') into bad
    from pg_class c cross join (select rolname from pg_roles where rolname in ('anon', 'authenticated')) r
   where c.relnamespace = 'public'::regnamespace and c.relkind in ('r','p','v','m')
     and has_table_privilege(r.rolname, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER');
  if bad is not null then raise exception 'post-check failed: table-level privileges remain: %', bad; end if;

  if not exists (select 1 from pg_roles where rolname = 'service_role' and rolbypassrls) then raise exception 'post-check failed: service_role lacks BYPASSRLS'; end if;
  if not has_table_privilege('service_role', 'public.reps', 'SELECT,INSERT,UPDATE,DELETE') then raise exception 'post-check failed: service_role lost access to reps'; end if;
  if has_column_privilege('authenticated', 'public.reps', 'email', 'SELECT') then raise exception 'post-check failed: authenticated can read reps.email'; end if;
  if has_table_privilege('authenticated', 'public.reps', 'UPDATE') or has_any_column_privilege('authenticated', 'public.reps', 'UPDATE') then raise exception 'post-check failed: authenticated can update reps'; end if;
end $$;

commit;
