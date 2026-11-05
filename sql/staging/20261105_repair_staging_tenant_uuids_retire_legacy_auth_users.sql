-- 20261105_repair_staging_tenant_uuids_retire_legacy_auth_users.sql   (GENERATED — OPTIONAL, separate approval)
--
-- STAGING ONLY — never apply to production. The two hand-written auth users from before Day 273 (ids 00000000-2711-0000-0000-00000000000a/b) have
-- malformed ids that no API predicate accepts, and Auth cannot change a user's id. They cannot sign in usefully. This
-- removes them (public.profiles cascades via profiles_user_id_fkey) so no malformed id remains.
-- Run ONLY after 20261105_repair_staging_tenant_uuids.sql. Not reversible (passwords are not recoverable); the working QA logins are untouched.
begin;
do $$
begin
  if exists (select 1 from auth.users where id in ('00000000-2711-0000-0000-00000000000a', '00000000-2711-0000-0000-00000000000b') and email not like '%@gravix.invalid') then
    raise exception 'refusing: a legacy id belongs to a non-synthetic email';
  end if;
end $$;
delete from public.profiles where user_id in ('00000000-2711-0000-0000-00000000000a', '00000000-2711-0000-0000-00000000000b');
delete from auth.users where id in ('00000000-2711-0000-0000-00000000000a', '00000000-2711-0000-0000-00000000000b') and email like '%@gravix.invalid';
commit;
