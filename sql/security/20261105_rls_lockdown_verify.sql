-- 20261105_rls_lockdown_verify.sql — READ-ONLY posture check. Returns one row per violation; ZERO rows = pass.
-- Intended access model: public tables are reachable ONLY by the service role (the API). The single exception is
-- authenticated SELECT of (id, org_id, company_id) on public.reps, limited by policy to the caller's own row.
with tbl as (
  select c.oid, c.relname, c.relkind, c.relrowsecurity rls from pg_class c where c.relnamespace = 'public'::regnamespace and c.relkind in ('r','p','v','m')
), roles as (select unnest(array['anon','authenticated']) r)
select 'rls_disabled' as violation, relname as object, '' as detail from tbl where relkind in ('r','p') and not rls
union all
select 'table_privilege', t.relname, r.r || ' has ' || p.priv
  from tbl t cross join roles r cross join (select unnest(array['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) priv) p
 where has_table_privilege(r.r, t.oid, p.priv)
union all
select 'column_privilege', t.relname || '.' || a.attname, r.r || ' can ' || p.priv
  from tbl t join pg_attribute a on a.attrelid = t.oid and a.attnum > 0 and not a.attisdropped
  cross join roles r cross join (select unnest(array['SELECT','INSERT','UPDATE','REFERENCES']) priv) p
 where has_column_privilege(r.r, t.oid, a.attnum, p.priv)
   and not (t.relname = 'reps' and r.r = 'authenticated' and p.priv = 'SELECT' and a.attname in ('id','org_id','company_id'))
union all
select 'sequence_privilege', c.relname, r.r || ' has ' || p.priv
  from pg_class c cross join roles r cross join (select unnest(array['USAGE','SELECT','UPDATE']) priv) p
 where c.relnamespace = 'public'::regnamespace and c.relkind = 'S' and has_sequence_privilege(r.r, c.oid, p.priv)
union all
select 'function_execute', pr.proname, r.r || ' can EXECUTE'
  from pg_proc pr cross join roles r
 where pr.pronamespace = 'public'::regnamespace and pr.prokind in ('f','p') and has_function_privilege(r.r, pr.oid, 'EXECUTE')
   and not exists (select 1 from pg_depend d where d.objid = pr.oid and d.deptype = 'e')
union all
select 'public_pseudo_role_grant', c.relname, 'PUBLIC has a privilege'
  from pg_class c, lateral aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
 where c.relnamespace = 'public'::regnamespace and c.relkind in ('r','p','v','m') and a.grantee = 0
union all
select 'policy_not_tenant_safe', schemaname || '.' || tablename, policyname || ' roles=' || roles::text
  from pg_policies
 where schemaname = 'public' and (qual is null or qual = 'true') and 'authenticated' = any (roles) and cmd in ('SELECT','ALL')
union all
select 'service_role_broken', 'service_role', 'missing BYPASSRLS or privileges'
 where not exists (select 1 from pg_roles where rolname = 'service_role' and rolbypassrls)
    or not has_table_privilege('service_role', 'public.reps', 'SELECT,INSERT,UPDATE,DELETE')
union all
select 'default_privileges_open', pg_get_userbyid(defaclrole) || ' ' || defaclobjtype::text, defaclacl::text
  from pg_default_acl
 where defaclnamespace = 'public'::regnamespace and defaclacl::text ~ '(anon|authenticated)='
   -- supabase_admin's seeded defaults cannot always be altered by `postgres`; they are acceptable ONLY while the event trigger
   -- below re-secures every new table. Defaults of any other role are never acceptable.
   and (pg_get_userbyid(defaclrole) <> 'supabase_admin' or not exists (select 1 from pg_event_trigger where evtname = 'gravix_secure_new_public_table'))
union all
select 'event_trigger_missing', 'gravix_secure_new_public_table', 'new tables would be created open'
 where not exists (select 1 from pg_event_trigger where evtname = 'gravix_secure_new_public_table')
   and exists (select 1 from pg_default_acl where defaclnamespace = 'public'::regnamespace and defaclacl::text ~ '(anon|authenticated)=')
union all
select 'own_row_policy_missing', 'reps', 'reps_select_self'
 where not exists (select 1 from pg_policies where schemaname = 'public' and tablename = 'reps' and policyname = 'reps_select_self' and qual ~ 'auth\.uid\(\)');
