-- 20261105_repair_staging_tenant_uuids_verify.sql   (GENERATED, READ-ONLY)
-- Lists every public uuid column holding a value that fails the API's strict predicate. After the repair (and the optional
-- legacy-auth retirement) staging must return ZERO rows. Safe to run any time.
do $$
declare c record; n bigint; bad text := '';
begin
  for c in
    select cl.table_name, cl.column_name
      from information_schema.columns cl
      join information_schema.tables tb on tb.table_schema = cl.table_schema and tb.table_name = cl.table_name and tb.table_type = 'BASE TABLE'
     where cl.table_schema = 'public' and cl.data_type = 'uuid'
  loop
    execute format('select count(*) from public.%I where %I is not null and %I::text !~* %L', c.table_name, c.column_name, c.column_name, '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') into n;
    if n > 0 then bad := bad || format(E'\n  %s.%s: %s row(s)', c.table_name, c.column_name, n); end if;
  end loop;
  if bad <> '' then raise notice 'columns failing the strict predicate:%', bad; else raise notice 'OK: every public uuid value passes the strict predicate'; end if;
end $$;
