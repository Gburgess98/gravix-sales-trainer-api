-- 20261105_rls_lockdown_rollback_production.sql — EMERGENCY ROLLBACK of 20261105_rls_lockdown.sql for the PRODUCTION project.
--
-- This RE-OPENS direct database access for anon/authenticated exactly as it was before the lockdown (Supabase defaults):
-- full table/sequence/function privileges for anon and authenticated, RLS off on the 49 tables that had it off at the
-- 2026-11-05 audit. Use ONLY to restore the previous state if the lockdown breaks something that cannot be fixed forward.
-- Policies that existed before (own-row policies on calls, call_pins, profiles, jobs, crm_auto_assign_runs) are untouched.
begin;
do $$
declare t text;
begin
  foreach t in array array['account_ai_summaries', 'account_ai_tasks', 'account_coaching_actions', 'account_escalations', 'accounts', 'activities', 'admin_config', 'assignments', 'audit_events', 'audit_logs', 'badges', 'bounties', 'call_scores', 'coach_assignments', 'coach_notes', 'companies', 'company_playbook', 'contacts', 'crm_accounts', 'crm_actions', 'crm_activities', 'crm_call_links', 'crm_contact_notes', 'crm_contacts', 'crm_demo_call_links', 'crm_opportunities', 'internal_users', 'knowledge_embeddings', 'offices', 'opportunities', 'org_limits', 'orgs', 'partners', 'pins', 'rep_memory', 'rep_xp_events', 'reps', 'score_cache', 'sparring_personas', 'sparring_sessions', 'sparring_turns', 'team_settings', 'titles', 'tmcs', 'user_badges', 'user_selected_title', 'user_titles', 'users', 'xp_events'] loop
    if to_regclass('public.' || t) is not null then execute format('alter table public.%I disable row level security', t); end if;
  end loop;
end $$;
drop policy if exists reps_select_self on public.reps;
revoke select (id, org_id, company_id) on public.reps from authenticated;
grant all on all tables    in schema public to anon, authenticated, service_role;
grant all on all sequences in schema public to anon, authenticated, service_role;
grant all on all functions in schema public to anon, authenticated, service_role;
alter default privileges for role postgres in schema public grant all on tables    to anon, authenticated, service_role;
alter default privileges for role postgres in schema public grant all on sequences to anon, authenticated, service_role;
alter default privileges for role postgres in schema public grant all on functions to anon, authenticated, service_role;
do $$ begin
  alter default privileges for role supabase_admin in schema public grant all on tables    to anon, authenticated, service_role;
  alter default privileges for role supabase_admin in schema public grant all on sequences to anon, authenticated, service_role;
  alter default privileges for role supabase_admin in schema public grant all on functions to anon, authenticated, service_role;
exception when insufficient_privilege then raise notice 'supabase_admin defaults not restored (insufficient privilege)';
end $$;
drop event trigger if exists gravix_secure_new_public_table;
drop function if exists public.gravix_secure_new_public_table();
commit;
