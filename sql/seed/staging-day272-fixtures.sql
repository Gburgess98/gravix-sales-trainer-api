-- Day 271 — SYNTHETIC staging fixtures for the Day-272 Scoring v2 stub proof.
-- STAGING ONLY. Do NOT run against production. All data below is synthetic
-- (deterministic 0000…-2711/2722 UUIDs, no real customer/user/email/recording).
-- Idempotent: safe to re-run. No secrets.
--
-- Day 273 — This seed no longer writes auth.users directly. Hand-crafted
-- auth.users rows are malformed for GoTrue (missing the fields the Auth service
-- populates), which made admin list/read fail with "Database error loading user".
-- LOGIN identities are now provisioned exclusively through the supported Supabase
-- Admin Auth API by `scripts/provision-staging-qa.ts`
-- (`npm run staging:qa -- create`), which creates the auth user and then upserts
-- its public.profiles row and public.reps tenant bridge into the org/company
-- below. This file seeds only the synthetic tenant + call fixtures, none of which
-- foreign-key to auth.users (only public.profiles.user_id does), so it is safe to
-- run without any auth rows present.

begin;

-- Tenant / org / company hierarchy -------------------------------------------
insert into public.tmcs (id, name, slug)
values ('00000000-2711-0000-0000-000000000001', 'Gravix Staging TMC', 'gravix-staging-tmc')
on conflict (id) do nothing;

insert into public.orgs (id, name)
values ('00000000-2711-0000-0000-000000000002', 'Gravix Staging QA')
on conflict (id) do nothing;

insert into public.companies (id, tmc_id, name, slug)
values ('00000000-2711-0000-0000-000000000003',
        '00000000-2711-0000-0000-000000000001',
        'Gravix Staging QA', 'gravix-staging-qa')
on conflict (id) do nothing;

-- People (synthetic tenant data; NOT login identities) ------------------------
-- The synthetic call-owner rep below is pure tenant data and does NOT foreign-key
-- to auth.users. Interactive QA logins (auth user + profile + rep bridge) are
-- provisioned separately via scripts/provision-staging-qa.ts (Admin Auth API),
-- never by hand-written auth.users / profiles rows here (Day 273).
insert into public.reps (id, org_id, company_id, tier, name, display_name, first_name, last_name, email)
values ('00000000-2711-0000-0000-00000000000c',
        '00000000-2711-0000-0000-000000000002',
        '00000000-2711-0000-0000-000000000003',
        'SalesRep',
        'Jamie Staging', 'Jamie Staging', 'Jamie', 'Staging', 'jamie.staging@gravix.invalid')
on conflict (id) do nothing;

-- Day-272 scoring proof call (UNSCORED — Day 272 scores it with v2+stub) -------
insert into public.calls (
  id, user_id, org_id, company_id, filename, title, status,
  storage_path, audio_path, transcript, analysis_json, created_at
) values (
  '00000000-2722-4000-8000-000000000001',
  '00000000-2711-0000-0000-00000000000b',
  '00000000-2711-0000-0000-000000000002',
  '00000000-2711-0000-0000-000000000003',
  'DAY272_SCORING_V2_STUB_PROOF', 'DAY272_SCORING_V2_STUB_PROOF', 'transcribed',
  'staging/day272/scoring-v2-stub-proof.txt',
  'staging/day272/scoring-v2-stub-proof.txt',
  E'rep: Thanks for taking the time today, I will keep this to fifteen minutes and we can agree a next step if it is a fit.\nbuyer: Sounds good. Right now we review calls manually.\nrep: We work with a number of teams on exactly this, so I have seen how it scales.\nbuyer: Honestly it feels a bit expensive.\nrep: Totally fair, most teams find the time saved pays it back within a quarter.\nrep: Shall we get a short follow-up in the diary for Thursday?',
  jsonb_build_object(
    'transcript', jsonb_build_object(
      'text', 'DAY272 synthetic transcript',
      'segments', jsonb_build_array(
        jsonb_build_object('idx',0,'speaker','rep','start_sec',0,'end_sec',6,'text','Thanks for taking the time today, I will keep this to fifteen minutes and we can agree a next step if it is a fit.'),
        jsonb_build_object('idx',1,'speaker','buyer','start_sec',6,'end_sec',11,'text','Sounds good. Right now we review calls manually.'),
        jsonb_build_object('idx',2,'speaker','rep','start_sec',11,'end_sec',17,'text','We work with a number of teams on exactly this, so I have seen how it scales.'),
        jsonb_build_object('idx',3,'speaker','buyer','start_sec',17,'end_sec',21,'text','Honestly it feels a bit expensive.'),
        jsonb_build_object('idx',4,'speaker','rep','start_sec',21,'end_sec',28,'text','Totally fair, most teams find the time saved pays it back within a quarter.'),
        jsonb_build_object('idx',5,'speaker','rep','start_sec',28,'end_sec',33,'text','Shall we get a short follow-up in the diary for Thursday?')
      )
    )
  ),
  now()
) on conflict (id) do nothing;

-- Day-272 v1-fallback call (already v1-scored; NO analysis_json.v2) ------------
insert into public.calls (
  id, user_id, org_id, company_id, filename, title, status,
  storage_path, audio_path, transcript, score_overall, ai_model, rubric, analysis_json, scored_at, created_at
) values (
  '00000000-2722-4000-8000-000000000002',
  '00000000-2711-0000-0000-00000000000b',
  '00000000-2711-0000-0000-000000000002',
  '00000000-2711-0000-0000-000000000003',
  'DAY272_V1_FALLBACK_PROOF', 'DAY272_V1_FALLBACK_PROOF', 'scored',
  'staging/day272/v1-fallback-proof.txt',
  'staging/day272/v1-fallback-proof.txt',
  E'rep: Quick intro and agenda.\nbuyer: Ok.\nrep: Some discovery questions.\nrep: Suggested a next step.',
  71, 'gpt-4o-mini:v1:v1',
  jsonb_build_object(
    'intro', jsonb_build_object('score',70,'notes','Clear enough open.'),
    'discovery', jsonb_build_object('score',74,'notes','Reasonable discovery.'),
    'objection', jsonb_build_object('score',68,'notes','Handled adequately.'),
    'close', jsonb_build_object('score',72,'notes','Next step proposed.'),
    '_meta', jsonb_build_object(
      'rubric_version','v1','prompt_version','v1','model_version','gpt-4o-mini:v1:v1',
      'scoring_model_version','gpt-4o-mini:v1:v1','scorecard_source','gravix_default')
  ),
  jsonb_build_object(
    'overall',71,'summary','Legacy v1-scored staging fixture.',
    'stages', jsonb_build_object(
      'intro', jsonb_build_object('score',70,'notes','Clear enough open.'),
      'discovery', jsonb_build_object('score',74,'notes','Reasonable discovery.'),
      'objection', jsonb_build_object('score',68,'notes','Handled adequately.'),
      'close', jsonb_build_object('score',72,'notes','Next step proposed.')),
    'moments', jsonb_build_array(),
    'suggestions', jsonb_build_array('Tighten the close.'),
    'voice', jsonb_build_object('clarity',70,'confidence',66,'filler_density',20,'pace',60,'overall',66)
  ),
  now(), now()
) on conflict (id) do nothing;

commit;
