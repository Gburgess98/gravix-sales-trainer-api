# Staging organisation identity repair — operation record and plan

Status: **PLAN FOR APPROVAL. Nothing has been applied** (no staging DDL/data change, no Railway/Vercel change, no deploy, no production access beyond read-only checks).
Related: `scripts/staging-tenant-ids.ts` (ids), `sql/staging/20261105_repair_staging_tenant_uuids*.sql` (generated SQL),
`scripts/validate-staging-tenant-uuids.ts`, `scripts/verify-staging-tenant-uuids.ts`, web commit `b97fe15` (proxy).

## 1. Problem
The API validates ids with a strict RFC-4122 predicate (`UUID_RE`, version nibble 1–5, variant nibble 8/9/a/b; identical in `crm.ts`, `reps.ts`, `calls.ts`, `accounts.ts`, `users.ts`, `pins.ts`, `tokenVerification.ts`). The Day-272 staging fixtures created the tenant as `00000000-2711-0000-0000-…` (nibbles 0/0), so `getOrgIdHeader` rejects the header **and** the `DEFAULT_ORG_ID` fallback (`Missing or invalid x-org-id`). Validation stays strict; the data and the proxy change.

## 2. Evidence (read-only)
**References in code/config (both repos):** `scripts/provision-staging-qa.ts`, `scripts/prove-account-ai-actor-fk-day-283.mjs`, `scripts/validate-staging-fixtures-day-276.ts` (stale call twins, intentionally legacy), `sql/seed/staging-day272-fixtures.sql`, `STAGING_QA_IDENTITY_RUNBOOK.md`, `sql/README.md`; web repo: docs only (no code). `.claude/settings.local.json` mentions ids in local allow-lists (not source).
**Railway staging `DEFAULT_ORG_ID`:** present, 36 chars, **equals the legacy org id** `00000000-2711-0000-0000-000000000002` (value is a synthetic tenant id, not a secret). `APP_ENV=staging`; `ALLOW_ORG_BYPASS` and `CRON_ORG_ID` are not set. No other Railway variable holds an old-pattern id. Vercel staging has no org variable.
**Staging database (PostgREST scan of all 72 tables):** 19 columns / 52 values fail the strict predicate; every one is one of 7 synthetic ids:

| Legacy id | Meaning | Maps to |
|---|---|---|
| `…2711-0000-0000-000000000001` | TMC | `…2711-4000-8000-000000000001` |
| `…2711-0000-0000-000000000002` | org "Gravix Staging QA" | `…2711-4000-8000-000000000002` |
| `…2711-0000-0000-000000000003` | company "Gravix Staging QA" | `…2711-4000-8000-000000000003` |
| `…2711-0000-0000-00000000000c` | synthetic rep (fixture call owner) | `…2711-4000-8000-00000000000c` |
| `…2711-0000-0000-00000000000b` | id the fixture calls/activities used as "owner" (no rep row) | → the rep `…00000c` |
| `…2711-0000-0000-00000000000a` / `…0b` | pre-Day-273 hand-written `auth.users` + `profiles` | retired by the **optional** separate file (Auth cannot change a user's id) |
| `…2722-0000-0000-000000000001` | stale call twin still referenced by `crm_activities.call_id` | → the valid twin `…2722-4000-8000-000000000001` |

**Dependency map (declared FKs + soft references, with staging row counts):** `tmcs.id`(1) → `companies.tmc_id`(1) · `orgs.id`(1) → `reps.org_id`(3, includes both real QA logins), `org_settings.org_id`(1), `calls.org_id`(9), `sparring_sessions.org_id`(4), `crm_activities.org_id`(2) · `companies.id`(1) → `reps.company_id`(3), `calls.company_id`(9), `assignments.company_id`(2), `sparring_sessions.company_id`(4) · `reps.id`(1, the synthetic rep) · `calls.user_id`(2), `crm_activities.user_id`(2)/`rep_id`(2) (owner `…0b`) · `crm_activities.call_id`(2, dangling stale twin) · `profiles.user_id`(2, FK → `auth.users`, ON DELETE CASCADE) · `sparring_sessions.meta` (jsonb embeds `company_id` and `team_settings_snapshot.org_id`). The repair sweeps **every** public uuid column and every jsonb value generically (not just this list) and aborts if any reference remains. Real QA logins keep their (valid) ids; only their `org_id`/`company_id` move.
**Production (read-only, counts only):** tenant tables are clean — `orgs`, `companies`, `tmcs`, `offices`, `reps`, `users`, `partners`: **0 malformed ids**; none of the staging ids appear. Six *operational* columns hold stray non-RFC values: zero-UUID org (`team_settings.org_id` 1, `coach_assignments.org_id` 3, `crm_auto_assign_runs.org_id` 1) and all-same-digit test ids (`calls.org_id` 5, `calls.user_id` 3, `jobs.user_id` 5). They are smoke/bypass-test leftovers, not tenant identity; **not touched** (separate cleanup decision; note zero-UUID org rows imply the bypass was used once in production).

## 3. What changed in code (committed, not applied)
1. **Web proxy (`b97fe15`)** — the org comes from the *verified user's* `reps.org_id` read with their own Bearer session (cookie lookup second). A client `x-org-id` that differs → `403 org_mismatch`; with no verified membership a client org is ignored. The default org is only an explicit last resort (always off-prod; in production only with `PROXY_ALLOW_DEFAULT_ORG=1`, which we leave **unset**). No UUID predicate added or duplicated.
2. **Seed/QA tooling (`2ee3cb3`)** — valid ids, one source (`staging-tenant-ids.ts`), generated SQL, validator using the app's own predicate (proves the zero-nibble pattern is rejected), read-only verifier.
3. **This record.**

**SQL behaviour was exercised against a PGlite model of the staging tables** (parents/children FKs, unique slugs, jsonb, `auth.users` cascade): forward moves everything; QA reps stay mapped; second run is a no-op; rollback restores the legacy ids; an unrepointable reference aborts and leaves the database untouched; a wrong-tenant guard refuses. That is a logic proof on a model — **not** proof against the real schema, which is why step 0 below is a dry run on staging.

## 4. Plan (each step needs approval; operator runs 1–5 in order, back-to-back)
0. **Dry run (no persistence).** In the staging SQL editor run the forward file with the last `commit;` replaced by `rollback;` (`sed '$ s/^commit;$/rollback;/' sql/staging/20261105_repair_staging_tenant_uuids.sql`). Expected: NOTICE `repointed N reference(s)` and no error; nothing persists. Any error = stop and report.
1. **Pre-check:** `APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<ref> DOTENV_CONFIG_PATH=.env.staging.local npm run staging:verify-uuids` → expected 52 failing values (baseline).
2. **Apply** `sql/staging/20261105_repair_staging_tenant_uuids.sql` (staging project only).
3. **Railway staging:** set `DEFAULT_ORG_ID` = `00000000-2711-4000-8000-000000000002` (the only configuration change; restart on save). Do this immediately after step 2 — until then header-less calls resolve to an org that no longer exists.
4. **Deploy web staging** from `b97fe15`+ (proxy change; no API redeploy needed — API code is unchanged).
5. **Optional, separate approval:** run `…_retire_legacy_auth_users.sql` to remove the two malformed synthetic auth users, then `npm run staging:verify-uuids` (expect exit 0). Without it, run the verifier with `--allow-legacy-auth` (only those two profiles remain).
6. **Verify** (signed in as the QA manager and rep — requires the operator's QA credentials; I cannot sign in): `npm run staging:qa -- verify`; CRM → Contacts loads (no `Missing or invalid x-org-id`); Manager Centre, Team, Calls, Assignments, Insights, Sparring load; reps still see only their own org; altering `x-org-id` in the browser returns `403 org_mismatch`.

## 5. Rollback
- **Before step 3:** run `sql/staging/20261105_repair_staging_tenant_uuids_rollback.sql` (restores the legacy tenant ids; QA logins stay mapped). Railway untouched.
- **After step 3:** run the rollback SQL **and** restore Railway `DEFAULT_ORG_ID` = `00000000-2711-0000-0000-000000000002`.
- Not reversible by the rollback: the `…0b` owner → rep merge (those two calls/activities stay owned by the synthetic rep) and, if step 5 ran, the two retired auth users (passwords are not recoverable). Web: redeploy the previous staging deployment (`b845dea`/`8510742`) to revert the proxy.
- Rollback restores ids the API rejects; use it only to return to the exact prior state.

## 6. Risks / notes
- Real-schema differences (triggers, RLS, extra FKs) are the main risk → dry run. The repair changes data only (no DDL), so no PostgREST schema reload is needed.
- The proxy's Bearer-scoped `reps` read depends on RLS allowing a user to read their own row (the cookie path made the same assumption). If it cannot, the proxy sends no org header and the API falls back to Railway `DEFAULT_ORG_ID`, which step 3 makes valid — so the repair works either way.
- The API's own header-less fallback (`DEFAULT_ORG_ID`) can attach a user with no verified org to the default org. Out of scope here (API behaviour unchanged by design); candidate hardening: require a verified org in the API too.
- Production strays above need their own decision; nothing here touches production.
