# RLS / direct-database-access lockdown (2026-11-05)

## Finding
Supabase seeds `anon` and `authenticated` with full privileges (SELECT/INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER) on every
`public` table via default privileges. RLS was **disabled on 48 staging / 49 production tables** (none forced), so anyone holding the
public anon key (shipped in the web bundle) could read — and on staging, write — tenant data straight through PostgREST.

| | Staging | Production |
|---|---|---|
| public tables | 72 | 72 |
| RLS disabled | 48 | 49 (+`admin_config`) |
| tables readable by anon (rows returned) | 10 | **35** (incl. reps, users, crm_contacts, call_scores, sparring_*, audit_events, assignments) |
| sensitive columns selectable | reps(email, phone_number), users/profiles/internal_users(email), crm_contacts(email), contacts(email, phone), companies(address, phone_number), crm_opportunities(contact_email) | same |
| anon write (staging probe, rolled back) | UPDATE reps/orgs, INSERT org_limits, TRUNCATE score_cache **succeeded** | not probed (read-only) |
| callable by anon/authenticated | `increment_rep_xp`, `match_knowledge_embeddings` + 24 others | same |
| storage | buckets private, no policies, `storage.objects` RLS on | same |
| realtime | no public tables in publication | same |
| pg_graphql | not enabled | not enabled |

Production holds a single org, so cross-tenant exposure is moot there; **anonymous exposure is real and unremediated.**

## Access model (verified from code)
* API: service role everywhere (BYPASSRLS). Anon-key use is limited to auth operations and key-unset fallbacks.
* Web: the only genuine direct table query is the proxy's own-row `reps` read (`org_id`) in `src/lib/proxyOrg.ts`.
* Therefore the intended model is **deny-all for anon/authenticated; service role only; one own-row exception**.

## Remediation — `sql/security/20261105_rls_lockdown.sql` (idempotent, one transaction)
1. `enable row level security` on every public table (not forced; service role bypasses).
2. Revoke all table/sequence/function privileges on schema `public` from `anon`, `authenticated`, `PUBLIC`; re-grant `service_role`.
3. Default privileges for `postgres` (and `supabase_admin` when permitted) no longer grant to anon/authenticated/PUBLIC.
4. Only direct read: `grant select (id, org_id, company_id) on reps to authenticated` + policy `reps_select_self` (`id = auth.uid()`).
   No UPDATE/INSERT/DELETE anywhere ⇒ clients cannot choose or change their organisation. Users without a rep row see zero rows.
5. Event trigger `gravix_secure_new_public_table` enables RLS and revokes grants on every newly created public table.
6. Post-condition block aborts the transaction if any of the above did not take effect.

Not changed: API `DEFAULT_ORG_ID` fallback, any API authorisation, production.

## Validation
* `sql/security/20261105_rls_lockdown_verify.sql` — read-only; **zero rows = pass** (RLS off, table/column/sequence/function grants,
  PUBLIC grants, non-tenant-safe policies, service_role broken, open default privileges, missing own-row policy).
* `npm run validate:rls-posture` (self-test, plants violations) and `npm run check:rls-posture` (static: migrations dated ≥ 20261105
  must enable RLS for any new table and never grant to anon/authenticated/PUBLIC); both in `check:release`.
* Live: `check-rls-posture.ts --live` with `DATABASE_URL`, or run the verify SQL via `supabase db query --linked -f …`.

## Staging result (applied 2026-11-05)
* verify SQL: 0 violations (baseline 48 RLS-off, 1008 table grants, 6053 column grants …).
* anon: 0/72 tables readable (all HTTP 401); OpenAPI denied; GraphQL not enabled.
* authenticated QA manager and rep: 0 tables readable via `select=*`; own `reps` row returned (1 of 3 reps in staging);
  `reps.email` 403; PATCH `reps.org_id` 403; INSERT `org_limits` 403; RPC `increment_rep_xp` 403.
* Hosted staging proxy as manager and rep: reps/me, CRM contacts + overview, team, command centre, calls, assignments, insights,
  sparring personas, admin-config all behave as before (rep correctly 403 on manager routes); org resolved from membership;
  tampered `x-org-id` ⇒ 403 `org_mismatch`.
* `staging:qa status` (service role) works. Staging has a single org, so a live second-tenant probe was not possible; cross-row denial
  is shown by a rep seeing only their own `reps` row.

## Rollback
* Staging: `sql/security/20261105_rls_lockdown_rollback_staging.sql` (restores RLS-off list from snapshot, grants, defaults, drops trigger).
* Production: `sql/security/20261105_rls_lockdown_rollback_production.sql` (same, from production snapshot).
  Rolling back re-opens anonymous access; use only for a verified outage and re-apply promptly.

## Production plan (NOT executed)
1. Apply forward migration in a maintenance-free window (instant, metadata only); run verify SQL (expect 0 rows).
2. Smoke: sign-in, `/v1/reps/me`, CRM, calls, assignments via production proxy; confirm `reps` own-row org lookup.
3. Confirm production proxy/API pair carries `PROXY_SHARED_SECRET` so the proxy path (not anon) remains the only entry.
