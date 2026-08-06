# Gravix — Staging QA Identity Runbook (Day 273)

Repeatable, **staging-only** provisioning of a disposable QA login identity so
browser QA can be repeated **without hand-repairing the database**. UK spelling.

> **No secrets in this document.** Only variable *names*, public URLs and
> non-sensitive identifiers appear here. The QA password lives only in the
> operator's environment and is never printed, committed or stored in the vault.

---

## Why this exists

The earlier staging seed INSERTed rows into `auth.users` directly. Those rows are
malformed for GoTrue (they omit the many fields the Auth service normally
populates), so `auth.admin` list/read failed with **"Database error loading
user"** and the identity could not sign in reliably. Day 272 worked around this
with a one-off manually-created Admin API user that was then deleted.

Day 273 removes the unsupported path:

- `sql/seed/staging-day272-fixtures.sql` no longer writes `auth.users` or the
  FK-dependent `public.profiles`. It seeds **only** the synthetic tenant + call
  fixtures (none of which foreign-key to `auth.users` — only `profiles.user_id`
  does).
- `scripts/provision-staging-qa.ts` creates the login identity through the
  **supported Supabase Admin Auth API** (`auth.admin.createUser`), then upserts
  its `public.profiles` row and `public.reps` tenant bridge into the synthetic
  org/company. Teardown deletes both the application rows and the Auth identity.

---

## What gets created

| Object | Value / binding |
|---|---|
| Auth user | `STAGING_QA_EMAIL` (default `staging-qa@gravix.invalid`), email-confirmed |
| `public.profiles` | `user_id` = auth id, `role` = `STAGING_QA_ROLE` (default `manager`) |
| `public.reps` bridge | `id` = auth id, `org_id`/`company_id` = synthetic staging tenant, `tier` = `Manager`/`SalesRep` |

The rep `id` equalling the auth id is what lets `src/lib/callAccess.ts`
`getRequesterOrgId` resolve the identity's org, so a manager QA identity can read
the synthetic call fixtures in that org (default `call_visibility` is `everyone`).

Synthetic tenant (matches the SQL seed):

- org `00000000-2711-0000-0000-000000000002` — *Gravix Staging QA*
- company `00000000-2711-0000-0000-000000000003` — *Gravix Staging QA*
- fixture call `00000000-2722-4000-8000-000000000001` — *DAY272_SCORING_V2_STUB_PROOF*

---

## Safety guard (refuses production)

The script will **not run** unless **both**:

1. `APP_ENV=staging`, and
2. the `SUPABASE_URL` project ref equals `EXPECTED_STAGING_SUPABASE_REF`
   (and differs from `PROD_SUPABASE_REF` when that is supplied).

Refs are compared, never printed. The password, any bearer token and the
service-role key are never printed.

---

## Environment (names only)

| Variable | Purpose |
|---|---|
| `SUPABASE_URL` | staging project URL |
| `SUPABASE_SERVICE_ROLE_KEY` (or `SUPABASE_SERVICE_KEY`) | staging service role |
| `SUPABASE_ANON_KEY` | staging anon key — `verify`/sign-in only |
| `EXPECTED_STAGING_SUPABASE_REF` | **required** guard — staging project ref |
| `PROD_SUPABASE_REF` | optional — asserts staging ≠ production |
| `APP_ENV=staging` | **required** guard |
| `STAGING_QA_PASSWORD` | **required** for `create`/`verify` — never printed |
| `STAGING_QA_EMAIL` | default `staging-qa@gravix.invalid` |
| `STAGING_QA_ROLE` | `manager` \| `rep` (default `manager`) |
| `STAGING_QA_FIXTURE_CALL_ID` | default Day-272 stub-proof call |

The operator holds all staging secrets in their environment / the hosting
provider's encrypted store. None are committed or printed.

---

## Procedure

Prerequisite: the synthetic tenant + call fixtures exist
(`sql/seed/staging-day272-fixtures.sql` applied to the staging project).

```bash
# 1) Provision (idempotent): auth user + profile + rep bridge
APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<staging-ref> \
STAGING_QA_PASSWORD=<generated> \
  npm run staging:qa -- create

# 2) Prove password sign-in (staging issuer) + tenant-scoped fixture read
APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<staging-ref> \
STAGING_QA_PASSWORD=<generated> SUPABASE_ANON_KEY=<staging-anon> \
  npm run staging:qa -- verify

# 3) (optional) Report presence without writes/login
APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<staging-ref> \
  npm run staging:qa -- status

# 4) Explicit teardown: delete rep bridge + Auth identity (profile cascades)
APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<staging-ref> \
  npm run staging:qa -- delete
```

`verify` proves, without printing any secret:

- password sign-in returns a session whose **token issuer host is the staging
  project**;
- the synthetic fixture call **is** visible to the QA identity under the API's
  own tenant-scope rule, and a foreign-org call **is not**.

---

## Offline safety check (CI / local)

```bash
npm run validate:staging-qa-identity
```

Self-test lane (no network, no secrets): the staging guard passes on a clean
config and refuses every planted unsafe target; the profile/rep rows satisfy the
`profiles_role_check` and `reps_tier_check` constraints; the tenant-scope rule
mirrors `canAccessCall`; the JWT issuer-host + redaction behave; the provisioning
script never logs the password, a token, keys or the project ref; and the SQL
seed no longer inserts `auth.users`/`profiles`. A LIVE-CONFIG lane (opt-in, still
no network) asserts the real environment passes the same guard.

---

## Teardown guarantees

- `delete` removes the `public.reps` bridge, then the Auth identity; the
  `profiles` row is removed by the `profiles_user_id_fkey … ON DELETE CASCADE`.
- The synthetic tenant + call fixtures are labelled and safe to retain or drop.
- Production and customer identities are never touched: the guard refuses any
  non-staging target, and all ids/emails are synthetic.
