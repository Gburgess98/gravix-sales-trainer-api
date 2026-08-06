# Gravix — Isolated Staging Environment (Day 271)

Foundation for a genuinely isolated, low-cost staging environment so Scoring v2
(and future changes) can be proven without touching production infrastructure,
data or customers. **Day 271 is setup + safeguards only — no Scoring v2
activation, no cloud resources created (see the blocker below).** UK spelling.

> **No secrets in this document.** Only variable *names*, public URLs and
> non-sensitive identifiers appear here.

---

## Status — cloud resources NOT created (blocked, honestly)

Day 271 stopped before creating cloud resources because **no authenticated cloud
organisation could be confirmed** from this workspace: the only CLI present is
`gh` (GitHub). There is **no Supabase CLI, no Vercel/hosting CLI**, so the staging
Supabase project and the staging API/WEB deployments cannot be created here, and
the authenticated Supabase/Vercel org cannot be verified. Per the Day-271 rule
("stop before resource creation if the authenticated cloud account or
organisation cannot be confirmed"), creation was not attempted, and no production
project was substituted.

**Delivered instead (safe, no cloud, no secrets):**
- `scripts/validate-staging-environment-day-271.ts`
  (`npm run validate:staging-environment`) — the environment safety guard that
  refuses dangerous staging↔production combinations (offline self-test + LIVE
  opt-in), with non-vacuity.
- WEB staging marker (`src/components/StagingBanner.tsx`) — visible only when
  `NEXT_PUBLIC_APP_ENV=staging`, invisible in production.
- This document + the updated `SCORING_V2_STAGING_RUNBOOK.md`.

**To finish provisioning, the operator must perform §Manual provisioning below.**

---

## Production architecture inventory (config-derived, redacted)

| Area | Production (redacted) | Staging requirement | Safe to reuse? |
|---|---|---|---|
| Supabase database | single project, ref redacted (no `staging` marker) | **separate** project | No |
| Supabase Auth | production project | separate project | No |
| Storage | `SUPABASE_STORAGE_BUCKET` (prod) | separate buckets | No |
| API hosting | `api.gravixbots.com` (custom domain) | separate staging deploy | No |
| WEB hosting | production `*.vercel.app` origin | separate staging deploy | No |
| OpenAI | `OPENAI_API_KEY` (prod) | **not required Day 271** (stub) | No |
| Anthropic | `ANTHROPIC_API_KEY` (prod) | not used (no Claude) | No |
| Slack | `SLACK_WEBHOOK_URL` (prod) | **disabled** in staging | No |
| Email | `POSTMARK_SERVER_TOKEN` / `EMAIL_FROM` (prod) | **disabled** in staging | No |
| Transcription | `DEEPGRAM_API_KEY` (prod) | not needed for the stub proof | No |

- **Migration mechanism:** 28 timestamped `sql/*.sql` files, applied in order
  (no Supabase CLI migration dir). Includes tables, RPCs/functions, RLS and any
  extensions (e.g. pgvector) as written in those files.
- **Seed mechanism:** `npm run seed:demo`, `seed:ufc-story`, `seed:ufc-intelligence`
  (tsx scripts that read `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` from env —
  therefore **env-targetable at a staging project**).
- **Env marker:** `APP_ENV` / `DEPLOYMENT_ENV` are **not yet referenced** in code;
  the WEB marker introduces `NEXT_PUBLIC_APP_ENV`. `NODE_ENV=development` locally
  is **not** proof of staging.

---

## Target staging architecture

| Resource | Name | URL |
|---|---|---|
| Supabase | `gravix-sales-trainer-staging` | provider-generated |
| API deploy | `gravix-sales-trainer-api-staging` | provider-generated |
| WEB deploy | `gravix-sales-trainer-web-staging` | provider-generated |

- Deployment branches: API `claude/sprint-3-api` (`6b996ea`+), WEB
  `claude/sprint-3-shell` (`92e6ab1`+).
- Region: choose sensibly for UK users + API latency (e.g. `eu-west`).
- Every staging deploy exposes a **staging marker**: API `APP_ENV=staging`
  (add to a safe health/version response), WEB `NEXT_PUBLIC_APP_ENV=staging`.

### Staging environment-variable NAMES (values via the provider's secret store)

- API: `APP_ENV=staging`, `SUPABASE_URL` (staging), `SUPABASE_SERVICE_ROLE_KEY`
  (staging), `SUPABASE_STORAGE_BUCKET` (staging), `CORS_ALLOW_ORIGINS` (staging
  WEB origin), `PUBLIC_API_BASE` (staging API), **`SCORING_CONTRACT=v1`**,
  **`SCORING_PROVIDER=stub`**, **`SKIP_SCORING_SIDE_EFFECTS=1`**. **Do NOT set**
  `SLACK_WEBHOOK_URL`, `POSTMARK_SERVER_TOKEN`, production `OPENAI_API_KEY`,
  `ANTHROPIC_API_KEY`.
- WEB: `NEXT_PUBLIC_APP_ENV=staging`, `NEXT_PUBLIC_API_BASE`/`API_PROXY_TARGET`
  (staging API), `NEXT_PUBLIC_SUPABASE_URL` + `NEXT_PUBLIC_SUPABASE_ANON_KEY`
  (staging), staging auth redirect URLs.

Secret ownership: the operator holds all staging secrets in the hosting
provider's encrypted store. None are committed or printed.

---

## Manual provisioning (operator actions — none automatable here)

1. **Confirm the authenticated Gravix org** on Supabase and the hosting provider
   (Vercel/Render/etc.). Confirm free-tier availability and that creation incurs
   **no cost** (stop and seek approval otherwise).
2. **Create the staging Supabase project** `gravix-sales-trainer-staging` (new DB,
   URL, anon + service-role keys, strong generated password). No production data
   import.
3. **Apply the schema**: run the 28 `sql/*.sql` files in timestamp order against
   the staging project (Supabase SQL editor or `psql`), including RLS + any
   extensions. Confirm the target is the staging ref, not production.
4. **Seed synthetic data** (see below), pointing the seed scripts at the staging
   env.
5. **Create the staging API deploy** with the env NAMES above; deploy `6b996ea`+.
6. **Create the staging WEB deploy** with the env NAMES above; deploy `92e6ab1`+.
7. Run `STAGING_API_BASE=<staging> STAGING_CONFIRMED=1 EXPECTED_API_SHA=6b996ea
   npm run validate:staging-environment` to verify isolation live.

If an AI key is required merely for API boot, prefer making stub mode boot
without it, or use an approved staging-only placeholder — never copy the
production key.

---

## Synthetic staging seed (deterministic, non-customer)

Use existing seed tooling against the staging project. Suggested labelled records
(no real customer data, no real emails/phones/recordings):

- company `Gravix Staging QA`
- UFC/default scorecard + the 8 approved Objection Library items
- **`DAY272_SCORING_V2_STUB_PROOF`** — dedicated call with transcript + segments,
  known stage content, no customer info (the Day-272 stub-proof fixture)
- **`DAY272_V1_FALLBACK_PROOF`** — a v1-only call for the fallback UI proof

### QA login identities (Day 273 — supported Admin Auth API only)

`sql/seed/staging-day272-fixtures.sql` seeds **only** the synthetic tenant + call
fixtures. It no longer writes `auth.users` or `public.profiles` directly: those
hand-written rows were malformed for GoTrue ("Database error loading user").
Provision disposable QA logins through the supported Supabase Admin Auth API:

```bash
APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<ref> STAGING_QA_PASSWORD=… \
  npm run staging:qa -- create      # auth user + profile + reps tenant bridge
… npm run staging:qa -- verify      # sign-in (staging issuer) + tenant-scoped read
… npm run staging:qa -- delete      # teardown (auth id + profile cascade + rep)
```

Offline check: `npm run validate:staging-qa-identity`. Full details in
`STAGING_QA_IDENTITY_RUNBOOK.md`.

---

## Integration-disable rules (staging)

Slack, Postmark/email, manager/rep notifications, paid AI and production webhooks
are **disabled** (unset) in staging, and `SKIP_SCORING_SIDE_EFFECTS=1` skips
score side effects. Behaviour is explicitly disabled — never faked-successful.

---

## Isolation validator + safety guard

`npm run validate:staging-environment` (`scripts/validate-staging-environment-day-271.ts`)
refuses: missing `APP_ENV`; staging on a production host; staging Supabase ==
production; staging WEB → production API; `SKIP_SCORING_SIDE_EFFECTS` ≠ 1;
`SCORING_CONTRACT` ≠ v1; provider ≠ stub; production Slack/Postmark set; seed
target == production; commit mismatch; production using the staging DB. Compares
safe identifiers only, redacts values, exits non-zero on unsafe config, and has a
network-free self-test lane.

## Health checks

- API: a safe health/version endpoint returns `app_env: "staging"` + commit;
  Supabase connectivity OK; `resolveScoringContract()` → `v1`; stub provider
  resolves; side effects report disabled; no OpenAI call.
- WEB: page loads, auth + API calls hit only staging endpoints (browser network
  log shows no production hosts), no console errors, staging marker visible,
  desktop + mobile usable.
- QA login: `npm run staging:qa -- verify` proves password sign-in from the
  staging issuer + tenant-scoped fixture access (Day 273; no secrets printed).

## Rollback / deletion

Staging is fully separate, so teardown never affects production: delete the
staging API + WEB deploys and the staging Supabase project when no longer needed.
The Day-272 fixtures are synthetic and labelled — safe to delete or retain as
staging test fixtures. To disable the marker, unset `NEXT_PUBLIC_APP_ENV`.

## Day 272 activation (no-cost stub)

Once provisioned and isolation is verified: follow `SCORING_V2_STAGING_RUNBOOK.md`
§4–§6 with `SCORING_CONTRACT=v2 SCORING_PROVIDER=stub SKIP_SCORING_SIDE_EFFECTS=1`
against `DAY272_SCORING_V2_STUB_PROOF`, verify `analysis_json.v2` persists + the
Day-268 UI renders it, prove the v1 fallback on `DAY272_V1_FALLBACK_PROOF`, then
roll back to `SCORING_CONTRACT=v1`. Paid OpenAI remains gated behind explicit
approval.
