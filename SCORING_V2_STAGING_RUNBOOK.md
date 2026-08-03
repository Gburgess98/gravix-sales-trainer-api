# Scoring v2 — Controlled Staging Activation Runbook (Day 270)

Safe, repeatable steps to prove the Day-269 opt-in `SCORING_CONTRACT=v2` path and
the Day-268 Call Review UI work end-to-end in a **real, isolated staging
environment** — starting with the **no-cost stub provider**. UK spelling.

> **Golden rule:** staging only. Never enable v2 on production, never write to the
> production database, never mutate a customer call, and never make a paid LLM
> call without explicit written approval (see §7).

---

## Day-270 status — LIVE staging run NOT performed (blocked, honestly)

The live activation was **not** run because a dedicated, unambiguously
non-production staging environment could not be identified from this workspace:

- The only API host in configuration is a **production** custom domain
  (`api.gravixbots.com`) plus a production `*.vercel.app` web origin — no
  `staging` host, no `.env.staging`, no deploy tooling to a distinct target.
- There is a **single Supabase project** (no `staging/dev/test` marker) and no
  isolated local database (no Docker / local Supabase). Staging and production
  database identity therefore **cannot be distinguished**.

Per the Day-270 STOP conditions, running `scoreWithLLM` here would write to the
production database and flipping `SCORING_CONTRACT` would change the production
domain — both forbidden. What was delivered instead, with **zero cost and no
DB/network**:

- `scripts/validate-scoring-v2-staging-activation-day-270.ts`
  (`npm run validate:scoring-v2-staging-activation`) — the repeatable validator
  with a self-test lane + an opt-in LIVE lane that refuses production hosts.
- Fixtures mirroring the exact persisted call row after a v2 **stub** score
  (`test/fixtures/scoring-v2/staging-stub-persisted-day-270.json`) and a mocked
  OpenAI score (`…-mock-openai-persisted-day-270.json`), both produced by the
  **production** seam `computeScoringV2Result`.
- A cross-repo proof that this exact stub `analysis_json` flows through the WEB
  Day-268 parser + view-model (degraded banner, four stage cards, "Not observed",
  contract v2 / stub provenance) — the API→WEB contract, end-to-end, no DB.

To run the real proof, supply a confirmed staging target and follow §2–§6.

**Day 271 update:** the staging-environment foundation + safety guard now exist —
see `STAGING_ENVIRONMENT.md` (architecture, resource names, env-var names,
schema/seed process, isolation validator `npm run validate:staging-environment`,
and the operator's manual provisioning steps). Cloud resources are **not yet
created** (no authenticated cloud org was confirmable in the workspace), so the
live proof still awaits the operator provisioning staging per that document.

---

## §1. Preconditions

- A staging deployment whose **database is provably separate** from production
  (a different Supabase project ref).
- The pushed commits deployed: API `7928f52`+, WEB `92e6ab1`+.
- A **dedicated staging test call** (non-customer, safe to re-score) with a
  transcript + segments. Never a production/customer call.

## §2. Identify staging safely (do not print secrets)

Confirm and record (values redacted): staging API URL, staging WEB URL, hosting
project/env name, deployed branch + commit SHAs, and the staging Supabase project
ref. **Prove the staging DB ref ≠ the production Supabase ref.** Stop if you
cannot distinguish them.

## §3. Capture rollback state

Record, for the staging env only, whether each of `SCORING_CONTRACT`,
`SCORING_PROVIDER`, `SKIP_SCORING_SIDE_EFFECTS` is *missing* / *set to <non-secret
value>* / *set-but-redacted*, plus the current deployment id and a structural
snapshot (not content) of the test call's `analysis_json`. Keep a rollback
checklist. No credentials in the record.

## §4. Enable v2 in staging (stub, no cost)

Set **only in staging** (preserve prior values for rollback):

```
SCORING_CONTRACT=v2
SCORING_PROVIDER=stub
SKIP_SCORING_SIDE_EFFECTS=1
```

Redeploy/restart the staging API only. Do not touch production variables, add
secrets, or change code defaults.

## §5. Trigger + verify one stub score

1. Ensure a **cache miss**: use the dedicated call's unique transcript hash (its
   v2 key lives in the `cachever=v2` + `provider=stub` namespace — it cannot read
   a v1 entry). Do not delete broad caches.
2. Score only the dedicated call. Confirm from safe observability the `[score]
   scored` log shows `contract=v2 provider=stub degraded=true
   degraded_reason=stub_provider`, and that **no** OpenAI/Anthropic call, Slack,
   email or rep-memory side effect occurred (`SKIP_SCORING_SIDE_EFFECTS=1`).
3. Read the call back and run the validator's LIVE lane:

```
STAGING_API_BASE=https://<staging-api> STAGING_CALL_ID=<dedicated> \
STAGING_AUTH_TOKEN=<token-via-env> STAGING_CONFIRMED=1 EXPECTED_API_SHA=7928f52 \
npm run validate:scoring-v2-staging-activation
```

It asserts: v1 projection intact (`overall/summary/4 stages/moments/suggestions/
voice/rubric._meta`), `analysis_json.v2` present + structurally valid (4 ordered
stages, stable ids, valid statuses/scores), honest stub degrade
(`degraded_score=true`, `degraded_reason="stub_provider"`, provider `stub`, model
`stub:v1`, no invented evidence), and v1/v2 provenance agreement. It refuses
production hosts and never prints the token or transcript.

4. Score the **same** call again → prove a **v2 cache hit** restores the full
   `analysis_json.v2` with no network call and no duplicate side effect.

## §6. Verify the Day-268 UI + v1 fallback

Open the staging Call Review page for the dedicated call and confirm the
provisional banner, four stage cards, expandable "Not observed" criteria (never
`0/100`), provider `stub` / contract `v2` provenance, no invented evidence, no
broken timestamp, no fake objection — expand/collapse by mouse + keyboard,
desktop + mobile, no console errors, no horizontal overflow. Confirm a separate
v1-only staging call still renders with **no** v2 banner/criteria.

## §7. Paid-OpenAI boundary (requires explicit approval)

Do **not** run `SCORING_PROVIDER=openai` with a real key in staging without the
user's written approval after seeing: expected model, max calls, estimated max
cost, staging target, fixture call, and rollback plan. Until then, the staging
proof is **stub-only**; mocked-OpenAI is validated with no network via the
Day-267/269 seam validators.

## §8. Rollback

Restore the staging env to the exact pre-Day-270 state: if the three variables
were previously missing, remove the overrides; if they had values, restore those
exact values. Redeploy/restart staging. Verify `resolveScoringContract()` returns
to its prior state (missing/invalid still → `v1`), the provider returns to prior,
production is untouched, and the labelled staging test fixture remains isolated.
Because the v1 path is byte-identical and v2 lives in a separate cache namespace,
rollback is inert — no migration, no cache purge.

## §9. Day-271 hand-off (single approved paid proof)

Once a staging env is confirmed and approval is granted: repeat §4–§6 with
`SCORING_PROVIDER=openai` for **one** score of the dedicated call, capping to a
single call on the expected model, and confirm the persisted `analysis_json.v2`
carries real grounded evidence + a non-degraded confidence, then roll back per §8.
