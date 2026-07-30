# Scoring v2 Runtime (Day 267)

**Type:** API runtime + no-cost proof. **Depends on:** Day 264 contract
(`WEB SCORING_OUTPUT_CONTRACT_V2.md`), Day 265 golden dataset, Day 266 harness.
UK spelling.

**What Day 267 delivers:** the criteria-level Scoring v2 runtime as an **additive,
self-contained module** (`src/lib/scoringV2.ts`) plus a no-cost validator
(`scripts/validate-scoring-output-v2-day-267.ts`, `npm run validate:scoring-output-v2`)
that proves the full pipeline against the Day-266 harness:

```
mocked provider JSON → parseAndValidateScoreV2 → validated ScoreV2
                     → projectScoreV2ToV1 → Day-266 harness gates
```

**What Day 267 does NOT do (by design):**

- It does **not** change production scoring. `scoreWithLLM` (src/lib/scoring.ts)
  is byte-for-byte untouched; it still returns the v1 shape from the OpenAI
  default path (Day 221/222 byte-identical guarantee intact).
- It does **not** flip the provider default, add Claude scoring, make any paid/
  live LLM call, or change the DB schema.
- Real model **semantic** quality is not proven — only shape, determinism and v1
  back-compat. Proving quality needs a separately authorised live run.

---

## Files

| File | Role |
|---|---|
| `src/lib/scoringV2.ts` | The v2 runtime: types, default criteria, id/weight resolution, prompt, cache key, defensive parser, evidence resolution, roll-ups, v1 projection, stub, fallback. Pure/deterministic — no DB/network/SDK. |
| `scripts/validate-scoring-output-v2-day-267.ts` | No-cost validator (`validate:scoring-output-v2`). |

`scoring.ts`, the Day-266 harness (`test/harness/scoring-v2-harness.ts`), the
Day-265 golden dataset and every existing validator are **unchanged**.

## The runtime entrypoint that WILL change on adoption

The production entrypoint is `scoreWithLLM(opts)` in `src/lib/scoring.ts`. When
v2 is adopted (a later day, with the Day-268 UI), the changes there are:

1. resolve the criteria spec: `resolveCriteriaSpec(resolvedScorecard)`;
2. build the v2 prompt (`buildScoringV2Prompt`) and key the cache with
   `buildScoreCacheKeyV2` (v2 namespace — see Cache versioning);
3. on the OpenAI path, run the raw response through `parseAndValidateScoreV2`;
   on `SCORING_PROVIDER=stub` use `buildStubScoreV2`; on failure use
   `buildFallbackScoreV2`;
4. persist the v2 object under `analysis_json.v2` and the v1 projection
   (`projectScoreV2ToV1`) exactly where the v1 shape lands today.

Day 267 delivered steps 1–4 as pure functions. **Day 269 wired them into
`scoreWithLLM` behind the off-by-default `SCORING_CONTRACT` switch** — see the
Day-269 section below.

---

## Day 269 — off-by-default production wiring

`scoreWithLLM` now selects the OUTPUT contract from `SCORING_CONTRACT`, a switch
**orthogonal** to `SCORING_PROVIDER`. **No environment is enabled**; the default
is `v1` and rollback is one env value.

### `SCORING_CONTRACT` values
- `v2` (exact, case/space-insensitive) → the v2 contract.
- unset · empty · `v1` · any other value (`true`, `enabled`, `latest`, `v3`, …)
  → `v1`. No aliases. `resolveScoringContract()` never throws.

### Off (`v1`) — the default, byte-identical
Every v1 line is re-added verbatim inside a guard that is false when the contract
is `v1`: the cache key is `buildDeterministicPromptKey` with identical args (a
frozen-key gate asserts byte-identity), the v1 stub/OpenAI branches and
`parseAndValidateScoreResponse` are unchanged, `scoringModelVersion` is the same
v1 expression, and persistence writes the same `analysis_json`/`rubric._meta` and
caches the same `result: parsed`. No `analysis_json.v2`, no v2 `_meta`.

### On (`v2`)
1. cache key → `buildScoreCacheKeyV2` (namespace `cachever=v2`, v2 prompt/rubric
   markers, Day-262 `provider=stub` segment still stacks);
2. resolve criteria via `resolveCriteriaSpec` (custom snapshot → custom criteria;
   built-in → `gravix-default-criteria-v1`; empty/malformed → degraded
   `insufficient_evidence`);
3. provider stays selected by `SCORING_PROVIDER`: `openai` builds the v2 prompt
   (`buildScoringV2Prompt`, `response_format: json_object`) and parses the raw
   through `parseAndValidateScoreV2`; `stub` uses `buildStubScoreV2` (no call);
4. deterministic roll-ups + `projectScoreV2ToV1` produce the v1-shaped result;
5. persist: the four v1 top-level fields untouched **plus** the full `ScoreV2`
   under `analysis_json.v2`, with `rubric._meta` stamped to agree
   (`contract_version`, `rubric_version=v2`, `prompt_version=scoring-prompt-v2`,
   `cache_key_version=v2`, confidence, degraded).

All of steps 1–5 run through one exported, hermetic seam,
`computeScoringV2Result(...)` (pure: no network, no DB) — the same function the
Day-269 validator drives with mocked raw JSON and `SCORING_PROVIDER=stub`.

### Provider ⟂ contract (independent dimensions)
`provider ∈ {openai, stub}` × `contract ∈ {v1, v2}`. `SCORING_CONTRACT` never
changes the provider; Claude is not added; no provider default changes.

### v2 failure handling (honest degradation)
Malformed/ungrounded model output or an OpenAI network failure → the Day-267
fallback with `degraded_score=true` and a specific reason
(`invalid_model_output`), never a result that looks fully model-scored. No
transcript → `no_transcript`. Invalid v2 is never persisted as a real score.

### Cache isolation
v1 keys are byte-identical (no `cachever`); v2 can never read a v1 entry; openai-v2
≠ stub-v2; custom-scorecard and context-versioned v2 each get their own namespace.
A v2 cache entry stores `{ ...v1Projection, v2: ScoreV2 }` so a cache hit restores
both. No manual purge — the version bump self-isolates.

### Side effects
Unchanged: 3 `updateCallScoreRow` / 2 `writeScoreCache` sites (no extra write from
the projection), `SKIP_SCORING_SIDE_EFFECTS` still gates Slack/email/rep-memory,
and stub/mocked validation touches neither DB nor network.

### Observability
One structured `[score] scored` log per run: `contract`, `provider`,
`prompt_version`, `rubric_version`, `cache_key_version`, `degraded`,
`degraded_reason`. No transcript text, no evidence quotes, no secrets.

### Validation
`npm run validate:scoring-v2-production-wiring`
(`scripts/validate-scoring-v2-production-wiring-day-269.ts`, no network/DB/paid)
— resolver, v1 byte-identity, 6-way cache isolation, provider⟂contract, the
production seam (stub + mocked OpenAI through the Day-266 gates), honest
degradation, the persisted shape + Day-268 integration fixture
(`test/fixtures/scoring-v2/production-persisted-day-269.json`), static wiring
invariants, and non-vacuity. Day 265–267 validators run unchanged.

### Rollout / rollback
- **Do not enable globally.** First prove in a controlled local/staging run:
  `SCORING_CONTRACT=v2 SCORING_PROVIDER=stub` (no cost), then
  `SCORING_CONTRACT=v2 SCORING_PROVIDER=openai` against **mocked** responses.
- A **real paid OpenAI** proof requires explicit approval (Day 270).
- **Rollback** = unset `SCORING_CONTRACT` or set `SCORING_CONTRACT=v1`. Because the
  v1 path is byte-identical and v2 lives in a separate cache namespace, rollback is
  inert — no migration, no cache purge, no provider change.

### Day 270 hand-off
Controlled staging activation proof: enable `SCORING_CONTRACT=v2` in a single
staging environment, score a known call with the stub then a mocked/real
(approved) OpenAI response, and confirm end-to-end that `analysis_json.v2`
persists and the Day-268 UI renders it — without touching production defaults.

---

## Scoring v2 types & storage location

The contract types (Day 264 §3) are implemented verbatim in `scoringV2.ts`:
`ContractVersion`, `CriterionStatus`, `EvidenceQuote`, `CriterionResult`,
`ObjectionMatch`, `StageResultV2`, `ScoreV2`, `Confidence`, `ScoreV2Provenance`,
plus `ScoreV1Projection` and `ScoreV2WithProjection`.

Fixed stage order is **intro → discovery → objection → close** (a single
`STAGES_V2` constant; no second stage-name set exists). Top-level v2 fields:
`contract_version:"v2"`, `overall_score`, `summary`, `stages[]`,
`objection_matches[]`, `confidence`, `degraded_score`, `degraded_reason`,
`voice`, `provenance`, `trend_delta`.

**Storage (no migration):** persist the v2 object under `analysis_json.v2`; the
v1 projection populates the existing top-level `analysis_json`/`rubric` shape.
v1 readers see v1; v2-aware readers opt into `analysis_json.v2`. `score_cache`
needs no schema change — only the key/version bump below.

## Default criteria set (built-in Gravix rubric)

`GRAVIX_DEFAULT_CRITERIA` (version `gravix-default-criteria-v1`): **one criterion
per stage** — the established product terminology from the UFC scorecard and the
Day-265 golden set (no invented taxonomy):

- intro — *Set agenda and establish credibility*
- discovery — *Uncover pain, current process and decision route*
- objection — *Isolate the objection and reframe value*
- close — *Secure clear next step and commitment*

Each carries label, stage, description, emphasis, `pass_fail`, `critical`, a
deterministic `criterion_id`, and a numeric weight. Built-in stage weights are
25/25/25/25 (matching `scorecardStudio.GRAVIX_DEFAULT_RUBRIC`). Custom scorecards
supply their own criteria from the activation snapshot (`resolveCriteriaSpec`).

## Criterion-id policy (Day 264 §4.1)

`resolveCriterionId`: prefer an authored persistent id if one ever exists;
otherwise derive `"<scorecard_version_id>:<stage>:<slug(label)>"` for custom/
company_default scorecards and `"gravix_default:<stage>:<slug(label)>"` for the
built-in rubric. Stable across re-scores of the same version, no DB change, no
random UUIDs. Duplicate/absent/tampered ids fail validation.

## Weight policy (Day 264 §4.2 — convention chosen)

**Criterion weights sum to 100 within a stage; the four stage weights sum to
100.** Authored per-criterion weights are used when every criterion in a stage
carries one; otherwise weights are distributed evenly with deterministic rounding
(`distributeWeights`) so the total is exactly 100 (e.g. 3 → 34/33/33). The two
conventions are never mixed.

## Prompt / rubric / cache versioning (Day 264 §6)

The v2 lane uses **new** version constants — the v1 exports in `scoring.ts`
(`RUBRIC_VERSION="v1"`, `SCORING_PROMPT_VERSION="v1"`) are untouched, so the v1
production path stays byte-identical:

| Marker | v1 (unchanged) | v2 |
|---|---|---|
| prompt version | `v1` | `scoring-prompt-v2` (`SCORING_PROMPT_VERSION_V2`) |
| rubric version | `v1` | `v2` (`RUBRIC_VERSION_V2`) |
| cache namespace | (no `cachever` token) | `cachever=v2` (`CACHE_KEY_VERSION_V2`) |

`buildScoreCacheKeyV2` guarantees:

- a v2 key can never equal a v1 key (distinct `cachever`/`rubric`/`prompt`/`model`
  tokens) — **no manual cache deletion required**;
- stub-v2 keys carry `provider=stub` and can never collide with openai-v2 keys —
  Day 262 provider isolation stacks with the version bump;
- the openai/default v2 key has no `provider=` segment (byte-clean default).

## Evidence-grounding policy (Day 267 §10/§11)

- A quote is **grounded** iff it is a whitespace-normalised substring of the full
  transcript AND (when a segment is cited) of that segment's text
  (`quoteIsGrounded`). The stored quote stays **byte-verbatim** — normalisation is
  used only for the match test, never to rewrite the quote.
- `resolveEvidenceItem` fills `start_sec`/`end_sec`/`speaker`/`segment_index` from
  the cited segment; when the transcript has no timestamps the spans are `null`
  but `segment_index` remains a valid span indicator. Timestamps are never
  fabricated.
- **Invented evidence is dropped**, never presented as fact. An observed
  criterion left with zero grounded quotes makes the response unusable →
  `InvalidModelOutputError` → the caller falls back to a degraded-but-valid score.

## Model-output rejection / degradation policy (Day 267 §10)

`parseAndValidateScoreV2` trusts nothing from the model. It rejects (throws
`invalid_model_output:<why>`) on: wrong `contract_version`, invented/duplicate/
missing stage, invented/missing criterion, invalid status, a score on
`not_observed`, an observed criterion with no score, ungrounded evidence, a
`partial`/`fail` with no `why_points_lost`, an empty summary, or a stage-weight
sum ≠ 100. Deterministic values — `criterion_id`, weights, stage/overall
roll-ups, `points_lost`, provenance, `trend_delta` — are **computed by the
runtime**, never read from the model (the validator plants a bogus
`overall_score` and `points_lost` and proves they are ignored). When the whole
response is unusable the caller uses `buildFallbackScoreV2` (degraded, valid v1
projection) instead of crashing.

## Deterministic roll-up policy (Day 267 §12)

- **Stage score** = weighted average of *observed* criteria (weights sum to 100),
  rounded; all-`not_observed` → `null`.
- **Stage status** = worst-wins over observed criteria (fail ▸ partial ▸ pass);
  all-`not_observed` → `not_observed`.
- **Overall** = weighted average over *observed* stages, re-normalised by their
  stage-weight sum, rounded; no observed stages → 0.
- **points_lost** = `stageWeight × (100 − score) × criterionWeight ÷ 10000`,
  rounded, for `partial`/`fail` only.

The model-authored `overall_score` is discarded; the runtime value is
authoritative.

## v1 projection policy (Day 267 §13/§14) — hybrid

`projectScoreV2ToV1` is a pure function producing every current v1 field
(`overall`, `summary`, four stage `{score,notes}`, `moments[]`, `suggestions[]`,
`voice`, `rubric` incl. `rubric._meta`). Policy (`projectionPolicyFor`):

- **custom_criteria_authoritative** (custom / company_default scorecards): the
  criteria-weighted v2 stage score IS the v1 stage score.
- **default_v1_parity** (built-in Gravix default): the existing v1 stage score
  stays authoritative and criteria are descriptive; when no prior v1 score is
  supplied it falls back to the v2 roll-up.

`not_observed` stages project the overall score into the v1 stage score (so v1
readers always see a number) with a "Not assessed on this call." note. `_meta`
keeps every existing v1 key unchanged and adds v2 keys (`contract_version`,
`confidence`, `degraded_score`, `criteria_count`, `cache_key_version`,
`criteria_version`, `stage_score_projection`).

**Harness alignment:** the Day-265 golden set is authored from the UFC (custom)
scorecard, so the Day-266 harness constant `STAGE_SCORE_PROJECTION` stays
`authoritative_v2` — it exercises the custom path, which the runtime treats as
`custom_criteria_authoritative`. The `default_v1_parity` branch governs only the
built-in rubric path, which the golden set does not exercise; the harness
constant is therefore left unchanged (not changed silently).

## Stub & heuristic behaviour (Day 267 §15/§16)

- `buildStubScoreV2` (SCORING_PROVIDER=stub): structurally valid v2, four stages,
  one `not_observed` criterion each, `overall 0`, `degraded_score=true`,
  `degraded_reason="stub_provider"`, `scoring_provider="stub"`,
  `scoring_model="stub:v1"`, no evidence, no network. Not made to look
  intelligent.
- `buildFallbackScoreV2(reason)`: same honest degraded shape with
  `scoring_model="heuristic:v1"` and a specific reason —
  `heuristic_fallback | no_transcript | invalid_model_output | insufficient_evidence`.

## Proof (all no-cost — `npm run validate:scoring-output-v2`)

Lane A mocked-provider JSON → runtime parse → ScoreV2 → v1 projection → Day-266
harness (5/5 calls, 20/20 criteria, 100% status/band/evidence/objection). Lane B
honest stub + fallback. Lane C cache v2 isolation (v1 ≠ openai-v2 ≠ stub-v2).
Lane D determinism (ids, weights, roll-ups, provenance, prompt). Lane E
non-vacuity — 22 planted violations, each caught by a named check. Lane F v1
regression. Self-check: the runtime opens no DB/network/SDK handle.

## Rollback

Since Day 269, `scoreWithLLM` is wired but **off by default** — the primary
rollback is operational: unset `SCORING_CONTRACT` (or set it to `v1`). The v1 path
is byte-identical and v2 uses a separate cache namespace, so this is inert — no
migration, no cache purge, no provider change (see the Day-269 section). To remove
the code entirely, `git revert` the Day-267 + Day-269 commits; nothing in the DB
schema changed.
