# Scoring v2 Deterministic Harness (Day 266)

Safety rail for Scoring Output Contract v2. It compares a **candidate v2 scoring
result** against the Day-265 human **golden dataset** and the v1 back-compat
projection — deterministically, with **no LLM/paid calls** — so scoring drift is
caught *before* any production runtime change. This is the rail, **not** the
scorer. The real v2 scorer arrives on Day 267.

Companion to `../gravix-sales-trainer-web/SCORING_OUTPUT_CONTRACT_V2.md` (contract)
and `KENDO_SCORING_PARITY_AUDIT.md`. UK spelling.

## Files

| File | Role |
|---|---|
| `test/harness/scoring-v2-harness.ts` | Pure comparison library (no env/DB/network/SDK). |
| `test/harness/build-candidate-results.ts` | One-time **deterministic** builder (no LLM) → materialises candidate fixtures from golden. |
| `test/fixtures/scoring-v2/golden-calls.json` | Day-265 human ground truth (5 calls). |
| `test/fixtures/scoring-v2/candidate-results.json` | Authored "correct" v2 candidate outputs + v1 projections. |
| `scripts/validate-scoring-v2-harness-day-266.ts` | Runner + gate. |

## Command

```bash
npm run validate:scoring-v2-harness
```

Exits non-zero on any gate failure. Makes **no** model calls. `test/` and
`scripts/` are outside `tsconfig` `include`, so this does not affect the API
typecheck baseline.

## Input / output boundary

- **Input:** one golden call + one candidate v2 result (with its `v1_projection`).
- **Output:** a per-call + aggregate report (`CallReport` / `HarnessReport`).
- The comparison is a **pure function**: no environment mutation, no DB, no
  network, no provider SDK, no hidden model call. `buildStubV2Candidate` is a
  pure deterministic transform too.

## Gating rules

**Structural** (fails the gate): `contract_version==="v2"`; four fixed stages
present, unique, in order; every stage has ≥1 criterion; stable `criterion_id`;
valid status; observed criterion has a numeric score; `not_observed` has `null`
score; criterion weights sum to 100 within a stage; stage weights sum to 100;
`partial`/`fail` carry `why_points_lost` **and** numeric `points_lost`; evidence
has a usable span (`segment_index` or `start_sec`); **every evidence quote is a
verbatim transcript substring**; `degraded_score` ⇒ `degraded_reason`; a
stub/heuristic result **must** be `degraded_score:true`; required provenance
present; `confidence.value` numeric.

**Golden agreement** (per criterion): status matches; **score-band** matches
(see policy); evidence grounded + overlaps golden; `why_points_lost`/
`coaching_action`/`suggested_drill` present as expected. Plus overall-band match
and objection-match agreement.

**v1 projection**: `overall`, `summary`, all four `stages.<s>.{score,notes}`,
`moments[]`, `suggestions[]`, `voice`, and `rubric._meta` (with the required v1
meta keys) are present and correctly typed — additive v2 metadata must not remove
or retype any v1 key.

### Score-band policy

A candidate's numeric criterion score **must fall inside the golden expected
band** (`excellent 85–100 · strong 70–84 · mixed 50–69 · weak 30–49 · poor 0–29`).
Exact integers are **not** required. `not_observed` requires a `null` score.

### Evidence-overlap policy

`POLICY.EVIDENCE_OVERLAP = "same_segment_containment"`. Overlap holds when, for at
least one golden evidence item, some candidate evidence item is on the **same
transcript `segment_index`** and the two quotes have a **containment**
relationship (one contains the other; exact match is the trivial case). No
embeddings, no LLM — deterministic and explainable. (The two rejected
alternatives were *exact-match-only* — too brittle — and *same-segment-any-text* —
too loose.)

### v1 stage-score projection policy

`POLICY.STAGE_SCORE_PROJECTION = "authoritative_v2"` — the v1 projection's stage
score **equals** the v2 stage score (criteria roll-up is authoritative). The
Day-266 candidate fixtures use this. `not_observed` stages are exempt from the
parity check and project to a neutral placeholder (`overall_score`, notes
"Not assessed on this call."). **Day 267 may switch the Gravix-default path to
`"v1_parity"`** by changing this one constant — the harness does not silently pick
production behaviour.

### Objection-match policy

Every golden objection must appear in the candidate with matching
`objection_item_key`, `objection_label` and `handled` (`handled|partially|missed`),
with verbatim `detected_text`. **Any candidate objection not in the golden set is
reported as an invented match** — so the no-objection call (`golden-001`) fails if
it receives any objection.

## Lanes

1. **Fixture lane** — authored candidates vs golden. Full structural + golden +
   v1-projection comparison. This is the meaningful gate.
2. **Stub lane** — a deterministic no-cost v2 stub (`buildStubV2Candidate`,
   mirroring a `SCORING_PROVIDER=stub` shape) is run through **structural
   validation + degraded-honesty checks only**. It is **not** proof of scoring
   quality — just that the harness handles the honest degraded shape a stub emits.
   If the *production* stub is ever exercised it must run with
   `SCORING_PROVIDER=stub`, `SKIP_SCORING_SIDE_EFFECTS=1`, no network, no DB
   writes, `degraded_score=true`, `degraded_reason="stub_provider"`.

## What Day 266 proves — and does not

**Proves:** the v2 contract shape is structurally enforceable; a correct candidate
agrees with the human golden set on status, score-band, verbatim evidence,
evidence overlap and objection matches; and the v2→v1 projection preserves every
v1 reader. Non-vacuity is demonstrated — 7 planted violations (wrong status,
out-of-band score, invented quote, missing span, missing `why_points_lost`,
removed v1 stage, invented objection) are each caught.

**Does not prove:** that any real model *understands* these calls. The candidates
are deterministic fixtures authored from the golden set, not model output. No
semantic parity with Kendo is claimed. The Kendo scoring gap stays open until a
real v2 scorer exists.

## Day 267 hand-off

Build the criteria-level v2 runtime output behind the v2 prompt/rubric/cache
version (contract §6), emitting the shape this harness validates, and run it
through this harness against the golden set. Decide the Gravix-default
stage-score projection policy (`authoritative_v2` vs `v1_parity`) by setting
`POLICY.STAGE_SCORE_PROJECTION`. No production default flip until the harness is
green on the real scorer.
