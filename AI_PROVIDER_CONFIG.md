# AI Provider Configuration (cost-safe setup)

Operational guide for how the Gravix API talks to AI providers, how to run it
**without accidentally burning API spend**, and how to avoid the Anthropic
"credit balance too low" trap. Companion to the architecture doc
`GRAVIX_AI_CORE_ARCHITECTURE.md` (WEB) and `SPARRING_ENGINE_EXTRACTION_PLAN.md`
(WEB). UK spelling throughout.

**Scope of truth:** this doc describes what the code does **today**. Anything not
yet implemented is called out explicitly under "Future" — do not treat those as
available.

---

## 1. Billing reality — Claude app ≠ Anthropic API

**These are two separate wallets:**

- **Claude.ai / Claude desktop app paid plan** (Pro/Team/Max) pays for the
  chat *product*. It does **not** grant API access and its balance is invisible
  to the backend.
- **Anthropic Console API billing** (console.anthropic.com) is what the Gravix
  backend uses via `ANTHROPIC_API_KEY`. Backend Claude calls (Sonnet/Opus/Haiku)
  spend **this** credit. If it is empty, calls fail with HTTP 400
  *"Your credit balance is too low to access the Anthropic API"* — exactly what
  Day 259 hit even though a valid key was present.

**Consequence:** having a Claude app subscription does **not** make the Claude
sparring brain work. Backend Claude needs a **Console API key with credit /
billing enabled**. OpenAI is the same model: `OPENAI_API_KEY` bills the OpenAI
platform account.

---

## 2. What is wired today

### 2.1 Sparring Prospect Brain (implemented — Day 258/259)

`src/lib/sparringBrain/` behind the `Brain` interface. Selection is config:

| Env | Meaning | Default |
|---|---|---|
| `SPARRING_BRAIN_PROVIDER` | canonical provider select: `openai` \| `claude` \| `stub` | — |
| `SPARRING_PROVIDER` | legacy alias, honoured if the canonical one is unset | — |
| *(neither set / invalid)* | resolves to **`openai`** | **openai** |
| `OPENAI_SPARRING_MODEL` | OpenAI brain model | `gpt-4o-mini` |
| `ANTHROPIC_SPARRING_MODEL` | Claude brain model | `claude-haiku-4-5-20251001` |

- **Default is OpenAI** and stays that way. Claude is **not** default and must not
  be until live parity + billing are proven (Day 259 verdict).
- The router **degrades to the deterministic `stub`** if the configured provider
  throws (bad key, no credit, outage) — a turn never crashes on provider
  availability.
- `stub` needs **no** API key and returns deterministic buyer lines.

### 2.2 Scoring / Coaching (implemented — pre-existing)

`src/lib/scoring.ts` → `getOpenAI()` (`src/lib/openai.ts`). This is a **separate**
concern from the sparring brain and is configured independently.

| Env | Meaning | Default |
|---|---|---|
| `SCORING_PROVIDER` | scoring provider select: `openai` \| `stub` (Day 261) | `openai` |
| `OPENAI_API_KEY` | required for the live scoring LLM call (and embeddings) | — |
| `AI_MODEL` | **the actual scoring model env** | `gpt-4o-mini` |
| `OPENAI_TIMEOUT_MS` | scoring LLM timeout | `8000` |
| `SKIP_SCORING_SIDE_EFFECTS` | when `=1`, skips a **subset** of scoring persistence side effects | off |

- **`SCORING_PROVIDER` (Day 261, implemented):** `openai` (default) is the
  unchanged production path. `stub` makes **no** paid LLM call and returns a
  deterministic score (`buildStubScore()` → model tag **`stub:v1`**), stamped in
  `rubric._meta.scoring_provider`. Anything other than `stub` (unset, invalid)
  resolves to `openai`, so a misconfiguration never silently disables real
  scoring. **This switch only prevents paid calls** — it does *not* skip DB /
  Slack / assignment side effects; those stay gated by `SKIP_SCORING_SIDE_EFFECTS`
  (see §5).
- Scoring runs `gpt-4o-mini` at `temperature: 0` (deterministic) against a JSON
  schema.
- **No-cost degrade (still present):** even on the `openai` path, if the LLM call
  fails — including when `OPENAI_API_KEY` is missing — scoring falls back to
  `heuristicScoreFallback()` (model tag `heuristic:v1`). `stub` is the *explicit*
  no-cost switch; the heuristic fallback is the *implicit* safety net.
- `callLLM()` in `src/lib/llm.ts` and embeddings (`src/lib/embeddings.ts`) also
  use OpenAI; embeddings need `OPENAI_API_KEY`.

> **Naming note (do not confuse):** the scoring *model* env is **`AI_MODEL`**, not
> `SCORING_MODEL` — there is still no `SCORING_MODEL` env. The scoring *provider*
> env is **`SCORING_PROVIDER`** (`openai`|`stub`, Day 261). There is no
> `SCORING_MODE` env.

### 2.3 No voice providers in brain/scoring

The AI Core voice pipeline (LiveKit / Deepgram / ElevenLabs) is **not built** —
that is Phase 2. There are **no** voice imports in `src/lib/sparringBrain/` or the
scoring code.

> Deepgram *does* appear in `src/routes/whisperer.ts`, but that is the separate,
> pre-existing **Whisperer** live-call STT subsystem — unrelated to the sparring
> brain, scoring, or the Phase-2 voice pipeline. Don't conflate them.

---

## 3. Env matrix (copy-paste profiles)

### A. Product/dev — paid but stable (recommended default)

```
SPARRING_BRAIN_PROVIDER=openai
OPENAI_API_KEY=<OpenAI platform key with billing>
AI_MODEL=gpt-4o-mini
SKIP_SCORING_SIDE_EFFECTS=0
# ANTHROPIC_API_KEY optional — only needed to trial the Claude brain
```

Sparring buyer + scoring both on OpenAI `gpt-4o-mini`. This is today's safe
production baseline.

### B. No-cost QA — deterministic, zero API spend

```
SPARRING_BRAIN_PROVIDER=stub
SCORING_PROVIDER=stub
SKIP_SCORING_SIDE_EFFECTS=1
# OPENAI_API_KEY may be ABSENT
# ANTHROPIC_API_KEY may be ABSENT
```

- Sparring brain → deterministic `stub`, no LLM, no key required.
- Scoring → `SCORING_PROVIDER=stub` returns a deterministic `stub:v1` score with
  **no** paid call and no key required (Day 261) — a clean explicit switch rather
  than failing over to the heuristic.
- `SKIP_SCORING_SIDE_EFFECTS=1` separately suppresses a subset of persistence
  writes. **Note:** `SCORING_PROVIDER=stub` prevents the *paid call*; it does not
  by itself skip DB/side-effect writes — set both for a fully inert QA run.
- **Cache isolation (Day 262):** the score cache key is namespaced by provider —
  a `stub` score is written under a `provider=stub` segment, so stub QA can never
  reuse or overwrite a production (`openai`) cache entry, and vice versa. The
  `openai`/default cache namespace is unchanged (byte-identical).
- The Day-262, Day-261, Day-260 and Day-258 validators all run in this keyless mode.

### C. Future Claude brain trial (behind the flag, not default)

```
SPARRING_BRAIN_PROVIDER=claude
ANTHROPIC_API_KEY=<Anthropic CONSOLE API key with credit/billing enabled>
ANTHROPIC_SPARRING_MODEL=claude-haiku-4-5-20251001   # or a Sonnet id when trialling
# scoring stays on OpenAI, configured separately:
OPENAI_API_KEY=<OpenAI key>
AI_MODEL=gpt-4o-mini
```

Only the *sparring buyer* moves to Claude; scoring stays on OpenAI. Requires
Console credit (see §1). Do **not** promote Claude to default until a live Claude
turn passes `validate:sparring-brain-claude-parity`.

---

## 4. Future (NOT implemented — do not document as live)

- **`SCORING_PROVIDER=stub` — DONE (Day 261); cache isolation DONE (Day 262).**
  A first-class no-cost scoring switch (§2.2) whose results live in a separate
  `provider=stub` cache namespace. A remaining follow-up could let `stub` also
  auto-skip side effects (today it only prevents paid calls; pair it with
  `SKIP_SCORING_SIDE_EFFECTS=1`).
- **`SCORING_MODEL` alias** — today the env is `AI_MODEL`; a rename/alias could
  align naming with `SPARRING_BRAIN_PROVIDER`. Not wired yet.
- **Claude as default sparring brain** — gated on Console billing + live parity.
- **Voice pipeline (Phase 2)** — LiveKit/Deepgram/ElevenLabs behind the
  transport/STT/TTS interfaces. Not started.

---

## 5. Guard

`npm run validate:ai-provider-config` (`scripts/validate-ai-provider-config-day-260.ts`)
statically enforces the cost-safe invariants above: default brain resolves to
OpenAI, the stub runs with no keys, an invalid provider never becomes Claude,
Claude is not default, no voice imports in brain/scoring, scoring default stays
`gpt-4o-mini`, and this doc keeps its billing-separation / no-cost-QA content. It
makes **no paid calls**.

`npm run validate:scoring-provider-stub` (`scripts/validate-scoring-provider-stub-day-261.ts`)
guards the scoring switch: default resolves `openai`, `SCORING_PROVIDER=stub`
yields a deterministic keyless `stub:v1` score with the fixed four-stage shape,
the stub branch makes no paid call, provenance is stamped, and
`SKIP_SCORING_SIDE_EFFECTS` stays the independent side-effect guard. Also
**no paid calls**.

`npm run validate:score-cache-provider-isolation`
(`scripts/validate-score-cache-provider-isolation-day-262.ts`) guards the cache
namespace: the `openai`/default key is byte-identical to the pre-Day-262 key, the
`stub` key differs and carries a `provider=stub` segment, context/scorecard
version segments still differentiate, and an invalid provider cannot collide with
the stub namespace. Pure key construction — **no paid calls**.
