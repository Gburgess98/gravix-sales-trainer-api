#!/usr/bin/env bash
# Validates the Day 106 Tier 2A deliverables: sparring data model hardening.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WEB_ROOT="${WEB_ROOT:-$HOME/Dev/gravix-sales-trainer-web}"
SQL="$ROOT/sql/20260612_sparring_data_model_hardening.sql"
SPARRING="$ROOT/src/routes/sparring.ts"
MANAGER="$ROOT/src/routes/manager.ts"
BACKFILL="$ROOT/scripts/backfill-sparring-data-model.ts"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

# ── Migration ──
[[ -s "$SQL" ]]
check "migration SQL exists" $?

grep -q "add column if not exists assignment_id" "$SQL" && \
grep -q "add column if not exists status" "$SQL" && \
grep -q "add column if not exists completed_at" "$SQL" && \
grep -q "add column if not exists state" "$SQL" && \
grep -q "add column if not exists company_id" "$SQL"
check "sparring_sessions additive columns in SQL" $?

grep -q "add column if not exists turn_score" "$SQL" && \
grep -q "add column if not exists state_snapshot" "$SQL"
check "sparring_turns additive columns in SQL" $?

grep -q "drop column if exists" "$SQL"
check "rollback block documented" $?

# ── Backfill ──
[[ -s "$BACKFILL" ]]
check "backfill script exists" $?

grep -q '"db:backfill-sparring"' "$ROOT/package.json"
check "db:backfill-sparring npm script exists" $?

grep -q -- "--apply" "$BACKFILL" && grep -q "dry run" "$BACKFILL"
check "backfill is dry-run by default with --apply" $?

grep -q "Dead assignment links" "$BACKFILL" && grep -q "Missing tenant links" "$BACKFILL"
check "backfill logs link/tenant gap counts" $?

# ── Write paths ──
grep -q "sparringHardeningColumns" "$SPARRING"
check "write paths probe hardening columns (fail-soft)" $?

grep -q 'payload.status = "active"' "$SPARRING" && grep -q "payload.assignment_id" "$SPARRING"
check "session create sets assignment/tenant/status/state" $?

grep -q "turn_score: structuredScore" "$SPARRING" && grep -q "state_snapshot: finalBrainState" "$SPARRING"
check "turn handler writes turn_score/state_snapshot" $?

grep -q 'completeUpdate.status = "completed"' "$SPARRING" && grep -q "completeUpdate.completed_at" "$SPARRING"
check "complete endpoint sets status/completed_at/tenant" $?

# ── Manager endpoint ──
grep -q '"/sparring-sessions"' "$MANAGER" && grep -q "cols.sessions" "$MANAGER"
check "manager endpoint prefers columns with Day 105 fallback" $?

# ── Web still wired (untouched but verify) ──
grep -q "'/v1/manager/sparring-sessions?days=30&limit=5'" "$WEB_ROOT/src/app/coaching/page.tsx" 2>/dev/null
check "Day 105 Recent Sparring web fetch intact" $?

# ── Stale artefacts ──
if ls "$ROOT/src/sparring/"*.js >/dev/null 2>&1; then
  check "no stale .js artefacts in src/sparring/" 1
else
  check "no stale .js artefacts in src/sparring/" 0
fi

# ── Earlier validations ──
bash "$ROOT/scripts/validate-tier-2a-day-104.sh" >/dev/null 2>&1
check "Day 104 validation still passes" $?

bash "$ROOT/scripts/validate-tier-2a-day-105.sh" >/dev/null 2>&1
check "Day 105 validation still passes" $?

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 106 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 106 validation PASSED"
echo "NOTE: run sql/20260612_sparring_data_model_hardening.sql in the Supabase SQL editor,"
echo "      then 'npm run db:backfill-sparring -- --apply' to populate the new columns."
