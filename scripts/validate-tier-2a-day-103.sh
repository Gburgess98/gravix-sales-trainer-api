#!/usr/bin/env bash
# Validates the Day 103 Tier 2A deliverables: structured turn scoring.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCORING="$ROOT/src/sparring/scoring.ts"
ROUTE="$ROOT/src/routes/sparring.ts"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

[[ -s "$SCORING" ]]
check "src/sparring/scoring.ts exists" $?

grep -q "export function scoreSparringTurn" "$SCORING" 2>/dev/null
check "scoreSparringTurn exists" $?

grep -q "scoreClarity" "$SCORING" && grep -q "scoreConfidence" "$SCORING" && \
grep -q "scoreObjectionHandling" "$SCORING" && grep -q "scoreProgression" "$SCORING"
check "four score dimensions exist" $?

grep -q "detectTurnFlags" "$SCORING" && grep -q "buildTurnFeedback" "$SCORING" && \
grep -q "mergeTurnScoreIntoState" "$SCORING"
check "flags/feedback/state-merge helpers exist" $?

grep -q "pendingMetaPatch.turn_scores" "$ROUTE" 2>/dev/null
check "route persists meta.turn_scores (capped)" $?

grep -q "slice(-100)" "$ROUTE" 2>/dev/null
check "turn_scores capped at 100 entries" $?

grep -q "turnScore: structuredScore" "$ROUTE" 2>/dev/null
check "response includes turnScore" $?

grep -q "mergeTurnScoreIntoState(brainState, structuredScore)" "$ROUTE" 2>/dev/null
check "repPerformance updated from structured score" $?

grep -q "micro_scores" "$ROUTE" 2>/dev/null
check "legacy micro_scores kept (backward compatible)" $?

# Stale build artefacts must not shadow TS sources
if ls "$ROOT/src/sparring/"*.js >/dev/null 2>&1; then
  check "no stale .js artefacts in src/sparring/" 1
else
  check "no stale .js artefacts in src/sparring/" 0
fi

# Unit assertions + earlier day validations
npx tsx "$ROOT/scripts/validate-sparring-scoring.ts" >/dev/null 2>&1
check "scoring unit assertions pass" $?

bash "$ROOT/scripts/validate-tier-2a-day-101.sh" >/dev/null 2>&1
check "Day 101 validation still passes" $?

bash "$ROOT/scripts/validate-tier-2a-day-102.sh" >/dev/null 2>&1
check "Day 102 validation still passes" $?

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 103 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 103 validation PASSED"
