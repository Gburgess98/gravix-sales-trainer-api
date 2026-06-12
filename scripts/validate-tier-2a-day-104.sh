#!/usr/bin/env bash
# Validates the Day 104 Tier 2A deliverables: session summary + completion.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SUMMARY="$ROOT/src/sparring/summary.ts"
ROUTE="$ROOT/src/routes/sparring.ts"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

[[ -s "$SUMMARY" ]]
check "src/sparring/summary.ts exists" $?

grep -q "export function buildSparringSessionSummary" "$SUMMARY" 2>/dev/null
check "buildSparringSessionSummary exists" $?

grep -q "aggregateTurnScores" "$SUMMARY" && grep -q "selectWeakMoments" "$SUMMARY" && \
grep -q "buildSummaryText" "$SUMMARY" && grep -q "buildNextBestAction" "$SUMMARY"
check "aggregation/weak-moment/text helpers exist" $?

grep -q "recommendSparringDrill" "$SUMMARY" 2>/dev/null && grep -q "Objection Handling Drill" "$SUMMARY"
check "recommended drill mapping exists" $?

grep -q '"/sessions/:id/complete"' "$ROUTE" 2>/dev/null
check "POST /sessions/:id/complete exists" $?

grep -q "session_summary: summary" "$ROUTE" 2>/dev/null && grep -q "summary: summary.summaryText" "$ROUTE"
check "summary persists to summary column + meta.session_summary" $?

grep -q "completeAssignmentsForTarget" "$ROUTE" 2>/dev/null
check "assignment auto-completion preserved" $?

# Stale build artefacts must not shadow TS sources
if ls "$ROOT/src/sparring/"*.js >/dev/null 2>&1; then
  check "no stale .js artefacts in src/sparring/" 1
else
  check "no stale .js artefacts in src/sparring/" 0
fi

npx tsx "$ROOT/scripts/validate-sparring-summary.ts" >/dev/null 2>&1
check "summary unit assertions pass" $?

bash "$ROOT/scripts/validate-tier-2a-day-101.sh" >/dev/null 2>&1
check "Day 101 validation still passes" $?

bash "$ROOT/scripts/validate-tier-2a-day-102.sh" >/dev/null 2>&1
check "Day 102 validation still passes" $?

bash "$ROOT/scripts/validate-tier-2a-day-103.sh" >/dev/null 2>&1
check "Day 103 validation still passes" $?

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 104 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 104 validation PASSED"
