#!/usr/bin/env bash
# Validates the Day 105 Tier 2A deliverables: manager sparring visibility.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WEB_ROOT="${WEB_ROOT:-$HOME/Dev/gravix-sales-trainer-web}"
MANAGER="$ROOT/src/routes/manager.ts"
SPARRING="$ROOT/src/routes/sparring.ts"
COACHING="$WEB_ROOT/src/app/coaching/page.tsx"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

grep -q '"/sparring-sessions"' "$MANAGER" 2>/dev/null
check "GET /v1/manager/sparring-sessions exists" $?

grep -q "router.use(requireManager)" "$MANAGER" 2>/dev/null || grep -q "requireManager" "$MANAGER"
check "manager router gated by requireManager" $?

grep -q "repHierarchy" "$MANAGER" 2>/dev/null && grep -q "isOfficeManager(userContext)" "$MANAGER"
check "rep-hierarchy tenant scoping applied to sparring sessions" $?

grep -q "session_summary" "$MANAGER" 2>/dev/null
check "items built from meta.session_summary with fallbacks" $?

# WEB wiring
grep -q "'/v1/manager/sparring-sessions?days=30&limit=5'" "$COACHING" 2>/dev/null
check "WEB /coaching fetches manager sparring sessions" $?

grep -q '"Recent Sparring"' "$COACHING" 2>/dev/null
check "WEB Recent Sparring card present" $?

grep -q "No completed sparring sessions yet." "$COACHING" 2>/dev/null && \
grep -q "Could not load recent sparring." "$COACHING"
check "WEB empty + error states present" $?

# Earlier flows intact
grep -q '"/sessions/:id/complete"' "$SPARRING" 2>/dev/null
check "Day 104 complete endpoint intact" $?

grep -q "pendingMetaPatch.turn_scores" "$SPARRING" 2>/dev/null
check "Day 103 turn scoring intact" $?

if ls "$ROOT/src/sparring/"*.js >/dev/null 2>&1; then
  check "no stale .js artefacts in src/sparring/" 1
else
  check "no stale .js artefacts in src/sparring/" 0
fi

bash "$ROOT/scripts/validate-tier-2a-day-104.sh" >/dev/null 2>&1
check "Day 104 validation still passes" $?

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 105 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 105 validation PASSED"
