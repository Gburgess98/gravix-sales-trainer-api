#!/usr/bin/env bash
# Validates the Day 101 Tier 2A deliverables: state manager + provider router.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE="$ROOT/src/sparring/state.ts"
PROVIDERS="$ROOT/src/sparring/providers.ts"
ROUTE="$ROOT/src/routes/sparring.ts"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

[[ -s "$STATE" ]];     check "src/sparring/state.ts exists" $?
[[ -s "$PROVIDERS" ]]; check "src/sparring/providers.ts exists" $?

grep -q "export function createInitialSparringState" "$STATE" 2>/dev/null
check "createInitialSparringState exists" $?

grep -q "export function updateSparringState" "$STATE" 2>/dev/null
check "updateSparringState exists" $?

grep -q "export function inferStageFromText" "$STATE" 2>/dev/null && \
grep -q "export function inferObjectionType" "$STATE" 2>/dev/null && \
grep -q "export function clampState" "$STATE" 2>/dev/null && \
grep -q "export function summariseStateForPrompt" "$STATE" 2>/dev/null
check "stage/objection/clamp/prompt helpers exist" $?

grep -q '"openai"' "$PROVIDERS" 2>/dev/null && grep -q '"claude"' "$PROVIDERS" 2>/dev/null && grep -q '"stub"' "$PROVIDERS" 2>/dev/null
check "providers openai/claude/stub exist" $?

grep -q "SPARRING_PROVIDER" "$PROVIDERS" 2>/dev/null
check "SPARRING_PROVIDER env referenced" $?

grep -q "provider_not_configured" "$PROVIDERS" 2>/dev/null
check "claude returns not_configured (Day 102 pending)" $?

grep -q "'/sessions/:id/messages'" "$ROUTE" 2>/dev/null
check "/sessions/:id/messages alias exists" $?

grep -q "'/sessions/:id/turns'" "$ROUTE" 2>/dev/null
check "existing /sessions/:id/turns route intact" $?

grep -q "generateBuyerReply" "$ROUTE" 2>/dev/null && grep -q "updateSparringState" "$ROUTE" 2>/dev/null
check "route wired to Brain (state + provider router)" $?

grep -q "state: brainState" "$ROUTE" 2>/dev/null
check "state persisted to session meta + returned" $?

# State manager unit assertions (pure, no network)
if npx tsx "$ROOT/scripts/validate-sparring-state.ts" >/dev/null 2>&1; then
  check "sparring state unit assertions pass" 0
else
  check "sparring state unit assertions pass" 1
fi

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 101 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 101 validation PASSED"
