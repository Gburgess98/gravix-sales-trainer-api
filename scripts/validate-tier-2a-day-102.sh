#!/usr/bin/env bash
# Validates the Day 102 Tier 2A deliverables: live Claude provider + meta-write fix.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROVIDERS="$ROOT/src/sparring/providers.ts"
ROUTE="$ROOT/src/routes/sparring.ts"

fail=0
check() {
  local label="$1" ok="$2"
  if [[ "$ok" == "0" ]]; then echo "OK    $label"; else echo "FAIL  $label"; fail=1; fi
}

grep -q '"@anthropic-ai/sdk"' "$ROOT/package.json" 2>/dev/null
check "@anthropic-ai/sdk in package.json" $?

grep -q "ANTHROPIC_API_KEY" "$PROVIDERS" 2>/dev/null
check "ANTHROPIC_API_KEY referenced" $?

grep -q "client.messages.create" "$PROVIDERS" 2>/dev/null
check "claude provider implementation exists (messages.create)" $?

grep -q "claude-haiku-4-5" "$PROVIDERS" 2>/dev/null
check "small fast Claude model configured" $?

grep -q "max_tokens: 200" "$PROVIDERS" 2>/dev/null
check "claude max_tokens <= 200" $?

grep -q '"claude"' "$PROVIDERS" 2>/dev/null && grep -q '"openai"' "$PROVIDERS" 2>/dev/null && grep -q '"stub"' "$PROVIDERS" 2>/dev/null
check "router still has openai/claude/stub" $?

grep -q "SPARRING_PROVIDER" "$PROVIDERS" 2>/dev/null
check "SPARRING_PROVIDER referenced" $?

grep -q "provider_not_configured" "$PROVIDERS" 2>/dev/null
check "controlled missing-key behaviour exists" $?

grep -q "toClaudeMessages" "$PROVIDERS" 2>/dev/null
check "claude history windowed/normalised" $?

# Meta-write fix: single final write merging pending micro additions
grep -q "pendingMetaPatch" "$ROUTE" 2>/dev/null
check "double-meta-write fixed (pendingMetaPatch merge)" $?

grep -c 'from("sparring_sessions")' "$ROUTE" >/dev/null 2>&1
PATCH_REFS=$(grep -c "pendingMetaPatch" "$ROUTE")
[[ "$PATCH_REFS" -ge 3 ]]
check "micro additions accumulate into the final write" $?

grep -q "'/sessions/:id/messages'" "$ROUTE" 2>/dev/null
check "/sessions/:id/messages alias intact" $?

# Stale build artefacts must not shadow the TS sources
if ls "$ROOT/src/sparring/"*.js >/dev/null 2>&1; then
  check "no stale .js artefacts shadowing src/sparring/*.ts" 1
else
  check "no stale .js artefacts shadowing src/sparring/*.ts" 0
fi

# Day 101 validation still passes
if bash "$ROOT/scripts/validate-tier-2a-day-101.sh" >/dev/null 2>&1; then
  check "Day 101 validation still passes" 0
else
  check "Day 101 validation still passes" 1
fi

if [[ $fail -ne 0 ]]; then
  echo "Tier 2A Day 102 validation FAILED"
  exit 1
fi
echo "Tier 2A Day 102 validation PASSED"
