#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:4000}"
TOKEN="${TOKEN:-}"

echo "== Smoke: health =="
curl -fsS "${API_BASE}/v1/debug/health" >/dev/null
echo "OK"

if [[ -n "${TOKEN}" ]]; then
  echo "== Smoke: auth =="
  curl -fsS -H "Authorization: Bearer ${TOKEN}" "${API_BASE}/v1/debug/auth" >/dev/null
  echo "OK"

  echo "== Smoke: db =="
  curl -fsS -H "Authorization: Bearer ${TOKEN}" "${API_BASE}/v1/debug/db" >/dev/null
  echo "OK"
else
  echo "SKIP auth/db (TOKEN not set). Run:"
  echo "  TOKEN='your_token' API_BASE='http://localhost:4000' bash scripts/smoke.sh"
fi

echo "✅ API smoke passed"