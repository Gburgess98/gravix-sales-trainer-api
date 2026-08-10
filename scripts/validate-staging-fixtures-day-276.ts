/**
 * validate-staging-fixtures-day-276.ts
 *
 * Regression for the Day-276 staging fixture-detail inconsistency. The staging DB
 * held DUPLICATE proof calls: the original malformed-UUID ids
 * (00000000-2722-0000-0000-…001/002) AND the cloned valid ids
 * (00000000-2722-4000-8000-…001/002). The library listed both twins; opening the
 * malformed twin failed because GET /v1/calls/:id correctly rejects the invalid
 * UUID, so the detail page showed no scored rubric while the library still showed
 * a score.
 *
 * OFFLINE self-test (default, no network, no secrets):
 *   • every fixture call id in the seed is a VALID UUID (the API's own predicate);
 *   • the seed removes the stale malformed twins (idempotent cleanup);
 *   • the V1 fallback fixture is score 71 with a rubric + analysis_json;
 *   • the :id route still validates UUIDs (so malformed ids can't be served);
 *   • foreign-organisation calls remain inaccessible (tenant-scope rule);
 *   • non-vacuity: the malformed twin ids are proven INVALID.
 *
 * Usage: npx tsx scripts/validate-staging-fixtures-day-276.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { evaluateTenantScope, STAGING_TENANT } from "./provision-staging-qa";

// The exact UUID predicate the API applies on GET /v1/calls/:id.
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const SEED = readFileSync(join(__dirname, "..", "sql", "seed", "staging-day272-fixtures.sql"), "utf8");
const CALLS = readFileSync(join(__dirname, "..", "src", "routes", "calls.ts"), "utf8");

// Ids
const VALID_V1 = "00000000-2722-4000-8000-000000000002";
const VALID_V2 = "00000000-2722-4000-8000-000000000001";
const STALE_V1 = "00000000-2722-0000-0000-000000000002";
const STALE_V2 = "00000000-2722-0000-0000-000000000001";

console.log("Day 276 — staging fixture integrity (offline self-test; no network, no secrets)\n");

// 1. UUID validity of the fixtures the app must open.
section("fixture UUIDs are valid (openable via GET /v1/calls/:id)");
gate("valid V1-fallback fixture id passes UUID predicate", UUID_RE.test(VALID_V1));
gate("valid V2-stub fixture id passes UUID predicate", UUID_RE.test(VALID_V2));

// 2. Non-vacuity: the stale twins are INVALID (why they were un-openable).
section("NON-VACUITY — the stale twins are malformed UUIDs");
gate("stale V1 twin id is an INVALID UUID", !UUID_RE.test(STALE_V1));
gate("stale V2 twin id is an INVALID UUID", !UUID_RE.test(STALE_V2));

// 3. Seed removes the stale twins and no longer references them as live rows.
section("seed removes the stale duplicate twins (idempotent)");
gate("seed deletes stale calls twins", new RegExp(`delete\\s+from\\s+public\\.calls[\\s\\S]{0,200}${STALE_V1}`, "i").test(SEED));
gate("seed deletes stale call_scores twins", new RegExp(`delete\\s+from\\s+public\\.call_scores[\\s\\S]{0,200}${STALE_V1}`, "i").test(SEED));
gate("seed does NOT insert a call at a stale twin id", !new RegExp(`insert[\\s\\S]*?values\\s*\\(\\s*'${STALE_V1}'`, "i").test(SEED));

// 4. Every call id the seed INSERTs is a valid UUID.
section("every seeded call id is a valid UUID");
const insertedCallIds = Array.from(SEED.matchAll(/into\s+public\.calls[\s\S]*?values\s*\(\s*'([0-9a-fA-F-]{36})'/gi)).map((m) => m[1]);
gate("seed inserts at least the two proof calls", insertedCallIds.length >= 2, `found ${insertedCallIds.length}`);
gate("all seeded call ids are valid UUIDs", insertedCallIds.every((id) => UUID_RE.test(id)), insertedCallIds.filter((id) => !UUID_RE.test(id)).join(", ") || "all valid");

// 5. V1 fallback fixture integrity (score 71 + rubric + analysis_json).
section("V1 fallback fixture is score 71 with a rubric");
gate("V1 fixture declares score_overall 71", /71,\s*'gpt-4o-mini:v1:v1'/.test(SEED));
gate("V1 fixture analysis_json overall is 71", /'overall',\s*71/.test(SEED));
gate("V1 fixture carries a rubric with stages", /'intro',\s*jsonb_build_object\('score'/.test(SEED));

// 6. The :id route still rejects malformed UUIDs (so twins can't be served).
section("API still validates the :id route UUID");
gate("calls.ts validates :id with a UUID predicate", /UUID_RE\.test\(id\)/.test(CALLS) && /return res\.status\(400\)\.json\(\{ ok: false, error: "invalid id" \}\)/.test(CALLS));

// 7. Foreign-organisation records remain inaccessible (tenant-scope rule).
section("tenant isolation preserved");
const REQ = "11111111-2222-4333-8444-555555555555";
gate("same-org member can read the org's fixture call",
  evaluateTenantScope({ requester: REQ, requesterOrgId: STAGING_TENANT.orgId, callUserId: "other", callOrgId: STAGING_TENANT.orgId }));
gate("foreign-org call is NOT accessible",
  !evaluateTenantScope({ requester: REQ, requesterOrgId: STAGING_TENANT.orgId, callUserId: "other", callOrgId: "00000000-9999-4000-8000-000000009999" }));
gate("requester with no resolvable org cannot read the call",
  !evaluateTenantScope({ requester: REQ, requesterOrgId: null, callUserId: "other", callOrgId: STAGING_TENANT.orgId }));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
