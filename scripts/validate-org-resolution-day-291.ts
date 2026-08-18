/**
 * validate-org-resolution-day-291.ts
 *
 * Regression for the Day-291 correction: a fresh auth-first MANAGER hit
 * GET /v1/admin/org-settings → 403 `no_org` while GET /v1/calls/paged?scope=company
 * → 200. Cause: the admin org-settings handlers resolved the requester's org with a
 * call-owned-only lookup (`calls` row owned by the requester), so a manager who has
 * not yet recorded a call had no org — even though their reps tenant bridge exists.
 * /v1/calls/paged uses the canonical `getRequesterOrgId` (call-owned, THEN reps),
 * so the two endpoints disagreed, and a non-prod DEFAULT_ORG_ID fallback in the
 * calls path masked the mismatch.
 *
 * OFFLINE self-test (no network, no secrets): asserts both admin org-settings
 * handlers resolve org through the SAME canonical `getRequesterOrgId` as the calls
 * endpoint, that the resolver bridges via reps, and that the admin path keeps NO
 * DEFAULT_ORG_ID fallback (so its 200 is honest proof that canonical resolution
 * worked). Non-vacuous: reverting to the call-owned-only lookup flips the gates.
 * Live behaviour (manager 200 + rep policy alignment) is proven against staging.
 *
 * Usage: npx tsx scripts/validate-org-resolution-day-291.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const ADMIN = readFileSync(join(__dirname, "..", "src", "routes", "admin.ts"), "utf8");
const CALLS = readFileSync(join(__dirname, "..", "src", "routes", "calls.ts"), "utf8");
const ACCESS = readFileSync(join(__dirname, "..", "src", "lib", "callAccess.ts"), "utf8");

// Isolate the two LEGITIMATE org-settings handlers (top-level GET + PATCH), which
// sit between the GET registration and the POST /users handler. A separate,
// pre-existing duplicate PATCH nested inside GET /status lives far below and is
// intentionally out of scope here.
const regionStart = ADMIN.indexOf('adminRouter.get("/org-settings"');
const regionEnd = ADMIN.indexOf('adminRouter.post("/users"');
const region = regionStart >= 0 && regionEnd > regionStart ? ADMIN.slice(regionStart, regionEnd) : "";

console.log("Day 291 — canonical requester-org resolution (offline; no network, no secrets)\n");

section("admin org-settings uses the canonical resolver");
gate("admin.ts imports getRequesterOrgId from lib/callAccess",
  /import\s*\{[^}]*\bgetRequesterOrgId\b[^}]*\}\s*from\s*['"]\.\.\/lib\/callAccess['"]/.test(ADMIN));
gate("org-settings region located (GET + top-level PATCH)", region.length > 0);
gate("both handlers resolve org via getRequesterOrgId(requester) (>=2 sites)",
  (region.match(/getRequesterOrgId\(\s*requester\s*\)/g) || []).length >= 2,
  `${(region.match(/getRequesterOrgId\(\s*requester\s*\)/g) || []).length} sites`);
gate("no call-owned-only org lookup remains in the region",
  !/\.from\(\s*["']calls["']\s*\)[\s\S]{0,160}\.eq\(\s*["']user_id["']\s*,\s*requester\s*\)/.test(region));
gate("still fail-closed: 403 no_org when org is unresolved",
  (region.match(/status\(403\)[\s\S]{0,60}no_org/g) || []).length >= 2);
gate("no DEFAULT_ORG_ID env fallback in the admin path (200 is honest proof)",
  !/process\.env\.DEFAULT_ORG_ID/.test(region));

section("canonical resolver bridges via the reps tenant table");
gate("getRequesterOrgId falls back to reps membership",
  /export async function getRequesterOrgId/.test(ACCESS) &&
  /\.from\(\s*["']reps["']\s*\)[\s\S]{0,160}\.eq\(\s*["']id["']\s*,\s*requester\s*\)/.test(ACCESS));

section("policy read and company-call endpoint agree (same resolver)");
gate("/v1/calls/paged resolves org via the same getRequesterOrgId",
  /getRequesterOrgId\(\s*requester\s*\)/.test(CALLS));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
