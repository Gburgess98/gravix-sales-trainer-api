/**
 * validate-accounts-company-scope-day-277.ts
 *
 * Regression for the Day-277 lighthouse-audit P1: an auth-first manager (created
 * via the supported Admin Auth API + reps bridge, with NO legacy public.users
 * row) got 403 "missing_company_context" on GET /v1/accounts because
 * getRequesterContext() resolved company from `users` only — while the same file's
 * resolveCompanyId() (used by the create handler) and routes/team.ts already fall
 * back to reps.company_id.
 *
 * OFFLINE self-test (no network, no secrets): asserts getRequesterContext still
 * reads `users` first (legacy preserved) AND falls back to the `reps` bridge for
 * company context. Non-vacuity: the reps-fallback markers must be present.
 *
 * Usage: npx tsx scripts/validate-accounts-company-scope-day-277.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const ACC = readFileSync(join(__dirname, "..", "src", "routes", "accounts.ts"), "utf8");
const TEAM = readFileSync(join(__dirname, "..", "src", "routes", "team.ts"), "utf8");

// Isolate the getRequesterContext function body.
const fnMatch = ACC.match(/async function getRequesterContext\([\s\S]*?\n\}/);
const fnBody = fnMatch ? fnMatch[0] : "";

console.log("Day 277 — accounts company-scope reps fallback (offline; no network, no secrets)\n");

section("getRequesterContext resolves company for auth-first identities");
gate("getRequesterContext exists", fnBody.length > 0);
gate("still queries the legacy users table first", /\.from\('users'\)/.test(fnBody) || /\.from\("users"\)/.test(fnBody));
gate("falls back to the reps bridge", /\.from\('reps'\)/.test(fnBody) || /\.from\("reps"\)/.test(fnBody));
gate("reps fallback selects company_id", /reps['"]\)\s*\n?\s*\.select\([^)]*company_id/.test(fnBody) || /\.from\(['"]reps['"]\)[\s\S]{0,120}company_id/.test(fnBody));
gate("returns a company_id from the reps fallback", /company_id:\s*String\(\(repRow/.test(fnBody) || /repRow[\s\S]{0,80}company_id/.test(fnBody));
gate("legacy path preserved (returns userRow when it has company_id)", /if\s*\(\s*userRow\?\.company_id\s*\)\s*return\s+userRow/.test(fnBody));

section("consistency with the rest of the platform");
gate("accounts.ts still defines resolveCompanyId (users→reps)", /async function resolveCompanyId/.test(ACC) && /\.from\(['"]reps['"]\)/.test(ACC));
gate("routes/team.ts already uses the users→reps bridge", /resolveCompanyId/.test(TEAM) && /\.from\("reps"\)/.test(TEAM));

section("NON-VACUITY — the reps fallback is what makes auth-first work");
// If the fallback block were removed, these two would both be false.
const hasFallbackBlock = /\.from\(['"]reps['"]\)/.test(fnBody) && /company_id:/.test(fnBody);
gate("reps-fallback block present in getRequesterContext", hasFallbackBlock);

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
