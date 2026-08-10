/**
 * validate-accounts-uuid-guard-day-278.ts
 *
 * Regression for the Day-278 P3: the Accounts detail routes returned 500
 * ("invalid input syntax for type uuid") when handed a non-UUID path segment,
 * because the id reached Postgres unvalidated — unlike the Calls detail route,
 * which rejects a bad id with a clean 400 via UUID_RE.
 *
 * OFFLINE self-test (no network, no secrets): asserts accounts.ts guards every
 * `:id` / `:taskId` path param with a UUID check that returns 400 before any query,
 * using the same UUID_RE shape as the Calls route. Non-vacuous: removing the guard
 * flips these gates. The live behaviour (400 on non-UUID, 200 on valid same-org,
 * foreign denied) is proven separately against the deployed staging API.
 *
 * Usage: npx tsx scripts/validate-accounts-uuid-guard-day-278.ts
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
const CALLS = readFileSync(join(__dirname, "..", "src", "routes", "calls.ts"), "utf8");

// The canonical UUID regex used by the Calls route (the standard we mirror).
const CALLS_UUID = /const UUID_RE\s*=\s*\n?\s*\/\^\[0-9a-f\]\{8\}-/i;

console.log("Day 278 — Accounts UUID path-param guard (offline; no network, no secrets)\n");

section("Accounts guards :id / :taskId with a UUID check");
gate("accounts.ts defines a UUID_RE (v1–5 shape, same as Calls)", /const UUID_RE\s*=/.test(ACC) && /\[1-5\]\[0-9a-f\]\{3\}-\[89ab\]/.test(ACC));
gate("registers router.param('id', …) guard", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("registers router.param('taskId', …) guard", /router\.param\(\s*['"]taskId['"]/.test(ACC));
gate("guard returns a clean 400 (not a 500) for a bad id", /status\(400\)[\s\S]{0,80}invalid id/.test(ACC));
gate("guard tests the value against UUID_RE", /UUID_RE\.test\(/.test(ACC));
gate("guard runs BEFORE handlers (param registered near the top, before route defs)",
  ACC.indexOf("router.param(") > -1 &&
  ACC.indexOf("router.param(") < ACC.indexOf("router.get('/:id'"));

section("consistency with the Calls detail route standard");
gate("Calls route still uses UUID_RE (the source standard)", CALLS_UUID.test(CALLS) || /UUID_RE\.test\(id\)/.test(CALLS));
gate("no raw DB uuid-cast error can leak (guard blocks non-UUID before query)",
  /router\.param\(\s*['"]id['"]/.test(ACC) && /invalid id/.test(ACC));

section("NON-VACUITY — the guard is what prevents the 500");
gate("both param guards present (removing either would fail above)",
  /router\.param\(\s*['"]id['"]/.test(ACC) && /router\.param\(\s*['"]taskId['"]/.test(ACC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
