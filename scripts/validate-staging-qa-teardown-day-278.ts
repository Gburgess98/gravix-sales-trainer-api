/**
 * validate-staging-qa-teardown-day-278.ts
 *
 * Regression for the Day-278 teardown-discipline gap: an audit could finish with a
 * disposable QA identity still present. The `teardown` action now runs `delete`
 * then re-checks presence and exits non-zero if anything remains, using the pure
 * `evaluateTeardown()` verdict.
 *
 * OFFLINE self-test (no network, no secrets): proves the verdict is correct AND
 * non-vacuous — a fully-absent state passes, and every simulated "still present"
 * state FAILS and names what remains. Also confirms the CLI wires the `teardown`
 * action and fails visibly (die) when something remains.
 *
 * Usage: npx tsx scripts/validate-staging-qa-teardown-day-278.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { evaluateTeardown } from "./provision-staging-qa";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

console.log("Day 278 — staging QA teardown discipline (offline; no network, no secrets)\n");

section("absent state passes");
const absent = evaluateTeardown({ authPresent: false, repPresent: false, profilePresent: false });
gate("all-absent → absent:true", absent.absent === true);
gate("all-absent → nothing remaining", absent.remaining.length === 0);

section("NON-VACUITY — any leftover fails and is named");
const authLeft = evaluateTeardown({ authPresent: true, repPresent: false, profilePresent: false });
gate("auth still present → absent:false", authLeft.absent === false);
gate("auth still present → names 'auth identity'", authLeft.remaining.includes("auth identity"));

const repLeft = evaluateTeardown({ authPresent: false, repPresent: true, profilePresent: false });
gate("reps bridge still present → absent:false", repLeft.absent === false && repLeft.remaining.includes("reps bridge"));

const profLeft = evaluateTeardown({ authPresent: false, repPresent: false, profilePresent: true });
gate("profile row still present → absent:false", profLeft.absent === false && profLeft.remaining.includes("profiles row"));

const allLeft = evaluateTeardown({ authPresent: true, repPresent: true, profilePresent: true });
gate("everything present → absent:false, all three named",
  allLeft.absent === false && allLeft.remaining.length === 3);

section("CLI wires the teardown action and fails visibly");
const SRC = readFileSync(join(__dirname, "provision-staging-qa.ts"), "utf8");
gate("Action type includes 'teardown'", /type Action =[^;]*"teardown"/.test(SRC));
gate("usage string lists teardown", /create\|verify\|status\|delete\|teardown/.test(SRC));
gate("teardown runs delete then re-checks presence", /await deleteIdentity\(admin, env\)[\s\S]{0,200}probePresence/.test(SRC));
gate("teardown DIES (exit non-zero) when something remains", /TEARDOWN INCOMPLETE/.test(SRC) && /die\(`TEARDOWN INCOMPLETE/.test(SRC));
gate("create|verify|status|delete all preserved", ["create", "verify", "status", "delete"].every((a) => new RegExp(`action === "${a}"`).test(SRC)));
gate("staging guard still applied before any client work", /guardOrExit\(env\)/.test(SRC) && /assertSafeStagingTarget/.test(SRC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
