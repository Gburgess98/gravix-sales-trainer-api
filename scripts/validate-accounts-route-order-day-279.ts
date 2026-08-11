/**
 * validate-accounts-route-order-day-279.ts
 *
 * Regression for the Day-279 defect: the literal GET /v1/accounts/escalations was
 * registered AFTER the generic GET /:id, so Express matched /:id first and the
 * Day-278 UUID guard rejected "escalations" as an invalid id — the literal handler
 * was unreachable.
 *
 * OFFLINE self-test (no network, no secrets): asserts every literal single-segment
 * GET route on the Accounts router is registered BEFORE the generic GET /:id, and
 * that the Day-278 UUID guard is still present. Non-vacuous: moving /escalations
 * back below /:id flips the gate. Live behaviour (reachable /escalations, 400 on
 * bad id, 404 on valid-missing, tenant isolation) is proven against staging.
 *
 * Usage: npx tsx scripts/validate-accounts-route-order-day-279.ts
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

// Position (char index) of the FIRST generic single-segment param GET route, /:id.
const genericIdPos = ACC.search(/router\.get\(\s*['"]\/:id['"]/);
// All GET route registrations with their path + position.
const getRoutes: Array<{ path: string; pos: number }> = [];
const re = /router\.get\(\s*['"](\/[^'"]*)['"]/g;
let m: RegExpExecArray | null;
while ((m = re.exec(ACC)) !== null) getRoutes.push({ path: m[1], pos: m.index });

// A literal single-segment route: "/word", no ":" param, no nested "/".
const isLiteralSingleSegment = (p: string) => /^\/[A-Za-z][A-Za-z0-9_-]*$/.test(p);

console.log("Day 279 — Accounts literal routes precede the generic /:id (offline; no network, no secrets)\n");

section("router order");
gate("generic GET /:id route exists", genericIdPos > -1);
const escalations = getRoutes.find((r) => r.path === "/escalations");
gate("literal GET /escalations exists (exactly once)",
  getRoutes.filter((r) => r.path === "/escalations").length === 1);
gate("GET /escalations is registered BEFORE GET /:id",
  !!escalations && escalations.pos < genericIdPos,
  escalations ? `escalations@${escalations.pos} vs /:id@${genericIdPos}` : "not found");

section("no literal single-segment GET may be shadowed by /:id (general guard)");
const shadowed = getRoutes.filter(
  (r) => isLiteralSingleSegment(r.path) && genericIdPos > -1 && r.pos > genericIdPos
);
gate("no literal single-segment GET is registered after /:id",
  shadowed.length === 0,
  shadowed.map((r) => r.path).join(", "));

section("Day-278 UUID guard preserved");
gate("router.param('id', …) still registered", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("router.param('taskId', …) still registered", /router\.param\(\s*['"]taskId['"]/.test(ACC));
gate("guard still returns 400 'invalid id'", /status\(400\)[\s\S]{0,80}invalid id/.test(ACC));

section("escalations handler is company-scoped (tenant isolation preserved)");
// The handler filters account_escalations by the requester's company_id. Day 279
// also corrected the column name from the non-existent `org_id` to `company_id`.
gate("escalations query scopes company_id to requester.company_id",
  /account_escalations[\s\S]{0,500}\.eq\(\s*['"]company_id['"]\s*,\s*requester\.company_id\s*\)/.test(ACC));
gate("escalations no longer filters a non-existent org_id column",
  !/account_escalations[\s\S]{0,500}\.eq\(\s*['"]org_id['"]/.test(ACC));
gate("escalations 403s without company context",
  /\/escalations[\s\S]{0,300}missing_company_context/.test(ACC) ||
  /router\.get\(\s*['"]\/escalations['"][\s\S]{0,400}missing_company_context/.test(ACC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
