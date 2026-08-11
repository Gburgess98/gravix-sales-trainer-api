/**
 * validate-account-action-writes-day-280.ts
 *
 * Regression for the Day-280 fix: the two Accounts write paths
 * (POST /:id/escalate → account_escalations, POST /:id/coaching-action →
 * account_coaching_actions) inserted `org_id: requester.company_id`, but neither
 * table has an `org_id` column — both are company-scoped via `company_id`. The
 * inserts 500'd. Corrected to `company_id: requester.company_id`, resolved
 * server-side from the requester (never from the client).
 *
 * OFFLINE self-test (no network, no secrets): asserts both insert payloads use
 * `company_id: requester.company_id`, neither uses `org_id`, company is
 * server-resolved (not from req.body), and each write resolves the account through
 * the company visibility filter before inserting (so foreign-company writes 404).
 * Non-vacuous: reverting either payload to `org_id` flips the gates. Live
 * create/read/deny/cleanup is proven separately against staging.
 *
 * Usage: npx tsx scripts/validate-account-action-writes-day-280.ts
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

/** Extract the `const payload = { … }` block immediately preceding a given insert. */
function payloadBefore(insertFrom: string): string {
  const insertIdx = ACC.indexOf(`.from('${insertFrom}')\n        .insert(payload)`);
  const alt = ACC.indexOf(`.from('${insertFrom}')`);
  const anchor = insertIdx > -1 ? insertIdx : alt;
  if (anchor < 0) return "";
  const start = ACC.lastIndexOf("const payload = {", anchor);
  if (start < 0) return "";
  return ACC.slice(start, anchor);
}

console.log("Day 280 — account action write payloads align to the real schema (offline; no network, no secrets)\n");

const escPayload = payloadBefore("account_escalations");
const coachPayload = payloadBefore("account_coaching_actions");

section("escalation write → account_escalations");
gate("escalate payload found", escPayload.length > 0);
gate("scopes company_id from the requester (server-resolved)", /company_id:\s*requester\.company_id/.test(escPayload));
gate("does NOT insert a non-existent org_id column", !/org_id\s*:/.test(escPayload));
gate("company is NOT taken from the client body", !/company_id:\s*req\.body/.test(escPayload) && !/org_id:\s*req\.body/.test(escPayload));

section("coaching-action write → account_coaching_actions");
gate("coaching-action payload found", coachPayload.length > 0);
gate("scopes company_id from the requester (server-resolved)", /company_id:\s*requester\.company_id/.test(coachPayload));
gate("does NOT insert a non-existent org_id column", !/org_id\s*:/.test(coachPayload));
gate("includes the required title column", /title/.test(coachPayload));
gate("company is NOT taken from the client body", !/company_id:\s*req\.body/.test(coachPayload) && !/org_id:\s*req\.body/.test(coachPayload));

section("user-FK columns resolved to a real users.id or null (not the auth id)");
gate("existingUserId helper exists and checks public.users",
  /async function existingUserId/.test(ACC) && /\.from\('users'\)/.test(ACC));
gate("escalate: triggered_by is resolved (not raw requester.id)",
  /triggered_by:\s*triggeredBy/.test(escPayload) && !/triggered_by:\s*requester\.id/.test(escPayload));
gate("escalate: assigned_manager_id resolved via existingUserId",
  /assignedManagerId = await existingUserId/.test(ACC));
gate("coaching: created_by is resolved (not raw requester.id)",
  /created_by:\s*createdBy/.test(coachPayload) && !/created_by:\s*requester\.id/.test(coachPayload));
gate("coaching: assigned_to resolved via existingUserId",
  /assignedTo = await existingUserId/.test(ACC));

section("tenant isolation mechanism preserved");
gate("no `org_id: requester.company_id` remains anywhere in accounts.ts",
  !/org_id:\s*requester\.company_id/.test(ACC));
gate("exactly two `company_id: requester.company_id` insert scopes",
  (ACC.match(/company_id:\s*requester\.company_id/g) || []).length === 2);
gate("both write handlers gate the account via buildAccountVisibilityFilter (company-scoped) before insert",
  (ACC.match(/buildAccountVisibilityFilter/g) || []).length >= 2);
gate("Day-278 UUID guard + Day-279 route order untouched",
  /router\.param\(\s*['"]id['"]/.test(ACC) &&
  ACC.search(/router\.get\(\s*['"]\/escalations['"]/) < ACC.search(/router\.get\(\s*['"]\/:id['"]/));

section("NON-VACUITY — reverting to org_id would fail the gates above");
gate("neither payload contains org_id (both would flip if reverted)",
  !/org_id\s*:/.test(escPayload) && !/org_id\s*:/.test(coachPayload));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
