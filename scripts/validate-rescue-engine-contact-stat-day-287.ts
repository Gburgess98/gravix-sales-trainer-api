/**
 * validate-rescue-engine-contact-stat-day-287.ts
 *
 * Day 287 — `GET /v1/accounts/:id/rescue-engine` was the last account-domain read
 * of the disconnected legacy `contacts` table (its `stats.contacts`). Aligns it to
 * canonical `crm_contacts.account_id` via the shared Day-286 helper, matching the
 * list and detail, without changing the rescue intelligence contract.
 *
 * OFFLINE self-test (no network, no secrets): asserts the rescue handler counts
 * contacts via the shared `countAccountContacts` (canonical crm_contacts), no
 * longer reads legacy `contacts`, keeps the account visibility gate (foreign →
 * 404), preserves the risk contract (which never depended on the contact count),
 * and that NO legacy `contacts` read remains anywhere in accounts.ts. Non-vacuous:
 * reverting the rescue count to a `contacts` read flips gates. Live rescue/list/
 * detail agreement runs separately against staging.
 *
 * Usage: npx tsx scripts/validate-rescue-engine-contact-stat-day-287.ts
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

function body(anchor: string): string {
  const start = ACC.indexOf(anchor);
  if (start < 0) return "";
  const next = ACC.indexOf("\nrouter.", start + anchor.length);
  return ACC.slice(start, next < 0 ? ACC.length : next);
}

console.log("Day 287 — rescue-engine contact stat canonicalised to crm_contacts (offline)\n");

const rescue = body("router.get('/:id/rescue-engine'");

section("rescue-engine uses the canonical shared count");
gate("handler found", rescue.length > 0);
gate("counts contacts via the shared countAccountContacts helper (no duplication)",
  /countAccountContacts\(\[account\.id\]\)/.test(rescue));
gate("stats.contacts reads the canonical count map", /contacts:\s*contactCountMap\.get\(account\.id\)/.test(rescue));
gate("rescue handler no longer reads the legacy `contacts` table", !/\.from\('contacts'\)/.test(rescue));

section("contract + risk logic preserved (contact count was never a risk input)");
gate("response still exposes stats.contacts", /stats:\s*\{[\s\S]*?contacts:/.test(rescue));
const churnIncrementLines = rescue.split("\n").filter((l) => /churnRiskScore\s*\+=/.test(l));
gate("churn risk still derived from owner/score/activity, never the contact count",
  /riskBand/.test(rescue) && churnIncrementLines.length >= 3 && churnIncrementLines.every((l) => !/contact/i.test(l)),
  `${churnIncrementLines.length} increment lines`);
gate("rescue recommendations + next best action unchanged in shape",
  /ai_rescue_recommendations/.test(rescue) && /next_best_action/.test(rescue) && /automated_escalation/.test(rescue));

section("scope + guards preserved");
gate("account gated by buildAccountVisibilityFilter (foreign → 404)",
  /buildAccountVisibilityFilter/.test(rescue) && /account_not_found/.test(rescue));
gate("no per-account N+1 contacts query reintroduced", !/\.from\('contacts'\)[\s\S]*?count:\s*'exact'/.test(rescue));

section("whole-file: no legacy account-domain contacts reads remain");
gate("accounts.ts has ZERO `.from('contacts')` reads (all canonical crm_contacts)",
  !/\.from\('contacts'\)/.test(ACC));
gate("shared helper reads crm_contacts by account_id", /async function countAccountContacts[\s\S]*?\.from\('crm_contacts'\)[\s\S]*?\.in\('account_id',\s*accountIds\)/.test(ACC));

section("Day 285/286 canonical reads still in place");
gate("detail still uses fetchAccountContacts", /fetchAccountContacts\(account\.id\)/.test(ACC));
gate("list still uses countAccountContacts over the visible ids",
  /countAccountContacts\(\s*\n?\s*\(accounts \|\| \[\]\)\.map\(\(a: any\) => a\.id\)/.test(ACC));

section("guards preserved");
gate("Day-278 UUID guard untouched", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("Day-279 route order intact", ACC.search(/router\.get\('\/escalations'/) < ACC.search(/router\.get\('\/:id'/));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
