/**
 * validate-accounts-list-contact-count-day-286.ts
 *
 * Day 286 — the accounts LIST (GET /v1/accounts) counted the disconnected legacy
 * `contacts` table for its per-account contact stat, while the detail (Day 285)
 * reads canonical `crm_contacts.account_id`. This aligns the list to the same
 * source with a single bulk count (no N+1), so list and detail can never disagree.
 *
 * OFFLINE self-test (no network, no secrets): asserts the list stat comes from a
 * bulk `crm_contacts` count keyed by the company-visible account ids, that the
 * per-account legacy `contacts` count is gone from the list handler, and that the
 * count is company-safe + schema-tolerant. Non-vacuous: reverting the list stat to
 * a `contacts` count, or removing the helper, flips gates. Live list/detail
 * agreement across link/relink/unlink is proven separately against staging.
 *
 * Usage: npx tsx scripts/validate-accounts-list-contact-count-day-286.ts
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

/** Body of the GET / list handler (up to the next router.*). */
function listBody(): string {
  const start = ACC.indexOf("router.get('/', async");
  if (start < 0) return "";
  const next = ACC.indexOf("\nrouter.", start + 10);
  return ACC.slice(start, next < 0 ? ACC.length : next);
}

console.log("Day 286 — accounts list contact count canonicalised to crm_contacts (offline)\n");

const list = listBody();

section("bulk canonical counter");
gate("countAccountContacts helper exists and reads crm_contacts", /async function countAccountContacts/.test(ACC) && /countAccountContacts[\s\S]*?\.from\('crm_contacts'\)/.test(ACC));
gate("counts by account_id over the given account ids (bulk .in, no N+1)", /countAccountContacts[\s\S]*?\.select\('account_id'\)[\s\S]*?\.in\('account_id',\s*accountIds\)/.test(ACC));
gate("schema-tolerant (falls back rather than throwing to the client)", /countAccountContacts[\s\S]*?catch \(e\)[\s\S]*?return counts/.test(ACC));

section("list handler uses the canonical count");
gate("list handler found", list.length > 0);
gate("stats.contacts comes from the crm_contacts count map", /contacts:\s*\n?\s*contactCounts\.get\(account\.id\)/.test(list));
gate("count map built from the company-visible account ids", /countAccountContacts\(\s*\n?\s*\(accounts \|\| \[\]\)\.map\(\(a: any\) => a\.id\)/.test(list));

section("legacy per-account `contacts` count removed from the list (no N+1, no drift)");
gate("list handler no longer reads the legacy `contacts` table", !/\.from\('contacts'\)/.test(list));
gate("no per-account contacts count-query remains in the list", !/\.from\('contacts'\)[\s\S]*?count:\s*'exact'/.test(list));

section("company scope preserved");
gate("list still applies buildAccountVisibilityFilter before counting", /buildAccountVisibilityFilter\(query,\s*requester\)/.test(list));

section("detail unchanged (still canonical crm_contacts)");
gate("GET /:id still uses fetchAccountContacts (same source ⇒ agreement)", /fetchAccountContacts\(account\.id\)/.test(ACC));

section("guards preserved");
gate("Day-278 UUID guard (router.param id) untouched", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("Day-279 route order intact (/escalations before /:id)",
  ACC.search(/router\.get\('\/escalations'/) < ACC.search(/router\.get\('\/:id'/));

section("NON-VACUITY — the list must not fall back to the legacy contacts table");
gate("no `.from('contacts')` with an account_id count anywhere the list would use",
  !/\.from\('contacts'\)\s*\n\s*\.select\('id',\s*\{ count: 'exact', head: true \}\)\s*\n\s*\.eq\('account_id',\s*account\.id\),\s*\n\s*\n\s*supa\s*\n\s*\.from\('calls'\)/.test(ACC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
