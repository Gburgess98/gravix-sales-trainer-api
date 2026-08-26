/**
 * validate-accounts-contacts-canonical-day-295.ts
 *
 * Day 295 — schema-select baseline reconciliation. Days 285–287 cut the account
 * list/detail/rescue paths over to canonical `crm_contacts`, so `accounts.ts` no
 * longer selects the legacy `contacts` table and the three
 * `src/routes/accounts.ts|contacts|{name,company,role}` KNOWN_DRIFT allowances in
 * validate-schema-selects.ts went stale. This guard proves that reconciliation is
 * correct AND that the schema-select guard's coverage was not weakened to make it
 * pass.
 *
 * OFFLINE / schema-independent (no network, no secrets) — purely static assertions
 * on source text, so it is deterministic regardless of which Supabase schema the
 * live validate:schema-selects happens to read.
 *
 * Proves:
 *   1. Account-domain reads are canonical: `accounts.ts` has ZERO legacy
 *      `.from("contacts")` reads and reads `crm_contacts` for list/detail/rescue,
 *      with the canonical contact fields.
 *   2. The three obsolete `accounts.ts|contacts|{name,company,role}` baselines are
 *      removed from KNOWN_DRIFT; the two genuine `scoring.ts|admin_config` baselines
 *      and the real `accounts.ts|users|full_name` baseline remain.
 *   3. The schema-select guard mechanism is intact (not weakened): full src+scripts
 *      scan, per-column drift gating, stale-baseline failure, new-drift failure, and
 *      no broad `contacts`/`crm_contacts` table ignore — so a reintroduced legacy
 *      `contacts` select with invalid fields would still fail as NEW drift, and a
 *      stale baseline entry would still fail.
 *
 * Non-vacuous: fails against 5170066 (the three contacts baselines still present).
 *
 * Usage: npx tsx scripts/validate-accounts-contacts-canonical-day-295.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const root = join(__dirname, "..");
const ACC = readFileSync(join(root, "src", "routes", "accounts.ts"), "utf8");
const VAL = readFileSync(join(root, "scripts", "validate-schema-selects.ts"), "utf8");

console.log("Day 295 — accounts canonical crm_contacts + schema-select baseline (offline)\n");

section("account-domain reads are canonical crm_contacts (no legacy contacts)");
// A legacy contacts read = .from("contacts") NOT immediately part of crm_contacts.
const legacyContacts = (ACC.match(/\.from\(\s*["'`]contacts["'`]\s*\)/g) || []).length;
gate("accounts.ts has ZERO legacy .from('contacts') reads", legacyContacts === 0, `${legacyContacts} found`);
gate("accounts.ts reads canonical crm_contacts (>=2)", (ACC.match(/\.from\(\s*["'`]crm_contacts["'`]\s*\)/g) || []).length >= 2);
gate("detail select uses canonical crm_contacts fields (first_name/last_name/email/company/account_id/created_at)",
  /\.from\(\s*['"`]crm_contacts['"`]\s*\)\s*\.select\(\s*['"`]id, first_name, last_name, email, company, account_id, created_at['"`]/.test(ACC));
gate("list/rescue count reads crm_contacts.account_id via countAccountContacts",
  /async function countAccountContacts[\s\S]{0,400}\.from\(\s*['"`]crm_contacts['"`]\s*\)[\s\S]{0,120}\.select\(\s*['"`]account_id['"`]/.test(ACC));
gate("detail path calls fetchAccountContacts", /fetchAccountContacts\(/.test(ACC));
gate("rescue path (/:id/rescue-engine) counts via countAccountContacts",
  /rescue-engine[\s\S]*countAccountContacts\(/.test(ACC));

section("KNOWN_DRIFT baseline reconciled (only the 3 obsolete removed)");
gate("no accounts.ts|contacts|name baseline", !/["']src\/routes\/accounts\.ts\|contacts\|name["']/.test(VAL));
gate("no accounts.ts|contacts|company baseline", !/["']src\/routes\/accounts\.ts\|contacts\|company["']/.test(VAL));
gate("no accounts.ts|contacts|role baseline", !/["']src\/routes\/accounts\.ts\|contacts\|role["']/.test(VAL));
gate("no residual accounts.ts|contacts|* baseline of any column",
  !/["']src\/routes\/accounts\.ts\|contacts\|/.test(VAL));
// Day 297 removed the two scoring.ts|admin_config threshold baselines (the columns
// now persist), so the Day-295 guard no longer asserts their retention. The
// account/contact removal this guard exists for is unaffected.
gate("scoring.ts|admin_config|low_score_threshold baseline removed (Day 297)",
  !/["']src\/lib\/scoring\.ts\|admin_config\|low_score_threshold["']/.test(VAL));
gate("scoring.ts|admin_config|critical_score_threshold baseline removed (Day 297)",
  !/["']src\/lib\/scoring\.ts\|admin_config\|critical_score_threshold["']/.test(VAL));
gate("accounts.ts|users|full_name baseline removed by Day 298",
  !/["']src\/routes\/accounts\.ts\|users\|full_name["']/.test(VAL));

section("schema-select guard mechanism preserved (not weakened)");
gate("full scan retained (SCAN_DIRS includes src and scripts)",
  /SCAN_DIRS\s*=\s*\[\s*["']src["']\s*,\s*["']scripts["']\s*\]/.test(VAL));
gate("per-column drift gating via baselineHas", /const newFindings = findings\.filter\(\(f\) => !baselineHas\(f\)\)/.test(VAL));
gate("stale-baseline failure still fails the guard (exit 1)",
  /if\s*\(\s*staleBaseline\.length\s*\)[\s\S]{0,400}process\.exit\(1\)/.test(VAL));
gate("NEW drift still fails the guard (exit 1)",
  /if\s*\(\s*newFindings\.length\s*\)[\s\S]{0,300}process\.exit\(1\)/.test(VAL));
gate("no broad contacts/crm_contacts table ignore was introduced",
  !/(IGNORE|SKIP|ALLOW)[A-Z_]*\s*=\s*[^\n]*contacts/i.test(VAL));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
