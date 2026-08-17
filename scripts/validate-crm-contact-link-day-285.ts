/**
 * validate-crm-contact-link-day-285.ts
 *
 * Day 285 — one CRM contact belongs to at most one account. Adds nullable
 * `crm_contacts.account_id` (FK → accounts.id ON DELETE SET NULL, indexed),
 * implements the WEB's link/unlink contracts, and makes `crm_contacts` the
 * canonical account-detail contact store.
 *
 * OFFLINE self-test (no network, no secrets): asserts the migration shape, both
 * routes exist and resolve tenant authority (account by company, contact by
 * owner), foreign combinations 404 without leaking, the client cannot override
 * scope, and GET /:id reads canonical crm_contacts (not the legacy `contacts`).
 * Non-vacuous: removing a route, reverting the read to `contacts`, or dropping the
 * migration flips gates. Live link/relink/unlink/isolation proof runs separately
 * against staging after the migration is applied.
 *
 * Usage: npx tsx scripts/validate-crm-contact-link-day-285.ts
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }
const root = join(__dirname, "..");
const CRM = readFileSync(join(root, "src", "routes", "crm.ts"), "utf8");
const ACC = readFileSync(join(root, "src", "routes", "accounts.ts"), "utf8");
const MIG_PATH = join(root, "sql", "20260814_crm_contacts_account_link.sql");

function body(src: string, anchor: string): string {
  const start = src.indexOf(anchor);
  if (start < 0) return "";
  const next = src.indexOf("\nrouter.", start + anchor.length);
  return src.slice(start, next < 0 ? src.length : next);
}

console.log("Day 285 — CRM contact↔account link (offline; no network, no secrets)\n");

section("migration shape (idempotent, nullable FK, on delete set null, indexed)");
const mig = existsSync(MIG_PATH) ? readFileSync(MIG_PATH, "utf8") : "";
gate("migration file exists", mig.length > 0);
gate("adds nullable crm_contacts.account_id (idempotent)", /add column if not exists account_id uuid null/i.test(mig));
gate("FK → accounts(id) ON DELETE SET NULL", /references public\.accounts\s*\(id\)\s*on delete set null/i.test(mig));
gate("FK added idempotently (guarded by pg_constraint)", /pg_constraint where conname = 'crm_contacts_account_id_fkey'/i.test(mig));
gate("indexes account_id (idempotent)", /create index if not exists idx_crm_contacts_account\s+on public\.crm_contacts \(account_id\)/i.test(mig));
// Scan SQL statements only (strip -- line comments), for destructive DDL/DML.
const migSql = mig.replace(/--[^\n]*/g, "");
gate("no destructive statements (drop table/column, truncate, delete from)",
  !/\b(drop\s+(table|column)|truncate|delete\s+from)\b/i.test(migSql));

section("POST /contacts/:contactId/link-account");
const link = body(CRM, 'router.post("/contacts/:contactId/link-account"');
gate("route registered", link.length > 0);
gate("account resolved by company (resolveCompanyAccount), foreign → account_not_found",
  /resolveCompanyAccount\(accountId,\s*companyId\)/.test(link) && /account_not_found/.test(link));
gate("contact resolved by owner (resolveOwnedContact), foreign → contact_not_found",
  /resolveOwnedContact\(contactId,\s*requester\)/.test(link) && /contact_not_found/.test(link));
gate("writes crm_contacts.account_id scoped to the owner (relink moves the FK)",
  /\.update\(\{\s*account_id:\s*account\.id\s*\}\)[\s\S]*?\.eq\("user_id",\s*requester\)/.test(link));
gate("UUID-guards both ids", /invalid_contact_id/.test(link) && /invalid_account_id/.test(link));
gate("company is server-resolved via getRepContext (not from client body)",
  /getRepContext\(requester\)/.test(link) && !/account_id:\s*req\.body[\s\S]*?\.update/.test(link));

section("POST /contacts/:contactId/unlink-account (deterministic + idempotent)");
const unlink = body(CRM, 'router.post("/contacts/:contactId/unlink-account"');
gate("route registered", unlink.length > 0);
gate("owner-scoped, clears account_id → null", /\.update\(\{\s*account_id:\s*null\s*\}\)[\s\S]*?\.eq\("user_id",\s*requester\)/.test(unlink));
gate("contact resolved by owner (foreign → contact_not_found)", /resolveOwnedContact\(contactId,\s*requester\)/.test(unlink) && /contact_not_found/.test(unlink));

section("helper authority (no parallel model)");
gate("resolveCompanyAccount scopes accounts by org_id === companyId",
  /async function resolveCompanyAccount[\s\S]*?\.eq\("org_id",\s*companyId\)/.test(CRM));
gate("resolveOwnedContact scopes crm_contacts by user_id === requester",
  /async function resolveOwnedContact[\s\S]*?\.eq\("user_id",\s*requester\)/.test(CRM));

section("GET /accounts/:id reads canonical crm_contacts (not legacy `contacts`)");
const getBody = body(ACC, "router.get('/:id'");
gate("detail linked_contacts come from fetchAccountContacts", /fetchAccountContacts\(account\.id\)/.test(getBody));
gate("fetchAccountContacts reads crm_contacts by account_id", /async function fetchAccountContacts[\s\S]*?\.from\('crm_contacts'\)[\s\S]*?\.eq\('account_id',\s*accountId\)/.test(ACC));
gate("detail read no longer uses the legacy contacts table for linked_contacts",
  !/\.from\('contacts'\)\s*\n\s*\.select\([\s\S]*?role,/.test(getBody));
gate("fetchAccountContacts is schema-tolerant (returns [] pre-migration, no 500)", /catch \(e\)[\s\S]*?return \[\];/.test(ACC));

section("guards preserved");
gate("Day-278 UUID guard (router.param id) untouched in accounts", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("Day-279 route order intact (/escalations before /:id)",
  ACC.search(/router\.get\('\/escalations'/) < ACC.search(/router\.get\('\/:id'/));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
