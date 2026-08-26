/**
 * validate-crm-actions-create-day-302.ts
 *
 * Day 302 — crm_actions lifecycle audit (src/routes/crm.ts). The manager inline
 * create `POST /crm/actions` builds a schema-tolerant retry ladder of insert
 * payloads and, on a missing-column error, should strip columns and try the next
 * (leaner) payload — falling back to the minimal `{user_id, contact_id, type,
 * title}` that crm_actions actually accepts.
 *
 * Pre-fix (7977e51) the "missing column → continue" test only matched the raw
 * Postgres "column ... does not exist" string. supabase-js/PostgREST instead
 * reports PGRST204 "Could not find the '<col>' column of '<table>' in the schema
 * cache". crm_actions has no `rep_id`/`source`, so the first attempt failed with
 * the PGRST204 form, the ladder BROKE instead of continuing, and the endpoint
 * always returned 500 — no action created.
 *
 * OFFLINE self-test (no network, no secrets) — static assertions on crm.ts.
 * Confirmed staging crm_actions NOT-NULL-without-default columns: user_id,
 * contact_id, type, title (status/importance/created_at/id have defaults); no
 * rep_id/source columns.
 *
 * Non-vacuous: fails against 7977e51 (detection matched only "does not exist").
 * Usage: npx tsx scripts/validate-crm-actions-create-day-302.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const C = readFileSync(join(__dirname, "..", "src", "routes", "crm.ts"), "utf8");

// Isolate the POST /crm/actions manager inline-create handler + its retry loop.
const postBlock = (() => {
  const a = C.indexOf('router.post("/actions"');
  if (a < 0) return "";
  const b = C.indexOf("action_create_failed", a);
  return b > a ? C.slice(a, C.indexOf("});", b)) : C.slice(a, a + 4000);
})();
// The missing-column decision that gates continue vs break.
const decision = (() => {
  const a = postBlock.indexOf("missing-column");
  const b = postBlock.indexOf("break;", a);
  return a >= 0 ? postBlock.slice(a, b > a ? b : a + 700) : "";
})();

console.log("Day 302 — POST /crm/actions missing-column retry (offline; no network, no secrets)\n");

section("handler + retry ladder present");
gate("POST /crm/actions handler located", postBlock.length > 0);
gate("retry ladder present (attempts array + loop)", /const attempts:\s*any\[\]\s*=/.test(postBlock) && /for \(const payload of attempts\)/.test(postBlock));
gate("minimal fallback payload = only real required columns", /\{\s*user_id:\s*requester,\s*contact_id:\s*contactIdRaw,\s*type,\s*title,?\s*\}/.test(postBlock.replace(/\s+/g, " ").replace(/, }/g, " }")) || /user_id: requester,\s*contact_id: contactIdRaw,\s*type,\s*title,/.test(postBlock));
gate("missing-column decision located", decision.length > 0);

section("missing-column detection recognises the PostgREST PGRST204 format (the fix)");
gate("continues on raw Postgres 'column ... does not exist'",
  /includes\(["']column["']\)\s*&&\s*msg\.includes\(["']does not exist["']\)/.test(decision));
gate("ALSO continues on PostgREST 'could not find the ... column'",
  /includes\(["']could not find the["']\)\s*&&\s*msg\.includes\(["']column["']\)/.test(decision));
gate("ALSO continues on 'schema cache'",
  /includes\(["']schema cache["']\)/.test(decision));
gate("the continue is gated by the combined missing-column check",
  /if\s*\(\s*isMissingColumn\s*\)\s*\{\s*continue;/.test(postBlock));

section("table-missing stays loud; scope contained to crm_actions create");
gate("missing table still returns a loud 500 (not silently swallowed)",
  /includes\(["']relation["']\)[\s\S]{0,220}crm_actions_table_missing/.test(postBlock));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
