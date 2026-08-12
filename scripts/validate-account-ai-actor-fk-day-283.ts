/**
 * validate-account-ai-actor-fk-day-283.ts
 *
 * Regression for the Day-283 fix. Day 282 wired the WEB account-detail handlers to
 * authenticate correctly, which made two more Accounts write paths reachable and
 * exposed the same actor-FK defect fixed in Day 280:
 *
 *   POST /:id/summary          → account_ai_summaries.generated_by
 *   POST /:id/tasks/generate   → account_ai_tasks.assigned_to / generated_by
 *
 * All three columns REFERENCE public.users(id). Auth-first identities (Admin Auth API
 * + reps bridge) have no public.users row, so writing their auth id into those columns
 * violated the FK (500 account_ai_summaries_generated_by_fkey /
 * account_ai_tasks_assigned_to_fkey). Confirmed nullable via the staging PostgREST
 * OpenAPI spec (neither is in the table `required` set). Corrected to resolve each
 * candidate through the existing Day-280 `existingUserId()` helper → a real users.id
 * or null. Legacy users-row identities resolve to self (unaffected).
 *
 * OFFLINE self-test (no network, no secrets): asserts both write handlers resolve
 * their actor ids via existingUserId and that NO raw `requester.id` (or
 * `account.owner_id || requester.id`) reaches an actor column. Non-vacuous: reverting
 * either payload to `requester.id` flips the gates. Live 200 proof (auth-first QA
 * manager: summary save + tasks generate) is run separately against staging.
 *
 * Usage: npx tsx scripts/validate-account-ai-actor-fk-day-283.ts
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

/** Slice a router handler body from its route declaration to the next router.* call. */
function handlerBody(decl: string): string {
  const start = ACC.indexOf(decl);
  if (start < 0) return "";
  const next = ACC.indexOf("router.", start + decl.length);
  return ACC.slice(start, next < 0 ? ACC.length : next);
}

console.log("Day 283 — account AI summary/tasks actor columns resolve to a real users.id or null (offline; no network, no secrets)\n");

const genBody = handlerBody("router.post('/:id/tasks/generate'");
const sumBody = handlerBody("router.post('/:id/summary'");

section("shared helper");
gate("existingUserId helper exists and checks public.users",
  /async function existingUserId/.test(ACC) && /\.from\('users'\)/.test(ACC));

section("summary write → account_ai_summaries.generated_by");
gate("summary handler found", sumBody.length > 0);
gate("resolves generated_by via existingUserId(requester.id)",
  /const generatedBy\s*=\s*await existingUserId\(\s*requester\.id\s*\)/.test(sumBody));
gate("payload writes the resolved id (generated_by: generatedBy)",
  /generated_by:\s*generatedBy\b/.test(sumBody));
gate("does NOT write the raw auth id (generated_by: requester.id)",
  !/generated_by:\s*requester\.id/.test(sumBody));

section("generated tasks → account_ai_tasks.assigned_to / generated_by");
gate("tasks/generate handler found", genBody.length > 0);
gate("resolves requester once via existingUserId (generatedById)",
  /const generatedById\s*=\s*await existingUserId\(\s*requester\.id\s*\)/.test(genBody));
gate("resolves the owner-or-requester assignee via existingUserId (ownerAssignee)",
  /const ownerAssignee\s*=\s*await existingUserId\(\s*[\s\S]*?account\.owner_id\s*\|\|\s*requester\.id[\s\S]*?\)/.test(genBody));
gate("every generated_by writes the resolved id (generated_by: generatedById)",
  (genBody.match(/generated_by:\s*generatedById\b/g) || []).length === 3);
gate("assignees write resolved ids only (generatedById | ownerAssignee)",
  /assigned_to:\s*generatedById\b/.test(genBody) &&
  (genBody.match(/assigned_to:\s*ownerAssignee\b/g) || []).length === 2);
gate("does NOT write the raw auth id (generated_by/assigned_to: requester.id)",
  !/generated_by:\s*requester\.id/.test(genBody) &&
  !/assigned_to:\s*requester\.id/.test(genBody));
gate("does NOT write the unresolved owner-or-requester fallback into assigned_to",
  !/assigned_to:\s*account\.owner_id\s*\|\|\s*requester\.id/.test(genBody) &&
  !/assigned_to:\s*\n\s*account\.owner_id\s*\|\|\s*requester\.id/.test(genBody));

section("visibility gate preserved (foreign-company writes 404 before insert)");
gate("both handlers gate the account via buildAccountVisibilityFilter",
  /buildAccountVisibilityFilter/.test(genBody) && /buildAccountVisibilityFilter/.test(sumBody));

section("NON-VACUITY — no raw requester.id reaches any account_ai actor column");
gate("no `generated_by: requester.id` anywhere in accounts.ts",
  !/generated_by:\s*requester\.id/.test(ACC));
gate("no `assigned_to: requester.id` anywhere in accounts.ts",
  !/assigned_to:\s*requester\.id/.test(ACC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
