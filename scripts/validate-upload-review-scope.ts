/**
 * Day 165 — Upload → Review Queue scope proof.
 *
 * The manager Review Queue and Command Centre both scope calls by the manager's
 * hierarchy (office_id / company_id) and only surface calls with status "scored".
 * Uploaded calls are created by /v1/upload/finalize, which previously did NOT
 * stamp office_id / company_id — so a company/office manager's own uploaded call
 * was filtered out and never appeared in their queue.
 *
 * This own-check (no live DB, Day 135 rhythm) asserts the invariant and the fix at
 * the source level so the pipeline cannot silently regress.
 */
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const server = readFileSync(join(root, "src", "server.ts"), "utf8");
const manager = readFileSync(join(root, "src", "routes", "manager.ts"), "utf8");

let fail = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) fail = 1;
}

// Isolate the finalize handler body so we assert on the right insert.
const finalizeStart = server.indexOf('"/v1/upload/finalize"');
const finalizeBody = finalizeStart >= 0 ? server.slice(finalizeStart, finalizeStart + 4000) : "";

check("finalize handler found", finalizeStart >= 0);
check("finalize stamps office_id on the call", /callInsert\.office_id\s*=/.test(finalizeBody));
check("finalize stamps company_id on the call", /callInsert\.company_id\s*=/.test(finalizeBody));
check("finalize still links account_id (Day 162 preserved)", /callInsert\.account_id\s*=/.test(finalizeBody));
check("finalize office/company lookup is fail-soft (try/catch)", /catch\s*\{\s*\/\*\s*fail-soft/.test(finalizeBody));

// The scope dependency that makes the stamping necessary.
check("review-queue filters status = scored", /review-queue[\s\S]{0,1200}\.eq\("status",\s*"scored"\)/.test(manager));
check("review-queue applies hierarchy filters", /review-queue[\s\S]{0,1600}applyHierarchyFilters/.test(manager));
check("applyHierarchyFilters scopes by office_id", /applyHierarchyFilters[\s\S]{0,400}\.eq\("office_id"/.test(manager));
check("applyHierarchyFilters scopes by company_id", /applyHierarchyFilters[\s\S]{0,400}\.eq\("company_id"/.test(manager));

if (fail) {
  console.log("Upload review-scope validation FAILED");
  process.exit(1);
}
console.log("Upload review-scope validation PASSED");
