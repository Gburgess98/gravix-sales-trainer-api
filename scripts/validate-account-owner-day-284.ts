/**
 * validate-account-owner-day-284.ts
 *
 * Day 284 — the WEB account-detail page has always called PATCH/DELETE
 * /v1/accounts/:id/owner, but the API routes did not exist (404 route-not-found).
 * This adds them: `accounts.owner_id` (uuid, nullable, NO FK — confirmed via the
 * staging PostgREST OpenAPI) holds a `reps.id`; assignment validates the rep is in
 * the requester's company, and the read path enriches owner_id → { id, full_name,
 * email } + ownership_status so refresh reflects the change.
 *
 * OFFLINE self-test (no network, no secrets): asserts both routes exist, resolve
 * the owner through the same-company `resolveOwnerRep` helper, gate the account via
 * `buildAccountVisibilityFilter` (foreign → 404), scope the write by org_id, and
 * that GET /:id enriches the owner. Non-vacuous: the route-existence and
 * same-company gates fail against the pre-fix code (routes absent). Contact
 * link/unlink is deliberately NOT implemented — asserted absent (needs a data-model
 * decision; crm_contacts has no account_id and no join table exists).
 *
 * Usage: npx tsx scripts/validate-account-owner-day-284.ts
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

/** Body of a handler starting at a given registration line up to the next router.* */
function handlerBody(anchor: string): string {
  const start = ACC.indexOf(anchor);
  if (start < 0) return "";
  const next = ACC.indexOf("\nrouter.", start + anchor.length);
  return ACC.slice(start, next < 0 ? ACC.length : next);
}

console.log("Day 284 — account ownership contract (offline; no network, no secrets)\n");

section("shared owner resolver (same-company, reps-backed)");
gate("resolveOwnerRep helper exists and reads reps", /async function resolveOwnerRep/.test(ACC) && /\.from\('reps'\)/.test(ACC));
gate("resolveOwnerRep enforces same company (rejects foreign rep)",
  /resolveOwnerRep[\s\S]*?String\(rep\.company_id\)\s*!==\s*String\(companyId\)/.test(ACC));

section("PATCH /:id/owner — assign");
const patchBody = handlerBody("router.patch('/:id/owner'");
gate("route registered", patchBody.length > 0);
gate("gates the account via buildAccountVisibilityFilter (foreign → 404)",
  /buildAccountVisibilityFilter/.test(patchBody) && /account_not_found/.test(patchBody));
gate("validates the owner via resolveOwnerRep (server authority)", /await resolveOwnerRep\(\s*ownerId/.test(patchBody));
gate("rejects a foreign/unknown owner with invalid_owner (no existence leak)", /invalid_owner/.test(patchBody));
gate("writes accounts.owner_id", /\.update\(\{\s*owner_id:\s*ownerId/.test(patchBody));
gate("scopes the update by org_id (defense-in-depth)", /\.eq\('org_id',\s*requester\.company_id\)/.test(patchBody));
gate("does NOT accept company/owner from client override into scope", !/owner_id:\s*req\.body[\s\S]*?\.update/.test(patchBody));

section("DELETE /:id/owner — unassign");
const delBody = handlerBody("router.delete('/:id/owner'");
gate("route registered", delBody.length > 0);
gate("gates the account via buildAccountVisibilityFilter (foreign → 404)",
  /buildAccountVisibilityFilter/.test(delBody) && /account_not_found/.test(delBody));
gate("clears owner_id (→ null)", /\.update\(\{\s*owner_id:\s*null/.test(delBody));
gate("scopes the update by org_id", /\.eq\('org_id',\s*requester\.company_id\)/.test(delBody));
gate("returns ownership_status unassigned", /ownership_status:\s*'unassigned'/.test(delBody));

section("GET /:id enriches owner for refresh persistence");
const getBody = handlerBody("router.get('/:id'");
gate("resolves owner via resolveOwnerRep", /const owner = await resolveOwnerRep\(account\.owner_id/.test(getBody));
gate("returns owner + ownership_status", /ownership_status:\s*account\.owner_id\s*\?\s*'assigned'\s*:\s*'unassigned'/.test(getBody));

section("guards preserved");
gate("Day-278 UUID guard (router.param id) untouched", /router\.param\(\s*['"]id['"]/.test(ACC));
gate("Day-279 route order intact (/escalations before /:id)",
  ACC.search(/router\.get\('\/escalations'/) < ACC.search(/router\.get\('\/:id'/));

section("contact link/unlink NOT invented (needs a data-model decision)");
gate("no link-account route added to accounts.ts", !/link-account/.test(ACC));

section("NON-VACUITY — the owner routes must actually exist");
gate("both owner routes present (both flip if reverted)",
  /router\.patch\('\/:id\/owner'/.test(ACC) && /router\.delete\('\/:id\/owner'/.test(ACC));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
