/**
 * validate-crm-opportunity-detail-dedup-day-305.ts
 *
 * Day 305 — de-duplicate the CRM opportunity DETAIL contract. Days 303/304 found two
 * registrations each of `GET /opportunities/:id` and `PATCH /opportunities/:id` on a
 * single Express router; the later pair was unreachable (first-registered wins) and
 * carried the phantom `crm_opportunities.value` column (staging has `amount`, no
 * `value`) plus a shadowed-only `PatchOpportunitySchema`.
 *
 * This guard asserts the shadowed detail/update duplicates are gone and only the
 * proven active handlers remain, with no `value` in the reachable detail/update
 * contract — WITHOUT touching pipeline or /:id/stage duplicates (still scoped out).
 *
 * OFFLINE self-test (no network). Non-vacuous: fails against b34c780 (two GET + two
 * PATCH /:id registrations; value-bearing selectCols; PatchOpportunitySchema present).
 *
 * Usage: npx tsx scripts/validate-crm-opportunity-detail-dedup-day-305.ts
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
const count = (re: RegExp) => (C.match(re) || []).length;

console.log("Day 305 — CRM opportunity detail de-duplication (offline)\n");

section("exactly one reachable GET/PATCH /opportunities/:id");
gate("exactly one GET /opportunities/:id registration", count(/router\.get\("\/opportunities\/:id"/g) === 1, `${count(/router\.get\("\/opportunities\/:id"/g)}`);
gate("exactly one PATCH /opportunities/:id registration", count(/router\.patch\("\/opportunities\/:id"/g) === 1, `${count(/router\.patch\("\/opportunities\/:id"/g)}`);

section("no phantom `value` in the reachable detail/update contract");
gate("no value-bearing opportunity selectCols remain",
  !C.includes("id,user_id,org_id,name,title,stage,amount,value,currency"));
gate("no PATCH writes patch.value on /opportunities/:id",
  !/patch\.value\s*=/.test(C));
gate("shadowed-only PatchOpportunitySchema removed (no code reference)",
  count(/\bPatchOpportunitySchema\b/g) === 0 || !/const PatchOpportunitySchema\s*=/.test(C));

section("active detail/update handlers preserved");
gate("active GET uses the selectCandidates ladder", /const selectCandidates = \[/.test(C) && /"id, user_id, name, title, stage, amount, currency, close_date/.test(C));
gate("active GET returns { ok, opportunity } with normalised stage", /opportunity:\s*\{[\s\S]{0,200}stage:\s*normaliseStage/.test(C));
gate("active PATCH uses OpportunityUpdateSchema + patchBase attempts ladder",
  /const body = OpportunityUpdateSchema\.parse/.test(C) && /const attempts:\s*any\[\]\s*=/.test(C));
gate("both detail handlers keep auth (getUserIdHeader) + UUID guard",
  (C.match(/router\.(get|patch)\("\/opportunities\/:id"[\s\S]{0,400}getUserIdHeader\(req\)[\s\S]{0,400}UUID_RE\.test\(id\)/g) || []).length === 2);

section("scope contained — pipeline + /:id/stage duplicates left intact, stages still first");
gate("stages route still registers before the (now single) GET /:id",
  C.indexOf('router.get("/opportunities/stages"') < C.indexOf('router.get("/opportunities/:id"'));
gate("did NOT remove /opportunities/:id/stage handlers", /router\.patch\("\/opportunities\/:id\/stage"/.test(C));
gate("Day-305 removal note present", /Day 305 — removed the SHADOWED duplicate/.test(C));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
