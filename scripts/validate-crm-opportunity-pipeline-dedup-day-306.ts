/**
 * validate-crm-opportunity-pipeline-dedup-day-306.ts
 *
 * Day 306 — de-duplicate the CRM opportunity PIPELINE contract. Days 303–305 found two
 * registrations of `GET /opportunities/pipeline` on a single Express router; the later
 * one was unreachable (first-registered wins) and returned a DIFFERENT shape
 * ({ stages, by_stage, opportunities, meta }) than the active earlier handler
 * ({ stages, columns, items }, which the WEB consumes). The shadowed handler had an
 * exclusive helper `selectCrmOpportunitiesSafe`.
 *
 * This guard asserts the shadowed pipeline duplicate + its exclusive helper are gone
 * and only the active handler remains, WITHOUT touching /pipeline/summary or the
 * /:id/stage duplicates (still scoped out).
 *
 * OFFLINE self-test (no network). Non-vacuous: fails against e33cd2b (two pipeline
 * registrations; by_stage shape; selectCrmOpportunitiesSafe present).
 *
 * Usage: npx tsx scripts/validate-crm-opportunity-pipeline-dedup-day-306.ts
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

// Isolate the single reachable pipeline handler.
const pipeStart = C.indexOf('router.get("/opportunities/pipeline"');
const pipeBody = pipeStart >= 0 ? C.slice(pipeStart, C.indexOf("\n});", pipeStart) + 4) : "";

console.log("Day 306 — CRM opportunity pipeline de-duplication (offline)\n");

section("exactly one reachable GET /opportunities/pipeline");
gate("exactly one GET /opportunities/pipeline registration",
  count(/router\.get\("\/opportunities\/pipeline"\s*,/g) === 1, `${count(/router\.get\("\/opportunities\/pipeline"\s*,/g)}`);
gate("/opportunities/pipeline/summary still present (single, untouched)",
  count(/router\.get\("\/opportunities\/pipeline\/summary"/g) === 1, `${count(/router\.get\("\/opportunities\/pipeline\/summary"/g)}`);

section("the remaining pipeline handler is the active { stages, columns, items } shape");
gate("pipeline handler located", pipeBody.length > 0);
gate("returns columns + items (active shape)", /columns/.test(pipeBody) && /items/.test(pipeBody) && /res\.json\(\{ ok: true, stages, columns, items \}\)/.test(pipeBody));
gate("shadowed { by_stage, opportunities, meta } response shape removed",
  !/by_stage:\s*byStage/.test(C) && !/opportunities:\s*rows/.test(C) && !/meta:\s*\{ select:/.test(C));
gate("active team scope preserved (requireManagerOrg + isManagerUser)",
  /requireManagerOrg\(req\)/.test(pipeBody) && /isManagerUser\(requester\)/.test(pipeBody));
gate("active selectCandidates ladder preserved", /const selectCandidates = \[/.test(pipeBody));

section("dead helper removed; scope contained");
gate("exclusive helper selectCrmOpportunitiesSafe removed (no def/call)",
  !/async function selectCrmOpportunitiesSafe/.test(C) && !/selectCrmOpportunitiesSafe\(/.test(C));
gate("did NOT remove /opportunities/pipeline/summary handler body", /GET \/v1\/crm\/opportunities\/pipeline\/summary/.test(C));
gate("did NOT remove /opportunities/:id/stage handlers", count(/router\.patch\("\/opportunities\/:id\/stage"/g) >= 1);
gate("detail de-dup from Day 305 intact (one GET + one PATCH /:id)",
  count(/router\.get\("\/opportunities\/:id"/g) === 1 && count(/router\.patch\("\/opportunities\/:id"/g) === 1);
gate("Day-306 removal note present", /Day 306 — removed the SHADOWED duplicate/.test(C));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
