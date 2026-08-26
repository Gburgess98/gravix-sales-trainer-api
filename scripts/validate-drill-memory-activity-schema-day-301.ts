/**
 * validate-drill-memory-activity-schema-day-301.ts
 *
 * Day 301 — Assignments dynamic contract audit. The two `drill_memory`
 * crm_activities writes in src/routes/assignments.ts (assignment completion and
 * assignment-creation "failure tracking") each set only `type`, `rep_id` and `meta`
 * — omitting the NOT-NULL `title` and `user_id` (crm_activities requires
 * type/title/user_id, no defaults). Every insert therefore violated the not-null
 * constraint and was swallowed by a best-effort catch, so an assignment could
 * complete (or be created) while its drill_memory tracking activity was silently
 * absent — the Day 299/300 side-effect-masking class.
 *
 * OFFLINE self-test (no network, no secrets) — static assertions on source.
 * Confirmed staging crm_activities NOT-NULL columns: type, title, user_id (no
 * defaults); no flag_ or section columns (drill metadata lives in meta).
 *
 * Non-vacuous: fails against 37a332c (no builder; both inserts omit title/user_id
 * and set only rep_id). Usage:
 *   npx tsx scripts/validate-drill-memory-activity-schema-day-301.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const A = readFileSync(join(__dirname, "..", "src", "routes", "assignments.ts"), "utf8");

function slice(from: string, to: string): string {
  const a = A.indexOf(from);
  if (a < 0) return "";
  const b = A.indexOf(to, a + from.length);
  return b > a ? A.slice(a, b) : A.slice(a, a + 800);
}

const builder = slice("function buildDrillMemoryActivityRow(", "\nexport function assignmentsRoutes(");
const completionBlock = slice("MEMORY TRACKING: Insert drill_memory", "[memory.update.failed]");
const creationBlock = slice("FAILURE TRACKING (CRITICAL)", "[memory.failure.track.failed]");

console.log("Day 301 — drill_memory crm_activities contract (offline; no network, no secrets)\n");

section("shared builder emits only real crm_activities columns");
gate("buildDrillMemoryActivityRow builder exists", builder.length > 0);
gate("builder emits NOT-NULL type = 'drill_memory'", /type:\s*["']drill_memory["']/.test(builder));
gate("builder emits NOT-NULL title", /\btitle:\s*`/.test(builder));
gate("builder emits NOT-NULL user_id", /\buser_id:\s*userId/.test(builder));
gate("builder keeps rep_id + meta", /\brep_id:\s*userId/.test(builder) && /\bmeta:\s*\{/.test(builder));
const builderReturnTop = (() => { const a = builder.indexOf("return {"); const b = builder.indexOf("meta: {", a); return a >= 0 && b > a ? builder.slice(a, b) : ""; })();
gate("builder return top-level emits NO nonexistent flag_* / section columns",
  builderReturnTop.length > 0 && !/\b(flag_key|flag_category|flag_severity|flag_section):/.test(builderReturnTop) && !/^\s*section:/m.test(builderReturnTop));
gate("drill metadata (section/completed) lives in meta", /meta:\s*\{[\s\S]*section,[\s\S]*completed,/.test(builder));

section("both drill_memory paths repaired via the builder");
gate("completion path present + uses the builder",
  completionBlock.length > 0 && /buildDrillMemoryActivityRow\(\{/.test(completionBlock));
gate("creation/failure-tracking path present + uses the builder",
  creationBlock.length > 0 && /buildDrillMemoryActivityRow\(\{/.test(creationBlock));
gate("exactly two builder call sites", (A.match(/buildDrillMemoryActivityRow\(\{/g) || []).length === 2,
  `${(A.match(/buildDrillMemoryActivityRow\(\{/g) || []).length}`);
gate("completion path completed:true, user from the completing rep (userId)",
  /completed:\s*true/.test(completionBlock) && /userId,\s*\n/.test(completionBlock));
gate("creation path completed:false, user from the assignment rep (rep_id)",
  /completed:\s*false/.test(creationBlock) && /userId:\s*rep_id/.test(creationBlock));
gate("provenance distinguishable (assignment_complete vs assignment_created)",
  /source:\s*["']assignment_complete["']/.test(completionBlock) && /source:\s*["']assignment_created["']/.test(creationBlock));

section("no legacy defect remains (no drill_memory insert omitting user_id/title)");
gate("no raw drill_memory insert with only type+rep_id+meta remains",
  !/insert\(\{\s*type:\s*["']drill_memory["'],\s*rep_id:/.test(A.replace(/\s+/g, " ")));
gate("no crm_activities insert sets rep_id without user_id in the drill paths",
  !/rep_id:\s*rep_id,\s*\n\s*meta:/.test(A) && !/type:\s*["']drill_memory["'],\s*\n\s*rep_id:\s*userId,\s*\n\s*meta:/.test(A));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
