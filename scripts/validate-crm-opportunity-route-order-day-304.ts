/**
 * validate-crm-opportunity-route-order-day-304.ts
 *
 * Day 304 — literal CRM opportunity routes must register before the parameterised
 * `GET /opportunities/:id`. On a single Express router the first-registered layer
 * wins, so a literal like `/opportunities/stages` registered AFTER `/:id` is captured
 * as id="stages" and rejected by the UUID guard (400 invalid_id) — the literal
 * handler is unreachable.
 *
 * This guard asserts (statically, by source order in src/routes/crm.ts):
 *   1. `GET /opportunities/stages` exists exactly once and registers before the
 *      first `GET /opportunities/:id`.
 *   2. Every literal `GET /opportunities/<segment>` has its FIRST registration
 *      before the first `GET /opportunities/:id` (prevents future shadowing).
 *   3. The `/:id` UUID guard that protects genuine detail requests is preserved.
 *
 * Non-vacuous: fails against 1ec931c (stages registered after /:id).
 * Usage: npx tsx scripts/validate-crm-opportunity-route-order-day-304.ts
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
const lines = C.split("\n");

// Collect GET /opportunities/* registrations in source order.
type Reg = { line: number; path: string };
const gets: Reg[] = [];
lines.forEach((ln, i) => {
  const m = ln.match(/^router\.get\("(\/opportunities\/[^"]*)"/);
  if (m) gets.push({ line: i + 1, path: m[1] });
});

const idRoutePath = "/opportunities/:id";
const firstIdLine = Math.min(...gets.filter((g) => g.path === idRoutePath).map((g) => g.line), Infinity);

console.log("Day 304 — CRM opportunity route order (offline; no network)\n");

section("GET /opportunities/:id present as the parameter route");
gate("a GET /opportunities/:id route exists", Number.isFinite(firstIdLine), `${firstIdLine}`);
gate("the /:id UUID guard still rejects non-UUID ids",
  /router\.get\("\/opportunities\/:id"[\s\S]{0,400}UUID_RE\.test\(id\)\)\s*return res\.status\(400\)\.json\(\{ ok: false, error: "invalid_id" \}\)/.test(C));

section("GET /opportunities/stages is reachable (registered before /:id, exactly once)");
const stagesRegs = gets.filter((g) => g.path === "/opportunities/stages");
gate("exactly one GET /opportunities/stages registration", stagesRegs.length === 1, `${stagesRegs.length}`);
const firstStagesLine = Math.min(...stagesRegs.map((g) => g.line), Infinity);
gate("GET /opportunities/stages registers before the first GET /opportunities/:id",
  Number.isFinite(firstStagesLine) && firstStagesLine < firstIdLine,
  `stages@${firstStagesLine} vs /:id@${firstIdLine}`);

section("no literal GET /opportunities/<segment> is shadowed by an earlier /:id");
// A "literal" segment does not start with ':'. Its FIRST registration must precede
// the first /:id, otherwise Express matches /:id first and the literal is dead.
const literals = new Map<string, number>(); // path -> first line
for (const g of gets) {
  const seg = g.path.slice("/opportunities/".length);
  if (!seg || seg.startsWith(":")) continue;
  if (!literals.has(g.path)) literals.set(g.path, g.line);
}
for (const [path, firstLine] of literals) {
  gate(`literal ${path} first-registers before /:id`, firstLine < firstIdLine, `first@${firstLine} vs /:id@${firstIdLine}`);
}

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
