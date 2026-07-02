/**
 * Day 168 — demo data visibility own-checks (Day 135 rhythm, no live DB).
 *
 * Asserts at source level that:
 *  - assignments.ts no longer applies unguarded office filters (the pattern
 *    that emitted .eq("office_id", null) for seeded demo managers and hid
 *    every assignment from the Command Centre);
 *  - the shared applyOrgScope helper carries the Day 166/167 company
 *    fallback and never broadens beyond company scope;
 *  - assignment creation stamps tenancy via the reps identity bridge;
 *  - /v1/team/users is tenant-scoped (no all-profiles listing) and keeps
 *    the { ok, items } response shape;
 *  - no forbidden lanes were opened (no TTS/voice, no LLM on the hot path).
 */
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const assignments = readFileSync(join(root, "src", "routes", "assignments.ts"), "utf8");
const team = readFileSync(join(root, "src", "routes", "team.ts"), "utf8");

let fail = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) fail = 1;
}

// ── assignments scoping ──
const unguarded = /q\s*=\s*q\.eq\("office_id",\s*managerContext\.office_id\)/;
check("assignments.ts has no unguarded office_id filter", !unguarded.test(assignments));

const scopeFn = assignments.match(/function applyOrgScope[\s\S]{0,700}?\n\}/)?.[0] ?? "";
check("applyOrgScope helper exists", scopeFn.length > 0);
check("applyOrgScope guards office_id before filtering", /if \(ctx\.office_id\) return query\.eq\("office_id", ctx\.office_id\);/.test(scopeFn));
check("applyOrgScope falls back to company scope", /if \(ctx\.company_id\) return query\.eq\("company_id", ctx\.company_id\);/.test(scopeFn));
check("applyOrgScope never filters beyond company (no org-wide eq)", !/\.eq\("org_id"/.test(scopeFn));
check("applyOrgScope applied at every manager query site", (assignments.match(/applyOrgScope\(q, managerContext\)/g) ?? []).length >= 5);

// ── assignment creation stamps tenancy for auth-first reps ──
const hierarchyFn = assignments.match(/async function getUserHierarchy[\s\S]{0,900}?\n\}/)?.[0] ?? "";
check("getUserHierarchy has reps identity-bridge fallback", /from\("reps"\)/.test(hierarchyFn));

// ── backfill repair script present and idempotent ──
let backfill = "";
try { backfill = readFileSync(join(root, "scripts", "backfill-assignment-tenant-stamps.ts"), "utf8"); } catch { /* missing */ }
check("backfill script exists", backfill.length > 0);
check("backfill only touches rows with company_id NULL (idempotent)", /\.is\("company_id", null\)/.test(backfill));

// ── team/users tenant scoping ──
check("team.ts no longer lists profiles unscoped", !/from\("profiles"\)\.select\(selectClause/.test(team) && !/CANDIDATES/.test(team));
check("team.ts resolves requester company (users → reps bridge)", /resolveCompanyId/.test(team) && /from\("reps"\)/.test(team));
check("team.ts returns empty list when no company resolvable", /if \(!companyId\) return res\.json\(\{ ok: true, items: \[\] \}\);/.test(team));
check("team.ts filters members by company_id", (team.match(/\.eq\("company_id", companyId\)/g) ?? []).length >= 2);
check("team/users keeps { ok, items } response shape", /res\.json\(\{ ok: true, items \}\)/.test(team));

// ── forbidden lanes stayed closed ──
const touched = assignments + team;
check("no ElevenLabs/TTS/Voice Agent added", !/elevenlabs|text.to.speech|voice.agent/i.test(touched));
check("no LLM on live hot path", !/openai|anthropic|chat\.completions|responses\.create/i.test(touched));

console.log();
if (fail) {
  console.log("Demo data visibility validation FAILED");
  process.exit(1);
}
console.log("Demo data visibility validation PASSED");
