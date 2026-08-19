/**
 * validate-org-settings-schema-day-291.ts
 *
 * Day-291 correction (org_settings schema + read contract). After the canonical
 * org fix, a resolved manager's GET /v1/admin/org-settings → 500 because
 * public.org_settings does not exist, and enforcement (getOrgCallVisibility)
 * swallowed the schema error into an "everyone" default (fail-open). GET was also
 * manager-only, so a rep could not read an "everyone" policy.
 *
 * OFFLINE self-test (no network, no secrets):
 *   1. Migration defines the smallest contract: one row per org (uuid PK), FK to
 *      orgs(id), call_visibility CHECK in the three allowed values, explicit
 *      default 'everyone', timestamps.
 *   2. Read contract: GET is readable by any authenticated member (requireUserId),
 *      PATCH stays manager-only (requireManager), and both resolve org server-side
 *      (getRequesterOrgId) — never from a client-supplied org id.
 *   3. Fail-closed: getOrgCallVisibility re-throws on transport/schema error (no
 *      "everyone" fallback) and only an absent row defaults to "everyone".
 *   4. calls/paged still enforces the three policies.
 *
 * Non-vacuous: fails against 9fbf053 (no migration file; GET requireManager;
 * getOrgCallVisibility try/catch → "everyone"). Live behaviour proven on staging.
 *
 * Usage: npx tsx scripts/validate-org-settings-schema-day-291.ts
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
const MIG_PATH = join(root, "sql", "20260819_org_settings.sql");
const ADMIN = readFileSync(join(root, "src", "routes", "admin.ts"), "utf8");
const CONFIG = readFileSync(join(root, "src", "lib", "adminConfig.ts"), "utf8");
const CALLS = readFileSync(join(root, "src", "routes", "calls.ts"), "utf8");

// Isolate the two legitimate org-settings handlers (top-level GET + PATCH).
const rs = ADMIN.indexOf('adminRouter.get("/org-settings"');
const re = ADMIN.indexOf('adminRouter.post("/users"');
const region = rs >= 0 && re > rs ? ADMIN.slice(rs, re) : "";
const getBlock = region.slice(0, region.indexOf('adminRouter.patch("/org-settings"'));
const patchBlock = region.slice(region.indexOf('adminRouter.patch("/org-settings"'));

console.log("Day 291 — org_settings schema + read contract (offline; no network, no secrets)\n");

section("migration defines the smallest contract");
const MIG = existsSync(MIG_PATH) ? readFileSync(MIG_PATH, "utf8") : "";
gate("migration file exists (sql/20260819_org_settings.sql)", MIG.length > 0);
gate("creates public.org_settings idempotently",
  /create table if not exists public\.org_settings/i.test(MIG));
gate("org_id uuid is the primary key (one row per org)",
  /org_id\s+uuid\s+primary key/i.test(MIG));
gate("call_visibility constrained to the three allowed values",
  /check\s*\(\s*call_visibility\s+in\s*\(\s*'everyone'\s*,\s*'managers'\s*,\s*'disabled'\s*\)\s*\)/i.test(MIG));
gate("explicit default 'everyone'",
  /call_visibility\s+text\s+not null\s+default\s+'everyone'/i.test(MIG));
gate("FK to orgs(id)",
  /foreign key\s*\(\s*org_id\s*\)\s*references\s+public\.orgs\s*\(\s*id\s*\)/i.test(MIG));
gate("timestamps present", /created_at\s+timestamptz/i.test(MIG) && /updated_at\s+timestamptz/i.test(MIG));
gate("constraints added idempotently (pg_constraint guards)",
  (MIG.match(/from pg_constraint where conname/gi) || []).length >= 2);

section("read contract — rep-readable GET, manager-only PATCH");
gate("admin.ts imports requireUserId",
  /import\s*\{[^}]*\brequireUserId\b[^}]*\}\s*from\s*['"]\.\.\/middleware\/requireUserId['"]/.test(ADMIN));
gate("GET /org-settings is guarded by requireUserId (any authenticated member)",
  /adminRouter\.get\(\s*["']\/org-settings["']\s*,\s*requireUserId\b/.test(ADMIN));
gate("GET is NOT manager-gated",
  !/adminRouter\.get\(\s*["']\/org-settings["']\s*,\s*requireManager\b/.test(ADMIN));
gate("PATCH /org-settings stays manager-only (requireManager)",
  /adminRouter\.patch\(\s*["']\/org-settings["']\s*,\s*requireManager\b/.test(patchBlock) ||
  /adminRouter\.patch\(\s*["']\/org-settings["']\s*,\s*requireManager\b/.test(region));

section("canonical org — server-resolved, no client override");
gate("GET resolves org via getRequesterOrgId", /getRequesterOrgId\(\s*requester\s*\)/.test(getBlock));
gate("PATCH resolves org via getRequesterOrgId", /getRequesterOrgId\(\s*requester\s*\)/.test(patchBlock));
gate("handlers never read an org id from the client (body/query)",
  !/req\.(body|query)[^\n;]*\borg_?_?[iI]d\b/.test(region));

section("fail-closed enforcement (never 'everyone' on error)");
gate("getOrgCallVisibility re-throws on error", /if\s*\(\s*error\s*\)\s*throw\s+error/.test(CONFIG));
gate("getOrgCallVisibility has no catch that returns 'everyone'",
  !/catch[\s\S]{0,120}return\s+["']everyone["']/.test(CONFIG));
gate("absent row still defaults to 'everyone' (explicit)",
  /\|\|\s*["']everyone["']/.test(CONFIG));
gate("admin GET fails closed on error (throws → 500, not 'everyone')",
  /if\s*\(\s*error\s*\)\s*throw\s+error/.test(getBlock));

section("calls/paged still enforces the three policies");
gate("disabled → 403", /visibility\s*===\s*["']disabled["'][\s\S]{0,120}company_calls_disabled/.test(CALLS));
gate("managers → gated (manager_only_access)", /visibility\s*===\s*["']managers["'][\s\S]{0,400}manager_only_access/.test(CALLS));
gate("company scope filters org_id server-side", /q\s*=\s*q\.eq\(\s*["']org_id["']\s*,\s*requesterOrgId\s*\)/.test(CALLS));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
