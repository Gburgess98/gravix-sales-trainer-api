/**
 * validate-admin-config-singleton-day-294.ts
 *
 * Day 294 — admin_config singleton repair. After Day 293 a valid manager keeps
 * Company Call Visibility authority, but the streak/XP/comeback section reports
 * "Couldn't load scoring settings" because staging's public.admin_config table has
 * ZERO rows: the canonical singleton (id=true) seed is missing, so the service's
 * `.eq("id", true).single()` returns "no rows" → 500.
 *
 * OFFLINE self-test (no network, no secrets):
 *   1. A migration idempotently restores the singleton row (id=true, DB defaults,
 *      ON CONFLICT DO NOTHING) and never overwrites existing values.
 *   2. getAdminConfig resolves EXACTLY the singleton (eq id=true), not an arbitrary
 *      row, and self-heals a genuinely absent row (idempotent ensure/upsert) then
 *      re-reads — without fabricating values.
 *   3. patchAdminConfig upserts the singleton (onConflict id), so it can
 *      create/restore an absent row while updating only the provided columns.
 *   4. Genuine transport/schema errors are still surfaced (throw), never faked.
 *   5. Day 293 manager capability (WEB) remains derived from /v1/reps/me, never
 *      from the admin-config request (best-effort cross-repo check).
 *
 * Non-vacuous: fails against API bcbec72 (no seed migration; service uses
 * `.eq("id", true).single()` with no maybeSingle / ensure / upsert).
 *
 * Usage: npx tsx scripts/validate-admin-config-singleton-day-294.ts
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
const MIG_PATH = join(root, "sql", "20260825_admin_config_singleton_seed.sql");
const SVC = readFileSync(join(root, "src", "services", "adminConfig.ts"), "utf8");

// Isolate each service function for tight, non-vacuous checks.
function fn(name: string): string {
  const a = SVC.indexOf(`export async function ${name}`);
  if (a < 0) return "";
  // next top-level export or EOF
  const b = SVC.indexOf("\nexport async function ", a + 1);
  return b > a ? SVC.slice(a, b) : SVC.slice(a);
}
const getFn = fn("getAdminConfig");
const patchFn = fn("patchAdminConfig");

console.log("Day 294 — admin_config singleton repair (offline; no network, no secrets)\n");

section("migration restores the canonical singleton idempotently");
const MIG = existsSync(MIG_PATH) ? readFileSync(MIG_PATH, "utf8") : "";
gate("migration file exists (sql/20260825_admin_config_singleton_seed.sql)", MIG.length > 0);
gate("seeds the singleton id=true",
  /insert\s+into\s+public\.admin_config\s*\(\s*id\s*\)\s*values\s*\(\s*true\s*\)/i.test(MIG));
gate("idempotent — ON CONFLICT DO NOTHING (never overwrites values)",
  /on\s+conflict\s*\(\s*id\s*\)\s*do\s+nothing/i.test(MIG));
gate("self-sufficient — create table if not exists", /create table if not exists public\.admin_config/i.test(MIG));
gate("does not touch other settings tables (org_settings/team_settings)",
  !/org_settings|team_settings/i.test(MIG));
gate("no destructive statements (delete/drop/truncate/update)",
  !/\b(delete|drop|truncate|update)\b/i.test(MIG));

section("getAdminConfig — resolves the singleton and self-heals an absent row");
gate("getAdminConfig located", getFn.length > 0);
gate("resolves EXACTLY the singleton (eq id=true)", /\.eq\(\s*["']id["']\s*,\s*true\s*\)/.test(getFn));
gate("does NOT select an arbitrary row (no bare .limit(1) fallback)", !/\.limit\(\s*1\s*\)/.test(getFn));
gate("tolerates absent row (maybeSingle before single)", /\.maybeSingle\(\)/.test(getFn));
gate("self-heals a genuinely absent singleton (ensure/upsert id=true)",
  /ensureAdminConfigSingleton\(\)/.test(getFn) && /upsert\(\s*\{\s*id:\s*true\s*\}\s*,\s*\{[^}]*onConflict:\s*["']id["']/.test(SVC));
gate("surfaces a real read error (throws, does not self-heal over it)",
  /first\.error[\s\S]{0,120}throw new Error/.test(getFn));
gate("does not fabricate config values (no hard-coded literal config object returned)",
  !/return\s*\{\s*streak_threshold:\s*\d/.test(getFn));

section("patchAdminConfig — can create/restore the singleton, updates provided cols");
gate("patchAdminConfig located", patchFn.length > 0);
gate("upserts the singleton onConflict id (creates if absent)",
  /upsert\(\s*\{\s*id:\s*true\s*,\s*\.\.\.clean\s*\}\s*,\s*\{[^}]*onConflict:\s*["']id["']/.test(patchFn));
gate("only the provided columns are written (clean allow-list of the 3 fields)",
  /clean\.streak_threshold/.test(patchFn) && /clean\.xp_multiplier/.test(patchFn) && /clean\.comeback_bonus/.test(patchFn));
gate("does not widen scope to low/critical score thresholds", !/low_score_threshold|critical_score_threshold/.test(patchFn));
gate("surfaces a real write error (throws, no fabricated success)",
  /if\s*\(\s*error\s*\|\|\s*!data\s*\)[\s\S]{0,80}throw new Error/.test(patchFn));

section("ensure helper is idempotent and honest");
gate("ensureAdminConfigSingleton uses ignoreDuplicates (ON CONFLICT DO NOTHING)",
  /ignoreDuplicates:\s*true/.test(SVC));
gate("ensure surfaces a real error (throws)", /Failed to ensure admin config singleton/.test(SVC));

section("Day 293 manager capability stays decoupled (WEB, best-effort)");
const WEB_PAGE = join(root, "..", "gravix-sales-trainer-web", "src", "app", "admin", "settings", "page.tsx");
if (existsSync(WEB_PAGE)) {
  const web = readFileSync(WEB_PAGE, "utf8");
  gate("WEB resolves manager capability from /v1/reps/me", /proxyFetch\(\s*["']\/v1\/reps\/me["']/.test(web));
  gate("WEB capability is NOT derived from getAdminConfig success",
    !/getAdminConfig\([\s\S]{0,300}setIsManager/.test(web));
} else {
  console.log("  SKIP  WEB admin/settings/page.tsx not found beside API repo (cross-repo check skipped)");
}

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
