/**
 * validate-scoring-thresholds-day-297.ts
 *
 * Day 297 — persist the low/critical score thresholds as a truthful admin_config
 * contract. Before this, `getScoringThresholds` selected two non-existent columns,
 * swallowed the schema error and returned hard-coded {low:65,critical:45}, and
 * `PATCH /v1/admin/config` validated/accepted the fields but the service dropped
 * them (responses reported nulls).
 *
 * OFFLINE self-test (no network, no secrets) — static assertions on source:
 *   1. Migration adds both integer columns (defaults 65/45), range checks 0..100,
 *      the invariant critical <= low, all idempotent + additive.
 *   2. Service persists + echoes both: AdminConfig type + SELECT_COLS + patch clean.
 *   3. PATCH enforces the ordering invariant on the EFFECTIVE pair (single-field
 *      patch validated against the stored companion), 400 before any mutation;
 *      Day-294 upsert/self-heal + real-error surfacing preserved.
 *   4. Runtime resolver reads the canonical singleton's persisted columns.
 *   5. Exactly the two scoring baselines are removed from KNOWN_DRIFT while
 *      accounts.ts|users|full_name is retained.
 *
 * Non-vacuous: fails against 0b093a4 (no migration; service/type lack the columns;
 * both scoring baselines still present; resolver uses .limit(1)).
 *
 * Usage: npx tsx scripts/validate-scoring-thresholds-day-297.ts
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
const MIG_PATH = join(root, "sql", "20260826_admin_config_score_thresholds.sql");
const SVC = readFileSync(join(root, "src", "services", "adminConfig.ts"), "utf8");
const ADMIN = readFileSync(join(root, "src", "routes", "admin.ts"), "utf8");
const SCORING = readFileSync(join(root, "src", "lib", "scoring.ts"), "utf8");
const VAL = readFileSync(join(root, "scripts", "validate-schema-selects.ts"), "utf8");

console.log("Day 297 — scoring threshold contract (offline; no network, no secrets)\n");

section("migration: additive, defaulted, range + ordering constrained, idempotent");
const MIG = existsSync(MIG_PATH) ? readFileSync(MIG_PATH, "utf8") : "";
gate("migration file exists (sql/20260826_admin_config_score_thresholds.sql)", MIG.length > 0);
gate("adds low_score_threshold integer NOT NULL default 65 (idempotent)",
  /add column if not exists low_score_threshold integer not null default 65/i.test(MIG));
gate("adds critical_score_threshold integer NOT NULL default 45 (idempotent)",
  /add column if not exists critical_score_threshold integer not null default 45/i.test(MIG));
gate("range check low 0..100", /check\s*\(\s*low_score_threshold between 0 and 100\s*\)/i.test(MIG));
gate("range check critical 0..100", /check\s*\(\s*critical_score_threshold between 0 and 100\s*\)/i.test(MIG));
gate("invariant check critical <= low", /check\s*\(\s*critical_score_threshold <= low_score_threshold\s*\)/i.test(MIG));
gate("constraints guarded idempotently (pg_constraint)", (MIG.match(/from pg_constraint where conname/gi) || []).length >= 3);
gate("no destructive / unrelated table changes",
  !/\b(drop|delete|truncate)\b/i.test(MIG) && !/\b(update|insert)\b/i.test(MIG) && !/org_settings|team_settings|crm_/i.test(MIG));

section("service: both thresholds persist and echo truthfully");
gate("AdminConfig type includes low_score_threshold", /low_score_threshold:\s*number/.test(SVC));
gate("AdminConfig type includes critical_score_threshold", /critical_score_threshold:\s*number/.test(SVC));
gate("SELECT_COLS selects both thresholds",
  /const SELECT_COLS\s*=[\s\S]{0,160}low_score_threshold[\s\S]{0,60}critical_score_threshold/.test(SVC));
gate("patchAdminConfig accepts + writes low_score_threshold",
  /patch\.low_score_threshold !== undefined\)\s*clean\.low_score_threshold = patch\.low_score_threshold/.test(SVC));
gate("patchAdminConfig accepts + writes critical_score_threshold",
  /patch\.critical_score_threshold !== undefined\)\s*clean\.critical_score_threshold = patch\.critical_score_threshold/.test(SVC));
gate("Day-294 upsert(onConflict id) preserved", /upsert\(\s*\{\s*id:\s*true\s*,\s*\.\.\.clean\s*\}\s*,\s*\{[^}]*onConflict:\s*["']id["']/.test(SVC));
gate("service still surfaces genuine DB errors (throw)", /throw new Error\(`Failed to update admin config/.test(SVC));

section("PATCH invariant on the effective pair (single-field validated vs stored companion)");
const patchBlock = (() => { const a = ADMIN.indexOf('adminRouter.patch("/config"'); const b = ADMIN.indexOf("adminRouter.get(\"/reps\"", a); return a >= 0 ? ADMIN.slice(a, b > a ? b : a + 4000) : ""; })();
gate("PATCH /config located", patchBlock.length > 0);
gate("effective-pair guard triggers when either threshold is supplied",
  /if\s*\(\s*low_score_threshold !== undefined \|\| critical_score_threshold !== undefined\s*\)/.test(patchBlock));
gate("single-field patch fetches the stored companion (getAdminConfig)",
  /const current = await getAdminConfig\(\)/.test(patchBlock) &&
  /effLow = current\.low_score_threshold/.test(patchBlock) &&
  /effCritical = current\.critical_score_threshold/.test(patchBlock));
gate("invalid ordering returns 400 before any mutation",
  /Number\(effCritical\) > Number\(effLow\)[\s\S]{0,160}status\(400\)/.test(patchBlock) &&
  patchBlock.indexOf("Number(effCritical) > Number(effLow)") < patchBlock.indexOf("patchAdminConfig("));
gate("response echoes the persisted thresholds", /updated\?\.low_score_threshold/.test(patchBlock) && /updated\?\.critical_score_threshold/.test(patchBlock));

section("runtime resolver consumes the persisted singleton");
gate("getScoringThresholds selects both persisted columns",
  /\.select\(\s*["']low_score_threshold, critical_score_threshold["']\s*\)/.test(SCORING));
gate("getScoringThresholds reads the canonical singleton (eq id=true), not an arbitrary row",
  /low_score_threshold, critical_score_threshold["']\s*\)\s*\.eq\(\s*["']id["']\s*,\s*true\s*\)/.test(SCORING) &&
  !/low_score_threshold, critical_score_threshold["']\s*\)\s*\.limit\(\s*1\s*\)/.test(SCORING));

section("schema-select baseline: exactly the two scoring entries removed");
gate("no scoring.ts|admin_config|low_score_threshold baseline",
  !/["']src\/lib\/scoring\.ts\|admin_config\|low_score_threshold["']/.test(VAL));
gate("no scoring.ts|admin_config|critical_score_threshold baseline",
  !/["']src\/lib\/scoring\.ts\|admin_config\|critical_score_threshold["']/.test(VAL));
gate("accounts.ts|users|full_name baseline removed by Day 298",
  !/["']src\/routes\/accounts\.ts\|users\|full_name["']/.test(VAL));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
