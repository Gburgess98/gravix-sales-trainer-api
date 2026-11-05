/**
 * RLS / grants posture regression check.
 *
 *   Static (default, offline, CI-safe):  npx tsx scripts/check-rls-posture.ts
 *     Fails when any migration under sql/ creates a public table without an accompanying `enable row level security`
 *     (or without relying on the lockdown), or grants privileges on public objects to anon / authenticated / PUBLIC.
 *   Live (needs a DB connection):        DATABASE_URL=… npx tsx scripts/check-rls-posture.ts --live
 *     Runs sql/security/20261105_rls_lockdown_verify.sql — ZERO rows = pass; any row = a public table lacks RLS or has an
 *     unsafe grant/function/default privilege.
 *   (Without a direct URL, run the same file through `supabase db query --linked -f sql/security/…_verify.sql`.)
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const root = join(__dirname, "..");
const CREATE_RE = /create\s+table\s+(?:if\s+not\s+exists\s+)?(?:"?public"?\.)?"?([a-z0-9_]+)"?/gi;
const GRANT_RE = /\bgrant\s+[^;]*\bon\b[^;]*\bto\b[^;]*\b(anon|authenticated|public)\b/gi;

export interface Finding { file: string; rule: string; detail: string }

/** Pure: analyse one SQL file's text. */
export function analyseSql(file: string, sqlRaw: string): Finding[] {
  const sql = sqlRaw.replace(/--[^\n]*/g, "");
  const out: Finding[] = [];
  // A migration is "secure by construction" for new tables if it enables RLS for them explicitly.
  const enabled = new Set([...sql.matchAll(/alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?(?:"?public"?\.)?"?([a-z0-9_]+)"?\s+enable\s+row\s+level\s+security/gi)].map((m) => m[1].toLowerCase()));
  const loopEnable = /for\s+\w+\s+in\s+select[\s\S]{0,200}enable row level security/i.test(sql); // generic enabling loop
  for (const m of sql.matchAll(CREATE_RE)) {
    const t = m[1].toLowerCase();
    // temp / non-public tables are out of scope
    const stmtStart = sql.lastIndexOf(";", m.index ?? 0);
    if (/\b(temp|temporary)\b/i.test(sql.slice(stmtStart + 1, (m.index ?? 0) + m[0].length))) continue;
    if (/create\s+table\s+(?:if\s+not\s+exists\s+)?"?(?!public)[a-z0-9_]+"?\./i.test(m[0])) continue;
    if (!enabled.has(t) && !loopEnable) out.push({ file, rule: "public_table_without_rls", detail: `create table ${t}: add "alter table public.${t} enable row level security" (and no grants to anon/authenticated/PUBLIC)` });
  }
  for (const m of sql.matchAll(GRANT_RE)) {
    const allowedColumnGrant = /grant\s+select\s*\(\s*id\s*,\s*org_id\s*,\s*company_id\s*\)\s+on\s+public\.reps\s+to\s+authenticated/i.test(m[0]);
    if (!allowedColumnGrant && !/^\s*grant[^;]*\bto\b[^;]*service_role\s*$/i.test(m[0])) out.push({ file, rule: "grant_to_public_role", detail: m[0].replace(/\s+/g, " ").slice(0, 120) });
  }
  return out;
}

function walk(dir: string, acc: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    const p = join(dir, f);
    if (statSync(p).isDirectory()) walk(p, acc);
    else if (f.endsWith(".sql")) acc.push(p);
  }
  return acc;
}

/** Files that legitimately contain the privilege statements this check polices. */
const EXEMPT = [/sql\/security\//, /sql\/00000000_base_schema\.sql$/, /sql\/staging\//, /sql\/seed\//];

/** Migrations dated before the lockdown are covered by it (RLS enabled + grants revoked on every existing table). Only NEWER ones are policed. */
const LOCKDOWN_DATE = 20261105;
export function isPostLockdown(rel: string): boolean {
  const m = /(?:^|\/)(\d{8})[a-z_]/i.exec(rel);
  return !!m && Number(m[1]) >= LOCKDOWN_DATE;
}

export function checkRepo(): Finding[] {
  const findings: Finding[] = [];
  for (const abs of walk(join(root, "sql"))) {
    const rel = abs.slice(root.length + 1);
    if (EXEMPT.some((re) => re.test(rel)) || !isPostLockdown(rel)) continue;
    findings.push(...analyseSql(rel, readFileSync(abs, "utf8")));
  }
  return findings;
}

async function live() {
  const url = process.env.DATABASE_URL;
  if (!url) { console.error("--live needs DATABASE_URL (or run the verify SQL through the Supabase CLI)"); process.exit(2); }
  const { Client } = await import("pg");
  const c = new Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
  await c.connect();
  const { rows } = await c.query(readFileSync(join(root, "sql/security/20261105_rls_lockdown_verify.sql"), "utf8"));
  await c.end();
  const by: Record<string, number> = {};
  for (const r of rows) by[r.violation] = (by[r.violation] || 0) + 1;
  console.log(rows.length ? `❌ ${rows.length} violation(s): ${JSON.stringify(by)}` : "✅ RLS posture clean (0 violations)");
  process.exit(rows.length ? 1 : 0);
}

if (process.argv[1] && /check-rls-posture/.test(process.argv[1])) {
  if (process.argv.includes("--live")) void live();
  else {
    const f = checkRepo();
    for (const x of f) console.log(`❌ ${x.file}: ${x.rule} — ${x.detail}`);
    console.log(f.length ? `\n❌ FAIL (${f.length})` : "✅ no migration creates an open public table or grants to anon/authenticated/PUBLIC");
    process.exit(f.length ? 1 : 0);
  }
}
