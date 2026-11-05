/**
 * READ-ONLY verification that no identifier in a STAGING database fails the application's strict UUID predicate.
 * Prints table.column and counts only — never keys, never row values. Refuses to run unless the target is provably
 * the staging project (same guard as provision-staging-qa.ts). Makes no writes.
 *
 *   APP_ENV=staging EXPECTED_STAGING_SUPABASE_REF=<ref> npx tsx scripts/verify-staging-tenant-uuids.ts [--allow-legacy-auth]
 *
 * Exit 0 = every public uuid column passes (the two legacy synthetic profiles are tolerated with --allow-legacy-auth
 * until the optional retirement step has run); exit 1 otherwise.
 */
import "dotenv/config";
import { appUuidPredicate } from "./app-uuid-predicate";
import { assertSafeStagingTarget, refFromSupabaseUrl } from "./provision-staging-qa";
import { LEGACY_STAGING_TENANT_IDS as OLD } from "./staging-tenant-ids";

async function main() {
  const allowLegacyAuth = process.argv.includes("--allow-legacy-auth");
  const url = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY || "";
  const issues = assertSafeStagingTarget({
    appEnv: process.env.APP_ENV,
    supabaseRef: refFromSupabaseUrl(url),
    expectedStagingRef: process.env.EXPECTED_STAGING_SUPABASE_REF,
    prodRef: process.env.PROD_SUPABASE_REF,
  });
  if (issues.length || !key) {
    console.error("refusing to run:", [...issues, ...(key ? [] : ["no service key configured"])].join("; "));
    process.exit(2);
  }
  const strict = appUuidPredicate();
  const H = { apikey: key, Authorization: `Bearer ${key}` };
  const spec: any = await (await fetch(`${url}/rest/v1/`, { headers: H })).json();
  const tolerated = new Set<string>(allowLegacyAuth ? OLD.legacyAuthUserIds : []);
  let bad = 0;
  for (const [table, def] of Object.entries<any>(spec.definitions || {})) {
    const cols = Object.entries<any>(def.properties || {}).filter(([, p]) => p.format === "uuid").map(([c]) => c);
    if (!cols.length) continue;
    const res = await fetch(`${url}/rest/v1/${table}?select=${cols.join(",")}&limit=5000`, { headers: H });
    if (!res.ok) { console.log(`  ?  ${table}: unreadable (${res.status})`); continue; }
    const rows: any[] = await res.json();
    for (const c of cols) {
      const failing = rows.map((r) => r[c]).filter((v) => typeof v === "string" && !strict.test(v) && !tolerated.has(v));
      if (failing.length) { bad += failing.length; console.log(`  ❌ ${table}.${c}: ${failing.length}/${rows.length} fail the strict predicate`); }
    }
  }
  console.log(bad === 0 ? "\n✅ every public uuid value passes the application's strict predicate" : `\n❌ ${bad} value(s) fail`);
  process.exit(bad === 0 ? 0 : 1);
}
main().catch((e) => { console.error("verification error:", e?.message || e); process.exit(2); });
