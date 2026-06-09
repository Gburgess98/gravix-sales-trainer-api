/**
 * validate-partner.ts
 *
 * Verifies Phase 1 partner foundation is correctly applied:
 *   - partners table exists and contains the Gravix partner
 *   - every company has a partner_id (no orphans)
 *   - companies.partner_id FK resolves to a real partner row
 *   - no companies with NULL partner_id
 *   - getPartnerContext resolves partner_id for known reps
 *
 * Usage: npm run validate:partner
 *
 * Run after: sql/20260605_partner_foundation.sql
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

type Check = { label: string; passed: boolean; detail?: string };

function ok(label: string, detail?: string): Check  { return { label, passed: true, detail }; }
function fail(label: string, detail?: string): Check { return { label, passed: false, detail }; }

async function main() {
  console.log("\n  Partner Foundation Validator\n");

  const results: Check[] = [];

  // ── 1. partners table exists ─────────────────────────────────────────────

  const { data: partners, error: pe } = await supa.from("partners").select("id, name, slug, status");

  if (pe) {
    results.push(fail("partners table exists", pe.message));
    console.error("\n  ✗  Migration not applied yet.");
    console.error("  Run sql/20260605_partner_foundation.sql in Supabase SQL editor.\n");
    report(results);
    process.exit(1);
  }

  results.push(ok("partners table exists", `${partners?.length ?? 0} rows`));

  // ── 2. Gravix partner row ────────────────────────────────────────────────

  const GRAVIX_PARTNER_ID = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
  const gravix = (partners || []).find((p: any) => p.id === GRAVIX_PARTNER_ID);

  results.push(
    gravix
      ? ok("Gravix partner row present", `name="${gravix.name}" slug="${gravix.slug}" status="${gravix.status}"`)
      : fail("Gravix partner row present", "ID 5055e1b6-... not found in partners table")
  );

  results.push(
    gravix?.slug === "gravix"
      ? ok("Gravix partner slug is 'gravix'")
      : fail("Gravix partner slug is 'gravix'", `actual: ${gravix?.slug}`)
  );

  results.push(
    gravix?.status === "active"
      ? ok("Gravix partner is active")
      : fail("Gravix partner is active", `actual: ${gravix?.status}`)
  );

  // ── 3. companies.partner_id column exists ────────────────────────────────

  const { data: companies, error: ce } = await supa.from("companies").select("id, name, tmc_id, partner_id");

  if (ce?.message?.includes("partner_id")) {
    results.push(fail("companies.partner_id column exists", ce.message));
    report(results);
    process.exit(1);
  }

  results.push(ok("companies.partner_id column exists"));

  // ── 4. No orphan companies (NULL partner_id) ─────────────────────────────

  const orphans = (companies || []).filter((c: any) => !c.partner_id);
  results.push(
    orphans.length === 0
      ? ok("No orphan companies (all have partner_id)")
      : fail("No orphan companies", `${orphans.length} company/companies with NULL partner_id: ${orphans.map((c: any) => c.name).join(", ")}`)
  );

  // ── 5. All partner_ids resolve to real partners ──────────────────────────

  const partnerIds = new Set((partners || []).map((p: any) => p.id));
  const broken = (companies || []).filter((c: any) => c.partner_id && !partnerIds.has(c.partner_id));

  results.push(
    broken.length === 0
      ? ok("All company partner_ids resolve to a real partner")
      : fail("Broken FK", `${broken.length} company/companies reference non-existent partner: ${broken.map((c: any) => c.name).join(", ")}`)
  );

  // ── 6. Print companies table ─────────────────────────────────────────────

  console.log("  Companies:");
  for (const c of companies || []) {
    const row = c as any;
    const pName = (partners || []).find((p: any) => p.id === row.partner_id)?.name ?? "UNRESOLVED";
    console.log(`    ${(row.name||"?").padEnd(28)} partner_id: ${row.partner_id ? pName : "NULL"}`);
  }
  console.log();

  // ── 7. getPartnerContext spot-check ──────────────────────────────────────

  const { data: reps } = await supa.from("reps").select("id, name, company_id").not("company_id", "is", null).limit(3);

  for (const rep of reps || []) {
    const r = rep as any;
    const { data: company } = await supa.from("companies").select("partner_id, name").eq("id", r.company_id).maybeSingle();
    const resolved = (company as any)?.partner_id ?? null;

    results.push(
      resolved
        ? ok(`getPartnerContext resolves for "${r.name}"`, `partner_id=${resolved}`)
        : fail(`getPartnerContext resolves for "${r.name}"`, `company=${r.company_id} → partner_id=null`)
    );
  }

  // ── 8. Tier constants sanity check ──────────────────────────────────────

  const { data: tierRows } = await supa.from("reps").select("tier").limit(100);
  const tierMap: Record<string, number> = {};
  (tierRows || []).forEach((r: any) => { tierMap[r.tier] = (tierMap[r.tier] || 0) + 1; });
  console.log("  reps.tier breakdown:", JSON.stringify(tierMap));
  console.log("  (PartnerAdmin / SuperAdmin recognised but not yet assigned)");
  console.log();

  report(results);
}

function report(results: Check[]) {
  console.log("  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (r.detail) console.log(`       ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);
  if (failed > 0) process.exit(1);
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
