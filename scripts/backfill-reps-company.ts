/**
 * backfill-reps-company.ts
 *
 * Phase 1 identity bridge: populate reps.company_id and reps.office_id
 * from the users table where IDs overlap, then apply known company
 * assignments for demo reps.
 *
 * Prerequisite: run sql/20260604_reps_company_office_bridge.sql in
 * the Supabase SQL editor first.
 *
 * Usage: tsx scripts/backfill-reps-company.ts
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

// Company IDs from companies table
const UFC_COMPANY_ID  = "bfb9604e-bc2f-46fa-be97-6461e98e1a19"; // Gravix Demo Company (UFC demo)
const GRAVIX_COMPANY_ID = "c1c17223-aa46-4998-8995-de6bf25a23e6"; // Gravix Test Company (George)

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

async function checkSchemaReady(): Promise<boolean> {
  const { data, error } = await supa.from("reps").select("company_id").limit(1);
  if (error?.message?.toLowerCase().includes("company_id")) {
    console.error("✗  reps.company_id column does not exist.");
    console.error("   Run sql/20260604_reps_company_office_bridge.sql in the Supabase SQL editor first.");
    return false;
  }
  return true;
}

async function main() {
  console.log("\n  Backfill — reps.company_id + reps.office_id\n");

  if (!(await checkSchemaReady())) process.exit(1);

  // 1. Fetch all reps
  const { data: reps, error: rErr } = await supa
    .from("reps")
    .select("id, name, tier, org_id, company_id, office_id");
  if (rErr) { console.error("✗  reps fetch:", rErr.message); process.exit(1); }

  // 2. Fetch all users
  const { data: users, error: uErr } = await supa
    .from("users")
    .select("id, email, company_id, office_id");
  if (uErr) { console.error("✗  users fetch:", uErr.message); process.exit(1); }

  const userMap = new Map((users || []).map((u: any) => [u.id, u]));

  let updated = 0;
  let skipped = 0;
  let noMatch = 0;

  for (const rep of reps || []) {
    const existing = rep as any;

    // Already has company_id — only update if it changed
    let targetCompanyId: string | null = existing.company_id ?? null;
    let targetOfficeId: string | null  = existing.office_id  ?? null;

    const userRow = userMap.get(existing.id) as any;

    if (userRow?.company_id) {
      // Backfill from users row (authoritative source)
      targetCompanyId = userRow.company_id;
      targetOfficeId  = userRow.office_id ?? null;
    } else if (!targetCompanyId) {
      // No users row and no existing company_id → Gravix Test Company.
      // No rep should remain with NULL company_id.
      targetCompanyId = GRAVIX_COMPANY_ID;
    }

    // Skip if nothing changed
    if (targetCompanyId === existing.company_id && targetOfficeId === existing.office_id) {
      skipped++;
      continue;
    }

    if (!targetCompanyId) { noMatch++; continue; }

    const { error: upErr } = await supa
      .from("reps")
      .update({ company_id: targetCompanyId, office_id: targetOfficeId })
      .eq("id", existing.id);

    if (upErr) {
      console.warn(`  ⚠  ${existing.name} (${existing.id}): ${upErr.message}`);
    } else {
      const prev = existing.company_id ? `${existing.company_id.slice(0, 8)}...` : "null";
      const next = targetCompanyId.slice(0, 8) + "...";
      console.log(`  ✓  ${(existing.name || existing.id).padEnd(25)} ${prev} → ${next}`);
      updated++;
    }
  }

  console.log(`\n  Updated: ${updated}  |  Already correct: ${skipped}  |  No match: ${noMatch}\n`);

  // 3. Verify
  const { data: verify } = await supa
    .from("reps")
    .select("name, company_id, office_id, tier")
    .order("tier")
    .order("name");

  console.log("  Final reps.company_id state:");
  console.log(`  ${"Name".padEnd(25)} ${"company_id".padEnd(36)} office_id`);
  console.log(`  ${"─".repeat(25)} ${"─".repeat(36)} ${"─".repeat(36)}`);
  for (const r of verify || []) {
    const row = r as any;
    console.log(`  ${(row.name||"?").padEnd(25)} ${(row.company_id||"null").padEnd(36)} ${row.office_id||"null"}`);
  }
}

main().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
