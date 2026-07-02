/**
 * Day 168 — backfill missing tenant stamps on assignments.
 *
 * Assignments created for auth-first reps (reps-table identities with no
 * users row) were stamped with office_id/company_id NULL, so every scoped
 * manager query hid them (Command Centre showed 0 open assignments while
 * rows existed). The creation path now falls back to the rep's reps row;
 * this script repairs the existing rows the same way.
 *
 * Idempotent: only touches rows where company_id IS NULL, resolves each
 * rep via users first then reps (Phase 1 identity bridge), and skips reps
 * that resolve nowhere. No schema change, no new data created.
 *
 * Run: npx tsx scripts/backfill-assignment-tenant-stamps.ts [--dry-run]
 */
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const dryRun = process.argv.includes("--dry-run");

async function resolveHierarchy(repId: string) {
  const { data: userRow } = await supa
    .from("users")
    .select("office_id, company_id")
    .eq("id", repId)
    .maybeSingle();
  if (userRow?.company_id || userRow?.office_id) {
    return { office_id: userRow.office_id || null, company_id: userRow.company_id || null };
  }
  const { data: repRow } = await supa
    .from("reps")
    .select("office_id, company_id")
    .eq("id", repId)
    .maybeSingle();
  if (repRow?.company_id || repRow?.office_id) {
    return { office_id: repRow.office_id || null, company_id: repRow.company_id || null };
  }
  return null;
}

async function main() {
  const { data: rows, error } = await supa
    .from("assignments")
    .select("id, rep_id")
    .is("company_id", null)
    .limit(2000);
  if (error) throw error;

  console.log(`${rows?.length ?? 0} assignments with company_id NULL${dryRun ? " (dry run)" : ""}`);

  const byRep = new Map<string, string[]>();
  for (const r of rows ?? []) {
    const repId = String(r.rep_id || "");
    if (!repId) continue;
    if (!byRep.has(repId)) byRep.set(repId, []);
    byRep.get(repId)!.push(String(r.id));
  }

  let updated = 0;
  let skipped = 0;
  for (const [repId, ids] of byRep) {
    const h = await resolveHierarchy(repId);
    if (!h?.company_id) {
      console.log(`SKIP  rep ${repId} — no company resolvable (${ids.length} rows)`);
      skipped += ids.length;
      continue;
    }
    if (dryRun) {
      console.log(`DRY   rep ${repId} → company ${h.company_id} office ${h.office_id} (${ids.length} rows)`);
      updated += ids.length;
      continue;
    }
    const { error: uErr } = await supa
      .from("assignments")
      .update({ company_id: h.company_id, office_id: h.office_id })
      .in("id", ids);
    if (uErr) throw uErr;
    console.log(`OK    rep ${repId} → company ${h.company_id} office ${h.office_id} (${ids.length} rows)`);
    updated += ids.length;
  }

  console.log(`\nDone: ${updated} stamped, ${skipped} skipped.`);
}

main().catch((e) => {
  console.error("backfill failed:", e?.message || e);
  process.exit(1);
});
