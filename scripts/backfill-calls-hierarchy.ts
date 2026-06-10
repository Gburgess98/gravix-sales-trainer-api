/**
 * backfill-calls-hierarchy.ts
 *
 * Dev-data fix (Sprint 4, Day 91): legacy calls predate the hierarchy
 * columns, so calls.company_id / calls.office_id are null and
 * office/company-scoped managers see empty-but-correct dashboards.
 *
 * Fills ONLY null company_id/office_id on calls, copying from the call
 * owner's users row (falling back to their reps row). Never overwrites
 * a non-null value.
 *
 * Usage:
 *   tsx scripts/backfill-calls-hierarchy.ts          # dry run (default)
 *   tsx scripts/backfill-calls-hierarchy.ts --apply  # write changes
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const APPLY = process.argv.includes("--apply");

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

type Hierarchy = { company_id: string | null; office_id: string | null };

async function main() {
  console.log(`\n  Backfill — calls.company_id + calls.office_id ${APPLY ? "(APPLY)" : "(dry run)"}\n`);

  const { data: calls, error } = await supa
    .from("calls")
    .select("id, user_id, company_id, office_id")
    .or("company_id.is.null,office_id.is.null")
    .limit(10000);

  if (error) {
    console.error("✗  Failed to read calls:", error.message);
    process.exit(1);
  }

  const rows = (calls ?? []).filter((c) => c.user_id);
  console.log(`Found ${rows.length} calls with a missing company_id/office_id.`);
  if (!rows.length) return;

  // Resolve each owner's hierarchy once (users first, reps as fallback)
  const userIds = Array.from(new Set(rows.map((c) => String(c.user_id))));
  const hierarchyByUser = new Map<string, Hierarchy>();

  const { data: users } = await supa
    .from("users")
    .select("id, company_id, office_id")
    .in("id", userIds);
  for (const u of users ?? []) {
    hierarchyByUser.set(String(u.id), {
      company_id: u.company_id || null,
      office_id: u.office_id || null,
    });
  }

  const unresolved = userIds.filter((id) => {
    const h = hierarchyByUser.get(id);
    return !h || (!h.company_id && !h.office_id);
  });
  if (unresolved.length) {
    const { data: reps } = await supa
      .from("reps")
      .select("id, company_id, office_id")
      .in("id", unresolved);
    for (const r of reps ?? []) {
      const existing = hierarchyByUser.get(String(r.id));
      hierarchyByUser.set(String(r.id), {
        company_id: existing?.company_id || (r as any).company_id || null,
        office_id: existing?.office_id || (r as any).office_id || null,
      });
    }
  }

  let updated = 0;
  let skipped = 0;

  for (const call of rows) {
    const h = hierarchyByUser.get(String(call.user_id));
    const patch: Record<string, string> = {};
    if (!call.company_id && h?.company_id) patch.company_id = h.company_id;
    if (!call.office_id && h?.office_id) patch.office_id = h.office_id;

    if (!Object.keys(patch).length) {
      skipped += 1;
      continue;
    }

    if (APPLY) {
      const { error: updErr } = await supa.from("calls").update(patch).eq("id", call.id);
      if (updErr) {
        console.error(`✗  ${call.id}: ${updErr.message}`);
        continue;
      }
    }
    updated += 1;
  }

  console.log(`${APPLY ? "Updated" : "Would update"}: ${updated} calls`);
  console.log(`Skipped (owner has no hierarchy data): ${skipped} calls`);
  if (!APPLY) console.log("\nRe-run with --apply to write changes.");
}

main().then(() => process.exit(0));
