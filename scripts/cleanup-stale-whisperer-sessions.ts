/**
 * cleanup-stale-whisperer-sessions.ts — Tier 2B Day 118
 *
 * Marks abandoned Whisperer sessions (status='active' older than the threshold)
 * as ended, so testing/live sessions that were never closed cleanly stop
 * inflating the "active" count in /coaching.
 *
 * Usage:
 *   tsx scripts/cleanup-stale-whisperer-sessions.ts            # dry run (default)
 *   tsx scripts/cleanup-stale-whisperer-sessions.ts --apply    # write changes
 *   STALE_MINUTES=60 tsx scripts/cleanup-stale-whisperer-sessions.ts --apply
 *
 * Safe + idempotent: only touches status='active' rows past the threshold.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

const APPLY = process.argv.includes("--apply");
const STALE_MINUTES = Math.max(1, parseInt(String(process.env.STALE_MINUTES || "30"), 10) || 30);

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

async function main() {
  console.log(`\n  Whisperer stale-session cleanup ${APPLY ? "(APPLY)" : "(dry run)"} — threshold ${STALE_MINUTES}m\n`);

  const cutoff = new Date(Date.now() - STALE_MINUTES * 60_000).toISOString();

  const { data, error } = await supa
    .from("whisperer_sessions")
    .select("id, started_at")
    .eq("status", "active")
    .lt("started_at", cutoff)
    .limit(5000);

  if (error) {
    if (String(error.message || "").toLowerCase().includes("whisperer_sessions")) {
      console.error("✗  whisperer_sessions table not found. Run sql/20260612_whisperer_stub_loop.sql first.");
      process.exit(1);
    }
    throw error;
  }

  const rows = data ?? [];
  console.log(`Sessions scanned (active, older than ${STALE_MINUTES}m): ${rows.length}`);
  if (!rows.length) {
    console.log("Nothing to clean up.");
    return;
  }

  if (!APPLY) {
    console.log(`Would mark ${rows.length} session(s) as ended. Re-run with --apply to write.`);
    return;
  }

  const nowIso = new Date().toISOString();
  let updated = 0;
  for (const row of rows) {
    const { error: updErr } = await supa
      .from("whisperer_sessions")
      .update({ status: "ended", ended_at: nowIso, updated_at: nowIso } as any)
      .eq("id", row.id)
      .eq("status", "active"); // guard: only if still active
    if (updErr) {
      console.error(`✗  ${row.id}: ${updErr.message}`);
      continue;
    }
    updated += 1;
  }
  console.log(`Updated: ${updated} session(s) → ended`);
}

main().then(() => process.exit(0));
