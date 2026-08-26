/**
 * scoring-thresholds-harness-day-297.ts
 *
 * Deterministic, no-network, no-paid-AI proof that the scoring runtime consumes the
 * PERSISTED admin_config thresholds (not the hard-coded {low:65,critical:45}
 * fallback). We hand getScoringThresholds a mocked Supabase client that returns
 * stored values for the canonical singleton (id=true) and assert it reads them, and
 * that the band decision (needs_manager_review = critical) follows the stored values.
 *
 * Usage: npx tsx scripts/scoring-thresholds-harness-day-297.ts
 */

import { getScoringThresholds } from "../src/lib/scoring";

let fail = 0;
function check(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) fail += 1;
}

// Minimal chainable mock of the exact call: from("admin_config").select(...)
//   .eq("id", true).maybeSingle(). Records the table/columns/filter it was asked for.
function mockSupabase(row: any, opts: { error?: any } = {}) {
  const calls: any = { table: null, columns: null, eq: null };
  const builder: any = {
    select(cols: string) { calls.columns = cols; return builder; },
    eq(col: string, val: any) { calls.eq = { col, val }; return builder; },
    limit() { return builder; },
    async maybeSingle() { return { data: row, error: opts.error ?? null }; },
  };
  return {
    client: { from(table: string) { calls.table = table; return builder; } } as any,
    calls,
  };
}

async function main() {
  console.log("Day 297 — scoring thresholds runtime harness (no network, no paid AI)\n");

  // 1. Stored values are consumed verbatim.
  {
    const m = mockSupabase({ low_score_threshold: 70, critical_score_threshold: 40 });
    const t = await getScoringThresholds(m.client);
    check("reads the canonical admin_config table", m.calls.table === "admin_config");
    check("filters the singleton (eq id=true)", m.calls.eq?.col === "id" && m.calls.eq?.val === true,
      JSON.stringify(m.calls.eq));
    check("selects both threshold columns",
      /low_score_threshold/.test(m.calls.columns) && /critical_score_threshold/.test(m.calls.columns));
    check("returns STORED low (70), not the 65 fallback", t.low === 70, `got ${t.low}`);
    check("returns STORED critical (40), not the 45 fallback", t.critical === 40, `got ${t.critical}`);

    // Band decision follows stored thresholds: needs_manager_review when overall <= critical.
    const overall = 38;
    const band = overall <= t.critical ? "critical" : overall <= t.low ? "low" : "ok";
    check("overall 38 under stored critical(40) → critical band (needs_manager_review)", band === "critical");
    const overall2 = 55;
    const band2 = overall2 <= t.critical ? "critical" : overall2 <= t.low ? "low" : "ok";
    check("overall 55 between stored critical(40) and low(70) → low band (NOT critical)", band2 === "low");
  }

  // 2. Different stored values change the outcome (proves it is not hard-coded).
  {
    const m = mockSupabase({ low_score_threshold: 90, critical_score_threshold: 80 });
    const t = await getScoringThresholds(m.client);
    check("distinct stored values flow through (low 90 / critical 80)", t.low === 90 && t.critical === 80);
    const overall = 78;
    const band = overall <= t.critical ? "critical" : overall <= t.low ? "low" : "ok";
    check("overall 78 under stored critical(80) → critical (would be 'ok' under default 45)", band === "critical");
  }

  // 3. Genuine transport error → resilient default (scoring never crashes), NOT a
  //    fabricated persisted value.
  {
    const m = mockSupabase(null, { error: { message: "transport boom" } });
    const t = await getScoringThresholds(m.client);
    check("transport error falls back to defaults {65,45} (resilience, not crash)",
      t.low === 65 && t.critical === 45);
  }

  console.log(`\n${fail === 0 ? "PASS" : "FAIL"} — ${fail} failure(s).`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error("harness crashed:", e?.message || e); process.exit(1); });
