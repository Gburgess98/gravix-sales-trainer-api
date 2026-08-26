/**
 * proof-review-flag-activity-day-300.ts
 *
 * Guarded staging proof for the Day 300 review_flag activity contract. Writes the
 * EXACT corrected payload (via the real exported buildReviewFlagActivityRow) for
 * both the primary and fallback provenance directly into staging crm_activities,
 * reads them back, verifies the canonical contract, then fail-safe cleans up.
 *
 * NO scoring execution, NO paid AI, NO Slack/email/assignment. crm_activities has
 * no FK constraints, so only synthetic DAY300 uuids are needed (no parent rows).
 *
 * Usage: npx tsx scripts/proof-review-flag-activity-day-300.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "crypto";
import { buildReviewFlagActivityRow } from "../src/lib/scoring";

function parseEnv(p: string): Record<string, string> {
  const o: Record<string, string> = {};
  for (const l of readFileSync(p, "utf8").split("\n")) {
    const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m) o[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
  return o;
}

let failures = 0;
const ok = (label: string, cond: boolean, detail?: string) => {
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${label}${!cond && detail ? `  — ${detail}` : ""}`);
  if (!cond) failures += 1;
};

async function main() {
  const env = parseEnv(join(__dirname, "..", ".env.staging.local"));
  const url = env.SUPABASE_URL;
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  const ref = (url.match(/https:\/\/([a-z0-9]+)\.supabase/) || [])[1];

  console.log("Day 300 — review_flag activity guarded staging proof\n");
  // ── Environment guard: STAGING ONLY ──────────────────────────────────────────
  if (ref !== "dnumqzxthfthsmfnzvdz") {
    console.error(`REFUSING: target ref is not the dedicated staging project (got ${ref?.slice(0, 6)}…). No write attempted.`);
    process.exit(1);
  }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…) — proceeding\n`);

  const svc = createClient(url, key);

  // Synthetic DAY300 context (no FKs on crm_activities → no parent rows required).
  const ctx = {
    userId: randomUUID(),
    orgId: randomUUID(),
    callId: randomUUID(),
    accountId: randomUUID(),
    contactId: randomUUID(),
  };
  const thresholds = { low: 65, critical: 45 };
  const primaryFlag = { flag_key: "overall_below_critical", type: "overall_below_critical", category: "score", label: "DAY300 overall below critical", severity: "critical", section: "overall", score: 30, timestamp: 12 };
  const fallbackFlag = { flag_key: "close_below_low", type: "close_below_low", category: "score", label: "DAY300 close below low", severity: "low", section: "close", score: 60, timestamp: null };

  const primaryRow = buildReviewFlagActivityRow({ flag: primaryFlag, userId: ctx.userId, orgId: ctx.orgId, callId: ctx.callId, accountId: ctx.accountId, contactId: ctx.contactId, source: "scoring_engine", thresholds });
  const fallbackRow = buildReviewFlagActivityRow({ flag: fallbackFlag, userId: ctx.userId, orgId: ctx.orgId, callId: ctx.callId, accountId: ctx.accountId, contactId: ctx.contactId, source: "scoring_engine_fallback", thresholds });

  const insertedIds: string[] = [];
  try {
    // ── Insert both provenance shapes ──────────────────────────────────────────
    const ins = await svc.from("crm_activities").insert([primaryRow, fallbackRow]).select("id");
    ok("both rows inserted (no schema error)", !ins.error && (ins.data?.length === 2), ins.error?.message);
    for (const r of ins.data || []) insertedIds.push((r as any).id);

    // ── Read back by synthetic call_id ─────────────────────────────────────────
    const back = await svc
      .from("crm_activities")
      .select("id,type,title,user_id,rep_id,org_id,call_id,account_id,contact_id,source,meta")
      .eq("call_id", ctx.callId)
      .order("source", { ascending: true });
    ok("read back exactly the 2 synthetic rows", !back.error && back.data?.length === 2, back.error?.message ?? `${back.data?.length}`);

    const rows = back.data || [];
    const prim = rows.find((r: any) => r.source === "scoring_engine");
    const fb = rows.find((r: any) => r.source === "scoring_engine_fallback");

    ok("provenance distinguishes primary vs fallback", !!prim && !!fb);

    for (const [name, row, flag] of [["primary", prim, primaryFlag], ["fallback", fb, fallbackFlag]] as const) {
      if (!row) { ok(`${name} row present`, false); continue; }
      ok(`${name}: type = 'review_flag'`, row.type === "review_flag");
      ok(`${name}: title is truthful (flag label)`, row.title === flag.label, JSON.stringify(row.title));
      ok(`${name}: NOT-NULL user_id persisted = synthetic rep`, row.user_id === ctx.userId);
      ok(`${name}: org_id / rep_id / call_id persisted`, row.org_id === ctx.orgId && row.rep_id === ctx.userId && row.call_id === ctx.callId);
      ok(`${name}: account_id / contact_id linkage persisted`, row.account_id === ctx.accountId && row.contact_id === ctx.contactId);
      const meta = (row.meta || {}) as any;
      ok(`${name}: meta preserves flag type/category/severity/section`,
        meta.flag_type === flag.type && meta.flag_category === "score" && meta.flag_severity === flag.severity && meta.flag_section === flag.section,
        JSON.stringify({ t: meta.flag_type, c: meta.flag_category, s: meta.flag_severity, sec: meta.flag_section }));
      ok(`${name}: meta preserves score/timestamp/threshold_band/thresholds`,
        meta.score === flag.score && meta.threshold_band === flag.severity && meta.thresholds?.low === 65 && meta.thresholds?.critical === 45);
      ok(`${name}: meta provenance matches source`, meta.provenance === row.source);
      // Prove no phantom columns leaked as top-level (schema would have rejected them anyway).
      ok(`${name}: no top-level summary/flag_* columns exist`, !("summary" in row) && !("flag_key" in row) && !("flag_severity" in row));
    }
  } finally {
    // ── Fail-safe cleanup ──────────────────────────────────────────────────────
    await svc.from("crm_activities").delete().eq("call_id", ctx.callId);
    const residue = await svc.from("crm_activities").select("id", { count: "exact", head: true }).eq("call_id", ctx.callId);
    ok("cleanup: zero DAY300 residue for synthetic call", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No scoring/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
