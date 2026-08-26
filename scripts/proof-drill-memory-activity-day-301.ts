/**
 * proof-drill-memory-activity-day-301.ts
 *
 * Guarded staging proof for the Day 301 drill_memory activity contract. Uses the
 * REAL exported buildDrillMemoryActivityRow to write the completion and
 * creation-tracking rows into staging crm_activities, reads them back, and proves
 * the canonical contract. Also proves the OLD payload (type+rep_id+meta, no
 * user_id) fails the not-null constraint (negative control). Fail-safe cleanup.
 *
 * NO scoring/assignment execution, NO paid AI, NO Slack/email/XP. crm_activities has
 * no FK constraints, so only synthetic DAY301 uuids are needed.
 *
 * Usage: npx tsx scripts/proof-drill-memory-activity-day-301.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "crypto";
import { buildDrillMemoryActivityRow } from "../src/routes/assignments";

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

  console.log("Day 301 — drill_memory activity guarded staging proof\n");
  if (ref !== "dnumqzxthfthsmfnzvdz") {
    console.error(`REFUSING: target is not the dedicated staging project (${ref?.slice(0, 6)}…). No write.`);
    process.exit(1);
  }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…)\n`);

  const svc = createClient(url, key);
  const userId = randomUUID(); // synthetic DAY301 rep

  const completionRow = buildDrillMemoryActivityRow({ userId, section: "close", completed: true, source: "assignment_complete", extraMeta: { completed_at: new Date().toISOString(), day301_probe: true } });
  const creationRow = buildDrillMemoryActivityRow({ userId, section: "objection", completed: false, source: "assignment_created", extraMeta: { created_from_assignment: true, assignment_id: randomUUID(), day301_probe: true } });

  try {
    // ── Negative control: the OLD payload must FAIL the not-null constraint ──────
    const bad = await svc.from("crm_activities").insert({ type: "drill_memory", rep_id: userId, meta: { section: "close", completed: true, day301_probe: true } } as any).select("id");
    ok("OLD payload (no user_id/title) is REJECTED by the not-null constraint", !!bad.error && /not-null|null value/i.test(bad.error.message), bad.error?.message ?? "unexpectedly inserted");

    // ── Corrected rows insert cleanly ───────────────────────────────────────────
    const ins = await svc.from("crm_activities").insert([completionRow, creationRow]).select("id");
    ok("both corrected drill_memory rows inserted (no schema error)", !ins.error && ins.data?.length === 2, ins.error?.message);

    // ── Read back by synthetic user ─────────────────────────────────────────────
    const back = await svc
      .from("crm_activities")
      .select("id,type,title,user_id,rep_id,source,meta")
      .eq("user_id", userId)
      .order("source", { ascending: true });
    ok("read back exactly the 2 synthetic rows", !back.error && back.data?.length === 2, back.error?.message ?? `${back.data?.length}`);

    const rows = back.data || [];
    const comp = rows.find((r: any) => r.source === "assignment_complete");
    const crea = rows.find((r: any) => r.source === "assignment_created");
    ok("provenance distinguishes completion vs creation", !!comp && !!crea);

    for (const [name, row, section, completed] of [["completion", comp, "close", true], ["creation", crea, "objection", false]] as const) {
      if (!row) { ok(`${name} row present`, false); continue; }
      ok(`${name}: type = 'drill_memory'`, row.type === "drill_memory");
      ok(`${name}: NOT-NULL title truthful`, typeof row.title === "string" && row.title.includes(section) && row.title.startsWith(completed ? "Drill completed" : "Drill assigned"), JSON.stringify(row.title));
      ok(`${name}: NOT-NULL user_id = synthetic rep`, row.user_id === userId);
      ok(`${name}: rep_id preserved`, row.rep_id === userId);
      const meta = (row.meta || {}) as any;
      ok(`${name}: meta preserves section + completed`, meta.section === section && meta.completed === completed);
      ok(`${name}: no top-level flag_*/section columns`, !("section" in row) && !("flag_section" in row));
    }
  } finally {
    await svc.from("crm_activities").delete().eq("user_id", userId);
    const residue = await svc.from("crm_activities").select("id", { count: "exact", head: true }).eq("user_id", userId);
    ok("cleanup: zero DAY301 residue", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No scoring/assignment/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
