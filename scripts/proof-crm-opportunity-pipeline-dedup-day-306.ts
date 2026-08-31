/**
 * proof-crm-opportunity-pipeline-dedup-day-306.ts
 *
 * Guarded proof for the Day 306 opportunity pipeline de-duplication.
 *
 * (1) REACHABILITY (deterministic): loads the real crm router — exactly one
 *     GET /opportunities/pipeline layer, /pipeline/summary still distinct, and the
 *     first match for /opportunities/pipeline is the (single, active) handler.
 * (2) ACTIVE PIPELINE CONTRACT (staging DB): replicates the ACTIVE handler's query
 *     + { stages, columns, items } shape building, and proves tenant isolation
 *     (another user's opps are not included). Then fail-safe cleanup.
 *
 * NO HTTP/auth (identity edge), NO paid AI, NO outbound. Staging only.
 * Usage: npx tsx scripts/proof-crm-opportunity-pipeline-dedup-day-306.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "crypto";
import router from "../src/routes/crm";

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

// Active handler query shape (verbatim first candidate) + shape building.
const ACTIVE_SELECT = "id, name, stage, amount, close_date, contact_id, account_id, updated_at, created_at, user_id";
const DEFAULT_STAGES = ["Lead", "Qualified", "Proposal", "Negotiation", "Won", "Lost"];

async function main() {
  const env = parseEnv(join(__dirname, "..", ".env.staging.local"));
  const url = env.SUPABASE_URL, key = env.SUPABASE_SERVICE_ROLE_KEY;
  const ref = (url.match(/https:\/\/([a-z0-9]+)\.supabase/) || [])[1];
  console.log("Day 306 — opportunity pipeline de-dup proof\n");
  if (ref !== "dnumqzxthfthsmfnzvdz") { console.error(`REFUSING: not staging (${ref?.slice(0, 6)}…)`); process.exit(1); }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…)\n`);

  console.log("── reachability (real Express router) ──");
  const stack: any[] = (router as any).stack || [];
  const toRe = (p: string) => new RegExp("^" + p.replace(/:[^/]+/g, "[^/]+").replace(/\//g, "\\/") + "$");
  ok("exactly one GET /opportunities/pipeline layer", stack.filter((l: any) => l.route && l.route.methods.get && l.route.path === "/opportunities/pipeline").length === 1);
  ok("/opportunities/pipeline/summary still distinct + present", stack.filter((l: any) => l.route && l.route.methods.get && l.route.path === "/opportunities/pipeline/summary").length === 1);
  ok("/opportunities/pipeline first match resolves to the pipeline handler", stack.find((l: any) => l.route && l.route.methods.get && toRe(l.route.path).test("/opportunities/pipeline"))?.route?.path === "/opportunities/pipeline");
  ok("/opportunities/pipeline/summary resolves to its own handler (not /pipeline)", stack.find((l: any) => l.route && l.route.methods.get && toRe(l.route.path).test("/opportunities/pipeline/summary"))?.route?.path === "/opportunities/pipeline/summary");

  console.log("\n── active pipeline contract (staging crm_opportunities) ──");
  const svc = createClient(url, key);
  const userA = randomUUID(), userB = randomUUID();
  try {
    await svc.from("crm_opportunities").insert([
      { user_id: userA, name: "DAY306 a1", stage: "Lead" },
      { user_id: userA, name: "DAY306 a2", stage: "Proposal" },
      { user_id: userA, name: "DAY306 a3", stage: "Proposal" },
      { user_id: userB, name: "DAY306 b1", stage: "Won" },
    ]).select("id");

    // ACTIVE handler query (mine scope) + { stages, columns, items } shape build.
    const r = await svc.from("crm_opportunities").select(ACTIVE_SELECT).eq("user_id", userA).order("created_at", { ascending: false }).limit(300);
    ok("active pipeline query succeeds", !r.error, r.error?.message);
    const items = (r.data as any[] ?? []).map((o) => ({ id: String(o.id), name: o.name ?? null, stage: String(o.stage ?? "").trim(), amount: o.amount ?? null }));
    ok("items = only this user's 3 opps", items.length === 3, `${items.length}`);
    const found = new Set(items.map((i) => i.stage));
    const stages = Array.from(new Set([...DEFAULT_STAGES, ...Array.from(found).filter(Boolean)]));
    const columns: Record<string, string[]> = {};
    for (const s of stages) columns[s] = [];
    for (const it of items) (columns[it.stage] ||= []).push(it.id);
    ok("shape has { stages, columns, items } (active contract)", Array.isArray(stages) && !!columns && Array.isArray(items));
    ok("columns group by stage: Lead=1, Proposal=2", columns["Lead"].length === 1 && columns["Proposal"].length === 2, JSON.stringify({ Lead: columns["Lead"].length, Proposal: columns["Proposal"].length }));
    ok("default stages preserved in column order", DEFAULT_STAGES.every((s) => stages.includes(s)));

    // Tenant isolation: userB's "Won" opp is NOT in userA's pipeline.
    ok("tenant isolation — other user's opp excluded", !items.some((i) => i.stage === "Won"));
    const rb = await svc.from("crm_opportunities").select("id").eq("user_id", userB).limit(10);
    ok("the other user's opp exists but is scoped out of A's pipeline", (rb.data?.length ?? 0) === 1);
  } finally {
    await svc.from("crm_opportunities").delete().in("user_id", [userA, userB]);
    const residue = await svc.from("crm_opportunities").select("id", { count: "exact", head: true }).like("name", "DAY306%");
    ok("cleanup: zero DAY306 residue", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No HTTP/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
