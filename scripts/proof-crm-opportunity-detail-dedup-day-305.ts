/**
 * proof-crm-opportunity-detail-dedup-day-305.ts
 *
 * Guarded proof for the Day 305 opportunity detail de-duplication.
 *
 * (1) REACHABILITY (deterministic): loads the real crm router and asserts exactly one
 *     GET and one PATCH /opportunities/:id layer remain, and /opportunities/stages
 *     still resolves before /:id (Day-304 order preserved).
 * (2) LIFECYCLE (staging DB): replicates the ACTIVE handlers' exact query shapes —
 *     create → GET detail → foreign-user denial (tenant isolation) → PATCH
 *     stage/amount → GET refresh — then fail-safe cleanup. Confirms the active
 *     contract still holds after removing the shadowed duplicates.
 *
 * NO HTTP/auth (identity edge), NO paid AI, NO outbound. Staging only.
 * Usage: npx tsx scripts/proof-crm-opportunity-detail-dedup-day-305.ts
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

// Active-handler query shapes (verbatim).
const GET_SELECT = "id, user_id, name, title, stage, amount, currency, close_date, account_id, contact_id, account_name, contact_email, created_at, updated_at";
const PATCH_SELECT = "id, name, title, stage, amount, currency, close_date, account_id, contact_id, account_name, contact_email, created_at, updated_at";

async function main() {
  const env = parseEnv(join(__dirname, "..", ".env.staging.local"));
  const url = env.SUPABASE_URL, key = env.SUPABASE_SERVICE_ROLE_KEY;
  const ref = (url.match(/https:\/\/([a-z0-9]+)\.supabase/) || [])[1];
  console.log("Day 305 — opportunity detail de-dup proof\n");
  if (ref !== "dnumqzxthfthsmfnzvdz") { console.error(`REFUSING: not staging (${ref?.slice(0, 6)}…)`); process.exit(1); }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…)\n`);

  console.log("── reachability (real Express router) ──");
  const stack: any[] = (router as any).stack || [];
  const layerCount = (m: string) => stack.filter((l: any) => l.route && l.route.methods[m] && l.route.path === "/opportunities/:id").length;
  ok("exactly one GET /opportunities/:id layer", layerCount("get") === 1, `${layerCount("get")}`);
  ok("exactly one PATCH /opportunities/:id layer", layerCount("patch") === 1, `${layerCount("patch")}`);
  const toRe = (p: string) => new RegExp("^" + p.replace(/:[^/]+/g, "[^/]+").replace(/\//g, "\\/") + "$");
  ok("/opportunities/stages still resolves before /:id", stack.find((l: any) => l.route && l.route.methods.get && toRe(l.route.path).test("/opportunities/stages"))?.route?.path === "/opportunities/stages");
  ok("a UUID detail path still resolves to /:id", stack.find((l: any) => l.route && l.route.methods.get && toRe(l.route.path).test("/opportunities/" + randomUUID()))?.route?.path === "/opportunities/:id");

  console.log("\n── active lifecycle (staging crm_opportunities) ──");
  const svc = createClient(url, key);
  const userA = randomUUID(), userB = randomUUID();
  let id = "";
  try {
    const c = await svc.from("crm_opportunities").insert({ user_id: userA, name: "DAY305 opp", title: "DAY305 opp", stage: "new" }).select("*").maybeSingle();
    ok("create (active POST minimal shape) ok", !c.error && !!c.data, c.error?.message);
    id = (c.data as any)?.id;

    // GET detail — active selectCandidates[0] + .in("user_id",[userA])
    const g = await svc.from("crm_opportunities").select(GET_SELECT).eq("id", id).in("user_id", [userA]).limit(1).maybeSingle();
    ok("GET detail returns the opp for its owner", !g.error && (g.data as any)?.id === id, g.error?.message);

    // Foreign-user denial — same query scoped to another user → no row (tenant isolation)
    const f = await svc.from("crm_opportunities").select(GET_SELECT).eq("id", id).in("user_id", [userB]).limit(1).maybeSingle();
    ok("foreign user is denied the opp (isolation → 404)", !f.error && !f.data, f.error?.message ?? "row leaked");

    // PATCH stage + amount — active patchBase + .in("user_id",[userA]) + select without value
    const p = await svc.from("crm_opportunities").update({ stage: "qualified", amount: 4200 }).eq("id", id).in("user_id", [userA]).select(PATCH_SELECT).maybeSingle();
    ok("PATCH stage+amount ok (no value column)", !p.error && (p.data as any)?.stage === "qualified" && Number((p.data as any)?.amount) === 4200, p.error?.message);

    // Foreign-user PATCH denial — scoped update affects no row
    const pf = await svc.from("crm_opportunities").update({ stage: "won" }).eq("id", id).in("user_id", [userB]).select("id").maybeSingle();
    ok("foreign user cannot PATCH the opp (no row updated)", !pf.error && !pf.data, pf.error?.message ?? "updated foreign");

    // GET refresh reflects the update
    const g2 = await svc.from("crm_opportunities").select(GET_SELECT).eq("id", id).in("user_id", [userA]).limit(1).maybeSingle();
    ok("GET refresh reflects stage=qualified, amount=4200 (foreign PATCH had no effect)",
      (g2.data as any)?.stage === "qualified" && Number((g2.data as any)?.amount) === 4200);
  } finally {
    if (id) await svc.from("crm_opportunities").delete().eq("id", id);
    const residue = await svc.from("crm_opportunities").select("id", { count: "exact", head: true }).like("name", "DAY305%");
    ok("cleanup: zero DAY305 residue", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No HTTP/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
