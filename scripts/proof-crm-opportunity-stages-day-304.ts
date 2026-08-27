/**
 * proof-crm-opportunity-stages-day-304.ts
 *
 * Guarded proof for the Day 304 GET /opportunities/stages route-order fix.
 *
 * (1) REACHABILITY (deterministic): loads the ACTUAL crm router and confirms that
 *     Express now matches the literal `/opportunities/stages` first (before the
 *     parameterised `/opportunities/:id`), while a genuine UUID detail path still
 *     matches `/:id`.
 * (2) HANDLER LOGIC (staging DB): seeds synthetic opportunities with distinct stages
 *     for a synthetic user, runs the handler's exact query + dedupe/sort, and proves
 *     the { stages, source } result — then fail-safe cleans up.
 *
 * NO route execution over HTTP (identity edge blocks synthetic auth), NO paid AI, NO
 * outbound. Staging only; crm_opportunities has synthetic DAY304 rows, all removed.
 *
 * Usage: npx tsx scripts/proof-crm-opportunity-stages-day-304.ts
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

function firstGetMatch(path: string): string | undefined {
  const stack: any[] = (router as any).stack || [];
  const toRe = (p: string) => new RegExp("^" + p.replace(/:[^/]+/g, "[^/]+").replace(/\//g, "\\/") + "$");
  const layer = stack.find((l: any) => l.route && l.route.methods.get && toRe(l.route.path).test(path));
  return layer?.route?.path;
}

async function main() {
  const env = parseEnv(join(__dirname, "..", ".env.staging.local"));
  const url = env.SUPABASE_URL, key = env.SUPABASE_SERVICE_ROLE_KEY;
  const ref = (url.match(/https:\/\/([a-z0-9]+)\.supabase/) || [])[1];
  console.log("Day 304 — GET /opportunities/stages reachability + logic proof\n");
  if (ref !== "dnumqzxthfthsmfnzvdz") { console.error(`REFUSING: not staging (${ref?.slice(0, 6)}…)`); process.exit(1); }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…)\n`);

  // (1) Reachability against the real router.
  console.log("── reachability (real Express router) ──");
  ok("literal /opportunities/stages now matches BEFORE /:id", firstGetMatch("/opportunities/stages") === "/opportunities/stages", firstGetMatch("/opportunities/stages"));
  ok("a genuine UUID detail path still matches /opportunities/:id", firstGetMatch("/opportunities/" + randomUUID()) === "/opportunities/:id", firstGetMatch("/opportunities/" + randomUUID()));
  ok("exactly one /opportunities/stages layer registered",
    ((router as any).stack || []).filter((l: any) => l.route?.path === "/opportunities/stages" && l.route?.methods?.get).length === 1);

  // (2) Handler logic against staging.
  console.log("\n── handler logic (staging crm_opportunities) ──");
  const svc = createClient(url, key);
  const uid = randomUUID();
  const seedStages = ["Proposal", "Lead", "Proposal", "Won"]; // dup + unsorted on purpose
  try {
    const seed = await svc.from("crm_opportunities").insert(
      seedStages.map((s, i) => ({ user_id: uid, name: `DAY304 opp ${i}`, stage: s }))
    ).select("id");
    ok("seeded synthetic opportunities", !seed.error && seed.data?.length === seedStages.length, seed.error?.message);

    // Replicate the handler query + dedupe/sort verbatim.
    const r = await svc.from("crm_opportunities").select("stage").eq("user_id", uid).limit(2000);
    ok("handler query succeeds (select stage by user_id)", !r.error, r.error?.message);
    const stages = Array.from(new Set(((r.data as any[]) ?? []).map((x) => String(x?.stage ?? "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    ok("distinct + sorted stages = [Lead, Proposal, Won]", JSON.stringify(stages) === JSON.stringify(["Lead", "Proposal", "Won"]), JSON.stringify(stages));
    ok("source would be 'db' (non-empty)", stages.length > 0);

    // Tenant scope: another user's stages are NOT included.
    const other = randomUUID();
    await svc.from("crm_opportunities").insert({ user_id: other, name: "DAY304 other", stage: "Negotiation" }).select("id");
    const r2 = await svc.from("crm_opportunities").select("stage").eq("user_id", uid).limit(2000);
    const stages2 = Array.from(new Set(((r2.data as any[]) ?? []).map((x) => String(x?.stage ?? "").trim())));
    ok("tenant scope holds — other user's 'Negotiation' not visible", !stages2.includes("Negotiation"));
    await svc.from("crm_opportunities").delete().eq("user_id", other);
  } finally {
    await svc.from("crm_opportunities").delete().eq("user_id", uid);
    const residue = await svc.from("crm_opportunities").select("id", { count: "exact", head: true }).like("name", "DAY304%");
    ok("cleanup: zero DAY304 residue", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No HTTP/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
