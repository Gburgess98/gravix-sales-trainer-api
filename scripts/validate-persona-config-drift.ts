/**
 * validate-persona-config-drift.ts
 *
 * Day 244 — locks in the companies.settings -> settings_json fixes.
 *
 * The company persona config is stored in companies.settings_json, but
 * admin.ts and sparring.ts both asked for a `settings` column that does not
 * exist. The three failure modes were all different and none said "schema":
 *
 *   GET  /v1/admin/persona-config  the select 42703'd, and the
 *        `if (error || !company)` guard turned it into 404 company_not_found
 *        for a company that plainly exists
 *   PATCH /v1/admin/persona-config  read the same missing column and then
 *        wrote it back via .update({ settings }), answering 500
 *   sparring getCompanyPersonaProfile  fell through to emptyProfile, so
 *        every sparring session silently ran with no company buyer persona
 *
 * Coverage:
 *   ✓ GET returns 200, not the misleading 404
 *   ✓ GET reads persona values seeded directly into settings_json
 *   ✓ PATCH returns 200 and persists into the settings_json COLUMN
 *   ✓ GET reads back what PATCH wrote (round trip)
 *   ✓ response keys preserved (`config` on GET, `company.settings` on PATCH)
 *   ✓ a company with empty settings_json answers 200 with empty defaults
 *   ✓ another company's persona is never visible or writable
 *
 * Self-cleaning: creates its own companies/reps/users fixtures and removes
 * them. It never touches the UFC or Gravix demo companies (Day 224 rule).
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:persona-config-drift
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY244::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function hit(method: string, path: string, userId: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(30_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";

// Company A carries a seeded persona; company B starts empty and must stay
// isolated from A.
const ORG = uid("org");
const COMPANY_A = uid("company-a");
const COMPANY_B = uid("company-b");
const MANAGER_A = uid("manager-a");
const MANAGER_B = uid("manager-b");

const SEEDED_PERSONA = {
  buyer_style: "day244_sceptical_buyer",
  industry_preset: "day244_fitness",
  objection_patterns: ["Day244 too expensive"],
  competitor_names: ["Day244 Competitor"],
  common_pushbacks: ["Day244 send me an email"],
  persona_memory: [],
  emotional_tuning: null,
};

const PATCHED_STYLE = "day244_patched_buyer";

async function seedFixtures() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [{ id: ORG, name: "Day244 Org (validator)" }], { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: coErr } = await supa.from("companies").upsert(
    [
      {
        id: COMPANY_A, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER,
        name: "Day244 Company A (validator)", slug: "day244-company-a-validator",
        settings_json: SEEDED_PERSONA,
      },
      {
        id: COMPANY_B, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER,
        name: "Day244 Company B (validator)", slug: "day244-company-b-validator",
        settings_json: {},
      },
    ],
    { onConflict: "id" }
  );
  if (coErr) throw new Error(`fixture company upsert failed: ${coErr.message}`);

  // requireManager reads reps.tier; the route resolves company from users.
  const { error: repErr } = await supa.from("reps").upsert(
    [
      { id: MANAGER_A, name: "Day244 Manager A", tier: "Manager", org_id: ORG, company_id: COMPANY_A },
      { id: MANAGER_B, name: "Day244 Manager B", tier: "Manager", org_id: ORG, company_id: COMPANY_B },
    ],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  const { error: userErr } = await supa.from("users").upsert(
    [
      { id: MANAGER_A, email: "day244-a@validator.local", role: "office_manager", org_id: ORG, company_id: COMPANY_A },
      { id: MANAGER_B, email: "day244-b@validator.local", role: "office_manager", org_id: ORG, company_id: COMPANY_B },
    ],
    { onConflict: "id" }
  );
  if (userErr) throw new Error(`fixture user upsert failed: ${userErr.message}`);
}

async function cleanup() {
  await supa.from("users").delete().in("id", [MANAGER_A, MANAGER_B]);
  await supa.from("reps").delete().in("id", [MANAGER_A, MANAGER_B]);
  await supa.from("companies").delete().in("id", [COMPANY_A, COMPANY_B]);
  await supa.from("orgs").delete().eq("id", ORG);
  console.log("\n  Cleanup: removed validator companies, reps and users.");
}

async function settingsJsonOf(companyId: string): Promise<any> {
  const { data } = await supa
    .from("companies").select("settings_json").eq("id", companyId).maybeSingle();
  return (data as any)?.settings_json ?? null;
}

const PATH = "/v1/admin/persona-config";

async function main() {
  console.log("\nDay 244 — companies.settings -> settings_json\n");

  await seedFixtures();

  // ── Read ─────────────────────────────────────────────────────────────────
  const get = await hit("GET", PATH, MANAGER_A);
  c("GET persona-config returns 200 (not the misleading 404)",
    get.status === 200, `got ${get.status} ${JSON.stringify(get.data)?.slice(0, 120)}`);

  c("GET response keeps its shape",
    get.data?.ok === true && typeof get.data?.config === "object" && "company_name" in (get.data ?? {}),
    `keys ${JSON.stringify(Object.keys(get.data ?? {}))}`);

  c("GET reads persona values out of the settings_json column",
    get.data?.config?.buyer_style === SEEDED_PERSONA.buyer_style &&
    get.data?.config?.industry_preset === SEEDED_PERSONA.industry_preset &&
    JSON.stringify(get.data?.config?.competitor_names) === JSON.stringify(SEEDED_PERSONA.competitor_names),
    `got ${JSON.stringify(get.data?.config)?.slice(0, 160)}`);

  // ── Empty settings ───────────────────────────────────────────────────────
  const getEmpty = await hit("GET", PATH, MANAGER_B);
  c("company with empty settings_json answers 200 with empty defaults",
    getEmpty.status === 200 &&
    getEmpty.data?.config?.buyer_style === null &&
    Array.isArray(getEmpty.data?.config?.objection_patterns) &&
    getEmpty.data.config.objection_patterns.length === 0,
    `got ${getEmpty.status} ${JSON.stringify(getEmpty.data?.config)?.slice(0, 140)}`);

  // ── Write ────────────────────────────────────────────────────────────────
  const patch = await hit("PATCH", PATH, MANAGER_A, { buyer_style: PATCHED_STYLE });
  c("PATCH persona-config returns 200 (was 500 on the missing column)",
    patch.status === 200, `got ${patch.status} ${JSON.stringify(patch.data)?.slice(0, 140)}`);

  c("PATCH response keeps its company.settings key",
    patch.data?.ok === true && typeof patch.data?.company?.settings === "object",
    `got ${JSON.stringify(patch.data?.company)?.slice(0, 140)}`);

  const stored = await settingsJsonOf(COMPANY_A);
  c("PATCH persists into the settings_json column",
    stored?.buyer_style === PATCHED_STYLE,
    `settings_json = ${JSON.stringify(stored)?.slice(0, 160)}`);

  // Pins the route's actual partial-update semantics, which only became
  // observable once the column fix let PATCH run at all: scalar fields fall
  // back to the stored value, but array fields are rebuilt from the body
  // alone, so omitting one clears it. That is pre-existing behaviour in
  // mergedSettings, not a consequence of the settings_json fix, and changing
  // it is a product decision — asserted here so it stays visible.
  c("PATCH keeps scalars but replaces arrays (documented merge quirk)",
    stored?.industry_preset === SEEDED_PERSONA.industry_preset &&
    Array.isArray(stored?.competitor_names) && stored.competitor_names.length === 0,
    `settings_json = ${JSON.stringify(stored)?.slice(0, 200)}`);

  const reread = await hit("GET", PATH, MANAGER_A);
  c("GET reads back what PATCH wrote (round trip)",
    reread.data?.config?.buyer_style === PATCHED_STYLE,
    `got ${JSON.stringify(reread.data?.config)?.slice(0, 140)}`);

  // ── Isolation ────────────────────────────────────────────────────────────
  c("another company's manager never sees company A's persona",
    getEmpty.data?.config?.buyer_style !== SEEDED_PERSONA.buyer_style &&
    getEmpty.data?.config?.buyer_style !== PATCHED_STYLE,
    `got ${JSON.stringify(getEmpty.data?.config?.buyer_style)}`);

  await hit("PATCH", PATH, MANAGER_B, { buyer_style: "day244_company_b_only" });
  const aAfter = await settingsJsonOf(COMPANY_A);
  const bAfter = await settingsJsonOf(COMPANY_B);
  c("a PATCH by another company leaves company A untouched",
    aAfter?.buyer_style === PATCHED_STYLE && bAfter?.buyer_style === "day244_company_b_only",
    `A = ${JSON.stringify(aAfter?.buyer_style)}, B = ${JSON.stringify(bAfter?.buyer_style)}`);
}

main()
  .catch((e) => {
    console.error("\n  Validator crashed:", e?.message || e);
    c("validator ran to completion", false, String(e?.message || e));
  })
  .finally(async () => {
    await cleanup();
    const passed = checks.filter((x) => x.passed).length;
    console.log(`\n  ${passed}/${checks.length} checks passed`);
    console.log(
      passed === checks.length
        ? "  Persona config drift validation PASSED\n"
        : "  Persona config drift validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
