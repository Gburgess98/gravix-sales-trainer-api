/**
 * validate-ufc-intelligence-seed.ts
 *
 * Day 224 — proves the PERSISTENT UFC Intelligence demo assets seeded by
 * npm run seed:ufc-intelligence are present, correct and demo-safe:
 *
 *   ✓ UFC context exists, is published at v1, and exactly one row is published
 *   ✓ the compiled block is non-empty, bounded, and carries the real UFC
 *     positioning / objections / no-go language — never draft-only text
 *   ✓ the draft working copy survives alongside the published snapshot
 *   ✓ "UFC Sales Scorecard" exists, is active and is the company default
 *   ✓ exactly one active version, over the fixed four stages only
 *   ✓ weights total 100 (20/30/30/20) and every stage has ≥1 criterion
 *   ✓ the relational Studio rows agree with the immutable snapshot
 *   ✓ the runtime resolves these assets (source company_default)
 *   ✓ the proof call's rubric._meta carries the real provenance
 *   ✓ the Nate Diaz hero call is untouched — still 45, still no provenance,
 *     which is the calm default state /calls/[id] renders (Day 223)
 *   ✓ cross-company isolation still holds against the seeded assets
 *
 * Read-only except for one throwaway cross-company fixture, which is removed.
 * No LLM call, no scoring, no writes to any seeded row.
 *
 * Requirements: npm run seed:demo && npm run seed:ufc-intelligence applied.
 * Server NOT required.
 * Usage: npm run validate:ufc-intelligence-seed
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

import { resolveActiveScorecard, resolvePublishedContext } from "../src/lib/intelligenceRuntime";
import { compileContextBlock } from "../src/lib/contextEngine";
import { IDS, SCORECARD_NAME, UFC_CONTEXT, STAGE_WEIGHTS, CRITERIA } from "./seed-ufc-intelligence";

const SUPA_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const UFC_COMPANY_ID = process.env.DEMO_COMPANY_ID || "bfb9604e-bc2f-46fa-be97-6461e98e1a19";
const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
// Day 172 convention: the human label lives in calls.filename, not calls.title
// (every demo call has title = null).
const HERO_CALL_FILENAME = "Nate Diaz — Price Objection Call";
const STAGES = ["intro", "discovery", "objection", "close"];
const MAX_BLOCK_CHARS = 6_000; // contextEngine's hard cap

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY224::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const XCOMPANY_ID = uid("cross-company");

/**
 * Order-insensitive canonical form. Postgres jsonb does not preserve object key
 * order, so a stored context never round-trips to the same JSON.stringify output
 * as the seed literal — compare canonically instead.
 */
function canonical(v: any): any {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.keys(v).sort().reduce((acc: any, k) => { acc[k] = canonical(v[k]); return acc; }, {});
  }
  return v;
}

async function cleanup() {
  await supa.from("companies").delete().eq("id", XCOMPANY_ID);
}

async function main() {
  console.log("\n  Day 224 — UFC Intelligence demo seed validation\n");

  const probe = await supa.from("company_context").select("id").limit(1);
  if (probe.error) {
    console.error("  ✗ intelligence migrations not applied — run the Day 218/219B SQL first.");
    process.exit(1);
  }

  try {
    // ---- Context ---------------------------------------------------------
    console.log("— Published UFC context");

    const { data: ctxRows } = await supa
      .from("company_context").select("id, status, version, compiled_context, context, published_at, published_by")
      .eq("company_id", UFC_COMPANY_ID);
    const rows = (ctxRows ?? []) as any[];
    const published = rows.filter((r) => r.status === "published");
    const drafts = rows.filter((r) => r.status === "draft");

    c("UFC context is seeded", rows.length > 0, "run npm run seed:ufc-intelligence");
    c("exactly one published context row", published.length === 1, `found ${published.length}`);
    c("published context is v1", published[0]?.version === 1, `v${published[0]?.version}`);
    c("published row is the seeded row", published[0]?.id === IDS.ctxPublished);
    c("published row is stamped published_at/published_by",
      Boolean(published[0]?.published_at) && Boolean(published[0]?.published_by));
    c("draft working copy survives alongside the published snapshot",
      drafts.length === 1 && drafts[0]?.id === IDS.ctxDraft);

    const compiled = String(published[0]?.compiled_context ?? "");
    c("compiled block is non-empty", compiled.length > 0, `${compiled.length} chars`);
    c("compiled block is bounded", compiled.length <= MAX_BLOCK_CHARS && !compiled.includes("[Context truncated]"),
      `${compiled.length} chars`);
    c("compiled block matches a deterministic recompile of the stored context",
      compiled === compileContextBlock(published[0]?.context));
    c("compiled block carries the UFC positioning",
      compiled.includes("premium, evidence-led sales coaching platform"));
    c("compiled block carries the UFC objections",
      compiled.includes("It's too expensive") &&
      compiled.includes("I need to think about it") &&
      compiled.includes("Just send me some info"));
    c("compiled block carries the no-go compliance language",
      compiled.includes("Guaranteed revenue increase") && compiled.includes("Risk-free"));
    c("compiled block carries the manager-first tone guidance",
      compiled.includes("Manager-first"));
    c("published context matches the seed payload",
      JSON.stringify(canonical(published[0]?.context)) === JSON.stringify(canonical(UFC_CONTEXT)));

    // ---- Scorecard -------------------------------------------------------
    console.log("— Active UFC scorecard");

    const { data: cards } = await supa
      .from("scorecards").select("id, name, status, is_company_default").eq("company_id", UFC_COMPANY_ID);
    const cardRows = (cards ?? []) as any[];
    const card = cardRows.find((r) => r.id === IDS.scorecard);

    c(`"${SCORECARD_NAME}" is seeded`, Boolean(card), "run npm run seed:ufc-intelligence");
    c("scorecard carries the demo-facing name", card?.name === SCORECARD_NAME, String(card?.name));
    c("scorecard is active", card?.status === "active", String(card?.status));
    c("scorecard is the company default", card?.is_company_default === true);
    c("exactly one active company-default scorecard for UFC",
      cardRows.filter((r) => r.is_company_default && r.status === "active").length === 1);

    const { data: vers } = await supa
      .from("scorecard_versions").select("id, version, status, call_types, snapshot")
      .eq("scorecard_id", IDS.scorecard);
    const verRows = (vers ?? []) as any[];
    const active = verRows.filter((v) => v.status === "active");

    c("exactly one active version", active.length === 1, `found ${active.length}`);
    c("active version is v1", active[0]?.version === 1, `v${active[0]?.version}`);
    c("active version is the seeded version", active[0]?.id === IDS.version);
    c("active version applies to all call types (company default)",
      Array.isArray(active[0]?.call_types) && active[0].call_types.length === 0);

    const snapStages = (active[0]?.snapshot?.stages ?? []) as any[];
    c("snapshot uses only the fixed four stages, in order",
      JSON.stringify(snapStages.map((s) => s.stage)) === JSON.stringify(STAGES),
      JSON.stringify(snapStages.map((s) => s.stage)));
    c("snapshot weights are 20/30/30/20",
      JSON.stringify(snapStages.map((s) => Number(s.weight))) === JSON.stringify([20, 30, 30, 20]),
      JSON.stringify(snapStages.map((s) => Number(s.weight))));
    c("snapshot weights total 100",
      snapStages.reduce((t, s) => t + (Number(s.weight) || 0), 0) === 100);
    c("every stage has at least one criterion",
      STAGES.every((st) => (snapStages.find((s) => s.stage === st)?.criteria ?? []).length >= 1),
      JSON.stringify(snapStages.map((s) => (s.criteria ?? []).length)));
    c("snapshot carries the seeded criteria labels",
      CRITERIA.every((cr) =>
        (snapStages.find((s) => s.stage === cr.stage)?.criteria ?? []).some((x: any) => x.label === cr.label)));

    // Relational Studio rows must agree with the immutable snapshot.
    const { data: weightRows } = await supa
      .from("scorecard_stage_weights").select("stage, weight").eq("scorecard_version_id", IDS.version);
    const { data: critRows } = await supa
      .from("scorecard_criteria").select("stage, label").eq("scorecard_version_id", IDS.version);
    c("relational stage weights exist for all four stages",
      (weightRows ?? []).length === 4 &&
      STAGES.every((st) => (weightRows as any[]).some((w) => w.stage === st)));
    c("relational weights agree with the snapshot",
      STAGES.every((st) =>
        Number((weightRows as any[]).find((w) => w.stage === st)?.weight) ===
        Number(snapStages.find((s) => s.stage === st)?.weight)));
    c("relational criteria agree with the seed",
      (critRows ?? []).length === CRITERIA.length &&
      CRITERIA.every((cr) => (critRows as any[]).some((x) => x.label === cr.label && x.stage === cr.stage)));
    c("no stray draft/superseded versions on the demo scorecard",
      verRows.every((v) => v.status === "active"), JSON.stringify(verRows.map((v) => v.status)));

    // ---- Runtime resolution ----------------------------------------------
    console.log("— Runtime resolution of the seeded assets");

    const liveCtx = await resolvePublishedContext(supa, UFC_COMPANY_ID);
    c("runtime resolves the seeded context at v1", liveCtx?.context_version === 1);
    c("runtime reads the published snapshot, not the draft",
      liveCtx?.compiled_context === compiled);

    const liveSc = await resolveActiveScorecard(supa, UFC_COMPANY_ID, null);
    c("runtime resolves the seeded scorecard", liveSc.scorecard_version_id === IDS.version);
    c("runtime reports source company_default", liveSc.source === "company_default");
    c("runtime reports the demo-facing scorecard name", liveSc.scorecard_name === SCORECARD_NAME);

    // ---- Proof call provenance -------------------------------------------
    console.log("— Proof call provenance (what /calls/[id] renders)");

    const { data: proof } = await supa
      .from("calls").select("id, title, filename, score_overall, rubric, company_id")
      .eq("id", IDS.proofCall).maybeSingle();
    const meta = (proof as any)?.rubric?._meta ?? {};

    c("proof call is seeded in the UFC company",
      Boolean(proof) && (proof as any).company_id === UFC_COMPANY_ID);
    c("proof call has a demo-facing label",
      String((proof as any)?.filename ?? "").includes("Nate Diaz") &&
      !String((proof as any)?.filename ?? "").endsWith(".mp3"), String((proof as any)?.filename));
    c("proof call is scored", Number((proof as any)?.score_overall) > 0, String((proof as any)?.score_overall));
    c("_meta.scorecard_name = UFC Sales Scorecard", meta.scorecard_name === SCORECARD_NAME, JSON.stringify(meta.scorecard_name));
    c("_meta.scorecard_source = company_default", meta.scorecard_source === "company_default", JSON.stringify(meta.scorecard_source));
    c("_meta.scorecard_id = the seeded scorecard", meta.scorecard_id === IDS.scorecard);
    c("_meta.scorecard_version_id = the seeded active version", meta.scorecard_version_id === IDS.version);
    c("_meta.scorecard_version = 1", meta.scorecard_version === 1, JSON.stringify(meta.scorecard_version));
    c("_meta.context_version = 1", meta.context_version === 1, JSON.stringify(meta.context_version));
    c("_meta.context_published_at matches the published row",
      meta.context_published_at === published[0]?.published_at, JSON.stringify(meta.context_published_at));
    c("_meta.scoring_model_version present", Boolean(meta.scoring_model_version));
    c("proof rubric keeps the fixed four-stage shape",
      JSON.stringify(Object.keys((proof as any)?.rubric ?? {}).sort()) ===
      JSON.stringify(["_meta", "close", "discovery", "intro", "objection"]));

    // ---- Hero call must stay the calm default state ------------------------
    console.log("— Hero call untouched (the Day 223 default state)");

    const { data: hero } = await supa
      .from("calls").select("id, score_overall, rubric").eq("filename", HERO_CALL_FILENAME).maybeSingle();
    const heroMeta = (hero as any)?.rubric?._meta ?? {};
    c("Nate Diaz hero call still exists", Boolean(hero));
    c("hero call still scores 45", Number((hero as any)?.score_overall) === 45, String((hero as any)?.score_overall));
    c("hero call carries NO scorecard provenance (renders as Gravix default)",
      !("scorecard_source" in heroMeta) && !("scorecard_id" in heroMeta));
    c("hero call carries NO context claim", !("context_version" in heroMeta));
    c("hero call is a different call from the proof call", (hero as any)?.id !== IDS.proofCall);

    // ---- Cross-company isolation -------------------------------------------
    console.log("— Cross-company isolation against the seeded assets");

    const { error: xErr } = await supa.from("companies").upsert(
      {
        id: XCOMPANY_ID,
        tmc_id: DEMO_PARTNER,
        partner_id: DEMO_PARTNER,
        name: "Day224 Cross Company (validator)",
        slug: "day224-cross-company-validator",
      },
      { onConflict: "id" }
    );
    if (xErr) throw new Error(`cross-company fixture failed: ${xErr.message}`);

    const xCtx = await resolvePublishedContext(supa, XCOMPANY_ID);
    c("another company resolves none of the seeded UFC context", xCtx === null);
    const xSc = await resolveActiveScorecard(supa, XCOMPANY_ID, null);
    c("another company resolves the Gravix default, never UFC's scorecard",
      xSc.source === "gravix_default" && xSc.scorecard_version_id !== IDS.version);
    const xScTyped = await resolveActiveScorecard(supa, XCOMPANY_ID, "discovery");
    c("call-type resolution does not leak UFC's company default either",
      xScTyped.source === "gravix_default");
  } finally {
    await cleanup();
  }

  const passed = checks.filter((x) => x.passed).length;
  console.log(`\n  ${passed}/${checks.length} checks passed`);
  if (passed !== checks.length) {
    console.log("  UFC Intelligence seed validation FAILED");
    process.exit(1);
  }
  console.log("  UFC Intelligence seed validation PASSED");
  console.log(`\n  Demo: /calls/${IDS.proofCall} shows "${SCORECARD_NAME} v1 · Company context v1 applied".`);
}

main().catch(async (e) => {
  console.error("validator crashed:", e?.message || e);
  try { await cleanup(); } catch { /* best effort */ }
  process.exit(1);
});
