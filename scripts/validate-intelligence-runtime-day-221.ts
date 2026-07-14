/**
 * validate-intelligence-runtime-day-221.ts
 *
 * Day 221 — Context + Scorecard resolution inside the scoring runtime.
 *
 * Proves, at the lib + database level (no LLM call, no running server):
 *   ✓ default path: no published context and no active custom scorecard →
 *     context resolves null, scorecard resolves Gravix Default, the score
 *     cache key is byte-identical to the pre-Day-221 format and both prompt
 *     blocks are empty strings (the only injection points into the prompt)
 *   ✓ published context is resolved only after publish; draft rows and
 *     draft edits after publish never affect resolution
 *   ✓ scorecard resolution chain: active call-type match → active company
 *     default → Gravix Default; archived scorecards and superseded/draft
 *     versions are never selected
 *   ✓ cross-company context/scorecards never leak into another company's
 *     resolution
 *   ✓ cache key changes when the context version changes and when the
 *     scorecard version changes; default key cannot collide with either
 *   ✓ rubric._meta stamping: scoring_model_version + rubric_version always;
 *     context_version/context_published_at only when context used;
 *     scorecard_id/version_id/version only when custom; scorecard_source
 *     and scorecard_name always; legacy keys untouched
 *   ✓ fixed four-stage rubric shape (intro/discovery/objection/close) is
 *     preserved and the scorecard prompt block renders within it
 *
 * End-to-end LLM scoring is deliberately not exercised — the repo has no
 * scoring test harness (npm test is a stub) and a live call would be paid
 * and non-deterministic. The injection points proven empty here are the
 * only places Day 221 touches the prompt.
 *
 * Self-cleaning: every fixture row (companies, company_context, scorecards,
 * scorecard_versions) is created under deterministic validator-owned ids and
 * removed at the end.
 *
 * Requirements: sql/20260714_company_context.sql +
 * sql/20260714b_scorecard_studio.sql applied. Server NOT required.
 * Usage: npm run validate:intelligence-runtime
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

import {
  GRAVIX_DEFAULT_SCORECARD_KEY,
  GRAVIX_DEFAULT_SCORECARD_NAME,
  buildContextPromptBlock,
  buildScorecardPromptBlock,
  gravixDefaultScorecard,
  resolveActiveScorecard,
  resolvePublishedContext,
} from "../src/lib/intelligenceRuntime";
import {
  RUBRIC_VERSION,
  SCORING_MODEL_VERSION,
  buildDeterministicPromptKey,
  buildRubricWithMeta,
} from "../src/lib/scoring";
import { compileContextBlock } from "../src/lib/contextEngine";

const SUPA_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY221::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

// Deterministic fixture ids — everything cleaned up at the end.
const DEMO_PARTNER = "5055e1b6-fb33-45c0-959a-d7dd45f98a13";
const COMPANY_A = uid("company-a");
const COMPANY_B = uid("company-b");
const COMPANY_EMPTY = uid("company-empty");
const CTX_A_DRAFT = uid("ctx-a-draft");
const CTX_A_PUB = uid("ctx-a-published");
const CTX_B_PUB = uid("ctx-b-published");
const SC_TYPE = uid("sc-calltype");
const SCV_TYPE = uid("scv-calltype");
const SC_DEFAULT = uid("sc-default");
const SCV_DEFAULT = uid("scv-default");
const SCV_DEFAULT_OLD = uid("scv-default-superseded");
const SC_ARCHIVED = uid("sc-archived");
const SCV_ARCHIVED = uid("scv-archived");
const SC_B = uid("sc-company-b");
const SCV_B = uid("scv-company-b");

const CONTEXT_A = {
  profile: { about: "Day221 Combat Fitness — validator fixture.", sales_motion: "inbound" },
  objections: [{ objection: "Too expensive", approved_response: "Anchor on outcomes, not price." }],
};

function snapshotFor(label: string) {
  return {
    stages: [
      { stage: "intro", weight: 10, criteria: [{ label: `${label} intro criterion`, emphasis: "standard", pass_fail: false, critical: false }] },
      { stage: "discovery", weight: 40, criteria: [{ label: `${label} goal question`, emphasis: "major", pass_fail: true, critical: true, scoring_guidance: "Full marks only if asked before pitching." }] },
      { stage: "objection", weight: 25, criteria: [{ label: `${label} isolated objection`, emphasis: "standard", pass_fail: false, critical: false }] },
      { stage: "close", weight: 25, criteria: [{ label: `${label} dated next step`, emphasis: "major", pass_fail: false, critical: false }] },
    ],
  };
}

async function seed() {
  const { error: compErr } = await supa.from("companies").upsert(
    [
      { id: COMPANY_A, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER, name: "Day221 Runtime Co A (validator)", slug: "day221-runtime-a-validator" },
      { id: COMPANY_B, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER, name: "Day221 Runtime Co B (validator)", slug: "day221-runtime-b-validator" },
      { id: COMPANY_EMPTY, tmc_id: DEMO_PARTNER, partner_id: DEMO_PARTNER, name: "Day221 Runtime Co Empty (validator)", slug: "day221-runtime-empty-validator" },
    ],
    { onConflict: "id" }
  );
  if (compErr) throw new Error(`fixture companies upsert failed: ${compErr.message}`);

  const now = new Date().toISOString();

  const { error: ctxErr } = await supa.from("company_context").upsert(
    [
      { id: CTX_A_DRAFT, company_id: COMPANY_A, status: "draft", version: 0, context: { profile: { about: "DRAFT ONLY — must never reach scoring." } } },
      { id: CTX_A_PUB, company_id: COMPANY_A, status: "published", version: 1, context: CONTEXT_A, compiled_context: compileContextBlock(CONTEXT_A), published_at: now },
      { id: CTX_B_PUB, company_id: COMPANY_B, status: "published", version: 3, context: { profile: { about: "Company B secret context." } }, compiled_context: compileContextBlock({ profile: { about: "Company B secret context." } }), published_at: now },
    ],
    { onConflict: "id" }
  );
  if (ctxErr) throw new Error(`fixture company_context upsert failed: ${ctxErr.message}`);

  const { error: scErr } = await supa.from("scorecards").upsert(
    [
      { id: SC_TYPE, company_id: COMPANY_A, name: "Day221 Discovery Card (validator)", status: "active", is_company_default: false },
      { id: SC_DEFAULT, company_id: COMPANY_A, name: "Day221 Default Card (validator)", status: "active", is_company_default: true },
      { id: SC_ARCHIVED, company_id: COMPANY_A, name: "Day221 Archived Card (validator)", status: "archived", is_company_default: false, archived_at: now },
      { id: SC_B, company_id: COMPANY_B, name: "Day221 B Card (validator)", status: "active", is_company_default: true },
    ],
    { onConflict: "id" }
  );
  if (scErr) throw new Error(`fixture scorecards upsert failed: ${scErr.message}`);

  const { error: scvErr } = await supa.from("scorecard_versions").upsert(
    [
      { id: SCV_TYPE, scorecard_id: SC_TYPE, company_id: COMPANY_A, version: 2, status: "active", call_types: ["discovery"], snapshot: snapshotFor("Discovery"), activated_at: now },
      { id: SCV_DEFAULT, scorecard_id: SC_DEFAULT, company_id: COMPANY_A, version: 3, status: "active", call_types: [], snapshot: snapshotFor("Default"), activated_at: now },
      { id: SCV_DEFAULT_OLD, scorecard_id: SC_DEFAULT, company_id: COMPANY_A, version: 2, status: "superseded", call_types: [], snapshot: snapshotFor("Superseded"), activated_at: now },
      { id: SCV_ARCHIVED, scorecard_id: SC_ARCHIVED, company_id: COMPANY_A, version: 1, status: "active", call_types: ["demo"], snapshot: snapshotFor("Archived"), activated_at: now },
      { id: SCV_B, scorecard_id: SC_B, company_id: COMPANY_B, version: 1, status: "active", call_types: ["discovery"], snapshot: snapshotFor("CompanyB"), activated_at: now },
    ],
    { onConflict: "id" }
  );
  if (scvErr) throw new Error(`fixture scorecard_versions upsert failed: ${scvErr.message}`);
}

async function cleanup() {
  await supa.from("scorecard_versions").delete().in("id", [SCV_TYPE, SCV_DEFAULT, SCV_DEFAULT_OLD, SCV_ARCHIVED, SCV_B]);
  await supa.from("scorecards").delete().in("id", [SC_TYPE, SC_DEFAULT, SC_ARCHIVED, SC_B]);
  await supa.from("company_context").delete().in("id", [CTX_A_DRAFT, CTX_A_PUB, CTX_B_PUB]);
  await supa.from("companies").delete().in("id", [COMPANY_A, COMPANY_B, COMPANY_EMPTY]);
  console.log("\n  Cleanup: removed Day221 runtime fixtures (versions, scorecards, context rows, companies).");
}

const KEY_ARGS = {
  callId: "day221-call",
  filename: "day221.mp3",
  sha256: "abc123",
  transcript: "Rep: Hello.\nProspect: Hi.",
};

async function main() {
  console.log("\nDay 221 — Intelligence runtime resolution validation\n");

  // Probe migrations first (fail-soft mirrors the runtime itself).
  const probeCtx = await supa.from("company_context").select("id").limit(1);
  const probeSc = await supa.from("scorecard_versions").select("id").limit(1);
  if (probeCtx.error || probeSc.error) {
    console.error("  ✗ intelligence migrations not applied — run Day 218/219B SQL first.");
    process.exit(1);
  }

  await seed();

  try {
    // ---- 1. Default path -------------------------------------------------
    console.log("— Default path (no published context, no custom scorecard)");

    const ctxEmpty = await resolvePublishedContext(supa, COMPANY_EMPTY);
    c("company with no context rows resolves null", ctxEmpty === null);

    const ctxNullCompany = await resolvePublishedContext(supa, null);
    c("null company id resolves null context", ctxNullCompany === null);

    const scEmpty = await resolveActiveScorecard(supa, COMPANY_EMPTY, null);
    c("company with no scorecards resolves Gravix Default", scEmpty.source === "gravix_default");
    c("Gravix Default carries the sentinel cache key", scEmpty.cache_key === GRAVIX_DEFAULT_SCORECARD_KEY);
    c(`Gravix Default is named "${GRAVIX_DEFAULT_SCORECARD_NAME}"`, scEmpty.scorecard_name === GRAVIX_DEFAULT_SCORECARD_NAME);

    const scNullCompany = await resolveActiveScorecard(supa, null, "discovery");
    c("null company id resolves Gravix Default", scNullCompany.source === "gravix_default");

    const defaultKey = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: null, scorecardCacheKey: scEmpty.cache_key });
    const legacyKey = buildDeterministicPromptKey(KEY_ARGS);
    c("default-path cache key is byte-identical to the legacy key", defaultKey.key === legacyKey.key, defaultKey.key);
    c("default-path cache key contains no context/scorecard segments", !defaultKey.key.includes("|context=") && !defaultKey.key.includes("|scorecard="));

    c("context prompt block is empty on the default path", buildContextPromptBlock(null) === "");
    c("scorecard prompt block is empty on the default path", buildScorecardPromptBlock(scEmpty) === "");
    c("scorecard prompt block is empty for the built-in default object", buildScorecardPromptBlock(gravixDefaultScorecard()) === "");

    const defaultRubric = buildRubricWithMeta({
      intro: { score: 50, notes: "n" }, discovery: { score: 50, notes: "n" },
      objection: { score: 50, notes: "n" }, close: { score: 50, notes: "n" },
      callSha256: "abc123", transcriptHash: defaultKey.transcriptHash,
      transcriptPresent: true, modelVersion: SCORING_MODEL_VERSION,
    });
    const dm = (defaultRubric as any)._meta;
    c("default _meta stamps scoring_model_version + rubric_version", dm.scoring_model_version === SCORING_MODEL_VERSION && dm.rubric_version === RUBRIC_VERSION);
    c("default _meta stamps scorecard_source=gravix_default", dm.scorecard_source === "gravix_default");
    c(`default _meta stamps scorecard_name="${GRAVIX_DEFAULT_SCORECARD_NAME}"`, dm.scorecard_name === GRAVIX_DEFAULT_SCORECARD_NAME);
    c("default _meta has no context/scorecard id fields", !("context_version" in dm) && !("scorecard_id" in dm) && !("scorecard_version_id" in dm));
    c("legacy _meta keys preserved", dm.prompt_version != null && dm.model_version != null && "call_sha256" in dm && "transcript_hash" in dm && "transcript_present" in dm);
    c("rubric keeps the fixed four-stage shape", JSON.stringify(Object.keys(defaultRubric).sort()) === JSON.stringify(["_meta", "close", "discovery", "intro", "objection"]));

    // ---- 2. Context resolution ------------------------------------------
    console.log("— Context resolution");

    const ctxA = await resolvePublishedContext(supa, COMPANY_A);
    c("published context resolves for company A", ctxA !== null);
    c("resolved context carries version 1", ctxA?.context_version === 1);
    c("resolved context carries published_at", Boolean(ctxA?.published_at));
    c("resolved compiled block contains published content", Boolean(ctxA?.compiled_context.includes("Day221 Combat Fitness")));
    c("resolved compiled block never contains draft content", !ctxA?.compiled_context.includes("DRAFT ONLY"));

    const { error: draftEditErr } = await supa
      .from("company_context")
      .update({ context: { profile: { about: "DRAFT EDIT after publish — still must never reach scoring." } }, updated_at: new Date().toISOString() })
      .eq("id", CTX_A_DRAFT);
    c("draft edit after publish saves", !draftEditErr, draftEditErr?.message);

    const ctxAAfterDraftEdit = await resolvePublishedContext(supa, COMPANY_A);
    c("draft edit does not change resolved context", JSON.stringify(ctxAAfterDraftEdit) === JSON.stringify(ctxA));

    const ctxPromptBlock = buildContextPromptBlock(ctxA);
    c("context prompt block is bounded and labelled with the version", ctxPromptBlock.includes("published v1") && ctxPromptBlock.includes("Day221 Combat Fitness"));

    // ---- 3. Scorecard resolution chain ----------------------------------
    console.log("— Scorecard resolution chain");

    const byType = await resolveActiveScorecard(supa, COMPANY_A, "discovery");
    c("call-type match beats company default", byType.scorecard_version_id === SCV_TYPE && byType.source === "custom");
    c("call-type match returns its call_types", JSON.stringify(byType.call_types) === JSON.stringify(["discovery"]));

    const noType = await resolveActiveScorecard(supa, COMPANY_A, null);
    c("null call type falls back to company default", noType.scorecard_version_id === SCV_DEFAULT && noType.source === "company_default");

    const unmatchedType = await resolveActiveScorecard(supa, COMPANY_A, "renewal_upsell");
    c("unmatched call type falls back to company default", unmatchedType.scorecard_version_id === SCV_DEFAULT && unmatchedType.source === "company_default");

    c("superseded version is never selected", noType.scorecard_version_id !== SCV_DEFAULT_OLD && byType.scorecard_version_id !== SCV_DEFAULT_OLD);
    const demoType = await resolveActiveScorecard(supa, COMPANY_A, "demo");
    c("archived scorecard's version is never selected (demo → company default)", demoType.scorecard_version_id === SCV_DEFAULT && demoType.scorecard_version_id !== SCV_ARCHIVED);

    c("resolved scorecard exposes id/version/name/snapshot", noType.scorecard_id === SC_DEFAULT && noType.scorecard_version === 3 && noType.scorecard_name.includes("Default Card") && Boolean(noType.snapshot));

    const scBlock = buildScorecardPromptBlock(noType);
    c("scorecard prompt block renders all four fixed stages", ["intro", "discovery", "objection", "close"].every((s) => scBlock.includes(`Stage ${s} (weight`)));
    c("scorecard prompt block renders criteria with emphasis flags", scBlock.includes("Default goal question") && scBlock.includes("[major, pass/fail, critical]"));
    c("scorecard prompt block pins the fixed JSON schema", scBlock.includes("fixed JSON schema"));

    // ---- 4. Cross-company isolation --------------------------------------
    console.log("— Cross-company isolation");

    const ctxB = await resolvePublishedContext(supa, COMPANY_B);
    c("company B resolves its own context (v3)", ctxB?.context_version === 3 && Boolean(ctxB?.compiled_context.includes("Company B secret context")));
    c("company A context never contains company B content", !ctxA?.compiled_context.includes("Company B secret context"));
    c("company B context never contains company A content", !ctxB?.compiled_context.includes("Day221 Combat Fitness"));

    const scB = await resolveActiveScorecard(supa, COMPANY_B, "discovery");
    c("company B resolves its own scorecard", scB.scorecard_version_id === SCV_B);
    c("company A never resolves company B's scorecard", byType.scorecard_version_id !== SCV_B && noType.scorecard_version_id !== SCV_B);
    c("company B never resolves company A's scorecards", ![SCV_TYPE, SCV_DEFAULT, SCV_ARCHIVED].includes(scB.scorecard_version_id ?? ""));

    // ---- 5. Cache keying --------------------------------------------------
    console.log("— Cache keying");

    const keyCtxV1 = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: 1, scorecardCacheKey: GRAVIX_DEFAULT_SCORECARD_KEY });
    const keyCtxV2 = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: 2, scorecardCacheKey: GRAVIX_DEFAULT_SCORECARD_KEY });
    c("cache key includes the context version when published", keyCtxV1.key.endsWith("|context=1"));
    c("cache key changes when the context version changes", keyCtxV1.key !== keyCtxV2.key);
    c("context-keyed entry never collides with the default key", keyCtxV1.key !== defaultKey.key);

    const keyScA = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: null, scorecardCacheKey: SCV_DEFAULT });
    const keyScB = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: null, scorecardCacheKey: SCV_TYPE });
    c("cache key includes the scorecard version id when custom", keyScA.key.endsWith(`|scorecard=${SCV_DEFAULT}`));
    c("cache key changes when the scorecard version changes", keyScA.key !== keyScB.key);
    c("scorecard-keyed entry never collides with the default key", keyScA.key !== defaultKey.key);

    const keyBoth = buildDeterministicPromptKey({ ...KEY_ARGS, contextVersion: 1, scorecardCacheKey: SCV_DEFAULT });
    c("combined context+scorecard key is distinct from all others", new Set([defaultKey.key, keyCtxV1.key, keyScA.key, keyBoth.key]).size === 4);

    // ---- 6. Meta stamping with resolution ---------------------------------
    console.log("— Metadata stamping");

    const customRubric = buildRubricWithMeta({
      intro: { score: 50, notes: "n" }, discovery: { score: 50, notes: "n" },
      objection: { score: 50, notes: "n" }, close: { score: 50, notes: "n" },
      callSha256: "abc123", transcriptHash: keyBoth.transcriptHash,
      transcriptPresent: true, modelVersion: SCORING_MODEL_VERSION,
      resolvedContext: ctxA, resolvedScorecard: noType,
    });
    const cm = (customRubric as any)._meta;
    c("_meta stamps context_version + context_published_at when used", cm.context_version === 1 && Boolean(cm.context_published_at));
    c("_meta stamps scorecard id/version id/version when custom", cm.scorecard_id === SC_DEFAULT && cm.scorecard_version_id === SCV_DEFAULT && cm.scorecard_version === 3);
    c("_meta stamps scorecard_source=company_default", cm.scorecard_source === "company_default");
    c("_meta stamps the custom scorecard name", String(cm.scorecard_name).includes("Default Card"));
    c("custom rubric keeps the fixed four-stage shape", JSON.stringify(Object.keys(customRubric).sort()) === JSON.stringify(["_meta", "close", "discovery", "intro", "objection"]));
  } finally {
    await cleanup();
  }

  const passed = checks.filter((x) => x.passed).length;
  console.log(`\n  ${passed}/${checks.length} checks passed`);
  if (passed !== checks.length) {
    console.log("  Intelligence runtime validation FAILED");
    process.exit(1);
  }
  console.log("  Intelligence runtime validation PASSED");
}

main().catch(async (e) => {
  console.error("validator crashed:", e?.message || e);
  try { await cleanup(); } catch { /* best effort */ }
  process.exit(1);
});
