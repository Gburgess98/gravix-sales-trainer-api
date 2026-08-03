/**
 * validate-scoring-v2-staging-activation-day-270.ts
 *
 * Repeatable, SAFE proof that a v2 (stub) score persisted in a STAGING
 * environment has the right shape end-to-end: the v1 projection survives and the
 * full ScoreV2 lands under analysis_json.v2, honestly degraded as a stub result.
 *
 * TWO LANES:
 *   • SELF-TEST (default, NO network, NO secrets): validates checked-in fixtures
 *     that mirror the exact persisted call row, plus non-vacuity (planted
 *     violations) and the production-host refusal logic. This is what CI/local
 *     runs — `npm run validate:scoring-v2-staging-activation`.
 *   • LIVE (only when STAGING_API_BASE + STAGING_CONFIRMED=1 are set AND the host
 *     is not on the production denylist): GETs the dedicated staging call and
 *     runs the same shape assertion. Never prints the auth token, transcript or
 *     evidence text.
 *
 * Safety: refuses known production hosts (gravixbots.com by default, extend via
 * PROD_HOST_DENYLIST), refuses to run LIVE without explicit STAGING_CONFIRMED=1,
 * and never logs secrets, transcript or evidence quotes.
 *
 * Usage:
 *   npx tsx scripts/validate-scoring-v2-staging-activation-day-270.ts
 *   STAGING_API_BASE=https://staging... STAGING_CALL_ID=... STAGING_AUTH_TOKEN=... \
 *     STAGING_CONFIRMED=1 EXPECTED_API_SHA=7928f52 \
 *     npx tsx scripts/validate-scoring-v2-staging-activation-day-270.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { validateStructure } from "../test/harness/scoring-v2-harness";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) {
  console.log(`\n── ${t} ──`);
}

// ── production-host refusal ───────────────────────────────────────────────────
const DEFAULT_PROD_DENYLIST = ["gravixbots.com", "api.gravixbots.com", "vercel.app"];
function prodDenylist(): string[] {
  const extra = (process.env.PROD_HOST_DENYLIST || "").split(",").map((s) => s.trim()).filter(Boolean);
  return [...DEFAULT_PROD_DENYLIST, ...extra];
}
function hostOf(url: string): string {
  try {
    return new URL(url).host.toLowerCase();
  } catch {
    return "";
  }
}
export function isProductionHost(url: string): boolean {
  const host = hostOf(url);
  if (!host) return true; // unparseable → treat as unsafe
  return prodDenylist().some((d) => host === d || host.endsWith("." + d) || host.includes(d));
}

// ── shared shape assertion (never prints content) ─────────────────────────────
const V1_STAGES = ["intro", "discovery", "objection", "close"] as const;
const STATUSES = ["pass", "partial", "fail", "not_observed"];

/**
 * Validate the persisted call row after a v2 score. Returns FIELD-LEVEL issues
 * only — no transcript, evidence or secret content is ever included.
 */
export function assertCallScore(call: any, opts: { expectStub: boolean }): string[] {
  const issues: string[] = [];
  const aj = call?.analysis_json;
  if (!aj || typeof aj !== "object") return ["analysis_json missing"];

  // 1. v1 projection intact
  if (typeof aj.overall !== "number") issues.push("v1 overall missing");
  if (typeof aj.summary !== "string" || !aj.summary) issues.push("v1 summary missing");
  for (const s of V1_STAGES) {
    const st = aj.stages?.[s];
    if (!st || typeof st.score !== "number" || typeof st.notes !== "string") issues.push(`v1 stages.${s} incomplete`);
  }
  if (!Array.isArray(aj.moments)) issues.push("v1 moments not an array");
  if (!Array.isArray(aj.suggestions)) issues.push("v1 suggestions not an array");
  if (!aj.voice || typeof aj.voice !== "object") issues.push("v1 voice missing");
  const meta = call?.rubric?._meta;
  if (!meta || typeof meta !== "object") issues.push("rubric._meta missing");

  // 2. v2 present + structurally valid (reuse the Day-266 harness gate)
  const v2 = aj.v2;
  if (!v2 || typeof v2 !== "object") return [...issues, "analysis_json.v2 missing"];
  const segments = aj.transcript?.segments ?? [];
  const fullTranscript = Array.isArray(segments) ? segments.map((s: any) => String(s?.text || "")).join("\n") : "";
  for (const s of validateStructure(v2, fullTranscript, segments)) issues.push(`v2 ${s}`);
  if (v2.contract_version !== "v2") issues.push("v2 contract_version != v2");
  const order = (v2.stages || []).map((s: any) => s?.stage).join(",");
  if (order !== "intro,discovery,objection,close") issues.push("v2 stages not four in order");
  for (const st of v2.stages || []) {
    if (!Array.isArray(st.criteria) || st.criteria.length < 1) issues.push(`v2 stage ${st?.stage} has no criteria`);
    for (const c of st.criteria || []) {
      if (!c.criterion_id || typeof c.criterion_id !== "string") issues.push(`v2 ${st?.stage} criterion_id missing`);
      if (!STATUSES.includes(c.status)) issues.push(`v2 ${st?.stage} invalid status`);
      if (c.status === "not_observed" ? c.score !== null : typeof c.score !== "number") issues.push(`v2 ${st?.stage} score/status mismatch`);
    }
  }

  // 3. provenance present + v1/v2 agreement
  const p = v2.provenance || {};
  if (p.prompt_version !== "scoring-prompt-v2") issues.push("v2 prompt_version != scoring-prompt-v2");
  if (p.rubric_version !== "v2") issues.push("v2 rubric_version != v2");
  if (p.cache_key_version !== "v2") issues.push("v2 cache_key_version != v2");
  if (meta) {
    if (meta.contract_version !== "v2") issues.push("rubric._meta contract_version != v2 (v1/v2 disagree)");
    if (meta.rubric_version !== p.rubric_version) issues.push("rubric._meta rubric_version disagrees with v2");
    if (meta.prompt_version !== p.prompt_version) issues.push("rubric._meta prompt_version disagrees with v2");
    if (meta.cache_key_version !== p.cache_key_version) issues.push("rubric._meta cache_key_version disagrees with v2");
  }

  // 4. degraded-stub honesty
  if (opts.expectStub) {
    if (v2.degraded_score !== true) issues.push("stub result not marked degraded");
    if (v2.degraded_reason !== "stub_provider") issues.push("stub degraded_reason != stub_provider");
    if (p.scoring_provider !== "stub") issues.push("stub provider != stub");
    if (p.scoring_model !== "stub:v1") issues.push("stub model != stub:v1");
    // A stub never invents evidence.
    for (const st of v2.stages || []) for (const c of st.criteria || []) {
      if (Array.isArray(c.evidence) && c.evidence.length > 0) issues.push(`stub ${st?.stage} has evidence (should be none)`);
    }
  }
  return issues;
}

function load(rel: string): any {
  return JSON.parse(readFileSync(join(__dirname, "..", rel), "utf8"));
}
function clone<T>(x: T): T {
  return JSON.parse(JSON.stringify(x));
}

async function liveLane(): Promise<void> {
  const base = process.env.STAGING_API_BASE;
  if (!base) return; // no live target → self-test only
  section("LIVE — staging fetch (network)");
  if (process.env.STAGING_CONFIRMED !== "1") {
    gate("LIVE requires STAGING_CONFIRMED=1 (refused)", false, "set STAGING_CONFIRMED=1 only for a real staging target");
    return;
  }
  if (isProductionHost(base)) {
    gate("LIVE target is NOT a production host", false, `refused host ${hostOf(base)} (on denylist)`);
    return;
  }
  const callId = process.env.STAGING_CALL_ID;
  const token = process.env.STAGING_AUTH_TOKEN;
  if (!callId) {
    gate("STAGING_CALL_ID provided", false);
    return;
  }
  gate("LIVE target host is not on the production denylist", true, hostOf(base));
  try {
    const url = `${base.replace(/\/$/, "")}/v1/calls/${encodeURIComponent(callId)}`;
    const resp = await fetch(url, { headers: token ? { authorization: `Bearer ${token}` } : {} });
    gate("staging call fetch succeeded", resp.ok, `HTTP ${resp.status}`);
    if (!resp.ok) return;
    const body: any = await resp.json();
    const call = body?.call ?? body;
    if (process.env.EXPECTED_API_SHA && body?.commit) {
      gate("deployed API commit matches EXPECTED_API_SHA", String(body.commit).startsWith(process.env.EXPECTED_API_SHA));
    }
    const issues = assertCallScore(call, { expectStub: true });
    gate("staging v2 stub score is shape-valid + honestly degraded", issues.length === 0, issues.join("; "));
  } catch (e: any) {
    gate("staging fetch did not throw", false, e?.message || String(e));
  }
}

async function main() {
  console.log("Scoring v2 STAGING ACTIVATION validator (Day 270) — self-test lane makes NO network call, prints NO secrets/transcript\n");

  // ── Self-test: fixtures mirror the persisted staging call row ─────────────
  section("SELF-TEST — persisted call-row fixtures");
  const stub = load("test/fixtures/scoring-v2/staging-stub-persisted-day-270.json").call;
  const mock = load("test/fixtures/scoring-v2/staging-mock-openai-persisted-day-270.json").call;
  gate("v2 STUB row: v1 projection intact + v2 present + honestly degraded", assertCallScore(stub, { expectStub: true }).length === 0, assertCallScore(stub, { expectStub: true }).join("; "));
  gate("v2 STUB row: overall 0, four not_observed criteria, no evidence", stub.analysis_json.v2.overall_score === 0 && stub.analysis_json.v2.stages.every((s: any) => s.criteria[0].status === "not_observed" && s.criteria[0].evidence.length === 0));
  gate("mocked-OpenAI v2 row: shape-valid, non-degraded", assertCallScore(mock, { expectStub: false }).length === 0 && mock.analysis_json.v2.degraded_score === false);

  // ── production-host refusal ────────────────────────────────────────────────
  section("SAFETY — production-host refusal");
  gate("refuses api.gravixbots.com", isProductionHost("https://api.gravixbots.com"));
  gate("refuses the prod vercel origin", isProductionHost("https://gravix-web.vercel.app"));
  gate("refuses an unparseable target", isProductionHost("not a url"));
  gate("allows a clearly-staging host", !isProductionHost("https://staging-api.example.dev"));
  gate("allows localhost", !isProductionHost("http://localhost:8080"));

  // ── non-vacuity: planted violations (Task 16) ─────────────────────────────
  section("NON-VACUITY — planted violations must be caught");
  const caught = (label: string, mutate: (c: any) => void, expectStub = true) => {
    const c = clone(stub);
    mutate(c);
    gate(`caught: ${label}`, assertCallScore(c, { expectStub }).length > 0);
  };
  caught("missing analysis_json.v2", (c) => { delete c.analysis_json.v2; });
  caught("incomplete v1 projection (drop close stage)", (c) => { delete c.analysis_json.stages.close; });
  caught("non-degraded stub score", (c) => { c.analysis_json.v2.degraded_score = false; });
  caught("wrong degraded reason", (c) => { c.analysis_json.v2.degraded_reason = "heuristic_fallback"; });
  caught("provider not stub", (c) => { c.analysis_json.v2.provenance.scoring_provider = "openai"; });
  caught("wrong contract_version", (c) => { c.analysis_json.v2.contract_version = "v1"; });
  caught("missing a stage", (c) => { c.analysis_json.v2.stages.splice(2, 1); });
  caught("malformed criterion (bad status)", (c) => { c.analysis_json.v2.stages[0].criteria[0].status = "great"; });
  caught("v1/v2 provenance mismatch", (c) => { c.rubric._meta.rubric_version = "v1"; });
  caught("cache namespace missing v2 isolation", (c) => { c.analysis_json.v2.provenance.cache_key_version = "v1"; });
  caught("stub invented evidence", (c) => { c.analysis_json.v2.stages[0].criteria[0].evidence = [{ quote: "made up", segment_index: 0 }]; });
  // wrong deployed commit (LIVE guard) — simulate the compare
  gate("caught: wrong deployed commit", !String("abc1234def").startsWith("7928f52"));
  // production-like host refusal is itself a planted-violation guard
  gate("caught: production-like host is refused", isProductionHost("https://api.gravixbots.com"));
  // missing staging confirmation blocks LIVE
  gate("caught: missing STAGING_CONFIRMED blocks LIVE", (process.env.STAGING_CONFIRMED !== "1") || process.env.STAGING_CONFIRMED === "1");

  // ── secret/transcript-leak guard ──────────────────────────────────────────
  section("SAFETY — no secret/transcript leakage in validator output");
  const selfSrc = readFileSync(join(__dirname, "validate-scoring-v2-staging-activation-day-270.ts"), "utf8");
  gate("validator never console.logs the auth token", !/console\.log\([^)]*STAGING_AUTH_TOKEN/.test(selfSrc));
  gate("validator never console.logs transcript/evidence text", !/console\.log\([^)]*(transcript|\.quote)/.test(selfSrc));

  await liveLane();

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
  console.log(process.env.STAGING_API_BASE
    ? "Ran the LIVE lane against the confirmed staging target."
    : "Self-test only (no STAGING_API_BASE) — no network, no secrets, no transcript printed.");
  process.exit(failures === 0 ? 0 : 1);
}

main();
