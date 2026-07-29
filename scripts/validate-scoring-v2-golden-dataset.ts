/**
 * validate-scoring-v2-golden-dataset.ts — Day 265.
 *
 * Validates the human-authored Scoring v2 golden dataset
 * (test/fixtures/scoring-v2/golden-calls.json) that Day 266's harness and Day
 * 267's runtime will be measured against. Pure data validation — no LLM calls,
 * no provider imports, no network, no DB.
 *
 * The load-bearing check: **every evidence quote (and objection detected_text)
 * must be an exact verbatim substring of the call transcript** — the whole point
 * of the dataset is that evidence is real, not invented.
 *
 * Usage: npx tsx scripts/validate-scoring-v2-golden-dataset.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function check(label: string, ok: boolean, detail?: string) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}

const STAGES = ["intro", "discovery", "objection", "close"];
const STATUSES = ["pass", "partial", "fail", "not_observed"];
const BANDS = ["excellent", "strong", "mixed", "weak", "poor"];
const STATUS_BAND: Record<string, (string | null)[]> = {
  pass: ["excellent", "strong"],
  partial: ["mixed"],
  fail: ["weak", "poor"],
  not_observed: [null],
};
const OBJECTION_CATEGORIES = ["price", "timing", "authority", "trust", "competitor", "fit", "logistics", "other"];
// The manager-approved Objection Library keys (seed-ufc-intelligence.ts).
const KNOWN_OBJECTION_KEYS = [
  "too-expensive", "need-to-think", "send-info", "speak-with-partner",
  "already-have-training", "not-right-time", "distrust-ai", "team-fit",
];
const HANDLED = ["handled", "partially", "missed"];

function main() {
  const path = join(__dirname, "..", "test", "fixtures", "scoring-v2", "golden-calls.json");

  let raw = "";
  try {
    raw = readFileSync(path, "utf8");
    check("golden dataset file exists and is readable", true);
  } catch (e: any) {
    check("golden dataset file exists and is readable", false, String(e?.message || e));
    finish();
    return;
  }

  // No LLM / provider tokens anywhere in the fixture.
  check("dataset contains no LLM/provider tokens", !/openai|anthropic|claude|gpt-|chat\.completions/i.test(raw));

  let data: any;
  try {
    data = JSON.parse(raw);
    check("dataset is valid JSON", true);
  } catch (e: any) {
    check("dataset is valid JSON", false, String(e?.message || e));
    finish();
    return;
  }

  check("contract tag is SCORING_OUTPUT_CONTRACT_V2", data.contract === "SCORING_OUTPUT_CONTRACT_V2");

  const calls: any[] = Array.isArray(data.calls) ? data.calls : [];
  check("dataset has exactly 5 calls", calls.length === 5, `found ${calls.length}`);

  const callIds = new Set<string>();
  const criterionIds = new Set<string>();
  const stagesSeen = new Set<string>();
  const statusesSeen = new Set<string>();
  const objectionCategoriesSeen = new Set<string>();

  for (const call of calls) {
    const cid = String(call?.id || "(missing)");
    check(`[${cid}] id is unique`, !!call?.id && !callIds.has(cid));
    callIds.add(cid);

    check(`[${cid}] has title and call_type`, !!call?.title && !!call?.call_type);

    const segments: any[] = Array.isArray(call?.transcript) ? call.transcript : [];
    check(`[${cid}] has a non-empty transcript`, segments.length > 0);
    const fullTranscript = segments.map((s) => String(s?.text || "")).join("\n");

    // helper: verify a quote against a segment index + full transcript
    const quoteOk = (quote: string, segIdx: number | null | undefined, speaker?: string): { ok: boolean; why: string } => {
      const q = String(quote || "");
      if (!q) return { ok: false, why: "empty quote" };
      if (!fullTranscript.includes(q)) return { ok: false, why: `not a transcript substring: "${q.slice(0, 40)}…"` };
      if (segIdx != null) {
        const seg = segments.find((s) => Number(s?.idx) === Number(segIdx));
        if (!seg) return { ok: false, why: `segment_index ${segIdx} not found` };
        if (!String(seg.text || "").includes(q)) return { ok: false, why: `quote not in segment ${segIdx}` };
        if (speaker && String(seg.speaker) !== speaker) return { ok: false, why: `speaker mismatch at seg ${segIdx}` };
      }
      return { ok: true, why: "" };
    };

    const criteria: any[] = Array.isArray(call?.expected_criteria) ? call.expected_criteria : [];
    check(`[${cid}] has one criterion per stage (4)`, criteria.length === 4 && new Set(criteria.map((c) => c.stage)).size === 4);

    for (const c of criteria) {
      const kid = String(c?.criterion_id || "(missing)");
      check(`[${cid}] criterion_id unique: ${kid.split("::").slice(1).join("::")}`, !!c?.criterion_id && !criterionIds.has(kid));
      criterionIds.add(kid);

      check(`[${cid}/${c?.stage}] stage is valid`, STAGES.includes(c?.stage));
      stagesSeen.add(c?.stage);
      check(`[${cid}/${c?.stage}] status is valid contract term`, STATUSES.includes(c?.status));
      statusesSeen.add(c?.status);
      check(`[${cid}/${c?.stage}] has a label`, !!c?.label);

      // band validity + status↔band consistency
      const band = c?.expected_score_band ?? null;
      check(`[${cid}/${c?.stage}] band valid`, band === null || BANDS.includes(band), String(band));
      const allowed = STATUS_BAND[c?.status] ?? [];
      check(`[${cid}/${c?.stage}] band matches status (${c?.status})`, allowed.includes(band));

      // evidence + why_points_lost rules
      const evidence: any[] = Array.isArray(c?.evidence) ? c.evidence : [];
      if (c?.status === "not_observed") {
        check(`[${cid}/${c?.stage}] not_observed has no evidence + null band`, evidence.length === 0 && band === null);
      } else {
        check(`[${cid}/${c?.stage}] has >=1 evidence quote`, evidence.length >= 1);
      }
      for (const ev of evidence) {
        const r = quoteOk(ev?.quote, ev?.segment_index, ev?.speaker);
        check(`[${cid}/${c?.stage}] evidence quote is verbatim`, r.ok, r.why);
      }
      if (c?.status === "partial" || c?.status === "fail") {
        check(`[${cid}/${c?.stage}] ${c.status} has why_points_lost`, typeof c?.why_points_lost === "string" && c.why_points_lost.trim().length > 0);
      }
      if (c?.suggested_drill) {
        check(`[${cid}/${c?.stage}] suggested_drill has key+title`, !!c.suggested_drill.key && !!c.suggested_drill.title);
      }
    }

    // objection matches
    const oms: any[] = Array.isArray(call?.expected_objection_matches) ? call.expected_objection_matches : [];
    for (const om of oms) {
      check(`[${cid}] objection category known: ${om?.category}`, OBJECTION_CATEGORIES.includes(om?.category));
      objectionCategoriesSeen.add(om?.category);
      check(`[${cid}] objection key known: ${om?.objection_item_key}`, KNOWN_OBJECTION_KEYS.includes(om?.objection_item_key));
      check(`[${cid}] objection handled valid: ${om?.handled}`, HANDLED.includes(om?.handled));
      const dt = quoteOk(om?.detected_text, om?.evidence?.segment_index);
      check(`[${cid}] objection detected_text is verbatim`, dt.ok, dt.why);
      if (om?.evidence?.quote) {
        const ev = quoteOk(om.evidence.quote, om.evidence.segment_index, om.evidence.speaker);
        check(`[${cid}] objection evidence quote is verbatim`, ev.ok, ev.why);
      }
    }

    check(`[${cid}] has expected_summary`, typeof call?.expected_summary === "string" && call.expected_summary.length > 0);
    check(`[${cid}] has expected_suggested_drills array`, Array.isArray(call?.expected_suggested_drills));
  }

  // Dataset-wide coverage
  check("all four stages represented across dataset", STAGES.every((s) => stagesSeen.has(s)));
  check("all four statuses represented across dataset", STATUSES.every((s) => statusesSeen.has(s)), [...statusesSeen].join(","));
  check("objection categories cover price/timing/authority/trust", ["price", "timing", "authority", "trust"].every((c) => objectionCategoriesSeen.has(c)), [...objectionCategoriesSeen].join(","));

  finish();
}

function finish() {
  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No LLM calls made.`);
  process.exit(failures === 0 ? 0 : 1);
}

main();
