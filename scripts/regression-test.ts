import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { scoreWithLLM } from "../src/lib/scoring";

type Range = {
  min: number;
  max: number;
};

type RegressionCase = {
  id: string;
  label: string;
  callId: string;
  expectedOverall: Range;
  expectedSections?: Partial<Record<"intro" | "discovery" | "objection" | "close", Range>>;
  notes?: string;
};

type SectionKey = "intro" | "discovery" | "objection" | "close";

type SectionResult = {
  score: number;
  notes: string;
};

type ScoreResult = {
  model: string;
  overall: number;
  summary: string;
  stages: Record<SectionKey, SectionResult>;
  moments?: Array<{
    timestamp?: number;
    type: string;
    text: string;
    severity?: string;
  }>;
  suggestions?: string[];
  voice?: {
    clarity: number;
    confidence: number;
    filler_density: number;
    pace: number;
    overall: number;
  };
};

const TEST_CASES: RegressionCase[] = [
  {
    id: "mid-1",
    label: "Mid call — some structure, weak objection + close",
    callId: "18c825d9-374e-49f3-a40f-4ffde9503513",
    expectedOverall: { min: 40, max: 55 },
    expectedSections: {
      intro: { min: 50, max: 70 },
      discovery: { min: 30, max: 55 },
      objection: { min: 20, max: 45 },
      close: { min: 20, max: 45 },
    },
    notes: "Average benchmark call under current rubric.",
  },
  {
    id: "weak-1",
    label: "Weak call — poor objection handling + close",
    callId: "73c6d3fd-1e84-41af-b4f4-9e6b8d38612c",
    expectedOverall: { min: 25, max: 50 },
    expectedSections: {
      objection: { min: 20, max: 50 },
      close: { min: 20, max: 50 },
    },
    notes: "Low-quality call for contrast",
  },
];

function getEnv(name: string): string {
  const value = String(process.env[name] || "").trim();
  if (!value) throw new Error(`Missing required env: ${name}`);
  return value;
}

function inRange(value: number, range: Range): boolean {
  return value >= range.min && value <= range.max;
}

function fmtRange(range: Range): string {
  return `${range.min}-${range.max}`;
}

function okLabel(ok: boolean): string {
  return ok ? "PASS" : "FAIL";
}

function padRight(input: string, len: number): string {
  return input.length >= len ? input : input + " ".repeat(len - input.length);
}

async function getCallMeta(supabase: SupabaseClient, callId: string) {
  const { data, error } = await supabase
    .from("calls")
    .select("id, filename, status, transcript, summary, score_overall, created_at")
    .eq("id", callId)
    .maybeSingle();

  if (error) throw new Error(`call lookup failed for ${callId}: ${error.message}`);
  if (!data) throw new Error(`call not found: ${callId}`);
  return data as {
    id: string;
    filename: string | null;
    status: string | null;
    transcript: string | null;
    summary: string | null;
    score_overall: number | null;
    created_at: string | null;
  };
}

function printDivider() {
  console.log("-".repeat(88));
}

async function runCase(supabase: SupabaseClient, testCase: RegressionCase) {
  const call = await getCallMeta(supabase, testCase.callId);

  if (!call.transcript || !call.transcript.trim()) {
    throw new Error(
      `Call ${testCase.callId} has no transcript. Upload/transcribe it first before using it in the regression pack.`
    );
  }

  const result = (await scoreWithLLM({
    supabase,
    callId: testCase.callId,
  })) as ScoreResult;

  const sectionChecks: Array<{
    key: SectionKey;
    expected: Range;
    actual: number;
    ok: boolean;
  }> = [];

  const keys: SectionKey[] = ["intro", "discovery", "objection", "close"];
  for (const key of keys) {
    const expected = testCase.expectedSections?.[key];
    if (!expected) continue;
    const actual = Number(result.stages?.[key]?.score);
    sectionChecks.push({
      key,
      expected,
      actual,
      ok: inRange(actual, expected),
    });
  }

  const overallOk = inRange(Number(result.overall), testCase.expectedOverall);
  const allSectionsOk = sectionChecks.every((x) => x.ok);
  const passed = overallOk && allSectionsOk;

  return {
    call,
    result,
    overallOk,
    sectionChecks,
    passed,
  };
}

async function main() {
  const supabaseUrl = getEnv("SUPABASE_URL");
  const supabaseServiceKey = getEnv("SUPABASE_SERVICE_ROLE_KEY");

  const supabase = createClient(supabaseUrl, supabaseServiceKey, {
    auth: { persistSession: false },
  });

  const runnableCases = TEST_CASES.filter(
    (x) => x.callId && x.callId !== "REPLACE_WITH_REAL_CALL_ID"
  );

  if (!runnableCases.length) {
    console.error("No runnable regression cases found.");
    console.error("Edit scripts/regression-test.ts and replace REPLACE_WITH_REAL_CALL_ID with real call IDs.");
    process.exit(1);
  }

  console.log("\nGravix Regression Pack");
  printDivider();
  console.log(`Cases: ${runnableCases.length}`);
  console.log(`Scoring model version: current runtime version from src/lib/scoring.ts`);
  printDivider();

  let passCount = 0;
  const failures: string[] = [];

  for (const testCase of runnableCases) {
    console.log(`\n${testCase.label}`);
    console.log(`Case ID: ${testCase.id}`);
    console.log(`Call ID: ${testCase.callId}`);
    if (testCase.notes) console.log(`Notes: ${testCase.notes}`);

    try {
      const outcome = await runCase(supabase, testCase);
      const { call, result, overallOk, sectionChecks, passed } = outcome;

      console.log(`File: ${call.filename || call.id}`);
      console.log(`Transcript present: ${call.transcript ? "yes" : "no"}`);
      console.log(
        `${padRight("Overall", 12)} actual=${String(result.overall).padStart(3)} expected=${fmtRange(
          testCase.expectedOverall
        ).padEnd(7)} ${okLabel(overallOk)}`
      );

      for (const section of sectionChecks) {
        console.log(
          `${padRight(section.key, 12)} actual=${String(section.actual).padStart(3)} expected=${fmtRange(
            section.expected
          ).padEnd(7)} ${okLabel(section.ok)}`
        );
      }

      console.log(`Summary: ${result.summary}`);
      console.log(`Result: ${okLabel(passed)}`);

      if (passed) {
        passCount += 1;
      } else {
        failures.push(`${testCase.id} (${testCase.label})`);
      }
    } catch (e: any) {
      const msg = String(e?.message || e);
      console.error(`Result: FAIL`);
      console.error(`Error: ${msg}`);
      failures.push(`${testCase.id} (${testCase.label}) — ${msg}`);
    }

    printDivider();
  }

  console.log(`\nSummary: ${passCount}/${runnableCases.length} passed`);

  if (failures.length) {
    console.log("Failed cases:");
    for (const failure of failures) console.log(`- ${failure}`);
    process.exit(1);
  }

  process.exit(0);
}

main().catch((e) => {
  console.error("Regression pack failed:", e?.message || e);
  process.exit(1);
});
