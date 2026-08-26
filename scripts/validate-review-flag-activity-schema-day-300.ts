/**
 * validate-review-flag-activity-schema-day-300.ts
 *
 * Day 300 — structured `review_flag` activity contract into `crm_activities`.
 *
 * Pre-fix (0456cb2): both the primary and fallback scoring paths inserted a
 * per-flag `crm_activities` row with TOP-LEVEL `summary` and `flag_key`/
 * `flag_category`/`flag_severity`/`flag_section` — none of which exist on
 * crm_activities — and omitted the NOT-NULL `title` and `user_id`. They also
 * referenced the wrong branch variable (`memoryCall` in the primary path, `call`
 * in the fallback path). Every insert therefore failed, but the best-effort catch
 * swallowed it, so a call could score while its review-flag activity was silently
 * absent.
 *
 * OFFLINE self-test (no network, no secrets) — static assertions on
 * src/lib/scoring.ts. Confirmed dedicated-staging crm_activities columns: id, type,
 * title, user_id, rep_id, org_id, call_id, account_id, contact_id, opportunity_id,
 * source, status, due_at, meta, created_at (NOT NULL: type, title, user_id) — no
 * `summary`, no `flag_*`.
 *
 * Non-vacuous: fails against 0456cb2 (no shared builder; top-level summary/flag_*;
 * no title). Usage: npx tsx scripts/validate-review-flag-activity-schema-day-300.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) { console.log(`\n── ${t} ──`); }

const S = readFileSync(join(__dirname, "..", "src", "lib", "scoring.ts"), "utf8");

function slice(from: string, to: string, src = S): string {
  const a = src.indexOf(from);
  if (a < 0) return "";
  const b = src.indexOf(to, a + from.length);
  return b > a ? src.slice(a, b) : src.slice(a);
}

// The shared builder body.
const builder = slice("function buildReviewFlagActivityRow(", "\nfunction shouldCreateAssignment(");
// Its return object (top-level keys) vs its meta block.
const builderReturn = slice("return {", "meta: {", builder);
const builderMeta = slice("meta: {", "\n  };", builder);
// The two structured review_flag insert blocks (comment markers are the anchors).
const primaryBlock = slice("review_flag (structured; analytics in meta) — primary path", "ensureCriticalCallAssignment");
const fallbackBlock = slice("review_flag (structured; analytics in meta) — fallback path", "ensureCriticalCallAssignment");

console.log("Day 300 — structured review_flag activity contract (offline; no network, no secrets)\n");

section("shared builder exists and prevents the two paths drifting");
gate("buildReviewFlagActivityRow builder exists", builder.length > 0);
gate("builder located with a return object + meta", builderReturn.length > 0 && builderMeta.length > 0);

section("only real top-level crm_activities columns (no summary / flag_* at top level)");
gate("builder top-level emits type:'review_flag'", /type:\s*["']review_flag["']/.test(builderReturn));
gate("builder top-level emits NOT-NULL title", /\btitle:\s*/.test(builderReturn));
gate("builder top-level emits NOT-NULL user_id", /\buser_id:\s*userId/.test(builderReturn));
gate("builder top-level has NO `summary` column", !/\bsummary:/.test(builderReturn));
gate("builder top-level has NO flag_* columns", !/\bflag_(key|category|severity|section):/.test(builderReturn));
gate("no review_flag row writes a top-level summary anywhere in scoring.ts",
  !/summary:\s*`\$\{flag/.test(S));
gate("no review_flag row writes top-level flag_key: flag.type (old shape) anywhere",
  !/flag_key:\s*flag\.type\b/.test(S));

section("flag analytics preserved inside meta");
gate("meta preserves flag_key", /flag_key:/.test(builderMeta));
gate("meta preserves flag_category", /flag_category:/.test(builderMeta));
gate("meta preserves flag_severity", /flag_severity:/.test(builderMeta));
gate("meta preserves flag_section", /flag_section:/.test(builderMeta));
gate("meta preserves score + timestamp", /score:/.test(builderMeta) && /timestamp:/.test(builderMeta));
gate("meta preserves threshold_band + thresholds", /threshold_band:/.test(builderMeta) && /thresholds:\s*\{/.test(builderMeta));

section("caller-derived linkage preserved (builder passes through)");
gate("builder returns org_id / rep_id / call_id", /\borg_id:\s*orgId/.test(builderReturn) && /\brep_id:\s*userId/.test(builderReturn) && /\bcall_id:\s*callId/.test(builderReturn));
gate("builder returns account_id / contact_id", /\baccount_id:\s*accountId/.test(builderReturn) && /\bcontact_id:\s*contactId/.test(builderReturn));

section("both primary and fallback paths covered, provenance distinguishable");
gate("primary review_flag block present + uses the builder", primaryBlock.length > 0 && /buildReviewFlagActivityRow\(/.test(primaryBlock));
gate("fallback review_flag block present + uses the builder", fallbackBlock.length > 0 && /buildReviewFlagActivityRow\(/.test(fallbackBlock));
gate("exactly two builder call sites", (S.match(/buildReviewFlagActivityRow\(\{/g) || []).length === 2, `${(S.match(/buildReviewFlagActivityRow\(\{/g) || []).length}`);
gate("primary source = 'scoring_engine'", /source:\s*'scoring_engine'/.test(primaryBlock));
gate("fallback source = 'scoring_engine_fallback'", /source:\s*'scoring_engine_fallback'/.test(fallbackBlock));
gate("primary path resolves userId/orgId from `call`", /userId:\s*\(call as any\)\?\.user_id/.test(primaryBlock) && /orgId:\s*\(call as any\)\?\.org_id/.test(primaryBlock));
gate("fallback path resolves userId/orgId from `memoryCall`", /userId:\s*\(memoryCall as any\)\?\.user_id/.test(fallbackBlock) && /orgId:\s*\(memoryCall as any\)\?\.org_id/.test(fallbackBlock));
gate("both paths pass account/contact linkage", /accountId:\s*\(linkRow as any\)\?\.account_id/.test(primaryBlock) && /accountId:\s*\(linkRow as any\)\?\.account_id/.test(fallbackBlock));

section("side-effect kill switch enforced; no paid AI / outbound in these paths");
gate("both review_flag blocks gated by !SKIP_SCORING_SIDE_EFFECTS",
  /!SKIP_SCORING_SIDE_EFFECTS && reviewFlags\.length > 0/.test(primaryBlock) &&
  /!SKIP_SCORING_SIDE_EFFECTS && reviewFlags\.length > 0/.test(fallbackBlock));
gate("builder is pure (no fetch / provider / outbound calls)",
  !/(fetch\(|openai|anthropic|slack|sendEmail|notify)/i.test(builder));
gate("review_flag blocks only touch crm_activities + calls linkage (no AI/outbound)",
  !/(openai|anthropic|slack|sendEmail)/i.test(primaryBlock) && !/(openai|anthropic|slack|sendEmail)/i.test(fallbackBlock));

console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
