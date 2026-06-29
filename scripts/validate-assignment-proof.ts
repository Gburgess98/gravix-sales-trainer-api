/**
 * validate-assignment-proof.ts — Day 155
 * Deterministic assertions for sparring completion proof merge
 * (mergeCompletionProof) used by PATCH /v1/assignments/manager/:id.
 * Usage: npx tsx scripts/validate-assignment-proof.ts
 */

import { mergeCompletionProof } from "../src/routes/assignments";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

const goodProof = {
  completed_via: "sparring_session_match",
  matched_sparring_session_id: "sess-123",
  completion_score: 82,
  completed_session_at: "2026-06-29T10:00:00.000Z",
  completed_from_dashboard: true,
};

// Known keys are persisted.
const merged = mergeCompletionProof({}, goodProof);
check("returns merged meta for a valid proof", merged !== null);
check("sets completed_via = sparring_session_match", merged?.completed_via === "sparring_session_match");
check("persists matched_sparring_session_id", merged?.matched_sparring_session_id === "sess-123");
check("persists numeric completion_score", merged?.completion_score === 82);
check("persists completed_session_at", merged?.completed_session_at === "2026-06-29T10:00:00.000Z");
check("sets completed_from_dashboard = true", merged?.completed_from_dashboard === true);

// Existing meta is preserved, not replaced.
const withExisting = mergeCompletionProof({ origin_label: "Coaching Queue", recommended_drill: "Pricing" }, goodProof);
check("preserves existing meta keys", withExisting?.origin_label === "Coaching Queue" && withExisting?.recommended_drill === "Pricing");

// Unknown keys are dropped.
const withUnknown = mergeCompletionProof({}, { ...goodProof, sneaky: "nope", completed_by: "rep" });
check("drops unknown proof keys (sneaky)", withUnknown?.sneaky === undefined);
check("drops unknown proof keys (completed_by)", withUnknown?.completed_by === undefined);

// Guards: no session id, non-object, missing → null (meta left untouched).
check("null when matched_sparring_session_id missing", mergeCompletionProof({}, { completion_score: 80 }) === null);
check("null when proof is null", mergeCompletionProof({}, null) === null);
check("null when proof is not an object", mergeCompletionProof({}, "x") === null);
check("null when session id is blank", mergeCompletionProof({}, { matched_sparring_session_id: "  " }) === null);

// Non-numeric score is ignored (key absent rather than NaN).
const badScore = mergeCompletionProof({}, { matched_sparring_session_id: "s1", completion_score: "82" });
check("ignores non-numeric completion_score", badScore !== null && !("completion_score" in badScore));

// completed_from_dashboard only true when strictly true.
const notDash = mergeCompletionProof({}, { matched_sparring_session_id: "s1", completed_from_dashboard: "yes" });
check("completed_from_dashboard false unless strictly true", notDash?.completed_from_dashboard === false);

if (failures > 0) {
  console.log(`\nAssignment proof validation FAILED (${failures})`);
  process.exit(1);
}
console.log("\nAssignment proof validation PASSED");
