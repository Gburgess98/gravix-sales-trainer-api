/**
 * Single source of truth for the synthetic STAGING tenant identifiers.
 *
 * The API validates ids with a strict RFC-4122 predicate (version nibble 1–5, variant nibble 8/9/a/b — see UUID_RE in
 * src/routes/crm.ts and siblings). The original Day-272 fixtures used `00000000-2711-0000-0000-…` (version 0, variant 0),
 * which that predicate rejects (`Missing or invalid x-org-id`). The valid replacements keep the same readable family:
 * `00000000-2711-4000-8000-…` (version 4, variant 8). Validation is NOT relaxed anywhere.
 */

/** Valid, deterministic ids used by the seed, the QA provisioning script and the proofs. */
export const STAGING_TENANT_IDS = {
  tmcId: "00000000-2711-4000-8000-000000000001",
  orgId: "00000000-2711-4000-8000-000000000002",
  companyId: "00000000-2711-4000-8000-000000000003",
  /** synthetic call-owner rep (pure tenant data, no auth FK) */
  ownerRepId: "00000000-2711-4000-8000-00000000000c",
} as const;

/** The malformed ids the repair retires. Referenced ONLY by the repair generator, its validator and the runbook. */
export const LEGACY_STAGING_TENANT_IDS = {
  tmcId: "00000000-2711-0000-0000-000000000001",
  orgId: "00000000-2711-0000-0000-000000000002",
  companyId: "00000000-2711-0000-0000-000000000003",
  ownerRepId: "00000000-2711-0000-0000-00000000000c",
  /** the id the Day-272 calls/crm_activities used as owner (there was never a rep row for it) */
  callOwnerId: "00000000-2711-0000-0000-00000000000b",
  /** hand-written auth.users/profiles from before Day 273 (cannot be re-id'd; retired by a separate, optional step) */
  legacyAuthUserIds: ["00000000-2711-0000-0000-00000000000a", "00000000-2711-0000-0000-00000000000b"],
  /** Day-276 stale call twins and the valid ids they were cloned to */
  staleCallIds: ["00000000-2722-0000-0000-000000000001", "00000000-2722-0000-0000-000000000002"],
} as const;

export const STAGING_FIXTURE_CALL_IDS = {
  stubProofCallId: "00000000-2722-4000-8000-000000000001",
  v1FallbackCallId: "00000000-2722-4000-8000-000000000002",
} as const;
