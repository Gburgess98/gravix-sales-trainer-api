// Go Live Day 39 (F-38-2) — the "score posted" activity written when a score job completes.
//
// Root cause of the Day-38 miss: the worker inserted `type: "call_scored"`, but
// public.activities has a CHECK constraint (activities_type_check) that only allows
// call_uploaded | score_posted | assignment_create | assignment_complete | sparring_session.
// The insert failed with a check violation, but Supabase query builders RETURN errors as
// `{ error }` (they do not throw) and the code never read it, so try/catch saw success.
// `call_scored` has no reader anywhere; `score_posted` is the schema's supported event.
//
// Contract: exactly one `score_posted` activity per completed score job (the job id is
// recorded in `payload.job_id`). Logging is FAIL-SOFT by design — the call stays `scored`
// — but never silent: the returned outcome is written into the score job's result
// (`activity.logged = false`, stable `error_code`) and logged. The caller must not claim
// the activity exists unless `logged === true`.

import type { SupabaseClient } from "@supabase/supabase-js";

export const SCORE_ACTIVITY_TYPE = "score_posted" as const;

export type ScoreActivityOutcome = { logged: true } | { logged: false; error_code: string };

export async function recordScorePostedActivity(
  client: SupabaseClient,
  a: {
    callId: string;
    jobId: string;
    orgId: string | null;
    actorUserId: string | null;
    overall: number;
    stages: { intro: number; discovery: number; objection: number; close: number };
    model: string;
  }
): Promise<ScoreActivityOutcome> {
  // activities.org_id is NOT NULL: an unresolvable org is a recorded failure, not a crash.
  if (!a.orgId) return { logged: false, error_code: "missing_org" };
  const r = (n: number) => Math.round(n);
  try {
    const { error } = await client.from("activities").insert({
      org_id: a.orgId,
      actor_user_id: a.actorUserId,
      type: SCORE_ACTIVITY_TYPE,
      summary: `Scored ${r(a.overall)} — Intro ${r(a.stages.intro)} / Disc ${r(a.stages.discovery)} / Obj ${r(a.stages.objection)} / Close ${r(a.stages.close)}`,
      call_id: a.callId,
      payload: { source: "score_worker", job_id: a.jobId, model: a.model, overall: r(a.overall) },
      created_at: new Date().toISOString(),
    });
    if (error) return { logged: false, error_code: String((error as { code?: string }).code || "insert_failed") };
    return { logged: true };
  } catch {
    return { logged: false, error_code: "exception" };
  }
}
