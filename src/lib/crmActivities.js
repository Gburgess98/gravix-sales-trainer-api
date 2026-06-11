/**
 * 🔥 Centralised CRM Activity Logger
 * All writes to crm_activities MUST go through this
 */
export async function logActivity(supa, input) {
    const { org_id, rep_id = null, call_id = null, type, source, meta = {}, } = input;
    if (!org_id) {
        console.warn("[crmActivities] Missing org_id");
        return { error: "missing_org_id" };
    }
    try {
        const { error } = await supa.from("crm_activities").insert({
            org_id,
            rep_id,
            call_id,
            type,
            source,
            meta,
            created_at: new Date().toISOString(),
        });
        if (error) {
            console.error("[crmActivities] insert error", error);
            return { error };
        }
        return { ok: true };
    }
    catch (e) {
        console.error("[crmActivities] fatal error", e);
        return { error: e.message };
    }
}
/**
 * 🔥 Helper: Review Flag Activity
 */
export async function logReviewFlag(supa, args) {
    return logActivity(supa, {
        org_id: args.org_id,
        rep_id: args.rep_id,
        call_id: args.call_id,
        type: "review_flag",
        source: "scoring",
        meta: {
            flag_key: args.flag_key,
            flag_section: args.flag_section,
            flag_severity: args.flag_severity,
            score: args.score ?? null,
        },
    });
}
/**
 * 🔥 Helper: Assignment Activity
 */
export async function logAssignmentCreated(supa, args) {
    return logActivity(supa, {
        org_id: args.org_id,
        rep_id: args.rep_id,
        type: "assignment_created",
        source: "assignment_system",
        meta: {
            assignment_id: args.assignment_id,
            assignment_origin: args.assignment_origin || "unknown",
            section: args.section || null,
            score_before: args.score_before ?? null,
        },
    });
}
/**
 * 🔥 Helper: Sparring Completed Activity
 */
export async function logSparringCompleted(supa, args) {
    return logActivity(supa, {
        org_id: args.org_id,
        rep_id: args.rep_id,
        type: "sparring_completed",
        source: "sparring_engine",
        meta: {
            session_id: args.session_id,
            score: args.score ?? null,
            difficulty: args.difficulty || null,
        },
    });
}
