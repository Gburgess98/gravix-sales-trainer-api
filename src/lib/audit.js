import { createClient } from "@supabase/supabase-js";
// ─── Action constants ────────────────────────────────────────────────────────
export const AUDIT_ACTIONS = {
    // Auth
    LOGIN: "login",
    LOGOUT: "logout",
    // Account lifecycle
    CREATE_ACCOUNT: "create_account",
    UPDATE_ACCOUNT: "update_account",
    DELETE_ACCOUNT: "delete_account",
    // User lifecycle
    CREATE_USER: "create_user",
    UPDATE_USER: "update_user",
    DELETE_USER: "delete_user",
    // Impersonation
    IMPERSONATE_USER: "impersonate_user",
    END_IMPERSONATION: "end_impersonation",
    // Company lifecycle
    UPDATE_COMPANY: "update_company",
    // Profile (self-service)
    UPDATE_PROFILE: "update_profile",
};
// ─── logAuditEvent — new audit_events table ──────────────────────────────────
// Targets the audit_events table (see sql/20260605c_audit_events.sql).
// Never throws — audit failures must not break business logic.
let _auditSupa = null;
function getAuditSupa() {
    if (_auditSupa)
        return _auditSupa;
    _auditSupa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false, autoRefreshToken: false } });
    return _auditSupa;
}
export async function logAuditEvent(payload) {
    try {
        await getAuditSupa().from("audit_events").insert({
            actor_user_id: payload.actorUserId,
            target_user_id: payload.targetUserId ?? null,
            action: payload.action,
            entity_type: payload.entityType ?? null,
            entity_id: payload.entityId ?? null,
            metadata: payload.metadata ?? {},
        });
    }
    catch {
        // Intentionally swallowed — audit must never break calling code.
    }
}
// --------------------------------------------------
// AUDIT EVENT BUILDER
// --------------------------------------------------
export function buildAuditEvent(event) {
    return {
        ...event,
        created_at: event.created_at || new Date().toISOString(),
    };
}
// --------------------------------------------------
// COMMON ACTION HELPERS
// --------------------------------------------------
export function auditUserCreated(actorId, targetUserId, companyId, officeId) {
    return buildAuditEvent({
        action: "user_created",
        actor_id: actorId,
        actor_type: "customer_user",
        target_type: "user",
        target_id: targetUserId,
        company_id: companyId || null,
        office_id: officeId || null,
    });
}
export function auditAssignmentCreated(actorId, assignmentId, companyId, officeId) {
    return buildAuditEvent({
        action: "assignment_created",
        actor_id: actorId,
        actor_type: "customer_user",
        target_type: "assignment",
        target_id: assignmentId,
        company_id: companyId || null,
        office_id: officeId || null,
    });
}
export function auditInternalAccess(actorId, action, metadata) {
    return buildAuditEvent({
        action,
        actor_id: actorId,
        actor_type: "internal_user",
        metadata: metadata || {},
    });
}
export function auditImpersonationStarted(actorId, targetUserId) {
    return buildAuditEvent({
        action: "impersonation_started",
        actor_id: actorId,
        actor_type: "internal_user",
        target_type: "user",
        target_id: targetUserId,
    });
}
export function auditSettingsUpdated(actorId, targetType, targetId, metadata) {
    return buildAuditEvent({
        action: "settings_updated",
        actor_id: actorId,
        actor_type: "customer_user",
        target_type: targetType,
        target_id: targetId,
        metadata: metadata || {},
    });
}
// --------------------------------------------------
// AUDIT PERSISTENCE
// --------------------------------------------------
export async function writeAuditEvent(supa, event) {
    const payload = buildAuditEvent(event);
    const { error } = await supa
        .from("audit_logs")
        .insert(payload);
    if (error) {
        console.error("❌ Failed to write audit log", error.message);
    }
    return {
        ok: !error,
        error,
    };
}
