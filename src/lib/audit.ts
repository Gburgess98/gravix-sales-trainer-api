export type AuditActorType =
  | "customer_user"
  | "internal_user";

export type AuditEvent = {
  action: string;
  actor_id: string;
  actor_type: AuditActorType;

  target_type?: string | null;
  target_id?: string | null;

  company_id?: string | null;
  office_id?: string | null;

  metadata?: Record<string, any>;

  created_at?: string;
};

// --------------------------------------------------
// AUDIT EVENT BUILDER
// --------------------------------------------------

export function buildAuditEvent(
  event: AuditEvent
): AuditEvent {
  return {
    ...event,
    created_at:
      event.created_at || new Date().toISOString(),
  };
}

// --------------------------------------------------
// COMMON ACTION HELPERS
// --------------------------------------------------

export function auditUserCreated(
  actorId: string,
  targetUserId: string,
  companyId?: string | null,
  officeId?: string | null
) {
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

export function auditAssignmentCreated(
  actorId: string,
  assignmentId: string,
  companyId?: string | null,
  officeId?: string | null
) {
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

export function auditInternalAccess(
  actorId: string,
  action: string,
  metadata?: Record<string, any>
) {
  return buildAuditEvent({
    action,
    actor_id: actorId,
    actor_type: "internal_user",
    metadata: metadata || {},
  });
}

export function auditImpersonationStarted(
  actorId: string,
  targetUserId: string
) {
  return buildAuditEvent({
    action: "impersonation_started",
    actor_id: actorId,
    actor_type: "internal_user",
    target_type: "user",
    target_id: targetUserId,
  });
}

export function auditSettingsUpdated(
  actorId: string,
  targetType: string,
  targetId: string,
  metadata?: Record<string, any>
) {
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

export async function writeAuditEvent(
  supa: any,
  event: AuditEvent
) {
  const payload = buildAuditEvent(event);

  const { error } = await supa
    .from("audit_logs")
    .insert(payload);

  if (error) {
    console.error(
      "❌ Failed to write audit log",
      error.message
    );
  }

  return {
    ok: !error,
    error,
  };
}
