export type InternalUser = {
  id: string;
  email: string;
  role: string;
  is_internal: boolean;
};

export type CustomerUser = {
  id: string;
  role: string;
  office_id?: string | null;
  company_id?: string | null;
  is_admin?: boolean;
};

// --------------------------------------------------
// INTERNAL GRAVIX SUPPORT HELPERS
// --------------------------------------------------

export function isInternalUser(user?: InternalUser | null) {
  return !!user?.is_internal;
}

export function isSupportUser(user?: InternalUser | null) {
  return user?.role === "support";
}

export function isSuperAdmin(user?: InternalUser | null) {
  return user?.role === "super_admin";
}

export function canAccessInternalPortal(user?: InternalUser | null) {
  return isSupportUser(user) || isSuperAdmin(user);
}

export function canImpersonateUsers(user?: InternalUser | null) {
  return isSuperAdmin(user);
}

// --------------------------------------------------
// CUSTOMER ADMIN HELPERS
// --------------------------------------------------

export function isCustomerAdmin(user?: CustomerUser | null) {
  return !!user?.is_admin;
}

export function canManageOffice(user?: CustomerUser | null) {
  return !!user?.is_admin || user?.role === "office_manager";
}

export function canManageCompany(user?: CustomerUser | null) {
  return (
    !!user?.is_admin ||
    user?.role === "company_manager"
  );
}

// --------------------------------------------------
// IMPERSONATION FOUNDATION
// --------------------------------------------------

export type ImpersonationContext = {
  internal_user_id: string;
  target_user_id: string;
  started_at: string;
  active: boolean;
};

export function buildImpersonationContext(
  internalUserId: string,
  targetUserId: string
): ImpersonationContext {
  return {
    internal_user_id: internalUserId,
    target_user_id: targetUserId,
    started_at: new Date().toISOString(),
    active: true,
  };
}

export function resolveImpersonatedUserId(
  internalUser: InternalUser | null | undefined,
  impersonatedUserId?: string | null
) {
  if (!internalUser) {
    return null;
  }

  if (!canImpersonateUsers(internalUser)) {
    return null;
  }

  if (!impersonatedUserId) {
    return null;
  }

  return impersonatedUserId;
}

export function buildEffectiveUserId(
  authUserId: string,
  internalUser?: InternalUser | null,
  impersonatedUserId?: string | null
) {
  const impersonated = resolveImpersonatedUserId(
    internalUser,
    impersonatedUserId
  );

  return impersonated || authUserId;
}

export function getImpersonationHeader(
  req: any
): string | null {
  const value = String(
    req.headers?.["x-impersonate-user-id"] || ""
  ).trim();

  return value || null;
}

// --------------------------------------------------
// INTERNAL ROUTE GUARDS
// --------------------------------------------------

export function requireInternalAccess(user?: InternalUser | null) {
  if (!user || !canAccessInternalPortal(user)) {
    throw new Error("internal_access_required");
  }
}

export function requireSuperAdmin(user?: InternalUser | null) {
  if (!user || !isSuperAdmin(user)) {
    throw new Error("super_admin_required");
  }
}