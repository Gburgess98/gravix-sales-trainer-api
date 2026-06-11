// --------------------------------------------------
// INTERNAL GRAVIX SUPPORT HELPERS
// --------------------------------------------------
export function isInternalUser(user) {
    return !!user?.is_internal;
}
export function isSupportUser(user) {
    return user?.role === "support";
}
export function isSuperAdmin(user) {
    return user?.role === "super_admin";
}
export function canAccessInternalPortal(user) {
    return isSupportUser(user) || isSuperAdmin(user);
}
export function canImpersonateUsers(user) {
    return isSuperAdmin(user);
}
// --------------------------------------------------
// CUSTOMER ADMIN HELPERS
// --------------------------------------------------
export function isCustomerAdmin(user) {
    return !!user?.is_admin;
}
export function canManageOffice(user) {
    return !!user?.is_admin || user?.role === "office_manager";
}
export function canManageCompany(user) {
    return (!!user?.is_admin ||
        user?.role === "company_manager");
}
export function buildImpersonationContext(internalUserId, targetUserId) {
    return {
        internal_user_id: internalUserId,
        target_user_id: targetUserId,
        started_at: new Date().toISOString(),
        active: true,
    };
}
export function resolveImpersonatedUserId(internalUser, impersonatedUserId) {
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
export function buildEffectiveUserId(authUserId, internalUser, impersonatedUserId) {
    const impersonated = resolveImpersonatedUserId(internalUser, impersonatedUserId);
    return impersonated || authUserId;
}
export function getImpersonationHeader(req) {
    const value = String(req.headers?.["x-impersonate-user-id"] || "").trim();
    return value || null;
}
// --------------------------------------------------
// INTERNAL ROUTE GUARDS
// --------------------------------------------------
export function requireInternalAccess(user) {
    if (!user || !canAccessInternalPortal(user)) {
        throw new Error("internal_access_required");
    }
}
export function requireSuperAdmin(user) {
    if (!user || !isSuperAdmin(user)) {
        throw new Error("super_admin_required");
    }
}
