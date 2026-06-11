export function isRep(user) {
    return user?.role === "rep";
}
export function isOfficeManager(user) {
    return user?.role === "office_manager";
}
export function isCompanyManager(user) {
    return user?.role === "company_manager";
}
export function isAdmin(user) {
    return !!user?.is_admin;
}
export function getEffectiveOfficeId(user) {
    if (!user)
        return null;
    // company managers can temporarily switch office context
    if (isCompanyManager(user) &&
        user.active_office_id) {
        return user.active_office_id;
    }
    // office managers locked to assigned office
    return user.office_id || null;
}
export function canAccessOffice(user, officeId) {
    if (!officeId)
        return true;
    // company managers can access all offices
    if (isCompanyManager(user)) {
        return true;
    }
    // office managers locked to assigned office
    return getEffectiveOfficeId(user) === officeId;
}
export function canAccessCompany(user, companyId) {
    if (!companyId)
        return true;
    return user.company_id === companyId;
}
export function canManageUsers(user) {
    return !!user.is_admin;
}
export function canViewRep(viewer, rep) {
    // reps only see themselves
    if (isRep(viewer)) {
        return viewer.id === rep.id;
    }
    // company manager sees whole company
    if (isCompanyManager(viewer)) {
        return viewer.company_id === rep.company_id;
    }
    // office manager sees office only
    if (isOfficeManager(viewer)) {
        return (getEffectiveOfficeId(viewer) ===
            rep.office_id);
    }
    return false;
}
export function buildUserContext(dbUser, activeOfficeId) {
    return {
        id: String(dbUser.id),
        role: String(dbUser.role || "rep"),
        office_id: dbUser.office_id || null,
        company_id: dbUser.company_id || null,
        active_office_id: activeOfficeId || null,
        is_admin: Boolean(dbUser.is_admin),
    };
}
// ─── Tier-based helpers (reps.tier system) ────────────────────────────────────
// These work with reps.tier strings, not users.role.
// Ordered narrowest → broadest: SalesRep < Manager < Owner < PartnerAdmin < SuperAdmin
export function isPartnerAdmin(tier) {
    return tier === "PartnerAdmin";
}
export function isSuperAdmin(tier) {
    return tier === "SuperAdmin";
}
export function isPartnerAdminOrAbove(tier) {
    return tier === "PartnerAdmin" || tier === "SuperAdmin";
}
export function isManagerOrAbove(tier) {
    return ["Manager", "Owner", "PartnerAdmin", "SuperAdmin"].includes(tier ?? "");
}
