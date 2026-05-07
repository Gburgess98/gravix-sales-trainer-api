export type UserContext = {
  id: string;
  role: string;
  office_id?: string | null;
  company_id?: string | null;
  active_office_id?: string | null;
  is_admin?: boolean;
};

export function isRep(user?: UserContext | null) {
  return user?.role === "rep";
}

export function isOfficeManager(user?: UserContext | null) {
  return user?.role === "office_manager";
}

export function isCompanyManager(user?: UserContext | null) {
  return user?.role === "company_manager";
}

export function isAdmin(user?: UserContext | null) {
  return !!user?.is_admin;
}

export function getEffectiveOfficeId(
  user?: UserContext | null
) {
  if (!user) return null;

  // company managers can temporarily switch office context
  if (
    isCompanyManager(user) &&
    user.active_office_id
  ) {
    return user.active_office_id;
  }

  // office managers locked to assigned office
  return user.office_id || null;
}

export function canAccessOffice(
  user: UserContext,
  officeId?: string | null
) {
  if (!officeId) return true;

  // company managers can access all offices
  if (isCompanyManager(user)) {
    return true;
  }

  // office managers locked to assigned office
  return getEffectiveOfficeId(user) === officeId;
}

export function canAccessCompany(
  user: UserContext,
  companyId?: string | null
) {
  if (!companyId) return true;

  return user.company_id === companyId;
}

export function canManageUsers(user: UserContext) {
  return !!user.is_admin;
}

export function canViewRep(
  viewer: UserContext,
  rep: UserContext
) {
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
    return (
      getEffectiveOfficeId(viewer) ===
      rep.office_id
    );
  }

  return false;
}

export function buildUserContext(
  dbUser: any,
  activeOfficeId?: string | null
): UserContext {
  return {
    id: String(dbUser.id),
    role: String(dbUser.role || "rep"),
    office_id: dbUser.office_id || null,
    company_id: dbUser.company_id || null,
    active_office_id: activeOfficeId || null,
    is_admin: Boolean(dbUser.is_admin),
  };
}