import { requireUserId } from "./requireUserId";
// Simple alias so routes can depend on "requireAuth" without caring about naming.
// For now: require a user id (rep or manager). Manager-only routes should still use requireManager.
export function requireAuth(req, res, next) {
    return requireUserId(req, res, next);
}
export default requireAuth;
