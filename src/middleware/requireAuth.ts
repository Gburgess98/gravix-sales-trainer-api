import type { Request, Response, NextFunction } from "express";
import { requireUserId } from "./requireUserId";

// Simple alias so routes can depend on "requireAuth" without caring about naming.
// For now: require a user id (rep or manager). Manager-only routes should still use requireManager.
export function requireAuth(req: Request, res: Response, next: NextFunction) {
  return requireUserId(req, res, next);
}

export default requireAuth;