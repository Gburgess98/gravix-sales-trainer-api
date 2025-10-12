import { Request, Response, NextFunction } from "express";

export function requireUserId(req: Request, res: Response, next: NextFunction) {
  const userId = req.header("x-user-id");
  if (!userId) return res.status(401).json({ error: "Missing x-user-id" });
  (req as any).userId = userId;
  next();
}