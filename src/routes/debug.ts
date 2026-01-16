import { Router } from "express";
import { requireAuth } from "../middleware/requireAuth";

const router = Router();

router.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "gravix-sales-trainer-api",
    time: new Date().toISOString(),
  });
});

router.get("/auth", requireAuth, (req, res) => {
  res.json({
    ok: true,
    user_id: req.user?.id ?? null,
    headers_seen: {
      authorization: Boolean(req.headers.authorization),
      x_user_id: Boolean(req.headers["x-user-id"]),
    },
  });
});

export default router;