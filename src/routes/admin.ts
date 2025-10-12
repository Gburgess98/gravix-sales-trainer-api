// src/routes/admin.ts
import { Router } from "express";
export const adminRouter = Router();

adminRouter.post("/force-score/:id", async (req, res) => {
  const { id } = req.params;
  try {
    const jobId = await req.services.scoring.enqueue({
      callId: id,
      userId: req.user?.id ?? "admin",
    });
    return res.json({ ok: true, jobId });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "enqueue failed" });
  }
});

// src/server.ts
// ...
// app.use("/v1/admin", authMiddleware(/* TODO: admin role */), adminRouter);
