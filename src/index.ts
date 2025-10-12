import "dotenv/config";
import express from "express";
import cors from "cors";

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: (process.env.WEB_ORIGIN || "http://localhost:3000").split(","),
  })
);

// Health + Ping
app.get("/health", (_req, res) => res.json({ status: "ok" }));
app.get("/v1/ping", (_req, res) => res.json({ pong: true }));

// DB sanity route
app.get("/v1/db/now", async (_req, res) => {
  try {
    const { supabaseAdmin } = await import("./lib/supabase");
    const { data, error } = await supabaseAdmin
      .from("profiles")
      .select("user_id")
      .limit(1);

    if (error) return res.status(500).json({ ok: false, error: error.message });
    res.json({ ok: true, profilesCount: data?.length ?? 0 });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e?.message || "unknown" });
  }
});

const port = Number(process.env.PORT || 4000);
app.listen(port, () => {
  console.log(`[api] listening on :${port}`);
});
app.get("/v1/env-check", (_req, res) => {
  const url = process.env.SUPABASE_URL || "";
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  res.json({
    urlStartsWith: url.slice(0, 12),      // should be "https://gekc"
    urlLength: url.length,
    hasSpaceBefore: /^\s/.test(url),
    hasSpaceAfter: /\s$/.test(url),
    keyPrefix: key.slice(0, 8),
    keyLength: key.length
  });
});
