import rateLimit from "express-rate-limit";

const jsonHandler = (message: string) => ({
  handler(_req: any, res: any) {
    res.status(429).json({ ok: false, error: message });
  },
});

// Global: 300 requests / minute per IP.
// Generous enough for normal app use; tight enough to blunt scripted abuse.
export const globalRateLimit = rateLimit({
  windowMs: 60 * 1000,
  max:      300,
  standardHeaders: "draft-7",
  legacyHeaders:   false,
  ...jsonHandler("rate_limit_exceeded"),
});

// Auth endpoints: 10 requests / 15 minutes per IP.
// Applied to /v1/auth/login and /v1/auth/reset-password to resist
// credential stuffing and account-enumeration attacks.
export const authRateLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max:      10,
  standardHeaders: "draft-7",
  legacyHeaders:   false,
  ...jsonHandler("auth_rate_limit_exceeded"),
});

// Upload endpoints: 30 requests / 10 minutes per IP.
// Each upload triggers transcription + scoring jobs; this prevents
// runaway costs from a compromised or misbehaving client.
export const uploadRateLimit = rateLimit({
  windowMs: 10 * 60 * 1000,
  max:      30,
  standardHeaders: "draft-7",
  legacyHeaders:   false,
  ...jsonHandler("upload_rate_limit_exceeded"),
});
