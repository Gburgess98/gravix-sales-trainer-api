// src/routes/auth.ts
// Auth endpoints that proxy Supabase auth for the frontend.
//
// POST /v1/auth/login          — email + password → session + user profile
// POST /v1/auth/logout         — invalidate server-side session
// POST /v1/auth/reset-password — trigger Supabase reset-password email
// GET  /v1/auth/me             — verify token, return user identity + tier

import { Router } from "express";
import type { Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";

export const authRouter = Router();

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Service-role client — used only for admin operations (role lookup, user fetch).
let _supa: ReturnType<typeof createClient> | null = null;
function getSupa() {
  if (_supa) return _supa;
  _supa = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!,
    { auth: { persistSession: false, autoRefreshToken: false } }
  );
  return _supa;
}

// Anon client — used for auth operations that must run as the user.
let _anonSupa: ReturnType<typeof createClient> | null = null;
function getAnonSupa() {
  if (_anonSupa) return _anonSupa;
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY!;
  _anonSupa = createClient(url, key, { auth: { persistSession: false, autoRefreshToken: false } });
  return _anonSupa;
}

function getBearerToken(req: Request): string | null {
  const raw = (req.header("authorization") || req.header("Authorization") || "").trim();
  const m = raw.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

// Fetch rep profile (name, tier, company_id, org_id) for a given auth user id.
async function fetchRepProfile(userId: string) {
  const { data } = await getSupa()
    .from("reps")
    .select("id, name, display_name, email, tier, company_id, org_id, is_active")
    .eq("id", userId)
    .maybeSingle();
  return data as any | null;
}

// ── POST /v1/auth/login ───────────────────────────────────────────────────────
authRouter.post("/login", async (req: Request, res: Response) => {
  try {
    const { email, password } = (req.body || {}) as { email?: string; password?: string };

    if (!email || typeof email !== "string") {
      return res.status(400).json({ ok: false, error: "email_required" });
    }
    if (!password || typeof password !== "string") {
      return res.status(400).json({ ok: false, error: "password_required" });
    }

    const trimmedEmail = email.trim().toLowerCase();

    const { data, error } = await getAnonSupa().auth.signInWithPassword({
      email:    trimmedEmail,
      password: password,
    });

    if (error || !data?.session || !data?.user) {
      const msg = error?.message || "invalid_credentials";
      const isInvalid = msg.toLowerCase().includes("invalid") || msg.toLowerCase().includes("credentials") || msg.toLowerCase().includes("password");
      return res.status(isInvalid ? 401 : 500).json({ ok: false, error: isInvalid ? "invalid_credentials" : msg });
    }

    const { session, user } = data;

    // Fetch the rep profile to get tier + name
    const rep = await fetchRepProfile(user.id);

    if (!rep) {
      // Auth succeeded but no reps row — return minimal identity so the frontend can handle onboarding
      return res.json({
        ok:           true,
        userId:       user.id,
        accessToken:  session.access_token,
        refreshToken: session.refresh_token,
        expiresIn:    session.expires_in ?? 3600,
        user: {
          id:         user.id,
          email:      user.email ?? trimmedEmail,
          name:       null,
          tier:       null,
          company_id: null,
          org_id:     null,
          is_active:  true,
          no_profile: true,
        },
      });
    }

    return res.json({
      ok:           true,
      userId:       user.id,
      accessToken:  session.access_token,
      refreshToken: session.refresh_token,
      expiresIn:    session.expires_in ?? 3600,
      user: {
        id:         rep.id,
        email:      user.email ?? rep.email ?? trimmedEmail,
        name:       rep.display_name ?? rep.name ?? null,
        tier:       rep.tier ?? null,
        company_id: rep.company_id ?? null,
        org_id:     rep.org_id ?? null,
        is_active:  rep.is_active ?? true,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "login_failed" });
  }
});

// ── POST /v1/auth/logout ──────────────────────────────────────────────────────
authRouter.post("/logout", async (req: Request, res: Response) => {
  try {
    const token = getBearerToken(req);
    const userId = String((req as any).userId || req.header("x-user-id") || "").trim();

    // Best-effort server-side session invalidation
    if (token && process.env.SUPABASE_SERVICE_ROLE_KEY) {
      try {
        // signOut with service role using the user's token
        await getAnonSupa().auth.admin.signOut(userId).catch(() => {});
      } catch { /* ignore — client must clear local state regardless */ }
    }

    return res.json({ ok: true });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "logout_failed" });
  }
});

// ── POST /v1/auth/reset-password ─────────────────────────────────────────────
authRouter.post("/reset-password", async (req: Request, res: Response) => {
  try {
    const { email } = (req.body || {}) as { email?: string };

    if (!email || typeof email !== "string") {
      return res.status(400).json({ ok: false, error: "email_required" });
    }

    const trimmedEmail = email.trim().toLowerCase();

    // Determine redirect URL (where the user lands after clicking the reset link)
    const redirectTo = (
      process.env.PASSWORD_RESET_REDIRECT_URL ||
      process.env.PUBLIC_WEB_BASE ||
      process.env.WEB_ORIGIN ||
      "http://localhost:3000"
    ) + "/auth/reset-confirm";

    const { error } = await getAnonSupa().auth.resetPasswordForEmail(trimmedEmail, { redirectTo });

    if (error) {
      // Don't leak whether the email exists — always return ok in production
      console.warn("[auth/reset-password] Supabase error:", error.message);
    }

    // Always return ok to avoid email enumeration
    return res.json({ ok: true, message: "If that email is registered, a reset link has been sent." });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "reset_failed" });
  }
});

// ── GET /v1/auth/me ───────────────────────────────────────────────────────────
// Verify caller identity and return their full profile + tier.
// Accepts: x-user-id header OR Authorization Bearer <JWT>
// NOTE: intentionally reads only explicit auth signals — does NOT fall back to
// (req as any).userId because that can be populated by the DEV_TEST_UID env
// variable, which would make unauthenticated requests appear authenticated.
authRouter.get("/me", async (req: Request, res: Response) => {
  try {
    const token  = getBearerToken(req);
    const userId = String(req.header("x-user-id") || "").trim();

    if (!userId && !token) {
      return res.status(401).json({ ok: false, error: "missing_identity" });
    }

    let verifiedUserId: string | null = null;
    let authEmail: string | null = null;

    // Prefer JWT verification when a token is present
    if (token) {
      const { data: { user }, error } = await getSupa().auth.getUser(token);
      if (error || !user) {
        return res.status(401).json({ ok: false, error: "invalid_token" });
      }
      verifiedUserId = user.id;
      authEmail      = user.email ?? null;
    } else {
      // x-user-id path — trusted via server.ts middleware chain
      if (!UUID_RE.test(userId)) {
        return res.status(401).json({ ok: false, error: "invalid_user_id" });
      }
      verifiedUserId = userId;
    }

    const rep = await fetchRepProfile(verifiedUserId);

    if (!rep) {
      return res.status(404).json({
        ok:     false,
        error:  "no_profile",
        userId: verifiedUserId,
        hint:   "Auth succeeded but no reps row found. Run seed or create user profile.",
      });
    }

    return res.json({
      ok: true,
      user: {
        id:         rep.id,
        email:      authEmail ?? rep.email ?? null,
        name:       rep.display_name ?? rep.name ?? null,
        tier:       rep.tier ?? null,
        company_id: rep.company_id ?? null,
        org_id:     rep.org_id ?? null,
        is_active:  rep.is_active ?? true,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "me_failed" });
  }
});

// ── POST /v1/auth/verify-role ─────────────────────────────────────────────────
// Debug / validation endpoint: confirm that a given user has the expected tier.
authRouter.post("/verify-role", async (req: Request, res: Response) => {
  try {
    const { userId, expectedTier } = (req.body || {}) as { userId?: string; expectedTier?: string };

    if (!userId || !UUID_RE.test(userId)) {
      return res.status(400).json({ ok: false, error: "userId_required" });
    }

    const rep = await fetchRepProfile(userId);
    if (!rep) {
      return res.status(404).json({ ok: false, error: "user_not_found", userId });
    }

    const tierMatch = !expectedTier || rep.tier === expectedTier;

    return res.json({
      ok:         tierMatch,
      userId:     rep.id,
      tier:       rep.tier,
      expected:   expectedTier ?? null,
      tier_match: tierMatch,
      is_active:  rep.is_active ?? true,
      company_id: rep.company_id ?? null,
      org_id:     rep.org_id ?? null,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "verify_role_failed" });
  }
});
