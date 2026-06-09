// src/routes/users.ts
// Self-service user profile endpoints.
// GET  /v1/users/me   — return own profile
// PATCH /v1/users/me  — update display_name, phone_number, timezone

import { Router } from "express";
import type { Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";
import { logAuditEvent, AUDIT_ACTIONS } from "../lib/audit";

export const usersRouter = Router();

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

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

function getUserId(req: Request): string | null {
  const id = String(
    (req as any).userId ||
    req.header("x-user-id") ||
    req.header("x-gravix-user-id") || ""
  ).trim();
  return id && UUID_RE.test(id) ? id : null;
}

// ── GET /v1/users/me ─────────────────────────────────────────────────────────
usersRouter.get("/me", async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    const { data: rep, error } = await getSupa()
      .from("reps")
      .select([
        "id", "name", "display_name", "first_name", "last_name",
        "email", "phone_number", "job_title", "department",
        "manager_id", "timezone", "is_active",
        "tier", "company_id", "org_id", "avatar_url",
        "xp", "created_at", "updated_at",
      ].join(", "))
      .eq("id", userId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    if (!rep)  return res.status(404).json({ ok: false, error: "user_not_found" });

    // Resolve company name
    let company_name: string | null = null;
    const cid = (rep as any).company_id;
    if (cid) {
      const { data: co } = await getSupa()
        .from("companies").select("name").eq("id", cid).maybeSingle();
      company_name = (co as any)?.name ?? null;
    }

    const r = rep as any;
    return res.json({
      ok: true,
      user: {
        id:           r.id,
        name:         r.name,
        display_name: r.display_name ?? r.name ?? null,
        first_name:   r.first_name   ?? null,
        last_name:    r.last_name    ?? null,
        email:        r.email        ?? null,
        phone:        r.phone_number ?? null,   // API surface uses "phone"
        job_title:    r.job_title    ?? null,
        department:   r.department   ?? null,
        manager_id:   r.manager_id   ?? null,
        timezone:     r.timezone     ?? "UTC",
        active:       r.is_active    ?? true,   // API surface uses "active"
        tier:         r.tier         ?? null,
        company_id:   r.company_id   ?? null,
        company_name,
        avatar_url:   r.avatar_url   ?? null,
        xp:           r.xp           ?? 0,
        created_at:   r.created_at,
        updated_at:   r.updated_at   ?? null,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "users_me_failed" });
  }
});

// ── PATCH /v1/users/me ───────────────────────────────────────────────────────
// Users may self-edit: display_name, first_name, last_name, phone, timezone, avatar_url.
// Tier, company, partner, email are NOT editable here.
usersRouter.patch("/me", async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "missing_user" });

    // Maps API field name → DB column name + max length
    const EDITABLE: Array<{ body: string; db: string; max?: number }> = [
      { body: "display_name", db: "display_name", max: 120 },
      { body: "first_name",   db: "first_name",   max: 80  },
      { body: "last_name",    db: "last_name",    max: 80  },
      { body: "phone",        db: "phone_number", max: 30  },
      { body: "timezone",     db: "timezone",     max: 60  },
      { body: "avatar_url",   db: "avatar_url",   max: 500 },
    ];

    const body = (req.body || {}) as Record<string, any>;

    // Fetch current values for audit diff
    const dbCols = EDITABLE.map(e => e.db).join(", ");
    const { data: current } = await getSupa()
      .from("reps")
      .select(dbCols)
      .eq("id", userId)
      .maybeSingle();

    const patch: Record<string, any> = { updated_at: new Date().toISOString() };
    const before: Record<string, any> = {};
    const after:  Record<string, any> = {};

    for (const { body: bodyField, db: dbField, max } of EDITABLE) {
      if (body[bodyField] === undefined) continue;
      const val = max
        ? String(body[bodyField]).trim().slice(0, max)
        : String(body[bodyField]).trim();
      before[bodyField] = (current as any)?.[dbField] ?? null;
      after[bodyField]  = val;
      patch[dbField]    = val;
    }

    if (Object.keys(patch).length === 1) {
      return res.status(400).json({ ok: false, error: "no_fields_to_update" });
    }

    const { error: patchErr } = await (getSupa()
      .from("reps") as any)
      .update(patch)
      .eq("id", userId);

    if (patchErr) return res.status(500).json({ ok: false, error: patchErr.message });

    await logAuditEvent({
      actorUserId:  userId,
      targetUserId: userId,
      action:       AUDIT_ACTIONS.UPDATE_USER,
      entityType:   "user",
      entityId:     userId,
      metadata:     { before, after, self_edit: true } as Record<string, unknown>,
    });

    return res.json({ ok: true, updated: Object.keys(after) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "users_me_patch_failed" });
  }
});
