import 'dotenv/config';
// src/routes/admin.ts
import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import { getRequesterOrgId } from "../lib/callAccess";
import { requireUserId } from "../middleware/requireUserId";
import { buildScoreSummaryBlocks } from "../lib/slackBlocks";
import { getAdminConfig, patchAdminConfig } from "../services/adminConfig";
import {
  auditUserCreated,
  writeAuditEvent,
} from "../lib/audit";
import { requirePartnerAdmin } from "../middleware/requirePartnerAdmin.ts";
import { requireSuperAdmin }   from "../middleware/requireSuperAdmin.ts";
import { getVisibleCompanies } from "../lib/partnerAccess.ts";
import { AUDIT_ACTIONS, logAuditEvent } from "../lib/audit.ts";
export const adminRouter = Router();

// --- Roles (lean RBAC v1) -------------------------------------
const ROLE_VALUES = [
  "rep",
  "office_manager",
  "company_manager",
] as const;

type Role = (typeof ROLE_VALUES)[number];

type PersonaConfigPayload = {
  buyer_style?: string | null;
  industry_preset?: string | null;
  objection_patterns?: string[];
  competitor_names?: string[];
  common_pushbacks?: string[];
  persona_memory?: string[];
  emotional_tuning?: {
    pressure_level?: number;
    trust_decay?: number;
    objection_aggression?: number;
  };
};

function isRole(x: any): x is Role {
  return typeof x === "string" && (ROLE_VALUES as readonly string[]).includes(x);
}

function normaliseStringArray(input: any): string[] {
  if (!Array.isArray(input)) {
    return [];
  }

  return input
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .slice(0, 100);
}

function isManagerRole(role: string | null | undefined) {
  return (
  role === "office_manager" ||
  role === "company_manager"
);
}

// --- Manager gate (MVP RBAC) ----------------------------------------------
// Uses x-user-id header and reps.tier to determine manager access.
// Later: replace with proper org_roles / auth-based RBAC.
async function requireManager(req: any, res: any, next: any) {
  try {
    const userId = String(req.header("x-user-id") || req.header("X-User-Id") || "").trim();
    if (!userId) return res.status(401).json({ ok: false, error: "missing_x_user_id" });

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });
    const { data: rep, error } = await supa
      .from("reps")
      .select("id,tier")
      .eq("id", userId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });

    const tier = String((rep as any)?.tier || "");
    const allowed = ["Manager", "Owner", "PartnerAdmin", "SuperAdmin"].includes(tier);
    if (!allowed) return res.status(403).json({ ok: false, error: "forbidden_not_manager" });

    return next();
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "require_manager_failed" });
  }
}

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

/* ----------------------------------------------------------------
   GET /v1/admin/org-settings
   Day 291 — readable by ANY authenticated tenant member (requireUserId), not
   just managers: a rep must be able to learn whether company calls are allowed so
   the WEB control matches backend enforcement. The org is resolved server-side
   from the caller's identity (getRequesterOrgId), never from the client, so a rep
   only ever sees their own org's policy. PATCH below stays manager-only.
----------------------------------------------------------------- */
adminRouter.get("/org-settings", requireUserId, async (req: any, res: any) => {
  try {
    const requester = String((req as any).userId || req.header("x-user-id") || "").trim();

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key);

    // Day 291 — resolve the requester's org via the canonical identity/tenant
    // bridge (call-owned org, then reps membership) rather than a call-owned-only
    // lookup. A fresh auth-first manager who has not yet recorded a call still
    // resolves their company through reps, so this policy read agrees with the
    // resolver /v1/calls/paged already uses — without leaning on a non-prod
    // org-id env fallback to mask the mismatch.
    const orgId = await getRequesterOrgId(requester);
    if (!orgId) return res.status(403).json({ ok: false, error: "no_org" });

    const { data, error } = await supa
      .from("org_settings")
      .select("call_visibility")
      .eq("org_id", orgId)
      .maybeSingle();

    if (error) throw error;

    return res.json({
      ok: true,
      settings: {
        call_visibility: data?.call_visibility || "everyone",
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "org_settings_failed" });
  }
});

/* ----------------------------------------------------------------
   PATCH /v1/admin/org-settings
----------------------------------------------------------------- */

adminRouter.patch("/org-settings", requireManager, async (req: any, res: any) => {
  try {
    const requester = String(req.header("x-user-id") || "").trim();
    const call_visibility = req.body?.call_visibility;

    if (!["everyone", "managers", "disabled"].includes(call_visibility)) {
      return res.status(400).json({ ok: false, error: "invalid_call_visibility" });
    }

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key);

    // Day 291 — same canonical org resolution as the GET above, so a manager who
    // can read the policy can also write it (both go through the reps bridge, not
    // a call-owned-only lookup).
    const orgId = await getRequesterOrgId(requester);
    if (!orgId) return res.status(403).json({ ok: false, error: "no_org" });

    const { data, error } = await supa
      .from("org_settings")
      .upsert(
        {
          org_id: orgId,
          call_visibility,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "org_id" }
      )
      .select()
      .single();

    if (error) throw error;

    return res.json({
      ok: true,
      settings: data,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "org_settings_update_failed" });
  }
});

/* ----------------------------------------------------------------
   POST /v1/admin/users
   Create a new user (rep/manager/admin)
----------------------------------------------------------------- */
adminRouter.post("/users", requireManager, async (req: any, res: any) => {
  try {
    const {
      email,
      role,
      manager_id,
      office_id,
    } = req.body;

    if (!email) {
      return res.status(400).json({ ok: false, error: "email_required" });
    }
    if (!office_id) {
      return res.status(400).json({ ok: false, error: "office_required" });
    }

    if (
      ![
        "rep",
        "office_manager",
        "company_manager",
      ].includes(role)
    ) {
      return res.status(400).json({
        ok: false,
        error: "invalid_role",
      });
    }
    if (role === "rep" && !manager_id) {
      return res.status(400).json({
        ok: false,
        error: "manager_required_for_rep",
      });
    }

    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!
    );

    const requester = req.header("x-user-id");

    const { data: orgRow } = await supa
      .from("calls")
      .select("org_id")
      .eq("user_id", requester)
      .limit(1)
      .maybeSingle();

    const orgId = orgRow?.org_id;
    if (!orgId) {
      return res.status(403).json({ ok: false, error: "no_org" });
    }

    // 🔥 USER LIMIT ENFORCEMENT (seat-based)
    const { count: userCount } = await supa
      .from("users")
      .select("*", { count: "exact", head: true })
      .eq("org_id", orgId);

    const { data: limitRow } = await supa
      .from("org_limits")
      .select("max_users")
      .eq("org_id", orgId)
      .maybeSingle();

    const maxUsers = limitRow?.max_users ?? 5; // default plan

    if ((userCount ?? 0) >= maxUsers) {
      return res.status(403).json({
        ok: false,
        error: "user_limit_reached",
        message: `User limit reached (${userCount}/${maxUsers}). Upgrade required.`,
      });
    }

    if (manager_id) {
      const { data: manager } = await supa
        .from("users")
        .select("id, role, office_id")
        .eq("id", manager_id)
        .maybeSingle();

      if (!manager) {
        return res.status(400).json({ ok: false, error: "invalid_manager_id" });
      }

      // reps cannot manage users
      if (manager.role === "rep") {
        return res.status(400).json({
          ok: false,
          error: "rep_cannot_be_manager",
        });
      }

      // office safety check
      if (
        role === "rep" &&
        manager.office_id &&
        office_id &&
        manager.office_id !== office_id
      ) {
        return res.status(400).json({
          ok: false,
          error: "manager_office_mismatch",
        });
      }
    }

    const { data, error } = await supa
      .from("users")
      .insert({
        email,
        role,
        manager_id: role === "rep" ? manager_id : null,
        office_id,
        org_id: orgId,
      })
      .select()
      .single();

    if (error) throw error;

    await writeAuditEvent(
      supa,
      auditUserCreated(
        String((req as any).authUserId || requester || ""),
        data.id,
        data.company_id || null,
        data.office_id || null
      )
    );

    return res.json({ ok: true, user: data });
  } catch (e: any) {
    console.error("[POST /users]", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/users
   Get all users for org (for dropdowns etc.)
----------------------------------------------------------------- */
adminRouter.get("/users", requireManager, async (req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!
    );

    const requester = req.header("x-user-id");

    const { data: orgRow } = await supa
      .from("calls")
      .select("org_id")
      .eq("user_id", requester)
      .limit(1)
      .maybeSingle();

    const orgId = orgRow?.org_id;

    const { data, error } = await supa
      .from("users")
      .select("*")
      .eq("org_id", orgId)
      .order("created_at", { ascending: false });

    if (error) throw error;

    return res.json({ ok: true, users: data });
  } catch (e: any) {
    console.error("[GET /users]", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

adminRouter.get("/status", async (_req, res) => {
  const out: any = { ok: true, checks: [] as any[] };

  function push(name: string, ok: boolean, detail?: any) {
    out.checks.push({ name, ok, detail });
    if (!ok) out.ok = false;
  }

  /* ----------------------------------------------------------------
     PATCH /v1/admin/org-settings
     Body: { call_visibility: 'everyone' | 'managers' | 'disabled' }
  ----------------------------------------------------------------- */
  adminRouter.patch("/org-settings", requireManager, async (req: any, res: any) => {
    try {
      const requester = String(req.header("x-user-id") || "").trim();
      const call_visibility = req.body?.call_visibility;

      if (!["everyone", "managers", "disabled"].includes(call_visibility)) {
        return res.status(400).json({ ok: false, error: "invalid_call_visibility" });
      }

      const url = process.env.SUPABASE_URL;
      const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
      if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

      const supa = createClient(url, key);

      // get org_id
      const { data: callRow } = await supa
        .from("calls")
        .select("org_id")
        .eq("user_id", requester)
        .not("org_id", "is", null)
        .limit(1)
        .maybeSingle();

      const orgId = callRow?.org_id;
      if (!orgId) return res.status(403).json({ ok: false, error: "no_org" });

      const { data, error } = await supa
        .from("org_settings")
        .upsert(
          {
            org_id: orgId,
            call_visibility,
          },
          { onConflict: "org_id" }
        )
        .select()
        .single();

      if (error) throw error;

      return res.json({
        ok: true,
        settings: data,
      });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || "org_settings_update_failed" });
    }
  });

  // --- ENV presence checks ---
  try {
    push("env:SUPABASE_URL", !!process.env.SUPABASE_URL);
    const hasServiceKey = !!(process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY);
    push("env:SUPABASE_SERVICE_KEY", hasServiceKey);
    push("env:SLACK_WEBHOOK_URL", !!process.env.SLACK_WEBHOOK_URL);
  } catch (e: any) {
    push("env:error", false, e?.message || String(e));
  }

  // --- Supabase Storage buckets ---
  try {
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (process.env.SUPABASE_URL && serviceKey) {
      const supa = createClient(process.env.SUPABASE_URL, serviceKey);
      const { data: buckets, error } = await supa.storage.listBuckets();
      push("supabase:buckets", !error, (buckets || []).map((b: any) => b.name));
    } else {
      push("supabase:buckets", false, "Missing SUPABASE envs");
    }
  } catch (e: any) {
    push("supabase:error", false, e?.message || String(e));
  }

  // --- Slack webhook smoke test (optional) ---
  try {
    const slack = process.env.SLACK_WEBHOOK_URL;
    if (slack) {
      // Use global fetch if available; ignore response body
      const r = await fetch(slack, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text: "Gravix status ping (admin/status)" }),
      } as any);
      push("slack:webhook_post", (r as any)?.ok === true || (r as any)?.status < 400, (r as any)?.status);
    } else {
      push("slack:webhook_post", false, "No SLACK_WEBHOOK_URL");
    }
  } catch (e: any) {
    push("slack:error", false, e?.message || String(e));
  }

  return res.json(out);
});

/* ----------------------------------------------------------------
   POST /v1/admin/test-slack  { text?: string }
   Sends a test message to the configured SLACK_WEBHOOK_URL.
----------------------------------------------------------------- */
adminRouter.post('/test-slack', async (req, res) => {
  try {
    const url = (process.env.SLACK_WEBHOOK_URL || '').trim();
    if (!url) return res.status(400).json({ ok: false, error: 'SLACK_WEBHOOK_URL not set' });

    const text = (req.body?.text as string | undefined) || 'Gravix test: webhook is live ✅';
    const payload = {
      text,
      blocks: [
        { type: 'section', text: { type: 'mrkdwn', text } },
        {
          type: 'context', elements: [
            { type: 'mrkdwn', text: `*Env:* ${process.env.NODE_ENV || 'dev'}  •  *API:* ${process.env.PUBLIC_API_BASE || 'local'}` }
          ]
        }
      ]
    };

    const r = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload)
    });

    return res.json({ ok: true, status: r.status });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'slack_failed' });
  }
});


/* ----------------------------------------------------------------
   GET /v1/admin/usage
----------------------------------------------------------------- */
adminRouter.get("/usage", requireManager, async (req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!
    );

    const requester = req.header("x-user-id");

    const { data: orgRow } = await supa
      .from("calls")
      .select("org_id")
      .eq("user_id", requester)
      .limit(1)
      .maybeSingle();

    const orgId = orgRow?.org_id;
    if (!orgId) {
      return res.status(403).json({ ok: false, error: "no_org" });
    }

    const { count } = await supa
      .from("users")
      .select("*", { count: "exact", head: true })
      .eq("org_id", orgId);

    const { data: limitRow } = await supa
      .from("org_limits")
      .select("max_users")
      .eq("org_id", orgId)
      .maybeSingle();

    const maxUsers = limitRow?.max_users ?? 5;

    return res.json({
      ok: true,
      usage: {
        used: count ?? 0,
        max: maxUsers,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

/* ----------------------------------------------------------------
   POST /v1/admin/send-slack?callId=...
   Fire-and-forget: posts a simple call summary to the Slack webhook.
----------------------------------------------------------------- */
adminRouter.post('/send-slack', async (req, res) => {
  try {
    const callId = (req.query.callId as string | undefined)?.trim();
    if (!callId) return res.status(400).json({ ok: false, error: 'missing_callId' });

    const webhook = (process.env.SLACK_WEBHOOK_URL || '').trim();
    if (!webhook) return res.status(400).json({ ok: false, error: 'no_webhook' });

    const text = `🎧 Send to Slack: Call *${callId}* was triggered manually`;
    const payload = {
      text,
      blocks: [
        { type: 'section', text: { type: 'mrkdwn', text } },
        {
          type: 'context',
          elements: [
            { type: 'mrkdwn', text: `Env: *${process.env.NODE_ENV || 'dev'}*` },
            { type: 'mrkdwn', text: `API: *${process.env.PUBLIC_API_BASE || 'local'}*` },
          ],
        },
      ],
    } as any;

    const r = await fetch(webhook, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });

    return res.json({ ok: r.ok, status: r.status });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'send_slack_failed' });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/preview-slack?callId=...
   Returns the JSON payload that would be sent to Slack (no posting).
----------------------------------------------------------------- */
adminRouter.get('/preview-slack', async (req, res) => {
  try {
    const callId = (req.query.callId as string | undefined)?.trim();
    if (!callId) return res.status(400).json({ ok: false, error: 'missing_callId' });

    const text = `🎧 Preview: Call *${callId}* would be posted to Slack`;
    const payload = {
      text,
      blocks: [
        { type: 'section', text: { type: 'mrkdwn', text } },
        {
          type: 'context',
          elements: [
            { type: 'mrkdwn', text: `Env: *${process.env.NODE_ENV || 'dev'}*` },
            { type: 'mrkdwn', text: `API: *${process.env.PUBLIC_API_BASE || 'local'}*` },
          ],
        },
      ],
    } as any;

    return res.json({ ok: true, payload });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || 'preview_failed' });
  }
});


adminRouter.post("/post-score-demo", async (_req, res) => {
  try {
    const slack = process.env.SLACK_WEBHOOK_URL;
    if (!slack) return res.status(400).json({ ok: false, error: "No SLACK_WEBHOOK_URL" });

    const payload = {
      blocks: buildScoreSummaryBlocks({
        callId: "demo-123",
        overall: 78,
        intro: 80,
        discovery: 74,
        pitch: 79,
        objection: 72,
        close: 77,
        vps: 81,
        rep: "Demo Rep",
      }),
    };

    const r = await fetch(slack, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    } as any);

    return res.json({ ok: r.ok, status: r.status });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

adminRouter.get('/whoami', (req, res) => {
  const uid = (req.header('x-user-id') || '').trim() || null;
  return res.json({ ok: true, userId: uid });
});

adminRouter.get('/whoami-org', (req, res) => {
  const headerOrg = (req.header('x-org-id') || '').trim() || null;
  const effectiveOrg = headerOrg || (process.env.DEFAULT_ORG_ID || null);
  return res.json({ ok: true, headerOrg, defaultOrgId: process.env.DEFAULT_ORG_ID || null, effectiveOrg });
});

/* ----------------------------------------------------------------
   GET /v1/admin/config
   Returns the single-row admin_config settings.
----------------------------------------------------------------- */
adminRouter.get("/config", requireManager, async (_req, res) => {
  try {
    const config = await getAdminConfig();

    return res.json({
      ok: true,
      config,
      scoring: {
        low: config?.low_score_threshold ?? null,
        critical: config?.critical_score_threshold ?? null,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "failed_to_load_config" });
  }
});

/* ----------------------------------------------------------------
   PATCH /v1/admin/config
   Body: {
     streak_threshold?: number,
     xp_multiplier?: number,
     comeback_bonus?: number,
     low_score_threshold?: number,
     critical_score_threshold?: number,
   }
----------------------------------------------------------------- */
adminRouter.patch("/config", requireManager, async (req, res) => {
  try {
    const {
      streak_threshold,
      xp_multiplier,
      comeback_bonus,
      low_score_threshold,
      critical_score_threshold,
    } = req.body ?? {};

    // light validation (MVP)
    if (streak_threshold !== undefined) {
      if (!Number.isInteger(streak_threshold) || streak_threshold < 1 || streak_threshold > 30) {
        return res.status(400).json({ ok: false, error: "streak_threshold must be an integer 1–30" });
      }
    }

    if (xp_multiplier !== undefined) {
      if (typeof xp_multiplier !== "number" || xp_multiplier < 0.1 || xp_multiplier > 10) {
        return res.status(400).json({ ok: false, error: "xp_multiplier must be a number 0.1–10" });
      }
    }

    if (comeback_bonus !== undefined) {
      if (!Number.isInteger(comeback_bonus) || comeback_bonus < 0 || comeback_bonus > 5000) {
        return res.status(400).json({ ok: false, error: "comeback_bonus must be an integer 0–5000" });
      }
    }

    if (low_score_threshold !== undefined) {
      if (!Number.isInteger(low_score_threshold) || low_score_threshold < 0 || low_score_threshold > 100) {
        return res.status(400).json({ ok: false, error: "low_score_threshold must be an integer 0–100" });
      }
    }

    if (critical_score_threshold !== undefined) {
      if (!Number.isInteger(critical_score_threshold) || critical_score_threshold < 0 || critical_score_threshold > 100) {
        return res.status(400).json({ ok: false, error: "critical_score_threshold must be an integer 0–100" });
      }
    }

    // Day 297 — enforce critical <= low against the EFFECTIVE pair. When only one
    // threshold is supplied, validate it against the stored companion so a
    // single-field patch cannot break the invariant. Invalid ordering → clean 400
    // with NO mutation (this check runs before patchAdminConfig).
    if (low_score_threshold !== undefined || critical_score_threshold !== undefined) {
      let effLow = low_score_threshold;
      let effCritical = critical_score_threshold;
      if (effLow === undefined || effCritical === undefined) {
        const current = await getAdminConfig();
        if (effLow === undefined) effLow = current.low_score_threshold;
        if (effCritical === undefined) effCritical = current.critical_score_threshold;
      }
      if (Number(effCritical) > Number(effLow)) {
        return res.status(400).json({
          ok: false,
          error: "critical_score_threshold must be less than or equal to low_score_threshold",
        });
      }
    }

    // no-op patch protection (optional but nice)
    if (
      streak_threshold === undefined &&
      xp_multiplier === undefined &&
      comeback_bonus === undefined &&
      low_score_threshold === undefined &&
      critical_score_threshold === undefined
    ) {
      return res.status(400).json({ ok: false, error: "no_fields_to_update" });
    }

    const updated = await patchAdminConfig({
      streak_threshold,
      xp_multiplier,
      comeback_bonus,
      low_score_threshold,
      critical_score_threshold,
    } as any);
    return res.json({
      ok: true,
      config: updated,
      scoring: {
        low: updated?.low_score_threshold ?? null,
        critical: updated?.critical_score_threshold ?? null,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "failed_to_update_config" });
  }
});


/* ----------------------------------------------------------------
   GET /v1/admin/reps
   Manager-only list of reps for RBAC + operations
----------------------------------------------------------------- */
adminRouter.get("/reps", requireManager, async (_req: any, res: any) => {
  try {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });
    const { data, error } = await supa
      .from("reps")
      .select("id,name,xp,tier,created_at")
      .order("created_at", { ascending: false })
      .limit(500);

    if (error) return res.status(500).json({ ok: false, error: error.message });
    return res.json({ ok: true, reps: data ?? [] });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_reps_list_failed" });
  }
});

/* ----------------------------------------------------------------
   PATCH /v1/admin/reps/:id
   Body: { tier: 'SalesRep' | 'TeamLead' | 'Manager' | 'Owner' }
----------------------------------------------------------------- */
adminRouter.patch("/reps/:id", requireManager, async (req: any, res: any) => {
  try {
    const targetId = String(req.params.id || "").trim();
    const tier = req.body?.tier as any;

    if (!targetId) return res.status(400).json({ ok: false, error: "missing_rep_id" });
    if (!isRole(tier)) return res.status(400).json({ ok: false, error: "invalid_tier" });

    const requesterId = String(req.header("x-user-id") || "").trim();

    // Guardrail: prevent demoting the last Manager/Owner
    if (requesterId && requesterId === targetId && !isManagerRole(tier)) {
      const url = process.env.SUPABASE_URL;
      const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
      if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

      const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });
      const { data: managers, error: mgrErr } = await supa
        .from("reps")
        .select("id,tier")
        .in("tier", ["Manager", "Owner"]);

      if (mgrErr) return res.status(500).json({ ok: false, error: mgrErr.message });
      if ((managers ?? []).length <= 1) {
        return res.status(400).json({ ok: false, error: "cannot_demote_last_manager" });
      }
    }

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key, { auth: { autoRefreshToken: false, persistSession: false } });
    const { data: updated, error } = await supa
      .from("reps")
      .update({ tier })
      .eq("id", targetId)
      .select("id,name,xp,tier,created_at")
      .single();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    if (!updated) return res.status(404).json({ ok: false, error: "rep_not_found" });

    return res.json({ ok: true, rep: updated });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_rep_patch_failed" });
  }
});

export default adminRouter;

/* ----------------------------------------------------------------
   GET /v1/admin/persona-config
----------------------------------------------------------------- */
adminRouter.get(
  "/persona-config",
  requireManager,
  async (req: any, res: any) => {
    try {
      const requester = String(req.header("x-user-id") || "").trim();

      const supa = createClient(
        process.env.SUPABASE_URL!,
        process.env.SUPABASE_SERVICE_ROLE_KEY!
      );

      const { data: requesterUser } = await supa
        .from("users")
        .select("company_id")
        .eq("id", requester)
        .maybeSingle();

      const companyId = requesterUser?.company_id;

      if (!companyId) {
        return res.status(403).json({
          ok: false,
          error: "missing_company_context",
        });
      }

      // The column is settings_json (companies has no `settings`). Aliased so
      // the reads below are unchanged. Previously this 42703'd and the
      // `if (error || !company)` guard turned it into a misleading 404
      // company_not_found for a company that exists.
      const { data: company, error } = await supa
        .from("companies")
        .select("id, name, settings:settings_json")
        .eq("id", companyId)
        .single();

      if (error || !company) {
        return res.status(404).json({
          ok: false,
          error: "company_not_found",
        });
      }

      const settings =
        company.settings && typeof company.settings === "object"
          ? company.settings
          : {};

      return res.json({
        ok: true,
        company_id: company.id,
        company_name: company.name,
        config: {
          buyer_style: settings.buyer_style || null,
          industry_preset: settings.industry_preset || null,
          objection_patterns: Array.isArray(settings.objection_patterns)
            ? settings.objection_patterns
            : [],
          competitor_names: Array.isArray(settings.competitor_names)
            ? settings.competitor_names
            : [],
          common_pushbacks: Array.isArray(settings.common_pushbacks)
            ? settings.common_pushbacks
            : [],
          persona_memory: Array.isArray(settings.persona_memory)
            ? settings.persona_memory
            : [],
          emotional_tuning:
            settings.emotional_tuning &&
            typeof settings.emotional_tuning === "object"
              ? settings.emotional_tuning
              : {
                  pressure_level: 50,
                  trust_decay: 50,
                  objection_aggression: 50,
                },
        },
      });
    } catch (e: any) {
      console.error("[persona-config:get]", e);

      return res.status(500).json({
        ok: false,
        error: e?.message || "persona_config_load_failed",
      });
    }
  }
);

/* ----------------------------------------------------------------
   PATCH /v1/admin/persona-config
----------------------------------------------------------------- */
adminRouter.patch(
  "/persona-config",
  requireManager,
  async (req: any, res: any) => {
    try {
      const requester = String(req.header("x-user-id") || "").trim();

      const body = (req.body || {}) as PersonaConfigPayload;

      const supa = createClient(
        process.env.SUPABASE_URL!,
        process.env.SUPABASE_SERVICE_ROLE_KEY!
      );

      const { data: requesterUser } = await supa
        .from("users")
        .select("company_id")
        .eq("id", requester)
        .maybeSingle();

      const companyId = requesterUser?.company_id;

      if (!companyId) {
        return res.status(403).json({
          ok: false,
          error: "missing_company_context",
        });
      }

      const { data: existingCompany } = await supa
        .from("companies")
        .select("settings:settings_json")
        .eq("id", companyId)
        .maybeSingle();

      const existingSettings =
        existingCompany?.settings &&
        typeof existingCompany.settings === "object"
          ? existingCompany.settings
          : {};

      const mergedSettings = {
        ...existingSettings,

        buyer_style:
          typeof body.buyer_style === "string"
            ? body.buyer_style.trim()
            : existingSettings.buyer_style || null,

        industry_preset:
          typeof body.industry_preset === "string"
            ? body.industry_preset.trim()
            : existingSettings.industry_preset || null,

        objection_patterns: normaliseStringArray(
          body.objection_patterns
        ),

        competitor_names: normaliseStringArray(
          body.competitor_names
        ),

        common_pushbacks: normaliseStringArray(
          body.common_pushbacks
        ),

        persona_memory: normaliseStringArray(
          body.persona_memory
        ),

        emotional_tuning: {
          pressure_level: Number(
            body.emotional_tuning?.pressure_level ?? 50
          ),
          trust_decay: Number(
            body.emotional_tuning?.trust_decay ?? 50
          ),
          objection_aggression: Number(
            body.emotional_tuning?.objection_aggression ?? 50
          ),
        },
      };

      const { data: updatedCompany, error } = await supa
        .from("companies")
        .update({
          // No aliasing on writes — this must be the real column name.
          settings_json: mergedSettings,
        })
        .eq("id", companyId)
        .select("id, name, settings:settings_json")
        .single();

      if (error || !updatedCompany) {
        return res.status(500).json({
          ok: false,
          error: error?.message || "persona_config_update_failed",
        });
      }

      await writeAuditEvent(
        supa,
        {
          actor_user_id: requester,
          event_type: "persona_config_updated",
          entity_type: "company",
          entity_id: companyId,
          company_id: companyId,
          office_id: null,
          metadata: {
            buyer_style: mergedSettings.buyer_style,
            industry_preset: mergedSettings.industry_preset,
            objection_count:
              mergedSettings.objection_patterns.length,
            memory_count:
              mergedSettings.persona_memory.length,
          },
        } as any
      );

      return res.json({
        ok: true,
        company: updatedCompany,
        config: updatedCompany.settings,
      });
    } catch (e: any) {
      console.error("[persona-config:patch]", e);

      return res.status(500).json({
        ok: false,
        error: e?.message || "persona_config_patch_failed",
      });
    }
  }
);

/* ----------------------------------------------------------------
   GET /v1/admin/partner/health
   Requires: PartnerAdmin or SuperAdmin
   Returns the caller's visible companies and confirms partner scope.
----------------------------------------------------------------- */
adminRouter.get("/partner/health", requirePartnerAdmin, async (req: any, res: any) => {
  try {
    const userId = String(req.header("x-user-id") || "").trim();
    const companies = await getVisibleCompanies(userId);

    return res.json({
      ok:        true,
      scope:     "partner",
      tier:      req.repTier ?? null,
      companies: companies.map((c) => ({ id: c.id, name: c.name })),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "partner_health_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/super/health
   Requires: SuperAdmin only
   Returns all companies across all partners.
----------------------------------------------------------------- */
adminRouter.get("/super/health", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const userId = String(req.header("x-user-id") || "").trim();
    const companies = await getVisibleCompanies(userId);

    return res.json({
      ok:        true,
      scope:     "super",
      tier:      req.repTier ?? null,
      companies: companies.map((c) => ({ id: c.id, name: c.name, partner_id: c.partner_id })),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "super_health_failed" });
  }
});
/* ----------------------------------------------------------------
   GET /v1/admin/super/partners
   Requires: SuperAdmin only
   Returns all partners with company counts.
----------------------------------------------------------------- */
adminRouter.get("/super/partners", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    // Fetch all partners
    const { data: partners, error: pErr } = await supa
      .from("partners")
      .select("id, name, status, created_at")
      .order("name");

    if (pErr) throw pErr;

    // Count companies per partner in one query
    const { data: companyCounts, error: cErr } = await supa
      .from("companies")
      .select("partner_id");

    if (cErr) throw cErr;

    const countMap = new Map<string, number>();
    for (const row of (companyCounts || [])) {
      const pid = String((row as any).partner_id ?? "");
      if (pid) countMap.set(pid, (countMap.get(pid) ?? 0) + 1);
    }

    const rows = (partners || []).map((p: any) => ({
      id:            p.id,
      name:          p.name,
      status:        p.status ?? "active",
      company_count: countMap.get(p.id) ?? 0,
      created_at:    p.created_at,
    }));

    return res.json({ ok: true, partners: rows });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "super_partners_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/partner/companies
   Requires: PartnerAdmin or SuperAdmin
   Returns scoped companies with rep counts.
----------------------------------------------------------------- */
adminRouter.get("/partner/companies", requirePartnerAdmin, async (req: any, res: any) => {
  try {
    const userId = String(req.header("x-user-id") || "").trim();
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const companies = await getVisibleCompanies(userId);
    if (!companies.length) return res.json({ ok: true, companies: [] });

    const companyIds = companies.map((c) => c.id);

    // Fetch partner names in one query
    const partnerIds = [...new Set(companies.map((c) => c.partner_id).filter(Boolean))] as string[];
    const { data: partnerRows } = partnerIds.length
      ? await supa.from("partners").select("id, name").in("id", partnerIds)
      : { data: [] };
    const partnerMap = new Map<string, string>(
      (partnerRows || []).map((p: any) => [p.id, p.name])
    );

    // Count reps per company
    const { data: repRows } = await supa
      .from("reps")
      .select("company_id")
      .in("company_id", companyIds);
    const repCountMap = new Map<string, number>();
    for (const row of (repRows || [])) {
      const cid = String((row as any).company_id ?? "");
      if (cid) repCountMap.set(cid, (repCountMap.get(cid) ?? 0) + 1);
    }

    const rows = companies.map((c) => ({
      id:           c.id,
      name:         c.name,
      partner_id:   c.partner_id,
      partner_name: c.partner_id ? (partnerMap.get(c.partner_id) ?? null) : null,
      rep_count:    repCountMap.get(c.id) ?? 0,
    }));

    return res.json({ ok: true, companies: rows });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "partner_companies_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/partner/users
   Requires: PartnerAdmin or SuperAdmin
   Returns reps for all companies visible to the caller.
----------------------------------------------------------------- */
adminRouter.get("/partner/users", requirePartnerAdmin, async (req: any, res: any) => {
  try {
    const userId = String(req.header("x-user-id") || "").trim();
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const companies = await getVisibleCompanies(userId);
    if (!companies.length) return res.json({ ok: true, users: [] });

    const companyIds = companies.map((c) => c.id);
    const companyMap = new Map<string, string>(companies.map((c) => [c.id, c.name]));

    // Fetch partner names
    const partnerIds = [...new Set(companies.map((c) => c.partner_id).filter(Boolean))] as string[];
    const { data: partnerRows } = partnerIds.length
      ? await supa.from("partners").select("id, name").in("id", partnerIds)
      : { data: [] };
    const partnerMap = new Map<string, string>(
      (partnerRows || []).map((p: any) => [p.id, p.name])
    );
    const companyPartnerMap = new Map<string, string>(
      companies.map((c) => [c.id, c.partner_id ? (partnerMap.get(c.partner_id) ?? "") : ""])
    );

    const { data: reps, error: rErr } = await supa
      .from("reps")
      .select("id, name, tier, company_id, created_at")
      .in("company_id", companyIds)
      .order("name");

    if (rErr) throw rErr;

    const rows = (reps || []).map((r: any) => ({
      id:           r.id,
      name:         r.name,
      tier:         r.tier,
      company_id:   r.company_id,
      company_name: r.company_id ? (companyMap.get(r.company_id) ?? null) : null,
      partner_name: r.company_id ? (companyPartnerMap.get(r.company_id) ?? null) : null,
      created_at:   r.created_at,
    }));

    return res.json({ ok: true, users: rows });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "partner_users_failed" });
  }
});

/* ----------------------------------------------------------------
   POST /v1/admin/super/impersonate
   Body: { targetUserId: string }
   Requires: SuperAdmin
   Returns: { ok, targetUserId, targetName, impersonationToken }
   The caller stores impersonationToken and sends it as
   x-impersonated-user-id on subsequent requests.
----------------------------------------------------------------- */
adminRouter.post("/super/impersonate", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const actorId    = String(req.header("x-user-id") || "").trim();
    const targetId   = String(req.body?.targetUserId || "").trim();

    if (!targetId) {
      return res.status(400).json({ ok: false, error: "targetUserId_required" });
    }

    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const { data: target } = await supa
      .from("reps")
      .select("id, name, tier")
      .eq("id", targetId)
      .maybeSingle();

    if (!target) {
      return res.status(404).json({ ok: false, error: "target_user_not_found" });
    }

    // Audit — fire-and-forget
    void logAuditEvent({
      actorUserId:   actorId,
      targetUserId:  targetId,
      action:        AUDIT_ACTIONS.IMPERSONATE_USER,
      entityType:    "user",
      entityId:      targetId,
      metadata:      { targetName: (target as any).name, targetTier: (target as any).tier },
    });

    return res.json({
      ok:                 true,
      targetUserId:       targetId,
      targetName:         (target as any).name ?? null,
      targetTier:         (target as any).tier ?? null,
      impersonationToken: targetId, // MVP: token IS the targetUserId; server validates actor tier on each request
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "impersonate_failed" });
  }
});

/* ----------------------------------------------------------------
   POST /v1/admin/super/stop-impersonation
   Requires: SuperAdmin
   Logs the end of an impersonation session. Client clears local state.
----------------------------------------------------------------- */
adminRouter.post("/super/stop-impersonation", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const actorId  = String(req.header("x-user-id") || "").trim();
    const targetId = String(req.body?.targetUserId || "").trim() || null;

    void logAuditEvent({
      actorUserId:  actorId,
      targetUserId: targetId,
      action:       AUDIT_ACTIONS.END_IMPERSONATION,
      entityType:   "user",
      entityId:     targetId ?? undefined,
    });

    return res.json({ ok: true });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "stop_impersonation_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/super/audit
   Requires: SuperAdmin
   Returns audit_events newest-first with cursor pagination.
   Query params: ?limit=50&cursor=<created_at ISO string>
----------------------------------------------------------------- */
/* ================================================================
   USER MANAGEMENT  — GET /v1/admin/users/:id
                    — PATCH /v1/admin/users/:id

   Scope matrix:
     Manager      → target must share same company_id
     PartnerAdmin → target must be in partner's companies
     SuperAdmin   → any user

   PATCH editable fields: phone, job_title, department, manager_id,
                          active, tier (not email, not company)
================================================================ */

async function assertUserEditScope(
  actorId: string,
  targetId: string,
  supa: ReturnType<typeof createClient>
): Promise<{ ok: true; actorTier: string } | { ok: false; status: number; error: string }> {
  const { data: actor } = await supa.from("reps").select("tier, company_id").eq("id", actorId).maybeSingle();
  if (!actor) return { ok: false, status: 401, error: "actor_not_found" };

  const actorTier = String((actor as any).tier ?? "");
  const actorCo   = String((actor as any).company_id ?? "");

  if (actorTier === "SuperAdmin") return { ok: true, actorTier };

  const { data: target } = await supa.from("reps").select("company_id").eq("id", targetId).maybeSingle();
  if (!target) return { ok: false, status: 404, error: "user_not_found" };

  const targetCo = String((target as any).company_id ?? "");

  if (actorTier === "Manager") {
    if (!actorCo || actorCo !== targetCo) return { ok: false, status: 403, error: "forbidden_scope" };
    return { ok: true, actorTier };
  }

  if (actorTier === "PartnerAdmin") {
    // Resolve partner from actor's company
    const { data: actorCompany } = await supa.from("companies").select("partner_id").eq("id", actorCo).maybeSingle();
    const partnerId = (actorCompany as any)?.partner_id ?? null;
    if (!partnerId) return { ok: false, status: 403, error: "forbidden_scope" };

    const { data: targetCompany } = await supa.from("companies").select("partner_id").eq("id", targetCo).maybeSingle();
    if ((targetCompany as any)?.partner_id !== partnerId) return { ok: false, status: 403, error: "forbidden_scope" };
    return { ok: true, actorTier };
  }

  return { ok: false, status: 403, error: "forbidden_scope" };
}

adminRouter.get("/users/:id", async (req: any, res: any) => {
  try {
    const actorId  = String(req.header("x-user-id") || "").trim();
    const targetId = String(req.params.id || "").trim();
    if (!actorId)  return res.status(401).json({ ok: false, error: "missing_user" });
    if (!targetId) return res.status(400).json({ ok: false, error: "missing_id" });

    const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

    const scope = await assertUserEditScope(actorId, targetId, supa);
    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });

    const { data: rep, error } = await supa
      .from("reps")
      .select("id, name, display_name, first_name, last_name, email, phone_number, job_title, department, manager_id, timezone, is_active, tier, company_id, org_id, avatar_url, xp, created_at, updated_at")
      .eq("id", targetId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    if (!rep)  return res.status(404).json({ ok: false, error: "user_not_found" });

    // Resolve company name + manager name
    const r = rep as any;
    let company_name: string | null = null;
    let manager_name: string | null = null;

    if (r.company_id) {
      const { data: co } = await supa.from("companies").select("name").eq("id", r.company_id).maybeSingle();
      company_name = (co as any)?.name ?? null;
    }
    if (r.manager_id) {
      const { data: mgr } = await supa.from("reps").select("name, display_name").eq("id", r.manager_id).maybeSingle();
      manager_name = (mgr as any)?.display_name ?? (mgr as any)?.name ?? null;
    }

    // Possible managers: reps in same company (for the dropdown)
    const { data: peers } = await supa
      .from("reps")
      .select("id, name, display_name")
      .eq("company_id", r.company_id)
      .neq("id", targetId)
      .order("name");

    return res.json({
      ok: true,
      user: {
        id: r.id, name: r.name, display_name: r.display_name ?? r.name,
        first_name: r.first_name, last_name: r.last_name, email: r.email,
        phone: r.phone_number, job_title: r.job_title, department: r.department,
        manager_id: r.manager_id, manager_name,
        timezone: r.timezone ?? "UTC", active: r.is_active ?? true,
        tier: r.tier, company_id: r.company_id, company_name,
        avatar_url: r.avatar_url, xp: r.xp ?? 0,
        created_at: r.created_at, updated_at: r.updated_at,
      },
      possible_managers: (peers || []).map((p: any) => ({
        id: p.id,
        name: p.display_name ?? p.name,
      })),
      actor_tier: scope.actorTier,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_user_get_failed" });
  }
});

adminRouter.patch("/users/:id", async (req: any, res: any) => {
  try {
    const actorId  = String(req.header("x-user-id") || "").trim();
    const targetId = String(req.params.id || "").trim();
    if (!actorId)  return res.status(401).json({ ok: false, error: "missing_user" });
    if (!targetId) return res.status(400).json({ ok: false, error: "missing_id" });

    const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

    const scope = await assertUserEditScope(actorId, targetId, supa);
    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });

    const body = (req.body || {}) as Record<string, any>;

    // Maps API request field name → DB column name
    const EDITABLE: Array<{ body: string; db: string }> = [
      { body: "display_name", db: "display_name" },
      { body: "first_name",   db: "first_name"   },
      { body: "last_name",    db: "last_name"    },
      { body: "phone",        db: "phone_number" },
      { body: "job_title",    db: "job_title"    },
      { body: "department",   db: "department"   },
      { body: "manager_id",   db: "manager_id"   },
      { body: "timezone",     db: "timezone"     },
      { body: "active",       db: "is_active"    },
      { body: "avatar_url",   db: "avatar_url"   },
      { body: "tier",         db: "tier"         },
    ];

    // Tier guard: only SuperAdmin can change tier
    if (body.tier !== undefined && scope.actorTier !== "SuperAdmin") {
      return res.status(403).json({ ok: false, error: "only_superadmin_can_change_tier" });
    }

    // Fetch current values for audit diff using DB column names
    const dbCols = EDITABLE.map(e => e.db).join(", ");
    const { data: current } = await supa
      .from("reps")
      .select(dbCols)
      .eq("id", targetId)
      .maybeSingle();

    const patch: Record<string, any> = { updated_at: new Date().toISOString() };
    const before: Record<string, any> = {};
    const after:  Record<string, any> = {};

    for (const { body: bodyField, db: dbField } of EDITABLE) {
      if (body[bodyField] === undefined) continue;
      let val: any = body[bodyField];
      if (dbField === "is_active") val = Boolean(val);
      if (typeof val === "string") val = val.trim();
      before[bodyField] = (current as any)?.[dbField] ?? null;
      after[bodyField]  = val;
      patch[dbField]    = val;   // ← write to correct DB column
    }

    if (Object.keys(patch).length === 1) {
      return res.status(400).json({ ok: false, error: "no_fields_to_update" });
    }

    const { error: patchErr } = await supa.from("reps").update(patch).eq("id", targetId);
    if (patchErr) return res.status(500).json({ ok: false, error: patchErr.message });

    await logAuditEvent({
      actorUserId:  actorId,
      targetUserId: targetId,
      action:       AUDIT_ACTIONS.UPDATE_USER,
      entityType:   "user",
      entityId:     targetId,
      metadata:     { before, after, actor_tier: scope.actorTier },
    });

    return res.json({ ok: true, updated: Object.keys(after) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_user_patch_failed" });
  }
});

/* ================================================================
   COMPANY MANAGEMENT — GET /v1/admin/companies/:id
                      — PATCH /v1/admin/companies/:id

   Scope: Manager → own company; PartnerAdmin → partner companies;
          SuperAdmin → all.
================================================================ */

async function assertCompanyEditScope(
  actorId: string,
  targetCompanyId: string,
  supa: ReturnType<typeof createClient>
): Promise<{ ok: true; actorTier: string } | { ok: false; status: number; error: string }> {
  const { data: actor } = await supa.from("reps").select("tier, company_id").eq("id", actorId).maybeSingle();
  if (!actor) return { ok: false, status: 401, error: "actor_not_found" };

  const actorTier = String((actor as any).tier ?? "");
  const actorCo   = String((actor as any).company_id ?? "");

  if (actorTier === "SuperAdmin") return { ok: true, actorTier };

  if (actorTier === "Manager") {
    if (actorCo !== targetCompanyId) return { ok: false, status: 403, error: "forbidden_scope" };
    return { ok: true, actorTier };
  }

  if (actorTier === "PartnerAdmin") {
    const { data: actorCompany } = await supa.from("companies").select("partner_id").eq("id", actorCo).maybeSingle();
    const partnerId = (actorCompany as any)?.partner_id ?? null;
    if (!partnerId) return { ok: false, status: 403, error: "forbidden_scope" };

    const { data: target } = await supa.from("companies").select("partner_id").eq("id", targetCompanyId).maybeSingle();
    if ((target as any)?.partner_id !== partnerId) return { ok: false, status: 403, error: "forbidden_scope" };
    return { ok: true, actorTier };
  }

  return { ok: false, status: 403, error: "forbidden_scope" };
}

adminRouter.get("/companies/:id", async (req: any, res: any) => {
  try {
    const actorId    = String(req.header("x-user-id") || "").trim();
    const companyId  = String(req.params.id || "").trim();
    if (!actorId)   return res.status(401).json({ ok: false, error: "missing_user" });
    if (!companyId) return res.status(400).json({ ok: false, error: "missing_id" });

    const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

    const scope = await assertCompanyEditScope(actorId, companyId, supa);
    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });

    const { data: co, error } = await supa
      .from("companies")
      .select("id, name, slug, website, industry, phone_number, address, is_active, partner_id, tmc_id, settings_json, created_at, updated_at")
      .eq("id", companyId)
      .maybeSingle();

    if (error) return res.status(500).json({ ok: false, error: error.message });
    if (!co)   return res.status(404).json({ ok: false, error: "company_not_found" });

    // Resolve partner name
    const c = co as any;
    let partner_name: string | null = null;
    if (c.partner_id) {
      const { data: p } = await supa.from("partners").select("name").eq("id", c.partner_id).maybeSingle();
      partner_name = (p as any)?.name ?? null;
    }

    // User count
    const { count: user_count } = await supa.from("reps").select("id", { count: "exact", head: true }).eq("company_id", companyId);

    return res.json({
      ok: true,
      company: {
        id: c.id, name: c.name, slug: c.slug,
        website: c.website, industry: c.industry, phone: c.phone_number, address: c.address,
        active: c.is_active ?? true,
        partner_id: c.partner_id, partner_name,
        created_at: c.created_at, updated_at: c.updated_at,
      },
      user_count: user_count ?? 0,
      actor_tier: scope.actorTier,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_company_get_failed" });
  }
});

adminRouter.patch("/companies/:id", async (req: any, res: any) => {
  try {
    const actorId   = String(req.header("x-user-id") || "").trim();
    const companyId = String(req.params.id || "").trim();
    if (!actorId)   return res.status(401).json({ ok: false, error: "missing_user" });
    if (!companyId) return res.status(400).json({ ok: false, error: "missing_id" });

    const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, { auth: { persistSession: false } });

    const scope = await assertCompanyEditScope(actorId, companyId, supa);
    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });

    const body = (req.body || {}) as Record<string, any>;

    // Maps API request field name → DB column name
    const EDITABLE: Array<{ body: string; db: string }> = [
      { body: "name",     db: "name"         },
      { body: "website",  db: "website"      },
      { body: "industry", db: "industry"     },
      { body: "phone",    db: "phone_number" },
      { body: "address",  db: "address"      },
      { body: "active",   db: "is_active"    },
    ];

    const dbCols = EDITABLE.map(e => e.db).join(", ");
    const { data: current } = await supa
      .from("companies")
      .select(dbCols)
      .eq("id", companyId)
      .maybeSingle();

    const patch: Record<string, any> = { updated_at: new Date().toISOString() };
    const before: Record<string, any> = {};
    const after:  Record<string, any> = {};

    for (const { body: bodyField, db: dbField } of EDITABLE) {
      if (body[bodyField] === undefined) continue;
      let val: any = body[bodyField];
      if (dbField === "is_active") val = Boolean(val);
      if (typeof val === "string") val = val.trim();
      before[bodyField] = (current as any)?.[dbField] ?? null;
      after[bodyField]  = val;
      patch[dbField]    = val;   // ← write to correct DB column
    }

    if (Object.keys(patch).length === 1) {
      return res.status(400).json({ ok: false, error: "no_fields_to_update" });
    }

    const { error: patchErr } = await supa.from("companies").update(patch).eq("id", companyId);
    if (patchErr) return res.status(500).json({ ok: false, error: patchErr.message });

    await logAuditEvent({
      actorUserId: actorId,
      action:      AUDIT_ACTIONS.UPDATE_COMPANY,
      entityType:  "company",
      entityId:    companyId,
      metadata:    { before, after, actor_tier: scope.actorTier },
    });

    return res.json({ ok: true, updated: Object.keys(after) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "admin_company_patch_failed" });
  }
});

/* ================================================================
   ORGANISATIONS — GET /v1/admin/organisations
   SuperAdmin only. Read-only listing of all organisations (orgs).
   Aggregated from reps.org_id; enriched with company and user counts.
================================================================ */

adminRouter.get("/organisations", requireSuperAdmin, async (_req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    // Aggregate org_id groupings from reps (source of truth for org membership)
    const { data: repRows, error: rErr } = await supa
      .from("reps")
      .select("org_id, company_id, created_at");

    if (rErr) throw rErr;

    // Build per-org stats
    const orgMap = new Map<string, { user_count: number; company_ids: Set<string>; earliest: string }>();
    for (const r of (repRows || []) as any[]) {
      const oid = String(r.org_id ?? "");
      if (!oid) continue;
      const entry = orgMap.get(oid) ?? { user_count: 0, company_ids: new Set(), earliest: r.created_at ?? "" };
      entry.user_count += 1;
      if (r.company_id) entry.company_ids.add(String(r.company_id));
      if (r.created_at && r.created_at < entry.earliest) entry.earliest = r.created_at;
      orgMap.set(oid, entry);
    }

    // Try to look up org names from a possible organisations/orgs table — ignore if absent
    const orgIds = Array.from(orgMap.keys());
    const nameMap = new Map<string, string>();
    try {
      const { data: orgs } = await (supa as any).from("organisations").select("id, name").in("id", orgIds);
      for (const o of (orgs || []) as any[]) nameMap.set(String(o.id), String(o.name ?? o.id));
    } catch { /* table may not exist */ }

    // Fetch partner info via companies
    const allCompanyIds = Array.from(new Set(
      (repRows || []).flatMap((r: any) => r.company_id ? [String(r.company_id)] : [])
    ));

    const partnerByCompany = new Map<string, { partner_id: string; partner_name: string }>();
    if (allCompanyIds.length) {
      const { data: coRows } = await supa
        .from("companies")
        .select("id, partner_id")
        .in("id", allCompanyIds);

      const partnerIds = [...new Set((coRows || []).map((c: any) => c.partner_id).filter(Boolean))] as string[];
      const partnerNames = new Map<string, string>();
      if (partnerIds.length) {
        const { data: pRows } = await supa.from("partners").select("id, name").in("id", partnerIds);
        for (const p of (pRows || []) as any[]) partnerNames.set(p.id, p.name);
      }

      for (const co of (coRows || []) as any[]) {
        if (co.partner_id) {
          partnerByCompany.set(String(co.id), {
            partner_id:   co.partner_id,
            partner_name: partnerNames.get(co.partner_id) ?? co.partner_id,
          });
        }
      }
    }

    const rows = orgIds.map((orgId) => {
      const stats = orgMap.get(orgId)!;
      const companyIds = Array.from(stats.company_ids);

      // Derive partner from first company that has one
      let partner_id: string | null = null;
      let partner_name: string | null = null;
      for (const cid of companyIds) {
        const p = partnerByCompany.get(cid);
        if (p) { partner_id = p.partner_id; partner_name = p.partner_name; break; }
      }

      return {
        id:            orgId,
        name:          nameMap.get(orgId) ?? orgId,
        partner_id,
        partner_name,
        company_count: stats.company_ids.size,
        user_count:    stats.user_count,
        created_at:    stats.earliest || null,
        is_active:     true,
      };
    });

    rows.sort((a, b) => (a.name > b.name ? 1 : -1));

    return res.json({ ok: true, organisations: rows, count: rows.length });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "organisations_list_failed" });
  }
});

/* ================================================================
   PLATFORM ADMIN — GET /v1/admin/platform
   SuperAdmin only. Aggregate stats across the full platform.
================================================================ */

adminRouter.get("/platform", requireSuperAdmin, async (_req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const [
      { count: partner_count },
      { count: company_count },
      { count: user_count },
      { count: audit_event_count },
      { count: calls_processed },
    ] = await Promise.all([
      supa.from("partners").select("id", { count: "exact", head: true }),
      supa.from("companies").select("id", { count: "exact", head: true }),
      supa.from("reps").select("id", { count: "exact", head: true }),
      supa.from("audit_events").select("id", { count: "exact", head: true }),
      supa.from("calls").select("id", { count: "exact", head: true }).in("status", ["scored", "processed"]),
    ]);

    // Storage: best-effort from Supabase storage API
    let storage_usage: { bucket: string; file_count: number }[] = [];
    try {
      const { data: buckets } = await supa.storage.listBuckets();
      for (const b of buckets || []) {
        const { data: files } = await supa.storage.from(b.name).list("", { limit: 1000 });
        storage_usage.push({ bucket: b.name, file_count: (files || []).length });
      }
    } catch { /* ignore storage errors */ }

    return res.json({
      ok: true,
      stats: {
        partners:        partner_count  ?? 0,
        companies:       company_count  ?? 0,
        users:           user_count     ?? 0,
        active_sessions: null,           // not tracked yet
        audit_events:    audit_event_count ?? 0,
        calls_processed: calls_processed   ?? 0,
      },
      storage: storage_usage,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "platform_stats_failed" });
  }
});

/* ================================================================
   SUPPORT TOOLS
   GET /v1/admin/support/users?q=&limit=  — user lookup
   GET /v1/admin/support/companies?q=&limit= — company lookup
   GET /v1/admin/support/impersonation-history?limit= — recent impersonation sessions
================================================================ */

adminRouter.get("/support/users", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const q     = typeof req.query.q     === "string" ? req.query.q.trim()     : "";
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    let query = supa
      .from("reps")
      .select("id, name, display_name, email, tier, company_id, org_id, is_active, created_at")
      .order("name")
      .limit(limit);

    if (q) {
      query = query.or(`name.ilike.%${q}%,email.ilike.%${q}%,display_name.ilike.%${q}%`);
    }

    const { data, error } = await query;
    if (error) throw error;

    const rows = (data || []).map((r: any) => ({
      id:           r.id,
      name:         r.display_name ?? r.name,
      email:        r.email ?? null,
      tier:         r.tier,
      company_id:   r.company_id ?? null,
      org_id:       r.org_id ?? null,
      is_active:    r.is_active ?? true,
      created_at:   r.created_at,
    }));

    return res.json({ ok: true, users: rows, count: rows.length });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "support_users_failed" });
  }
});

adminRouter.get("/support/companies", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const q     = typeof req.query.q     === "string" ? req.query.q.trim()     : "";
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    let query = supa
      .from("companies")
      .select("id, name, website, industry, partner_id, is_active, created_at")
      .order("name")
      .limit(limit);

    if (q) {
      query = query.or(`name.ilike.%${q}%,website.ilike.%${q}%,industry.ilike.%${q}%`);
    }

    const { data, error } = await query;
    if (error) throw error;

    // Enrich with partner names and user counts
    const partnerIds = [...new Set((data || []).map((c: any) => c.partner_id).filter(Boolean))] as string[];
    const partnerNames = new Map<string, string>();
    if (partnerIds.length) {
      const { data: pRows } = await supa.from("partners").select("id, name").in("id", partnerIds);
      for (const p of (pRows || []) as any[]) partnerNames.set(p.id, p.name);
    }

    const companyIds = (data || []).map((c: any) => c.id).filter(Boolean);
    const userCountMap = new Map<string, number>();
    if (companyIds.length) {
      const { data: repRows } = await supa
        .from("reps")
        .select("company_id")
        .in("company_id", companyIds);
      for (const r of (repRows || []) as any[]) {
        if (r.company_id) userCountMap.set(r.company_id, (userCountMap.get(r.company_id) ?? 0) + 1);
      }
    }

    const rows = (data || []).map((c: any) => ({
      id:           c.id,
      name:         c.name,
      website:      c.website ?? null,
      industry:     c.industry ?? null,
      partner_id:   c.partner_id ?? null,
      partner_name: c.partner_id ? (partnerNames.get(c.partner_id) ?? null) : null,
      user_count:   userCountMap.get(c.id) ?? 0,
      is_active:    c.is_active ?? true,
      created_at:   c.created_at,
    }));

    return res.json({ ok: true, companies: rows, count: rows.length });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "support_companies_failed" });
  }
});

adminRouter.get("/support/impersonation-history", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const { data, error } = await supa
      .from("audit_events")
      .select("id, actor_user_id, target_user_id, action, metadata, created_at")
      .in("action", ["impersonate_user", "end_impersonation"])
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;

    return res.json({ ok: true, events: data ?? [], count: (data ?? []).length });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "impersonation_history_failed" });
  }
});

/* ================================================================
   AUDIT LISTING — GET /v1/admin/super/audit
================================================================ */

adminRouter.get("/super/audit", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    const rawLimit  = Number(req.query.limit ?? 50);
    const limit     = Math.min(Math.max(rawLimit, 1), 200);
    const cursor    = typeof req.query.cursor === "string" ? req.query.cursor.trim() : null;
    const action    = typeof req.query.action === "string" ? req.query.action.trim() : null;
    const actorId   = typeof req.query.actorId === "string" ? req.query.actorId.trim() : null;

    let q = supa
      .from("audit_events")
      .select("id, actor_user_id, target_user_id, action, entity_type, entity_id, metadata, created_at")
      .order("created_at", { ascending: false })
      .limit(limit + 1);

    if (cursor)  q = q.lt("created_at", cursor);
    if (action)  q = q.eq("action", action);
    if (actorId) q = q.eq("actor_user_id", actorId);

    const { data, error } = await q;
    if (error) throw error;

    const events = (data || []).slice(0, limit);
    const nextCursor = data && data.length > limit
      ? data[limit - 1]?.created_at ?? null
      : null;

    return res.json({
      ok:         true,
      events,
      nextCursor,
      count:      events.length,
    });
  } catch (e: any) {
    console.error("[audit:list] failed", e);
    return res.status(500).json({ ok: false, error: e?.message || "audit_list_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/partner/licences
   Requires: PartnerAdmin or SuperAdmin
   Returns the licence pool + per-company allocations for the
   caller's partner. available = purchased - sum(allocated).
----------------------------------------------------------------- */
adminRouter.get("/partner/licences", requirePartnerAdmin, async (req: any, res: any) => {
  try {
    const supa   = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );
    const userId = String(req.header("x-user-id") || "").trim();

    // Resolve the caller's partner_id via their company
    const { data: rep } = await supa
      .from("reps")
      .select("company_id")
      .eq("id", userId)
      .maybeSingle();

    const companyId = (rep as any)?.company_id ?? null;
    if (!companyId) {
      return res.status(400).json({ ok: false, error: "no_company_linked" });
    }

    const { data: company } = await supa
      .from("companies")
      .select("partner_id, name")
      .eq("id", companyId)
      .maybeSingle();

    const partnerId = (company as any)?.partner_id ?? null;
    if (!partnerId) {
      return res.status(400).json({ ok: false, error: "no_partner_linked" });
    }

    // Fetch the pool
    const { data: pool, error: poolErr } = await supa
      .from("licence_pools")
      .select("id, plan_type, purchased, reserved, renewed_at, expires_at, created_at")
      .eq("partner_id", partnerId)
      .maybeSingle();

    if (poolErr) throw poolErr;

    // Fetch per-company allocations, enrich with company names
    const { data: allocations, error: allocErr } = await supa
      .from("company_licences")
      .select("id, company_id, allocated, created_at")
      .eq("partner_id", partnerId)
      .order("created_at");

    if (allocErr) throw allocErr;

    // Enrich with company names
    const companyIds = (allocations || []).map((a: any) => a.company_id);
    let companyNameMap = new Map<string, string>();
    if (companyIds.length > 0) {
      const { data: cos } = await supa
        .from("companies")
        .select("id, name")
        .in("id", companyIds);
      for (const c of (cos || [])) {
        companyNameMap.set((c as any).id, (c as any).name);
      }
    }

    const companies = (allocations || []).map((a: any) => ({
      id:           a.id,
      company_id:   a.company_id,
      company_name: companyNameMap.get(a.company_id) ?? null,
      allocated:    a.allocated,
      created_at:   a.created_at,
    }));

    const totalAllocated = companies.reduce((sum: number, c: any) => sum + (c.allocated ?? 0), 0);
    const purchased      = (pool as any)?.purchased ?? 0;

    return res.json({
      ok:         true,
      partner_id: partnerId,
      pool: pool
        ? {
            id:         (pool as any).id,
            plan_type:  (pool as any).plan_type,
            purchased,
            reserved:   (pool as any).reserved ?? 0,
            renewed_at: (pool as any).renewed_at ?? null,
            expires_at: (pool as any).expires_at ?? null,
            created_at: (pool as any).created_at,
          }
        : null,
      companies,
      totals: {
        purchased,
        allocated:  totalAllocated,
        available:  purchased - totalAllocated,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "partner_licences_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/super/licences
   Requires: SuperAdmin only
   Returns all partners with their pool + per-company allocations.
----------------------------------------------------------------- */
adminRouter.get("/super/licences", requireSuperAdmin, async (req: any, res: any) => {
  try {
    const supa = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );

    // All partners
    const { data: partners, error: pErr } = await supa
      .from("partners")
      .select("id, name, status")
      .order("name");
    if (pErr) throw pErr;

    // All pools (one per partner)
    const { data: pools, error: poolErr } = await supa
      .from("licence_pools")
      .select("id, partner_id, plan_type, purchased, reserved, renewed_at, expires_at, created_at");
    if (poolErr) throw poolErr;

    // All company allocations
    const { data: allocations, error: allocErr } = await supa
      .from("company_licences")
      .select("id, partner_id, company_id, allocated, created_at");
    if (allocErr) throw allocErr;

    // Enrich company allocations with names
    const allCompanyIds = [...new Set((allocations || []).map((a: any) => a.company_id))] as string[];
    let companyNameMap = new Map<string, string>();
    if (allCompanyIds.length > 0) {
      const { data: cos } = await supa
        .from("companies")
        .select("id, name")
        .in("id", allCompanyIds);
      for (const c of (cos || [])) {
        companyNameMap.set((c as any).id, (c as any).name);
      }
    }

    const poolMap  = new Map((pools || []).map((p: any) => [p.partner_id, p]));
    const allocMap = new Map<string, any[]>();
    for (const a of (allocations || [])) {
      const pid = (a as any).partner_id;
      if (!allocMap.has(pid)) allocMap.set(pid, []);
      allocMap.get(pid)!.push(a);
    }

    const result = (partners || []).map((p: any) => {
      const pool       = poolMap.get(p.id) ?? null;
      const cos        = (allocMap.get(p.id) || []).map((a: any) => ({
        id:           a.id,
        company_id:   a.company_id,
        company_name: companyNameMap.get(a.company_id) ?? null,
        allocated:    a.allocated,
      }));
      const purchased     = (pool as any)?.purchased ?? 0;
      const totalAllocated = cos.reduce((s: number, c: any) => s + (c.allocated ?? 0), 0);

      return {
        partner_id:   p.id,
        partner_name: p.name,
        status:       p.status,
        pool: pool
          ? {
              id:         (pool as any).id,
              plan_type:  (pool as any).plan_type,
              purchased,
              reserved:   (pool as any).reserved ?? 0,
              renewed_at: (pool as any).renewed_at ?? null,
              expires_at: (pool as any).expires_at ?? null,
            }
          : null,
        companies:    cos,
        totals: {
          purchased,
          allocated:  totalAllocated,
          available:  purchased - totalAllocated,
        },
      };
    });

    return res.json({
      ok:       true,
      partners: result,
      count:    result.length,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "super_licences_failed" });
  }
});

/* ----------------------------------------------------------------
   GET /v1/admin/context/options
   Requires: PartnerAdmin or SuperAdmin

   Returns the set of partners and companies the caller is
   permitted to switch into, plus their current context.

   SuperAdmin  → all partners, all companies
   PartnerAdmin → own partner only, own partner's companies

   Response:
   {
     ok: true,
     currentPartnerId: string | null,
     currentCompanyId: string | null,
     partners: [{ id, name }],
     companies: [{ id, name, partner_id }]
   }
----------------------------------------------------------------- */
adminRouter.get("/context/options", requirePartnerAdmin, async (req: any, res: any) => {
  try {
    const supa   = createClient(
      process.env.SUPABASE_URL!,
      process.env.SUPABASE_SERVICE_ROLE_KEY!,
      { auth: { autoRefreshToken: false, persistSession: false } }
    );
    const userId = String(req.header("x-user-id") || "").trim();
    const tier   = String(req.repTier || "");

    // Resolve current context from the caller's reps row
    const { data: rep } = await supa
      .from("reps")
      .select("company_id")
      .eq("id", userId)
      .maybeSingle();

    const currentCompanyId = (rep as any)?.company_id ?? null;

    // Resolve current partner_id via the caller's company
    let currentPartnerId: string | null = null;
    if (currentCompanyId) {
      const { data: co } = await supa
        .from("companies")
        .select("partner_id")
        .eq("id", currentCompanyId)
        .maybeSingle();
      currentPartnerId = (co as any)?.partner_id ?? null;
    }

    // Companies are scoped by getVisibleCompanies:
    //   SuperAdmin   → all companies
    //   PartnerAdmin → own partner's companies
    const companies = await getVisibleCompanies(userId);

    // Partners list:
    //   SuperAdmin   → all partners
    //   PartnerAdmin → only their own partner
    let partners: { id: string; name: string }[] = [];

    if (tier === "SuperAdmin") {
      const { data: allPartners, error: pErr } = await supa
        .from("partners")
        .select("id, name")
        .order("name");
      if (pErr) throw pErr;
      partners = (allPartners || []).map((p: any) => ({ id: p.id, name: p.name }));
    } else if (currentPartnerId) {
      // PartnerAdmin — only their own partner
      const { data: ownPartner } = await supa
        .from("partners")
        .select("id, name")
        .eq("id", currentPartnerId)
        .maybeSingle();
      if (ownPartner) {
        partners = [{ id: (ownPartner as any).id, name: (ownPartner as any).name }];
      }
    }

    return res.json({
      ok:               true,
      currentPartnerId,
      currentCompanyId,
      partners,
      companies: companies.map((c) => ({
        id:         c.id,
        name:       c.name,
        partner_id: c.partner_id,
      })),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "context_options_failed" });
  }
});
