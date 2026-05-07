import 'dotenv/config';
// src/routes/admin.ts
import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import { buildScoreSummaryBlocks } from "../lib/slackBlocks";
import { getAdminConfig, patchAdminConfig } from "../services/adminConfig";
import {
  auditUserCreated,
  writeAuditEvent,
} from "../lib/audit";
export const adminRouter = Router();

// --- Roles (lean RBAC v1) -------------------------------------
const ROLE_VALUES = [
  "rep",
  "office_manager",
  "company_manager",
] as const;
type Role = (typeof ROLE_VALUES)[number];

function isRole(x: any): x is Role {
  return typeof x === "string" && (ROLE_VALUES as readonly string[]).includes(x);
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
    const allowed = tier === "Manager" || tier === "Owner";
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
----------------------------------------------------------------- */
adminRouter.get("/org-settings", requireManager, async (req: any, res: any) => {
  try {
    const requester = String(req.header("x-user-id") || "").trim();

    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) return res.status(500).json({ ok: false, error: "server_missing_supabase_env" });

    const supa = createClient(url, key);

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

    if (
      low_score_threshold !== undefined &&
      critical_score_threshold !== undefined &&
      critical_score_threshold > low_score_threshold
    ) {
      return res.status(400).json({
        ok: false,
        error: "critical_score_threshold must be less than or equal to low_score_threshold",
      });
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
