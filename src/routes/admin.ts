import 'dotenv/config';
// src/routes/admin.ts
import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import { buildScoreSummaryBlocks } from "../lib/slackBlocks";
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

adminRouter.get("/status", async (_req, res) => {
  const out: any = { ok: true, checks: [] as any[] };

  function push(name: string, ok: boolean, detail?: any) {
    out.checks.push({ name, ok, detail });
    if (!ok) out.ok = false;
  }

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
    if (!url) return res.status(400).json({ ok:false, error:'SLACK_WEBHOOK_URL not set' });

    const text = (req.body?.text as string | undefined) || 'Gravix test: webhook is live ✅';
    const payload = {
      text,
      blocks: [
        { type: 'section', text: { type: 'mrkdwn', text } },
        { type: 'context', elements: [
          { type:'mrkdwn', text:`*Env:* ${process.env.NODE_ENV || 'dev'}  •  *API:* ${process.env.PUBLIC_API_BASE || 'local'}` }
        ]}
      ]
    };

    const r = await fetch(url, {
      method:'POST',
      headers:{ 'content-type':'application/json' },
      body: JSON.stringify(payload)
    });

    return res.json({ ok: true, status: r.status });
  } catch (e:any) {
    return res.status(500).json({ ok:false, error: e?.message || 'slack_failed' });
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

export default adminRouter;
