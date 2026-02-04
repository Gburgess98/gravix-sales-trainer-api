// api/src/routes/reps.ts
import { Router } from "express";
import type { Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";

const repsRouter = Router();

// Reuse your env vars
const supabaseUrl = process.env.SUPABASE_URL!;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY!;
const sb = createClient(supabaseUrl, supabaseKey);

// Helpers
function pct(numer: number, denom: number) {
  if (!denom) return 0;
  return numer / denom; // 0..1, frontend normalises either way
}

// ------------------------------
// Option I — Permissions hardening (org-level)
//
// Contract:
// - Requests MUST include x-user-id (requester identity)
// - Requests MUST include x-org-id (org scope)
// - reps table must support org_id for enforcement
//
// If org_id is not available in schema, we fail-closed (security > convenience)
// ------------------------------
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getRequesterId(req: Request) {
  const id = String(req.header("x-user-id") || "").trim();
  if (!id || !UUID_RE.test(id)) return null;
  return id;
}

function getOrgId(req: Request) {
  const id = String(req.header("x-org-id") || "").trim();
  if (!id || !UUID_RE.test(id)) return null;
  return id;
}

async function assertOrgScopeOr403(args: {
  requesterId: string;
  orgId: string;
  repId: string;
}) {
  const { requesterId, orgId, repId } = args;

  // We require org_id to exist for enforcement.
  // If the column doesn't exist, Supabase will return an error — treat that as fail-closed.
  const { data: requesterRow, error: requesterErr } = await sb
    .from("reps")
    .select("id,org_id")
    .eq("id", requesterId)
    .single();

  if (requesterErr) throw new Error(`org_scope_lookup_failed:requester:${requesterErr.message}`);
  if (!requesterRow) throw new Error("org_scope_missing_requester");
  if (String((requesterRow as any).org_id || "") !== orgId) throw new Error("forbidden_org_scope");

  const { data: targetRow, error: targetErr } = await sb
    .from("reps")
    .select("id,org_id")
    .eq("id", repId)
    .single();

  if (targetErr) throw new Error(`org_scope_lookup_failed:target:${targetErr.message}`);
  if (!targetRow) throw new Error("rep_not_found");
  if (String((targetRow as any).org_id || "") !== orgId) throw new Error("forbidden_org_scope");
}

repsRouter.get("/:id/overview", async (req: Request, res: Response) => {
  try {
    const repId = String(req.params.id || "").trim();
    if (!repId || !UUID_RE.test(repId)) return res.status(400).json({ ok: false, error: "invalid_rep_id" });

    const requesterId = getRequesterId(req);
    const orgId = getOrgId(req);
    if (!requesterId) return res.status(401).json({ ok: false, error: "missing_requester" });
    if (!orgId) return res.status(400).json({ ok: false, error: "missing_org" });

    await assertOrgScopeOr403({ requesterId, orgId, repId });

    // ---- Rep basics
    const { data: repRow, error: repErr } = await sb
      .from("reps")
      .select("id,org_id,name,email,avatar_url,xp,tier")
      .eq("id", repId)
      .single();
    if (repErr) throw repErr;
    if (!repRow) return res.status(404).json({ ok: false, error: "rep_not_found" });

    // ---- Totals (calls, avg score, win rate)
    const { data: callsAgg, error: callsAggErr } = await sb
      .rpc("rep_calls_aggregate", { p_rep_id: repId }); 
    // ^ If you don’t have a RPC, you can approximate with:
    // const { data: calls, error: callsErr } = await sb
    //   .from("calls")
    //   .select("id,created_at,final_score,outcome")
    //   .eq("rep_id", repId);
    // Then compute totals locally.

    if (callsAggErr) throw callsAggErr;

    // Fallback compute when no RPC:
    let totalCalls = 0, avgScore = 0, wins = 0;
    if (Array.isArray(callsAgg) && callsAgg.length) {
      // Expecting something like [{ total_calls, avg_score, wins }]
      totalCalls = callsAgg[0].total_calls ?? 0;
      avgScore = callsAgg[0].avg_score ?? 0;
      wins = callsAgg[0].wins ?? 0;
    } else {
      // leave zeros
    }

    // ---- Trends (xp daily, score daily, voice daily)
    const { data: xpTrend, error: xpErr } = await sb
      .from("rep_xp_daily")
      .select("date,value")
      .eq("rep_id", repId)
      .order("date", { ascending: true })
      .limit(60);

    const { data: scoreTrend, error: scoreErr } = await sb
      .from("rep_score_daily")
      .select("date,value")
      .eq("rep_id", repId)
      .order("date", { ascending: true })
      .limit(60);

    const { data: voiceTrend, error: voiceErr } = await sb
      .from("rep_voice_daily")
      .select("date,value")
      .eq("rep_id", repId)
      .order("date", { ascending: true })
      .limit(60);

    if (xpErr) throw xpErr;
    if (scoreErr) throw scoreErr;
    if (voiceErr) {
      // voice is optional for now — don’t fail the endpoint
    }

    // ---- Recent: assignments, calls, activity
    const { data: recentAssignments } = await sb
      .from("assignments")
      .select("id,title,status,created_at")
      .eq("rep_id", repId)
      .order("created_at", { ascending: false })
      .limit(5);

    const { data: recentCalls } = await sb
      .from("calls")
      .select("id,created_at,final_score")
      .eq("rep_id", repId)
      .order("created_at", { ascending: false })
      .limit(5);

    const { data: recentActivity } = await sb
      .from("activity_log")
      .select("id,t as t,text")
      .eq("rep_id", repId)
      .order("t", { ascending: false })
      .limit(5);

    // ---- Assemble response
    const payload = {
      rep: {
        id: repRow.id,
        name: repRow.name,
        email: repRow.email,
        avatar_url: repRow.avatar_url,
      },
      xp: repRow.xp ?? 0,
      tier: repRow.tier ?? "SalesRep",
      totals: {
        calls: totalCalls,
        avgScore: avgScore,
        winRate: pct(wins, totalCalls) // 0..1 (frontend handles 0..100 too)
      },
      trends: {
        xp: xpTrend ?? [],
        score: scoreTrend ?? [],
        voice: voiceTrend ?? [] // ok if empty
      },
      recent: {
        assignments: recentAssignments ?? [],
        calls: (recentCalls ?? []).map(c => ({
          id: c.id,
          created_at: c.created_at,
          score: c.final_score ?? 0
        })),
        activity: recentActivity ?? []
      }
    };

    // #5 (cache header) — we’ll set this here too:
    res.set("Cache-Control", "public, max-age=15");
    return res.json({ ok: true, ...payload });
  } catch (err: any) {
    const msg = String(err?.message || err);
    console.error("GET /v1/reps/:id/overview error", msg);

    if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
    if (msg === "rep_not_found") return res.status(404).json({ ok: false, error: "rep_not_found" });
    if (msg.startsWith("org_scope_lookup_failed:")) return res.status(500).json({ ok: false, error: "org_scope_not_supported" });

    return res.status(500).json({ ok: false, error: "server_error" });
  }
});


// GET /v1/reps/:id/xp -> { ok, repId, total_xp, last_event_at }
repsRouter.get("/:id/xp", async (req: Request, res: Response) => {
  res.setHeader("Cache-Control", "no-store");
  const repId = String(req.params.id || "").trim();
  if (!repId || !UUID_RE.test(repId)) return res.status(400).json({ ok: false, error: "invalid_rep_id" });

  const requesterId = getRequesterId(req);
  const orgId = getOrgId(req);
  if (!requesterId) return res.status(401).json({ ok: false, error: "missing_requester" });
  if (!orgId) return res.status(400).json({ ok: false, error: "missing_org" });

  try {
    await assertOrgScopeOr403({ requesterId, orgId, repId });
    const { data, error } = await sb
      .from("xp_events")
      .select("delta, amount, created_at")
      .eq("rep_id", repId);

    if (error) {
      // Be tolerant: return ok:true with a warning so frontend doesn't crash if table missing
      return res.status(200).json({ ok: true, repId, total_xp: 0, last_event_at: null, warning: error.message });
    }

    const total = (data || []).reduce((sum: number, row: any) => {
      const inc = Number.isFinite(row.delta) ? Number(row.delta) : Number(row.amount || 0);
      return sum + (Number.isFinite(inc) ? inc : 0);
    }, 0);

    const last = (data || [])
      .map((r: any) => new Date(r.created_at).getTime())
      .filter((t: number) => Number.isFinite(t))
      .sort((a, b) => b - a)[0];

    return res.json({
      ok: true,
      repId,
      total_xp: total,
      last_event_at: last ? new Date(last).toISOString() : null,
    });
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
    if (msg === "rep_not_found") return res.status(404).json({ ok: false, error: "rep_not_found" });
    if (msg.startsWith("org_scope_lookup_failed:")) return res.status(500).json({ ok: false, error: "org_scope_not_supported" });
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

export default repsRouter;