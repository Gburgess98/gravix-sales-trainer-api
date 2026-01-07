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

repsRouter.get("/:id/overview", async (req: Request, res: Response) => {
  try {
    const repId = req.params.id;

    // ---- Rep basics
    const { data: repRow, error: repErr } = await sb
      .from("reps")
      .select("id,name,email,avatar_url,xp,tier")
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
    return res.json(payload);
  } catch (err: any) {
    console.error("GET /v1/reps/:id/overview error", err);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});


// GET /v1/reps/:id/xp -> { ok, repId, total_xp, last_event_at }
repsRouter.get("/:id/xp", async (req: Request, res: Response) => {
  res.setHeader("Cache-Control", "no-store");
  const repId = String(req.params.id || "").trim();
  if (!repId) return res.status(400).json({ ok: false, error: "repId required" });
  try {
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
    return res.status(500).json({ ok: false, error: e?.message || "internal_error" });
  }
});

export default repsRouter;