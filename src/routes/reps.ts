// api/src/routes/reps.ts
import { Router } from "express";
import type { Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";

const repsRouter = Router();

// Reuse your env vars
const supabaseUrl = process.env.SUPABASE_URL!;
const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_ANON_KEY!;

const sb = createClient(supabaseUrl, supabaseKey);

// Helpers
function pct(numer: number, denom: number) {
  if (!denom) return 0;
  return numer / denom;
}

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getRequesterId(req: Request) {
  const id = String(req.header("x-user-id") || "").trim();

  if (!id || !UUID_RE.test(id)) {
    return null;
  }

  return id;
}

function getOrgId(req: Request) {
  const id = String(req.header("x-org-id") || "").trim();

  if (!id || !UUID_RE.test(id)) {
    return null;
  }

  return id;
}

async function assertOrgScopeOr403(args: {
  requesterId: string;
  orgId: string;
  repId: string;
}) {
  const { requesterId, orgId, repId } = args;

  const { data: requesterRow, error: requesterErr } =
    await sb
      .from("reps")
      .select("id,org_id")
      .eq("id", requesterId)
      .single();

  if (requesterErr) {
    throw new Error(
      `org_scope_lookup_failed:requester:${requesterErr.message}`
    );
  }

  if (!requesterRow) {
    throw new Error("org_scope_missing_requester");
  }

  if (
    String((requesterRow as any).org_id || "") !== orgId
  ) {
    throw new Error("forbidden_org_scope");
  }

  const { data: targetRow, error: targetErr } =
    await sb
      .from("reps")
      .select("id,org_id")
      .eq("id", repId)
      .single();

  if (targetErr) {
    throw new Error(
      `org_scope_lookup_failed:target:${targetErr.message}`
    );
  }

  if (!targetRow) {
    throw new Error("rep_not_found");
  }

  if (
    String((targetRow as any).org_id || "") !== orgId
  ) {
    throw new Error("forbidden_org_scope");
  }
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
      .select("id,created_at,score_overall")
      .eq("user_id", repId)
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
          score: c.score_overall ?? 0
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

    return res.status(500).json({
      ok: false,
      error: "server_error",
    });
  }
});

// ------------------------------
// AI Coaching Profile
// ------------------------------

// GET /v1/reps/:id/weakness-trends
repsRouter.get("/:id/weakness-trends", async (req: Request, res: Response) => {
  try {
    const repId = String(req.params.id || "").trim();

    if (!repId || !UUID_RE.test(repId)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_rep_id",
      });
    }

    const requesterId = getRequesterId(req);
    const orgId = getOrgId(req);

    if (!requesterId) {
      return res.status(401).json({
        ok: false,
        error: "missing_requester",
      });
    }

    if (!orgId) {
      return res.status(400).json({
        ok: false,
        error: "missing_org",
      });
    }

    await assertOrgScopeOr403({
      requesterId,
      orgId,
      repId,
    });

    const { data: calls, error: callsErr } = await sb
      .from("calls")
      .select(
        "id,created_at,score_overall,intro_score,discovery_score,objection_score,close_score"
      )
      .eq("user_id", repId)
      .order("created_at", { ascending: true })
      .limit(100);

    if (callsErr) {
      throw callsErr;
    }

    const safeCalls = Array.isArray(calls)
      ? calls
      : [];

    const trends = {
      intro: [] as any[],
      discovery: [] as any[],
      objection_handling: [] as any[],
      closing: [] as any[],
      overall: [] as any[],
    };

    for (const call of safeCalls) {
      const date = call.created_at;

      trends.intro.push({
        date,
        value: Number(call.intro_score || 0),
      });

      trends.discovery.push({
        date,
        value: Number(call.discovery_score || 0),
      });

      trends.objection_handling.push({
        date,
        value: Number(call.objection_score || 0),
      });

      trends.closing.push({
        date,
        value: Number(call.close_score || 0),
      });

      trends.overall.push({
        date,
        value: Number(call.score_overall || 0),
      });
    }

    function calculateTrendDelta(values: number[]) {
      if (values.length < 2) {
        return 0;
      }

      const midpoint = Math.floor(values.length / 2);

      const firstHalf = values.slice(0, midpoint);
      const secondHalf = values.slice(midpoint);

      const firstAvg =
        firstHalf.reduce((a, b) => a + b, 0) /
        Math.max(firstHalf.length, 1);

      const secondAvg =
        secondHalf.reduce((a, b) => a + b, 0) /
        Math.max(secondHalf.length, 1);

      return Math.round((secondAvg - firstAvg) * 10) / 10;
    }

    const introDelta = calculateTrendDelta(
      trends.intro.map((x) => x.value)
    );

    const discoveryDelta = calculateTrendDelta(
      trends.discovery.map((x) => x.value)
    );

    const objectionDelta = calculateTrendDelta(
      trends.objection_handling.map((x) => x.value)
    );

    const closingDelta = calculateTrendDelta(
      trends.closing.map((x) => x.value)
    );

    const overallDelta = calculateTrendDelta(
      trends.overall.map((x) => x.value)
    );

    const momentumScore = Math.round(
      (
        introDelta +
        discoveryDelta +
        objectionDelta +
        closingDelta +
        overallDelta
      ) / 5
    );

    const regressionWarnings: string[] = [];

    if (introDelta < -5) {
      regressionWarnings.push(
        "Cold open performance declining"
      );
    }

    if (objectionDelta < -5) {
      regressionWarnings.push(
        "Objection handling confidence declining"
      );
    }

    if (closingDelta < -5) {
      regressionWarnings.push(
        "Closing performance weakening"
      );
    }

    const aiSummary =
      momentumScore > 5
        ? "AI detected strong coaching improvement momentum."
        : momentumScore < -5
          ? "AI detected rep regression requiring intervention."
          : "AI detected stable performance with moderate variation.";

    const replayImprovementTrend = safeCalls
      .filter((c) => Number(c.score_overall || 0) > 0)
      .slice(-10)
      .map((c) => ({
        call_id: c.id,
        score: Number(c.score_overall || 0),
        created_at: c.created_at,
      }));

    const coachingCompletionTrend = {
      completed_assignments_estimate:
        safeCalls.filter(
          (c) => Number(c.score_overall || 0) >= 70
        ).length,

      struggling_sessions:
        safeCalls.filter(
          (c) => Number(c.score_overall || 0) < 50
        ).length,
    };

    return res.json({
      ok: true,
      rep_id: repId,

      momentum_score: momentumScore,

      regression_warnings: regressionWarnings,

      ai_summary: aiSummary,

      trends,

      deltas: {
        intro: introDelta,
        discovery: discoveryDelta,
        objection_handling: objectionDelta,
        closing: closingDelta,
        overall: overallDelta,
      },

      replay_improvement_trend: replayImprovementTrend,

      coaching_completion_trend:
        coachingCompletionTrend,
    });
  } catch (err: any) {
    const msg = String(err?.message || err);

    console.error(
      "GET /v1/reps/:id/weakness-trends error",
      msg
    );

    if (msg === "forbidden_org_scope") {
      return res.status(403).json({
        ok: false,
        error: "forbidden_org_scope",
      });
    }

    if (msg === "rep_not_found") {
      return res.status(404).json({
        ok: false,
        error: "rep_not_found",
      });
    }

    return res.status(500).json({
      ok: false,
      error: "server_error",
    });
  }
});

// GET /v1/reps/:id/daily-feed
repsRouter.get("/:id/daily-feed", async (req: Request, res: Response) => {
  try {
    const repId = String(req.params.id || "").trim();

    if (!repId || !UUID_RE.test(repId)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_rep_id",
      });
    }

    const requesterId = getRequesterId(req);
    const orgId = getOrgId(req);

    if (!requesterId) {
      return res.status(401).json({
        ok: false,
        error: "missing_requester",
      });
    }

    if (!orgId) {
      return res.status(400).json({
        ok: false,
        error: "missing_org",
      });
    }

    await assertOrgScopeOr403({
      requesterId,
      orgId,
      repId,
    });

    const { data: calls, error: callsErr } = await sb
      .from("calls")
      .select(
        "id,created_at,score_overall,intro_score,discovery_score,objection_score,close_score"
      )
      .eq("user_id", repId)
      .order("created_at", { ascending: false })
      .limit(20);

    if (callsErr) {
      throw callsErr;
    }

    const safeCalls = Array.isArray(calls)
      ? calls
      : [];

    const latestCall = safeCalls[0] || null;

    const categoryScores = {
      intro: Number(latestCall?.intro_score || 0),
      discovery: Number(latestCall?.discovery_score || 0),
      objection_handling: Number(latestCall?.objection_score || 0),
      closing: Number(latestCall?.close_score || 0),
    };

    const weakestEntry = Object.entries(categoryScores)
      .sort((a, b) => a[1] - b[1])[0];

    const weakestArea = weakestEntry
      ? {
          category: weakestEntry[0],
          score: weakestEntry[1],
        }
      : null;

    const overallScores = safeCalls.map((c) =>
      Number(c.score_overall || 0)
    );

    const recentAvg = overallScores.length
      ? overallScores.slice(0, 5).reduce((a, b) => a + b, 0) /
        Math.min(overallScores.length, 5)
      : 0;

    const historicalAvg = overallScores.length
      ? overallScores.reduce((a, b) => a + b, 0) /
        overallScores.length
      : 0;

    const momentumDelta = Math.round(
      (recentAvg - historicalAvg) * 10
    ) / 10;

    const momentumInsight =
      momentumDelta > 5
        ? "Strong upward momentum detected."
        : momentumDelta < -5
          ? "Performance momentum slipping."
          : "Performance momentum stable.";

    const regressionWarnings: string[] = [];

    if (categoryScores.objection_handling < 55) {
      regressionWarnings.push(
        "Objection handling confidence below target"
      );
    }

    if (categoryScores.closing < 55) {
      regressionWarnings.push(
        "Closing confidence weakening"
      );
    }

    const recommendedReplay = safeCalls
      .filter((c) => Number(c.score_overall || 0) < 60)
      .map((c) => ({
        call_id: c.id,
        score: Number(c.score_overall || 0),
        created_at: c.created_at,
      }))[0] || null;

    let recommendedDrill = "General confidence reinforcement";

    if (weakestArea?.category === "objection_handling") {
      recommendedDrill =
        "Price objection pressure replay";
    }

    if (weakestArea?.category === "closing") {
      recommendedDrill =
        "Closing confidence repetition drill";
    }

    if (weakestArea?.category === "discovery") {
      recommendedDrill =
        "Deep discovery questioning practice";
    }

    if (weakestArea?.category === "intro") {
      recommendedDrill =
        "Cold open pattern interrupt training";
    }

    const coachingUrgency =
      weakestArea && weakestArea.score < 50
        ? "high"
        : weakestArea && weakestArea.score < 70
          ? "medium"
          : "low";

    const aiMotivationMessage =
      momentumDelta > 5
        ? "AI detected measurable coaching progress. Keep stacking consistency."
        : momentumDelta < -5
          ? "AI detected regression risk. Focus on rebuilding confidence through repetition."
          : "Consistency is building. Keep reinforcing your fundamentals.";

    const todaysFocus =
      weakestArea?.category === "objection_handling"
        ? "Control emotional objections without rushing the close."
        : weakestArea?.category === "closing"
          ? "Increase certainty and conviction during closing moments."
          : weakestArea?.category === "discovery"
            ? "Slow down and deepen qualification questions."
            : weakestArea?.category === "intro"
              ? "Improve pattern interrupts and opening energy."
              : "Continue reinforcing strong fundamentals.";

    const coachingSummary =
      `AI identified ${
        weakestArea?.category?.replace(/_/g, " ") ||
        "general performance"
      } as today's main coaching focus.`;

    return res.json({
      ok: true,
      rep_id: repId,

      coaching_summary: coachingSummary,

      weakest_area: weakestArea,

      momentum_insight: momentumInsight,

      momentum_delta: momentumDelta,

      regression_warnings: regressionWarnings,

      recommended_replay: recommendedReplay,

      recommended_drill: recommendedDrill,

      coaching_urgency: coachingUrgency,

      ai_motivation_message: aiMotivationMessage,

      todays_focus: todaysFocus,
    });
  } catch (err: any) {
    const msg = String(err?.message || err);

    console.error(
      "GET /v1/reps/:id/daily-feed error",
      msg
    );

    if (msg === "forbidden_org_scope") {
      return res.status(403).json({
        ok: false,
        error: "forbidden_org_scope",
      });
    }

    if (msg === "rep_not_found") {
      return res.status(404).json({
        ok: false,
        error: "rep_not_found",
      });
    }

    return res.status(500).json({
      ok: false,
      error: "server_error",
    });
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