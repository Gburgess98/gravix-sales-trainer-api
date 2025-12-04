import { Router } from 'express';
import { createClient } from '@supabase/supabase-js';

const router = Router();

// ---- Supabase client (service role for server-side aggregations) ----
const SUPABASE_URL = process.env.SUPABASE_URL as string;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY as string;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  // Defer throwing until runtime hit so server can still boot for health checks
  console.warn('⚠️ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing – /v1/dashboard/* will fail.');
}

const supabase = (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
  : null;

// Admin Supabase client (uses env directly) — bypasses RLS for analytics
const sbAdmin = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

function isoDaysAgo(days: number) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString();
}

// ---- GET /v1/dashboard/kpis ----
// Returns compact KPI payload for CRM Overview cards + sparklines
router.get('/kpis', async (req, res) => {
  try {
    if (!supabase) throw new Error('Supabase not configured');

    res.set('Cache-Control', 'public, max-age=15');

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
    const since = isoDaysAgo(days);
    const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();

    // Defaults keep UI rendering even if queries fail
    let total_calls = 0;
    let avg_score_overall: number | null = null;
    let conversion_rate_90d: number | null = null; // fraction (0..1) if `won` exists

    const callsAnalyzed: Array<{ date: string; value: number }> = [];
    const avgScore: Array<{ date: string; value: number }> = [];
    const winRate: Array<{ date: string; value: number }> = []; // percent for sparkline

    const top_accounts: Array<{ account_id: string; name?: string; avg_score: number }> = [];
    const top_reps: Array<{ user_id: string; name?: string; avg_score: number }> = [];

    // Prefer admin client; fall back to regular supabase (might be constrained by RLS)
    const db = sbAdmin ?? supabase;
    if (!db) throw new Error('No Supabase client available');

    let callQuery = db
      .from('calls')
      .select('id, created_at, status, score_overall, account_id, user_id, org_id', { count: 'exact' })
      .gte('created_at', since)
      .limit(50000);
    if (orgId) callQuery = callQuery.eq('org_id', orgId);

    const { data: calls, error: callsErr, count } = await callQuery;
    const supportsWon = Array.isArray(calls) && calls.some((c: any) => typeof (c as any).won !== 'undefined');
    if (callsErr) {
      console.error('KPI calls query error:', callsErr, { since, orgId });
      return res.json({
        ok: true,
        total_calls: 0,
        avg_score_overall: null,
        conversion_rate_90d: null,
        callsAnalyzed: [],
        avgScore: [],
        winRate: [],
        top_accounts: [],
        top_reps: [],
        since,
      });
    }

    // Debug: log count and a few sample ids
    if (process.env.NODE_ENV !== 'production') {
      console.log('[KPI] calls count:', count ?? (calls?.length || 0), 'since:', since, 'orgId:', orgId || '(none)');
      if (calls && calls.length) console.log('[KPI] sample ids:', calls.slice(0, 3).map(c => (c as any).id));
    }

    total_calls = calls?.length ?? 0;

    const scored = (calls || []).filter(c => c.status === 'scored' && typeof (c as any).score_overall === 'number');
    if (scored.length) {
      const sum = scored.reduce((s, c: any) => s + (c.score_overall || 0), 0);
      avg_score_overall = Math.round(sum / scored.length);
    }

    // If a boolean `won` field exists, compute a simple 90d conversion
    if (supportsWon && total_calls > 0) {
      const wonCount = (calls || []).filter((c: any) => (c as any).won === true).length;
      conversion_rate_90d = Math.round((wonCount / total_calls) * 100) / 100;
    } else {
      conversion_rate_90d = null;
    }

    // Day rollups for sparklines
    const dayMap: Record<string, { calls: number; scoredCount: number; scoredSum: number; won: number }> = {};
    for (const c of calls || []) {
      const key = new Date((c as any).created_at).toISOString().slice(0, 10);
      if (!dayMap[key]) dayMap[key] = { calls: 0, scoredCount: 0, scoredSum: 0, won: 0 };
      dayMap[key].calls += 1;
      const sc = Number((c as any).score_overall);
      if ((c as any).status === 'scored' && Number.isFinite(sc)) {
        dayMap[key].scoredCount += 1;
        dayMap[key].scoredSum += sc;
      }
      if (supportsWon && (c as any).won === true) dayMap[key].won += 1;
    }

    for (const day of Object.keys(dayMap).sort()) {
      const d = dayMap[day];
      callsAnalyzed.push({ date: day, value: d.calls });
      avgScore.push({ date: day, value: d.scoredCount ? Math.round(d.scoredSum / d.scoredCount) : 0 });
      winRate.push({
        date: day,
        value: supportsWon && d.calls ? Math.round((d.won / d.calls) * 100) : 0,
      });
    }

    // Top accounts by avg score from scored calls
    const acctAgg: Record<string, { sum: number; n: number }> = {};
    for (const c of scored) {
      const a = String((c as any).account_id || '');
      if (!a) continue;
      acctAgg[a] = acctAgg[a] || { sum: 0, n: 0 };
      acctAgg[a].sum += Number((c as any).score_overall) || 0;
      acctAgg[a].n += 1;
    }
    const acctRows = Object.entries(acctAgg)
      .map(([account_id, v]) => ({ account_id, avg_score: Math.round(v.sum / v.n) }))
      .sort((a, b) => b.avg_score - a.avg_score)
      .slice(0, 8);
    top_accounts.push(...acctRows);

    // Top reps by avg score
    const repAgg: Record<string, { sum: number; n: number }> = {};
    for (const c of scored) {
      const u = String((c as any).user_id || '');
      if (!u) continue;
      repAgg[u] = repAgg[u] || { sum: 0, n: 0 };
      repAgg[u].sum += Number((c as any).score_overall) || 0;
      repAgg[u].n += 1;
    }
    const repRows = Object.entries(repAgg)
      .map(([user_id, v]) => ({ user_id, avg_score: Math.round(v.sum / v.n) }))
      .sort((a, b) => b.avg_score - a.avg_score)
      .slice(0, 10);
    top_reps.push(...repRows);

    return res.json({
      ok: true,
      total_calls,
      avg_score_overall,
      conversion_rate_90d,
      callsAnalyzed,
      avgScore,
      winRate,
      top_accounts,
      top_reps,
      since,
    });
  } catch (err: any) {
    console.error('GET /v1/dashboard/kpis error:', err);
    return res.json({
      ok: true,
      total_calls: 0,
      avg_score_overall: null,
      conversion_rate_90d: null,
      callsAnalyzed: [],
      avgScore: [],
      winRate: [],
      top_accounts: [],
      top_reps: [],
      // expose since for debugging even on error
      since: typeof since !== 'undefined' ? since : undefined,
    });
  }
});

// ---- GET /v1/dashboard/leaderboard ----
// Query params: days=90&limit=10&minCalls=3
router.get('/leaderboard', async (req, res) => {
  try {
    if (!supabase) throw new Error('Supabase not configured');

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
    const limit = Math.max(1, Math.min(100, parseInt(String(req.query.limit ?? '10'), 10) || 10));
    const minCalls = Math.max(0, Math.min(1000, parseInt(String(req.query.minCalls ?? '3'), 10) || 3));
    const since = isoDaysAgo(days);
    const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();

    // Pull all scored calls since cutoff; aggregate in Node for portability
    let callQuery = supabase
      .from('calls')
      .select('user_id, score_overall, created_at, org_id')
      .gte('created_at', since)
      .not('score_overall', 'is', null)
      .limit(50000);
    if (orgId) callQuery = callQuery.eq('org_id', orgId);
    const { data: calls, error: callsErr } = await callQuery;

    if (callsErr) throw callsErr;

    type Acc = { user_id: string; sum: number; calls: number; xp: number };
    const byUser = new Map<string, Acc>();

    for (const c of calls || []) {
      const uid = c.user_id as string;
      const s = Number(c.score_overall);
      if (!uid || Number.isNaN(s)) continue;
      const acc = byUser.get(uid) || { user_id: uid, sum: 0, calls: 0, xp: 0 };
      acc.sum += s;
      acc.calls += 1;
      if (s >= 70) acc.xp += 10; // +10 XP per call >= 70
      byUser.set(uid, acc);
    }

    // Completed drills per user (assignee)
    const { data: assigns, error: aErr } = await supabase
      .from('coach_assignments')
      .select('assignee_user_id, status, created_at')
      .gte('created_at', since)
      .limit(50000);
    if (aErr && aErr.message && !/relation .* does not exist/i.test(aErr.message)) {
      // If table exists but error happened, surface it; if table missing in dev, just skip XP add
      throw aErr;
    }
    for (const a of assigns || []) {
      const uid = String((a as any).assignee_user_id || '');
      const st = String((a as any).status || '');
      if (!uid) continue;
      const acc = byUser.get(uid) || { user_id: uid, sum: 0, calls: 0, xp: 0 };
      if (st === 'completed') acc.xp += 5; // +5 XP per completed drill
      byUser.set(uid, acc);
    }

    // Name hydration: profiles(id, display_name) → users(id, full_name, email) → fallback "Rep"
    const repIds = Array.from(byUser.keys()).filter(Boolean);
    const nameById = new Map<string, string>();

    if (repIds.length) {
      const { data: profs } = await supabase
        .from('profiles')
        .select('id, display_name')
        .in('id', repIds);
      for (const p of profs || []) {
        const id = String((p as any).id);
        const name = (p as any).display_name || '';
        if (id && name) nameById.set(id, name);
      }
      const missing = repIds.filter(id => !nameById.has(id));
      if (missing.length) {
        const { data: users } = await supabase
          .from('users')
          .select('id, full_name, email')
          .in('id', missing);
        for (const u of users || []) {
          const id = String((u as any).id);
          const name = (u as any).full_name || (u as any).email || 'Rep';
          if (id && !nameById.has(id)) nameById.set(id, name);
        }
      }
    }

    let rows = Array.from(byUser.values())
      .map(acc => ({
        user_id: acc.user_id,
        name: nameById.get(acc.user_id) || 'Rep',
        avg_score: acc.calls ? acc.sum / acc.calls : 0,
        calls: acc.calls,
        xp: acc.xp,
      }))
      .filter(x => (minCalls ? x.calls >= minCalls : true))
      .sort((a, b) => {
        const d = b.avg_score - a.avg_score;
        if (Math.abs(d) > 1e-9) return d;
        return b.calls - a.calls;
      })
      .slice(0, limit);

    const reps = rows.map((r, i) => ({
      repId: r.user_id,
      name: r.name,
      calls: r.calls,
      avgScore: Math.round(r.avg_score),
      xp: r.xp,
      rank: i + 1,
    }));

    res.set('Cache-Control', 'public, max-age=15');
    res.json({ ok: true, items: rows, reps, since });
  } catch (err: any) {
    console.error('GET /v1/dashboard/leaderboard error:', err);
    res.status(500).json({ ok: false, error: err?.message || 'leaderboard_failed' });
  }
});

// --- Dashboard: Rep Summary (avg score, calls, XP, top account) ---
router.get('/rep-summary', async (req, res) => {
  try {
    if (!supabase) throw new Error('Supabase not configured');
    const userId = String(req.query.userId || '').trim();
    if (!userId) return res.status(400).json({ ok: false, error: 'userId required' });

    const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
    const since = isoDaysAgo(days);
    const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();

    // Calls for this rep (last N days)
    let callsQ = supabase
      .from('calls')
      .select('id, account_id, score_overall, created_at, org_id')
      .eq('user_id', userId)
      .gte('created_at', since)
      .order('created_at', { ascending: false })
      .limit(5000);
    if (orgId) callsQ = callsQ.eq('org_id', orgId);
    const { data: calls, error: cErr } = await callsQ;
    if (cErr) throw cErr;

    let sum = 0, n = 0, xp = 0;
    const byAccount = new Map<string, { sum: number; n: number }>();
    for (const c of calls || []) {
      const sc = Number((c as any).score_overall);
      if (Number.isFinite(sc)) {
        sum += sc; n += 1;
        if (sc >= 70) xp += 10; // +10 XP per call >= 70
      }
      const accId = (c as any).account_id || null;
      if (accId) {
        const cur = byAccount.get(accId) || { sum: 0, n: 0 };
        if (Number.isFinite(sc)) { cur.sum += sc; cur.n += 1; }
        byAccount.set(accId, cur);
      }
    }

    // Completed drills XP (+5 each) — skip silently if table not present
    let assignsQ = supabase
      .from('coach_assignments')
      .select('status, created_at, org_id')
      .eq('assignee_user_id', userId)
      .gte('created_at', since)
      .limit(5000);
    if (orgId) assignsQ = assignsQ.eq('org_id', orgId);
    const { data: assigns, error: aErr } = await assignsQ;
    if (!aErr) {
      for (const a of assigns || []) {
        if (String((a as any).status) === 'completed') xp += 5;
      }
    }

    // Top account by avg score
    let topAccount: { account_id: string; avg_score: number; calls: number; name?: string } | null = null;
    for (const [account_id, agg] of byAccount.entries()) {
      const avg = agg.n ? agg.sum / agg.n : 0;
      if (!topAccount || avg > topAccount.avg_score) topAccount = { account_id, avg_score: avg, calls: agg.n };
    }
    if (topAccount) {
      const { data: acc } = await supabase
        .from('accounts')
        .select('id, name, domain')
        .eq('id', topAccount.account_id)
        .maybeSingle();
      if (acc) topAccount.name = (acc as any).name || (acc as any).domain || topAccount.account_id;
    }

    // A few recent calls for the panel
    const recent = (calls || []).slice(0, 10).map(c => ({
      id: (c as any).id,
      created_at: (c as any).created_at,
      score_overall: (c as any).score_overall,
      account_id: (c as any).account_id,
    }));

    res.set('Cache-Control', 'public, max-age=15');
    return res.json({
      ok: true,
      userId,
      days,
      avg_score: n ? (sum / n) : null,
      calls: n,
      xp,
      topAccount,
      recent,
      since,
    });
  } catch (err: any) {
    console.error('GET /v1/dashboard/rep-summary error:', err);
    res.status(500).json({ ok: false, error: err?.message || 'rep_summary_failed' });
  }
});

export default router;