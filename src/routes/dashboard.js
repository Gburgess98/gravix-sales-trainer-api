import { Router } from 'express';
import { createClient } from '@supabase/supabase-js';
import { isCompanyManager, isOfficeManager, } from '../lib/permissions.ts';
const router = Router();
// ---- Supabase client (service role for server-side aggregations) ----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
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
// User context helper for hierarchy filtering
async function getUserContext(db, userId) {
    if (!userId)
        return null;
    const { data } = await db
        .from('users')
        .select('id, role, office_id, company_id, is_admin')
        .eq('id', userId)
        .maybeSingle();
    if (!data)
        return null;
    return {
        id: String(data.id),
        role: String(data.role || 'rep'),
        office_id: data.office_id || null,
        company_id: data.company_id || null,
        is_admin: Boolean(data.is_admin),
    };
}
function applyHierarchyFilters(query, user) {
    if (!user)
        return query;
    if (isOfficeManager(user)) {
        return query.eq('office_id', user.office_id);
    }
    if (isCompanyManager(user)) {
        return query.eq('company_id', user.company_id);
    }
    return query;
}
function isoDaysAgo(days) {
    const d = new Date();
    d.setDate(d.getDate() - days);
    return d.toISOString();
}
function parseVoiceScoreFromRubric(rubric) {
    if (!rubric)
        return null;
    const direct = Number(rubric?.voice_score);
    if (Number.isFinite(direct))
        return direct;
    const nestedDirect = Number(rubric?.voiceScore);
    if (Number.isFinite(nestedDirect))
        return nestedDirect;
    const vr = rubric?.voice_rubric ?? rubric?.voiceRubric ?? null;
    if (vr && typeof vr === 'object') {
        const tone = Number(vr?.tone);
        const clarity = Number(vr?.clarity);
        const confidence = Number(vr?.confidence);
        const filler = Number(vr?.filler);
        const close = Number(vr?.close);
        const vals = [tone, clarity, confidence, filler, close].filter(Number.isFinite);
        if (vals.length) {
            const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
            return Math.round(avg);
        }
    }
    return null;
}
function safeDay(value) {
    const s = String(value || '').trim();
    if (!s)
        return null;
    const d = new Date(s);
    if (!Number.isFinite(d.getTime()))
        return null;
    return d.toISOString().slice(0, 10);
}
function asObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
}
function readAnalysisJson(row) {
    return asObject(row?.analysis_json);
}
function readReviewFlags(row) {
    const analysis = readAnalysisJson(row);
    return Array.isArray(analysis?.review_flags) ? analysis.review_flags : [];
}
function readThresholdBand(row) {
    const analysis = readAnalysisJson(row);
    const band = String(analysis?.threshold_band ?? '').trim();
    return band || null;
}
function readNeedsManagerReview(row) {
    const analysis = readAnalysisJson(row);
    return Boolean(analysis?.needs_manager_review);
}
function normalizeSkillLabel(value) {
    const key = String(value || '').trim().toLowerCase();
    if (!key)
        return 'Unknown';
    if (key === 'intro')
        return 'Intro';
    if (key === 'discovery')
        return 'Discovery';
    if (key === 'objection' || key === 'objection handling')
        return 'Objection handling';
    if (key === 'close' || key === 'closing')
        return 'Closing';
    if (key === 'price_handling' || key === 'price handling')
        return 'Price handling';
    if (key === 'follow_up' || key === 'follow-up discipline' || key === 'follow up discipline')
        return 'Follow-up discipline';
    if (key === 'execution')
        return 'Execution';
    return value;
}
function inferWeakestSkillFromCall(row) {
    const analysis = readAnalysisJson(row);
    const direct = String(analysis?.weakest_skill ?? '').trim();
    if (direct && direct.toLowerCase() !== 'unknown')
        return normalizeSkillLabel(direct);
    const breakdown = asObject(analysis?.skill_breakdown);
    if (breakdown) {
        const labelMap = {
            intro: 'Intro',
            discovery: 'Discovery',
            objection: 'Objection handling',
            close: 'Closing',
            closing: 'Closing',
            price_handling: 'Price handling',
            follow_up: 'Follow-up discipline',
            execution: 'Execution',
        };
        const scored = Object.entries(breakdown)
            .map(([key, value]) => ({ key, value: Number(value) }))
            .filter((x) => Number.isFinite(x.value) && labelMap[x.key]);
        if (scored.length) {
            scored.sort((a, b) => a.value - b.value);
            return labelMap[scored[0].key];
        }
    }
    const flags = readReviewFlags(row);
    const weakness = flags.find((f) => String(f?.type || '').endsWith('_weakness'));
    if (weakness) {
        return normalizeSkillLabel(String(weakness?.section || '').trim());
    }
    return null;
}
function titleCaseLabel(value) {
    return String(value || '')
        .replace(/_/g, ' ')
        .split(' ')
        .filter(Boolean)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ');
}
function incrementMap(map, key) {
    const clean = String(key || '').trim();
    if (!clean)
        return;
    map.set(clean, (map.get(clean) ?? 0) + 1);
}
function mapToRankedList(map, limit = 10) {
    return Array.from(map.entries())
        .map(([key, count]) => ({ key, label: titleCaseLabel(key), count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);
}
// ---- GET /v1/dashboard/kpis ----
// Returns compact KPI payload for CRM Overview cards + sparklines
router.get('/kpis', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        res.set('Cache-Control', 'public, max-age=15');
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        const userId = String(req.authUserId || '').trim();
        // Defaults keep UI rendering even if queries fail
        let total_calls = 0;
        let avg_score_overall = null;
        let conversion_rate_90d = null; // fraction (0..1) if `won` exists
        const callsAnalyzed = [];
        const avgScore = [];
        const winRate = []; // percent for sparkline
        const top_accounts = [];
        const top_reps = [];
        // Prefer admin client; fall back to regular supabase (might be constrained by RLS)
        const db = sbAdmin ?? supabase;
        if (!db)
            throw new Error('No Supabase client available');
        let callQuery = db
            .from('calls')
            .select('id, created_at, status, score_overall, account_id, user_id, org_id, office_id, company_id', { count: 'exact' })
            .gte('created_at', since)
            .limit(50000);
        if (orgId)
            callQuery = callQuery.eq('org_id', orgId);
        const userContext = await getUserContext(db, userId);
        callQuery = applyHierarchyFilters(callQuery, userContext);
        const { data: calls, error: callsErr, count } = await callQuery;
        const supportsWon = Array.isArray(calls) && calls.some((c) => typeof c.won !== 'undefined');
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
            if (calls && calls.length)
                console.log('[KPI] sample ids:', calls.slice(0, 3).map(c => c.id));
        }
        total_calls = calls?.length ?? 0;
        const scored = (calls || []).filter(c => c.status === 'scored' && typeof c.score_overall === 'number');
        if (scored.length) {
            const sum = scored.reduce((s, c) => s + (c.score_overall || 0), 0);
            avg_score_overall = Math.round(sum / scored.length);
        }
        // If a boolean `won` field exists, compute a simple 90d conversion
        if (supportsWon && total_calls > 0) {
            const wonCount = (calls || []).filter((c) => c.won === true).length;
            conversion_rate_90d = Math.round((wonCount / total_calls) * 100) / 100;
        }
        else {
            conversion_rate_90d = null;
        }
        // Day rollups for sparklines
        const dayMap = {};
        for (const c of calls || []) {
            const key = new Date(c.created_at).toISOString().slice(0, 10);
            if (!dayMap[key])
                dayMap[key] = { calls: 0, scoredCount: 0, scoredSum: 0, won: 0 };
            dayMap[key].calls += 1;
            const sc = Number(c.score_overall);
            if (c.status === 'scored' && Number.isFinite(sc)) {
                dayMap[key].scoredCount += 1;
                dayMap[key].scoredSum += sc;
            }
            if (supportsWon && c.won === true)
                dayMap[key].won += 1;
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
        const acctAgg = {};
        for (const c of scored) {
            const a = String(c.account_id || '');
            if (!a)
                continue;
            acctAgg[a] = acctAgg[a] || { sum: 0, n: 0 };
            acctAgg[a].sum += Number(c.score_overall) || 0;
            acctAgg[a].n += 1;
        }
        const acctRows = Object.entries(acctAgg)
            .map(([account_id, v]) => ({ account_id, avg_score: Math.round(v.sum / v.n) }))
            .sort((a, b) => b.avg_score - a.avg_score)
            .slice(0, 8);
        top_accounts.push(...acctRows);
        // Top reps by avg score
        const repAgg = {};
        for (const c of scored) {
            const u = String(c.user_id || '');
            if (!u)
                continue;
            repAgg[u] = repAgg[u] || { sum: 0, n: 0 };
            repAgg[u].sum += Number(c.score_overall) || 0;
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
    }
    catch (err) {
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
// --- FLAG ANALYTICS (Day 65 — upgraded) ---
router.get("/flags/summary", async (req, res) => {
    try {
        const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
        const days = Number(req.query.days || 7);
        const since = new Date(Date.now() - days * 86400000).toISOString();
        const { data, error } = await svc
            .from("crm_activities")
            // Read from meta JSONB — flag_key/section/severity columns may not exist yet (see
            // sql/20260528_crm_activities_flag_columns.sql). After that migration runs the values
            // will also appear as top-level columns, but meta is always populated.
            .select("meta, rep_id, created_at")
            .eq("type", "review_flag")
            .gte("created_at", since);
        if (error)
            throw error;
        const rows = data || [];
        // --- AGG MAPS ---
        const sectionMap = new Map();
        const severityMap = new Map();
        const repMap = new Map();
        for (const r of rows) {
            const section = normalizeSkillLabel(String(r.meta?.flag_section || "unknown"));
            const severity = String(r.meta?.flag_severity || "unknown").toLowerCase();
            const rep = String(r.rep_id || "unknown");
            incrementMap(sectionMap, section);
            incrementMap(severityMap, severity);
            incrementMap(repMap, rep);
        }
        // --- RANKED OUTPUT (for charts) ---
        const bySection = mapToRankedList(sectionMap, 8);
        const bySeverity = mapToRankedList(severityMap, 5);
        // --- TOP PROBLEM AREA ---
        const topSection = bySection.length
            ? { section: bySection[0].label, count: bySection[0].count }
            : null;
        // --- REPS AT RISK ---
        const repsAtRisk = Array.from(repMap.entries())
            .filter(([_, count]) => count >= 3)
            .map(([rep_id, count]) => ({ rep_id, count }))
            .sort((a, b) => b.count - a.count)
            .slice(0, 10);
        return res.json({
            ok: true,
            window_days: days,
            totals: {
                flags: rows.length,
            },
            breakdown: {
                by_section: bySection,
                by_severity: bySeverity,
            },
            insights: {
                top_problem_area: topSection,
                reps_at_risk: repsAtRisk,
            },
        });
    }
    catch (e) {
        console.error("[flags/summary] error", e);
        res.status(500).json({ ok: false, error: e.message });
    }
});
// 🔥 COMPANY WEAKNESS AGGREGATION (Day 66)
async function getCompanyWeaknessAggregation(supa) {
    const { data, error } = await supa
        .from("crm_activities")
        .select("meta, rep_id, created_at")
        .eq("type", "review_flag")
        .limit(5000);
    if (error)
        throw error;
    const bySection = {};
    for (const row of data || []) {
        const m = row.meta || {};
        const section = m.flag_section || "general";
        const severity = m.flag_severity || "low";
        if (!bySection[section]) {
            bySection[section] = {
                section,
                total_failures: 0,
                critical_count: 0,
                reps_affected: new Set(),
                last_seen: row.created_at,
            };
        }
        const item = bySection[section];
        item.total_failures++;
        item.reps_affected.add(row.rep_id);
        if (severity === "critical") {
            item.critical_count++;
        }
        if (new Date(row.created_at) > new Date(item.last_seen)) {
            item.last_seen = row.created_at;
        }
    }
    const ranked = Object.values(bySection)
        .map((s) => ({
        ...s,
        reps_affected_count: s.reps_affected.size,
        priority: (s.total_failures * 2) +
            (s.critical_count * 5) +
            (s.reps_affected.size * 3),
    }))
        .sort((a, b) => b.priority - a.priority);
    return ranked;
}
// 🔥 API: Company Weakness
router.get("/company-weakness", async (req, res) => {
    try {
        const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
        const data = await getCompanyWeaknessAggregation(supa);
        return res.json({
            ok: true,
            weaknesses: data,
            top_problem: data[0] || null,
        });
    }
    catch (e) {
        console.error("[company.weakness] error", e);
        res.status(500).json({ ok: false, error: e.message });
    }
});
// 🔥 REP IMPROVEMENT TRACKING (Day 66)
router.get("/rep-improvement", async (req, res) => {
    try {
        const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
        const repId = String(req.query.rep_id || "").trim();
        if (!repId) {
            return res.status(400).json({ ok: false, error: "rep_id required" });
        }
        const days = Number(req.query.days || 30);
        const since = new Date(Date.now() - days * 86400000).toISOString();
        const { data, error } = await supa
            .from("calls")
            .select("created_at, score_overall")
            .eq("user_id", repId)
            .gte("created_at", since)
            .order("created_at", { ascending: true });
        if (error)
            throw error;
        const dayAgg = {};
        for (const row of data || []) {
            const score = Number(row.score_overall);
            if (!Number.isFinite(score))
                continue;
            const day = new Date(row.created_at).toISOString().slice(0, 10);
            if (!dayAgg[day]) {
                dayAgg[day] = { total: 0, count: 0 };
            }
            dayAgg[day].total += score;
            dayAgg[day].count += 1;
        }
        const trend = Object.keys(dayAgg)
            .sort()
            .map((date) => ({
            date,
            avg_score: Math.round(dayAgg[date].total / dayAgg[date].count),
        }));
        const first = trend[0]?.avg_score ?? null;
        const last = trend[trend.length - 1]?.avg_score ?? null;
        const improvement = first !== null && last !== null ? last - first : null;
        return res.json({
            ok: true,
            rep_id: repId,
            trend,
            improvement,
            start_score: first,
            current_score: last,
        });
    }
    catch (e) {
        console.error("[rep.improvement] error", e);
        res.status(500).json({ ok: false, error: e.message });
    }
});
// ---- GET /v1/dashboard/reporting-summary ----
// Main manager reporting read layer
// Query params: days=7&orgId=<uuid>
router.get('/reporting-summary', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '7'), 10) || 7));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        const userId = String(req.authUserId || '').trim();
        const db = sbAdmin ?? supabase;
        if (!db)
            throw new Error('No Supabase client available');
        let callsQuery = db
            .from('calls')
            .select('id,user_id,org_id,office_id,company_id,created_at,score_overall,analysis_json')
            .gte('created_at', since)
            .order('created_at', { ascending: false })
            .limit(50000);
        if (orgId)
            callsQuery = callsQuery.eq('org_id', orgId);
        const userContext = await getUserContext(db, userId);
        callsQuery = applyHierarchyFilters(callsQuery, userContext);
        const { data: calls, error: callsErr } = await callsQuery;
        if (callsErr)
            throw callsErr;
        let coachAssignmentsQuery = db
            .from('coach_assignments')
            .select('id,assignee_user_id,status,created_at,source,meta')
            .gte('created_at', since)
            .order('created_at', { ascending: false })
            .limit(50000);
        const { data: coachAssignments, error: coachErr } = await coachAssignmentsQuery;
        if (coachErr && !(String(coachErr?.message || '').toLowerCase().includes('does not exist'))) {
            throw coachErr;
        }
        const today = new Date().toISOString().slice(0, 10);
        const callRows = (calls || []);
        const assignmentRows = (coachAssignments || []);
        const totalCalls = callRows.length;
        const scoredCalls = callRows.filter((row) => Number.isFinite(Number(row?.score_overall))).length;
        const criticalCallsToday = callRows.filter((row) => {
            const day = safeDay(row?.created_at);
            return day === today && (readThresholdBand(row) === 'critical' || readNeedsManagerReview(row));
        }).length;
        const criticalCallsThisWeek = callRows.filter((row) => {
            return readThresholdBand(row) === 'critical' || readNeedsManagerReview(row);
        }).length;
        const flaggedCallsThisWeek = callRows.filter((row) => {
            return readReviewFlags(row).length > 0 || Boolean(readThresholdBand(row)) || readNeedsManagerReview(row);
        }).length;
        const flaggedCallRate = totalCalls ? Math.round((flaggedCallsThisWeek / totalCalls) * 100) : 0;
        // --- AUTO / MANUAL ASSIGNMENTS ---
        let autoAssignmentsCreated = 0;
        let manualAssignmentsCreated = 0;
        // NEW: track linkage to flags
        let assignmentsFromCriticalFlags = 0;
        let assignmentsFromLowFlags = 0;
        for (const row of assignmentRows) {
            const source = String(row?.source ?? row?.meta?.source ?? '').trim().toLowerCase();
            const meta = row?.meta || {};
            if (source === 'flagged_call_auto') {
                autoAssignmentsCreated++;
                // 🔥 NEW — understand WHY assignment was created
                const flagSeverity = String(meta?.flag_severity || '').toLowerCase();
                if (flagSeverity === 'critical') {
                    assignmentsFromCriticalFlags++;
                }
                else if (flagSeverity === 'low') {
                    assignmentsFromLowFlags++;
                }
            }
            else {
                manualAssignmentsCreated++;
            }
        }
        const assignmentAutoRate = assignmentRows.length
            ? Math.round((autoAssignmentsCreated / assignmentRows.length) * 100)
            : 0;
        const completedAssignments = assignmentRows.filter((row) => String(row?.status || '').toLowerCase() === 'completed').length;
        const assignmentCompletionRate = assignmentRows.length
            ? Math.round((completedAssignments / assignmentRows.length) * 100)
            : 0;
        const openAssignments = assignmentRows.filter((row) => String(row?.status || '').toLowerCase() !== 'completed').length;
        // --- NEW: FLAG SECTION BREAKDOWN (for charts + decisions) ---
        const flagSectionCounts = new Map();
        for (const row of callRows) {
            const flags = readReviewFlags(row);
            for (const flag of flags) {
                const section = normalizeSkillLabel(String(flag?.section || 'unknown'));
                incrementMap(flagSectionCounts, section);
            }
        }
        const weakestSkillCounts = new Map();
        const reviewFlagTypeCounts = new Map();
        const reviewFlagSeverityCounts = new Map();
        for (const row of callRows) {
            const skill = inferWeakestSkillFromCall(row);
            if (skill)
                incrementMap(weakestSkillCounts, skill);
            for (const flag of readReviewFlags(row)) {
                incrementMap(reviewFlagTypeCounts, String(flag?.type || 'unknown'));
                incrementMap(reviewFlagSeverityCounts, String(flag?.severity || 'unknown'));
            }
        }
        const weakestSkills = mapToRankedList(weakestSkillCounts, 8);
        const flagsBySection = mapToRankedList(flagSectionCounts, 8);
        const weakestTeamSkill = weakestSkills.length
            ? { skill: weakestSkills[0].label, count: weakestSkills[0].count }
            : null;
        const reviewFlagsByType = mapToRankedList(reviewFlagTypeCounts, 12);
        const reviewFlagsBySeverity = mapToRankedList(reviewFlagSeverityCounts, 6);
        const helpByRep = new Map();
        for (const row of callRows) {
            const repId = String(row?.user_id || '').trim();
            if (!repId)
                continue;
            const item = helpByRep.get(repId) || {
                rep_id: repId,
                flagged_calls: 0,
                critical_calls: 0,
                avg_score_sum: 0,
                avg_score_n: 0,
                open_assignments: 0,
                skill_counts: new Map(),
            };
            const score = Number(row?.score_overall);
            if (Number.isFinite(score)) {
                item.avg_score_sum += score;
                item.avg_score_n += 1;
            }
            const flagged = readReviewFlags(row).length > 0 || Boolean(readThresholdBand(row)) || readNeedsManagerReview(row);
            if (flagged)
                item.flagged_calls += 1;
            if (readThresholdBand(row) === 'critical' || readNeedsManagerReview(row))
                item.critical_calls += 1;
            const skill = inferWeakestSkillFromCall(row);
            if (skill)
                incrementMap(item.skill_counts, skill);
            helpByRep.set(repId, item);
        }
        for (const row of assignmentRows) {
            const repId = String(row?.assignee_user_id || '').trim();
            if (!repId)
                continue;
            const item = helpByRep.get(repId) || {
                rep_id: repId,
                flagged_calls: 0,
                critical_calls: 0,
                avg_score_sum: 0,
                avg_score_n: 0,
                open_assignments: 0,
                skill_counts: new Map(),
            };
            const status = String(row?.status || '').toLowerCase();
            if (status !== 'completed')
                item.open_assignments += 1;
            helpByRep.set(repId, item);
        }
        const repsNeedingHelp = Array.from(helpByRep.values())
            .map((item) => {
            const topSkill = mapToRankedList(item.skill_counts, 1)[0] || null;
            return {
                rep_id: item.rep_id,
                flagged_calls: item.flagged_calls,
                critical_calls: item.critical_calls,
                open_assignments: item.open_assignments,
                avg_score: item.avg_score_n ? Math.round(item.avg_score_sum / item.avg_score_n) : null,
                weakest_skill: topSkill ? topSkill.label : null,
            };
        })
            .filter((item) => item.critical_calls > 0 || item.flagged_calls > 0 || item.open_assignments >= 2 || (typeof item.avg_score === 'number' && item.avg_score < 65))
            .sort((a, b) => {
            if (b.critical_calls !== a.critical_calls)
                return b.critical_calls - a.critical_calls;
            if (b.flagged_calls !== a.flagged_calls)
                return b.flagged_calls - a.flagged_calls;
            if (b.open_assignments !== a.open_assignments)
                return b.open_assignments - a.open_assignments;
            return (a.avg_score ?? 999) - (b.avg_score ?? 999);
        })
            .slice(0, 10);
        return res.json({
            ok: true,
            days,
            since,
            totals: {
                calls: totalCalls,
                scored_calls: scoredCalls,
                assignments: assignmentRows.length,
                open_assignments: openAssignments,
                completed_assignments: completedAssignments,
            },
            critical_calls_today: criticalCallsToday,
            critical_calls_this_week: criticalCallsThisWeek,
            flagged_calls_this_week: flaggedCallsThisWeek,
            flagged_call_rate: flaggedCallRate,
            auto_assignments_created: autoAssignmentsCreated,
            manual_assignments_created: manualAssignmentsCreated,
            // 🔥 NEW — THIS IS HUGE FOR MANAGERS
            assignments_from_critical_flags: assignmentsFromCriticalFlags,
            assignments_from_low_flags: assignmentsFromLowFlags,
            assignment_auto_rate: assignmentAutoRate,
            assignment_completion_rate: assignmentCompletionRate,
            weakest_team_skill: weakestTeamSkill,
            weakest_skills: weakestSkills,
            // 🔥 NEW — powers charts + manager decisions
            flags_by_section: flagsBySection,
            review_flags_by_type: reviewFlagsByType,
            review_flags_by_severity: reviewFlagsBySeverity,
            reps_needing_help: repsNeedingHelp,
        });
    }
    catch (err) {
        console.error('GET /v1/dashboard/reporting-summary error:', err);
        return res.status(500).json({ ok: false, error: err?.message || 'reporting_summary_failed' });
    }
});
// ---- GET /v1/dashboard/leaderboard ----
// Query params: days=90&limit=10&minCalls=3
router.get('/leaderboard', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
        const limit = Math.max(1, Math.min(100, parseInt(String(req.query.limit ?? '10'), 10) || 10));
        const minCalls = Math.max(0, Math.min(1000, parseInt(String(req.query.minCalls ?? '3'), 10) || 3));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        const userId = String(req.authUserId || '').trim();
        // Pull all scored calls since cutoff; aggregate in Node for portability
        let callQuery = supabase
            .from('calls')
            .select('user_id, score_overall, created_at, org_id, office_id, company_id')
            .gte('created_at', since)
            .not('score_overall', 'is', null)
            .limit(50000);
        if (orgId)
            callQuery = callQuery.eq('org_id', orgId);
        const userContext = await getUserContext(supabase, userId);
        callQuery = applyHierarchyFilters(callQuery, userContext);
        const { data: calls, error: callsErr } = await callQuery;
        if (callsErr)
            throw callsErr;
        const byUser = new Map();
        for (const c of calls || []) {
            const uid = c.user_id;
            const s = Number(c.score_overall);
            if (!uid || Number.isNaN(s))
                continue;
            const acc = byUser.get(uid) || { user_id: uid, sum: 0, calls: 0, xp: 0 };
            acc.sum += s;
            acc.calls += 1;
            if (s >= 70)
                acc.xp += 10; // +10 XP per call >= 70
            byUser.set(uid, acc);
        }
        // Completed drills per user (assignee)
        const { data: assigns, error: aErr } = await supabase
            .from('assignments')
            .select('rep_id, status, created_at')
            .gte('created_at', since)
            .limit(50000);
        if (aErr && aErr.message && !/relation .* does not exist/i.test(aErr.message)) {
            // If table exists but error happened, surface it; if table missing in dev, just skip XP add
            throw aErr;
        }
        for (const a of assigns || []) {
            const uid = String(a.rep_id || '');
            const st = String(a.status || '');
            if (!uid)
                continue;
            const acc = byUser.get(uid) || { user_id: uid, sum: 0, calls: 0, xp: 0 };
            if (st === 'completed')
                acc.xp += 5; // +5 XP per completed drill
            byUser.set(uid, acc);
        }
        // Name hydration: profiles(id, display_name) → users(id, full_name, email) → fallback "Rep"
        const repIds = Array.from(byUser.keys()).filter(Boolean);
        const nameById = new Map();
        if (repIds.length) {
            const { data: profs } = await supabase
                .from('profiles')
                .select('id, display_name')
                .in('id', repIds);
            for (const p of profs || []) {
                const id = String(p.id);
                const name = p.display_name || '';
                if (id && name)
                    nameById.set(id, name);
            }
            const missing = repIds.filter(id => !nameById.has(id));
            if (missing.length) {
                const { data: users } = await supabase
                    .from('users')
                    .select('id, full_name, email')
                    .in('id', missing);
                for (const u of users || []) {
                    const id = String(u.id);
                    const name = u.full_name || u.email || 'Rep';
                    if (id && !nameById.has(id))
                        nameById.set(id, name);
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
            if (Math.abs(d) > 1e-9)
                return d;
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
    }
    catch (err) {
        console.error('GET /v1/dashboard/leaderboard error:', err);
        res.status(500).json({ ok: false, error: err?.message || 'leaderboard_failed' });
    }
});
// --- Dashboard: Rep Summary (avg score, calls, XP, top account) ---
router.get('/rep-summary', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        const userId = String(req.query.userId || '').trim();
        if (!userId)
            return res.status(400).json({ ok: false, error: 'userId required' });
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '90'), 10) || 90));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        // Calls for this rep (last N days)
        let callsQ = supabase
            .from('calls')
            .select('id, account_id, score_overall, created_at, org_id, office_id, company_id')
            .eq('user_id', userId)
            .gte('created_at', since)
            .order('created_at', { ascending: false })
            .limit(5000);
        if (orgId)
            callsQ = callsQ.eq('org_id', orgId);
        const { data: calls, error: cErr } = await callsQ;
        if (cErr)
            throw cErr;
        let sum = 0, n = 0, xp = 0;
        const byAccount = new Map();
        for (const c of calls || []) {
            const sc = Number(c.score_overall);
            if (Number.isFinite(sc)) {
                sum += sc;
                n += 1;
                if (sc >= 70)
                    xp += 10; // +10 XP per call >= 70
            }
            const accId = c.account_id || null;
            if (accId) {
                const cur = byAccount.get(accId) || { sum: 0, n: 0 };
                if (Number.isFinite(sc)) {
                    cur.sum += sc;
                    cur.n += 1;
                }
                byAccount.set(accId, cur);
            }
        }
        // Completed drills XP (+5 each) — skip silently if table not present
        let assignsQ = supabase
            .from('assignments')
            .select('status, created_at, org_id')
            .eq('rep_id', userId)
            .gte('created_at', since)
            .limit(5000);
        if (orgId)
            assignsQ = assignsQ.eq('org_id', orgId);
        const { data: assigns, error: aErr } = await assignsQ;
        if (!aErr) {
            for (const a of assigns || []) {
                if (String(a.status) === 'completed')
                    xp += 5;
            }
        }
        // Top account by avg score
        let topAccount = null;
        for (const [account_id, agg] of byAccount.entries()) {
            const avg = agg.n ? agg.sum / agg.n : 0;
            if (!topAccount || avg > topAccount.avg_score)
                topAccount = { account_id, avg_score: avg, calls: agg.n };
        }
        if (topAccount) {
            const { data: acc } = await supabase
                .from('accounts')
                .select('id, name, domain')
                .eq('id', topAccount.account_id)
                .maybeSingle();
            if (acc)
                topAccount.name = acc.name || acc.domain || topAccount.account_id;
        }
        // A few recent calls for the panel
        const recent = (calls || []).slice(0, 10).map(c => ({
            id: c.id,
            created_at: c.created_at,
            score_overall: c.score_overall,
            account_id: c.account_id,
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
    }
    catch (err) {
        console.error('GET /v1/dashboard/rep-summary error:', err);
        res.status(500).json({ ok: false, error: err?.message || 'rep_summary_failed' });
    }
});
// ---- GET /v1/dashboard/voice-score-summary ----
// Query params: days=30&repId=<uuid>
router.get('/voice-score-summary', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '30'), 10) || 30));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        const repIdRaw = String(req.query.repId ?? '').trim();
        const repId = repIdRaw || null;
        let q = supabase
            .from('call_scores')
            // office_id / company_id do not exist on call_scores (schema drift); unused here.
            .select('call_id, user_id, created_at, rubric')
            .gte('created_at', since)
            .order('created_at', { ascending: false })
            .limit(10000);
        if (repId)
            q = q.eq('user_id', repId);
        const { data: scoreRows, error: scoreErr } = await q;
        if (scoreErr)
            throw scoreErr;
        const rows = (scoreRows || []);
        let latest_voice_score = null;
        let latest_close_strength = null;
        let fillerDensitySum = 0;
        let fillerDensityCount = 0;
        let callsReviewedThisWeek = 0;
        const weekCutoff = isoDaysAgo(7);
        for (const row of rows) {
            const rubric = row?.rubric ?? null;
            const voiceScore = parseVoiceScoreFromRubric(rubric);
            if (latest_voice_score === null && Number.isFinite(voiceScore)) {
                latest_voice_score = Number(voiceScore);
            }
            const closeStrength = Number(rubric?.voice_rubric?.close ??
                rubric?.voiceRubric?.close ??
                rubric?.close);
            if (latest_close_strength === null && Number.isFinite(closeStrength)) {
                latest_close_strength = Math.round(closeStrength);
            }
            const fillerDensity = Number(rubric?.review_tags?.filler_density ??
                rubric?.reviewTags?.filler_density ??
                rubric?.review_tags?.fillerDensity ??
                rubric?.reviewTags?.fillerDensity);
            if (Number.isFinite(fillerDensity)) {
                fillerDensitySum += fillerDensity;
                fillerDensityCount += 1;
            }
            const createdAt = String(row?.created_at || '');
            if (createdAt && createdAt >= weekCutoff) {
                callsReviewedThisWeek += 1;
            }
        }
        const average_filler_trend = fillerDensityCount
            ? Number((fillerDensitySum / fillerDensityCount).toFixed(4))
            : null;
        res.set('Cache-Control', 'public, max-age=15');
        return res.json({
            ok: true,
            scope: repId ? 'rep' : 'team',
            rep_id: repId,
            org_id: orgId || null,
            latest_voice_score,
            average_filler_trend,
            latest_close_strength,
            calls_reviewed_this_week: callsReviewedThisWeek,
            since,
        });
    }
    catch (err) {
        console.error('GET /v1/dashboard/voice-score-summary error:', err);
        return res.status(500).json({ ok: false, error: err?.message || 'voice_score_summary_failed' });
    }
});
// ---- GET /v1/dashboard/voice-score-trend ----
// Query params: days=30&repId=<uuid>
router.get('/voice-score-trend', async (req, res) => {
    try {
        if (!supabase)
            throw new Error('Supabase not configured');
        const days = Math.max(1, Math.min(365, parseInt(String(req.query.days ?? '30'), 10) || 30));
        const since = isoDaysAgo(days);
        const orgId = (req.query.orgId ? String(req.query.orgId) : '').trim();
        const repIdRaw = String(req.query.repId ?? '').trim();
        const repId = repIdRaw || null;
        // Prefer snapshots from call_scores because Day 55 writes voice data there even if
        // dedicated columns do not exist on calls yet.
        // office_id / company_id do not exist on call_scores (schema drift); unused here.
        let q = supabase
            .from('call_scores')
            .select('call_id, user_id, created_at, rubric')
            .gte('created_at', since)
            .order('created_at', { ascending: true })
            .limit(10000);
        if (repId)
            q = q.eq('user_id', repId);
        const { data: scoreRows, error: scoreErr } = await q;
        if (scoreErr)
            throw scoreErr;
        const dayAgg = {};
        for (const row of scoreRows || []) {
            const rubric = row?.rubric;
            const voiceScore = parseVoiceScoreFromRubric(rubric);
            if (!Number.isFinite(voiceScore))
                continue;
            const day = safeDay(row?.created_at);
            if (!day)
                continue;
            if (!dayAgg[day])
                dayAgg[day] = { total: 0, count: 0 };
            dayAgg[day].total += Number(voiceScore);
            dayAgg[day].count += 1;
        }
        const trend = Object.keys(dayAgg)
            .sort()
            .map((date) => ({
            date,
            voice_score: Math.round(dayAgg[date].total / dayAgg[date].count),
        }));
        const latest_avg = trend.length ? trend[trend.length - 1].voice_score : null;
        res.set('Cache-Control', 'public, max-age=15');
        return res.json({
            ok: true,
            scope: repId ? 'rep' : 'team',
            rep_id: repId,
            org_id: orgId || null,
            trend,
            latest_avg,
            since,
        });
    }
    catch (err) {
        console.error('GET /v1/dashboard/voice-score-trend error:', err);
        return res.status(500).json({ ok: false, error: err?.message || 'voice_score_trend_failed' });
    }
});
export default router;
