import express, { Request, Response } from 'express';
import { createClient } from '@supabase/supabase-js';

const router = express.Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

function getUserId(req: Request) {
  return String(req.header('x-user-id') || '').trim();
}

async function getRequesterContext(userId: string) {
  if (!userId) return null;

  const { data } = await supa
    .from('users')
    .select(`
      id,
      role,
      company_id,
      office_id,
      manager_id,
      is_admin
    `)
    .eq('id', userId)
    .maybeSingle();

  return data || null;
}

function buildAccountVisibilityFilter(query: any, requester: any) {
  if (!requester?.company_id) {
    return query;
  }

  query = query.eq('company_id', requester.company_id);

  if (requester.role === 'office_manager' && requester.office_id) {
    query = query.eq('office_id', requester.office_id);
  }

  return query;
}

/* ----------------------------------------------------------------
   GET /v1/accounts
----------------------------------------------------------------- */
router.get('/', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);

    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({
        ok: false,
        error: 'missing_company_context',
      });
    }

    let query = supa
      .from('accounts')
      .select(`
        id,
        name,
        domain,
        company_id,
        office_id,
        created_at,
        updated_at
      `)
      .order('updated_at', { ascending: false })
      .limit(100);

    query = buildAccountVisibilityFilter(query, requester);

    const { data: accounts, error } = await query;

    if (error) {
      throw error;
    }

    const enrichedAccounts = await Promise.all(
      (accounts || []).map(async (account: any) => {
        const [contactsRes, callsRes] = await Promise.all([
          supa
            .from('contacts')
            .select('id', { count: 'exact', head: true })
            .eq('account_id', account.id),

          supa
            .from('calls')
            .select('id, created_at', { count: 'exact' })
            .eq('account_id', account.id)
            .order('created_at', { ascending: false })
            .limit(1),
        ]);

        return {
          ...account,

          stats: {
            contacts:
              contactsRes.count || 0,

            calls:
              callsRes.count || 0,
          },

          latest_activity_at:
            callsRes.data?.[0]?.created_at ||
            account.updated_at ||
            account.created_at,
        };
      })
    );

    return res.json({
      ok: true,
      accounts: enrichedAccounts,
    });
  } catch (e: any) {
    console.error('[accounts:list] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'accounts_list_failed',
    });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/:id
----------------------------------------------------------------- */
router.get('/:id', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);

    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({
        ok: false,
        error: 'missing_company_context',
      });
    }

    const accountId = String(req.params.id || '').trim();

    if (!accountId) {
      return res.status(400).json({
        ok: false,
        error: 'invalid_account_id',
      });
    }

    let accountQuery = supa
      .from('accounts')
      .select(`
        id,
        name,
        domain,
        company_id,
        office_id,
        created_at,
        updated_at
      `)
      .eq('id', accountId)
      .limit(1);

    accountQuery = buildAccountVisibilityFilter(accountQuery, requester);

    const { data: accountRows, error: accountError } = await accountQuery;

    if (accountError) {
      throw accountError;
    }

    const account = accountRows?.[0];

    if (!account) {
      return res.status(404).json({
        ok: false,
        error: 'account_not_found',
      });
    }

    const [contactsRes, callsRes] = await Promise.all([
      supa
        .from('contacts')
        .select(`
          id,
          name,
          email,
          company,
          role,
          created_at,
          updated_at
        `)
        .eq('account_id', account.id)
        .order('updated_at', { ascending: false })
        .limit(100),

      supa
        .from('calls')
        .select(`
          id,
          title,
          score,
          duration_seconds,
          created_at,
          rep_id,
          account_id
        `)
        .eq('account_id', account.id)
        .order('created_at', { ascending: false })
        .limit(100),
    ]);

    const calls = callsRes.data || [];

    const timeline = calls.map((call: any) => ({
      type: 'call',
      id: call.id,
      title: call.title || 'Sales call',
      created_at: call.created_at,
      metadata: {
        score: call.score,
        duration_seconds: call.duration_seconds,
        rep_id: call.rep_id,
      },
    }));

    return res.json({
      ok: true,

      account: {
        ...account,

        stats: {
          contacts: contactsRes.data?.length || 0,
          calls: calls.length || 0,

          avg_score:
            calls.length > 0
              ? Math.round(
                  calls.reduce(
                    (acc: number, c: any) =>
                      acc + Number(c.score || 0),
                    0
                  ) / calls.length
                )
              : null,
        },
      },

      linked_contacts:
        contactsRes.data || [],

      linked_calls:
        calls,

      timeline,
    });
  } catch (e: any) {
    console.error('[accounts:detail] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_detail_failed',
    });
  }
});


/* ----------------------------------------------------------------
   GET /v1/accounts/:id/rescue-engine
----------------------------------------------------------------- */
router.get('/:id/rescue-engine', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);

    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({
        ok: false,
        error: 'missing_company_context',
      });
    }

    const accountId = String(req.params.id || '').trim();

    if (!accountId) {
      return res.status(400).json({
        ok: false,
        error: 'invalid_account_id',
      });
    }

    let accountQuery = supa
      .from('accounts')
      .select(`
        id,
        name,
        domain,
        company_id,
        office_id,
        owner_id,
        created_at,
        updated_at
      `)
      .eq('id', accountId)
      .limit(1);

    accountQuery = buildAccountVisibilityFilter(
      accountQuery,
      requester
    );

    const { data: accountRows, error: accountError } =
      await accountQuery;

    if (accountError) {
      throw accountError;
    }

    const account = accountRows?.[0];

    if (!account) {
      return res.status(404).json({
        ok: false,
        error: 'account_not_found',
      });
    }

    const [callsRes, contactsRes, ownerRes] = await Promise.all([
      supa
        .from('calls')
        .select(`
          id,
          score,
          created_at,
          duration_seconds,
          rep_id
        `)
        .eq('account_id', account.id)
        .order('created_at', { ascending: false })
        .limit(100),

      supa
        .from('contacts')
        .select('id', { count: 'exact', head: true })
        .eq('account_id', account.id),

      account.owner_id
        ? supa
            .from('users')
            .select('id,email,role,full_name')
            .eq('id', account.owner_id)
            .maybeSingle()
        : Promise.resolve({ data: null, error: null }),
    ]);

    const calls = callsRes.data || [];

    const avgScore =
      calls.length > 0
        ? Math.round(
            calls.reduce(
              (acc: number, c: any) =>
                acc + Number(c.score || 0),
              0
            ) / calls.length
          )
        : 0;

    const recentCalls = calls.slice(0, 5);

    const recentAvg =
      recentCalls.length > 0
        ? Math.round(
            recentCalls.reduce(
              (acc: number, c: any) =>
                acc + Number(c.score || 0),
              0
            ) / recentCalls.length
          )
        : 0;

    const hasOwner = !!account.owner_id;

    const noRecentActivity =
      calls.length === 0 ||
      (() => {
        const latest = calls[0]?.created_at;

        if (!latest) return true;

        const latestTs = new Date(latest).getTime();
        const nowTs = Date.now();

        const diffDays =
          (nowTs - latestTs) /
          (1000 * 60 * 60 * 24);

        return diffDays > 14;
      })();

    let churnRiskScore = 0;

    if (!hasOwner) churnRiskScore += 25;

    if (avgScore < 60) churnRiskScore += 35;

    if (recentAvg < 55) churnRiskScore += 20;

    if (noRecentActivity) churnRiskScore += 20;

    churnRiskScore = Math.min(100, churnRiskScore);

    const riskBand =
      churnRiskScore >= 75
        ? 'critical'
        : churnRiskScore >= 55
          ? 'high'
          : churnRiskScore >= 35
            ? 'medium'
            : 'low';

    const rescueRecommendations: string[] = [];

    if (!hasOwner) {
      rescueRecommendations.push(
        'Assign an accountable rep or manager immediately.'
      );
    }

    if (avgScore < 60) {
      rescueRecommendations.push(
        'Review recent failed calls and run replay coaching.'
      );
    }

    if (recentAvg < avgScore) {
      rescueRecommendations.push(
        'Recent performance is declining. Trigger manager intervention.'
      );
    }

    if (noRecentActivity) {
      rescueRecommendations.push(
        'No recent account activity detected. Schedule immediate outreach.'
      );
    }

    const nextBestAction =
      !hasOwner
        ? 'Assign ownership and launch rescue workflow.'
        : avgScore < 60
          ? 'Run targeted replay coaching for recent failed calls.'
          : noRecentActivity
            ? 'Re-engage account with high-priority outreach.'
            : 'Continue reinforcing account relationship momentum.';

    const managerInterventionSuggestions = [
      riskBand === 'critical'
        ? 'Escalate to senior sales leadership immediately.'
        : null,

      avgScore < 60
        ? 'Assign replay coaching sessions to account owner.'
        : null,

      !hasOwner
        ? 'Route account into manager rescue queue.'
        : null,

      noRecentActivity
        ? 'Create urgent re-engagement workflow.'
        : null,
    ].filter(Boolean);

    const saveWorkflow = {
      urgency: riskBand,

      stages:
        riskBand === 'critical'
          ? [
              'Executive review',
              'Assign rescue owner',
              'Immediate client outreach',
              'Replay coaching enforcement',
              'Daily monitoring',
            ]
          : riskBand === 'high'
            ? [
                'Manager intervention',
                'Coaching reinforcement',
                'Targeted follow-up',
                'Risk review',
              ]
            : [
                'Continue monitoring',
                'Maintain coaching consistency',
              ],
    };

    const escalationRouting = {
      route:
        riskBand === 'critical'
          ? 'executive_escalation'
          : riskBand === 'high'
            ? 'manager_intervention'
            : hasOwner
              ? 'assigned_owner'
              : 'manager_queue',

      escalation_required:
        riskBand === 'critical' ||
        riskBand === 'high',
    };

    return res.json({
      ok: true,

      account: {
        id: account.id,
        name: account.name,
        domain: account.domain,
      },

      owner: ownerRes.data || null,

      stats: {
        avg_score: avgScore,
        recent_avg_score: recentAvg,
        calls: calls.length,
        contacts: contactsRes.count || 0,
      },

      churn_risk_score: churnRiskScore,

      risk_band: riskBand,

      ai_rescue_recommendations:
        rescueRecommendations,

      next_best_action: nextBestAction,

      manager_intervention_suggestions:
        managerInterventionSuggestions,

      automated_escalation:
        escalationRouting,

      account_save_workflow:
        saveWorkflow,

      ai_summary:
        riskBand === 'critical'
          ? 'AI detected severe account instability requiring immediate intervention.'
          : riskBand === 'high'
            ? 'AI detected elevated churn risk and declining account health.'
            : riskBand === 'medium'
              ? 'AI detected moderate account risk requiring monitoring.'
              : 'AI detected stable account health with manageable risk.',
    });
  } catch (e: any) {
    console.error('[accounts:rescue-engine] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_rescue_engine_failed',
    });
  }
});

export default router;
