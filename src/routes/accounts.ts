import express, { Request, Response, NextFunction } from 'express';
import { createClient } from '@supabase/supabase-js';
import { logAuditEvent, AUDIT_ACTIONS } from '../lib/audit.ts';

const router = express.Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

// Day 278 — reject a non-UUID path parameter with a stable 400 *before* any DB
// query, so a bad account/task id can't reach Postgres and surface as a 500 with
// "invalid input syntax for type uuid" (which leaks the DB type and turns a client
// mistake into a server error). Mirrors the Calls detail route's UUID_RE guard.
// Registered as router.param so it covers every Accounts route with an :id / :taskId
// path segment. Valid-id resolution and tenant isolation are unchanged.
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function requireUuidParam(req: Request, res: Response, next: NextFunction, value: string) {
  if (!UUID_RE.test(String(value ?? ''))) {
    return res.status(400).json({ ok: false, error: 'invalid id' });
  }
  next();
}
router.param('id', requireUuidParam);
router.param('taskId', requireUuidParam);

function getUserId(req: Request) {
  return String(
    (req as any).userId ||
    req.header("x-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

async function getRequesterContext(userId: string) {
  if (!userId) return null;

  const { data: userRow } = await supa
    .from('users')
    .select(`
    id,
    company_id,
    office_id,
    role,
    manager_id,
    is_admin
  `)
    .eq('id', userId)
    .maybeSingle();

  if (userRow?.company_id) return userRow;

  // Day 277 — auth-first identities (created via the supported Admin Auth API +
  // reps bridge, with no legacy public.users row) carry their company on the
  // reps row. Fall back to it so company-scoped Accounts reads/writes resolve
  // instead of 403 "missing_company_context". Mirrors resolveCompanyId() (used by
  // the create handler) and routes/team.ts. Tenant isolation is unchanged: the
  // resolved company is the identity's own reps.company_id.
  const { data: repRow } = await supa
    .from('reps')
    .select('company_id, manager_id, tier')
    .eq('id', userId)
    .maybeSingle();

  if ((repRow as any)?.company_id) {
    return {
      id: userId,
      company_id: String((repRow as any).company_id),
      office_id: null,
      role: (userRow as any)?.role ?? (repRow as any).tier ?? null,
      manager_id: (repRow as any).manager_id ?? (userRow as any)?.manager_id ?? null,
      is_admin: (userRow as any)?.is_admin ?? false,
    };
  }

  return userRow || null;
}

/**
 * Day 280 — account_escalations / account_coaching_actions have user-FK columns
 * (triggered_by, assigned_manager_id, created_by, assigned_to) that REFERENCE
 * public.users(id). Auth-first identities (Admin Auth API + reps bridge) have no
 * public.users row, so writing their auth id into those columns violates the FK.
 * These columns are nullable, so resolve a candidate id to a real users.id or null.
 * Legacy users-row identities are unaffected (their id resolves to itself).
 */
async function existingUserId(id: string | null | undefined): Promise<string | null> {
  const v = String(id ?? '').trim();
  if (!v) return null;
  const { data } = await supa.from('users').select('id').eq('id', v).maybeSingle();
  return data ? v : null;
}

function buildAccountVisibilityFilter(query: any, requester: any) {
  if (!requester?.company_id) {
    return query;
  }

  query = query.eq('org_id', requester.company_id);

  return query;
}

/* ----------------------------------------------------------------
   Day 284 — account ownership. `accounts.owner_id` (uuid, nullable, no FK) holds a
   `reps.id`. Assignment validates the rep is in the requester's company (server
   authority — the client cannot pick a foreign owner); the read path enriches
   `owner_id` → { id, full_name, email } so the UI reflects it after refresh.
----------------------------------------------------------------- */
async function resolveOwnerRep(
  ownerId: string | null | undefined,
  companyId: string
): Promise<{ id: string; full_name: string | null; email: string | null } | null> {
  const id = ownerId ? String(ownerId).trim() : '';
  if (!id) return null;

  const { data } = await supa
    .from('reps')
    .select('id, company_id, display_name, name, first_name, last_name, email')
    .eq('id', id)
    .maybeSingle();

  const rep: any = data;
  if (!rep || String(rep.company_id) !== String(companyId)) return null;

  const full_name =
    rep.display_name ||
    rep.name ||
    [rep.first_name, rep.last_name].filter(Boolean).join(' ').trim() ||
    null;

  return { id: rep.id, full_name, email: rep.email ?? null };
}

/* ----------------------------------------------------------------
   Day 285 — an account's contacts are canonical in `crm_contacts` (single
   nullable `account_id` FK; see sql/20260814_crm_contacts_account_link.sql), not
   the disconnected legacy `contacts` table. Reading by account_id is
   company-safe: the account is already company-gated before this runs, and a link
   can only be created against an account in the linker's company. Schema-tolerant:
   before the migration lands the column is absent — return [] rather than 500.
----------------------------------------------------------------- */
async function fetchAccountContacts(accountId: string): Promise<any[]> {
  try {
    const { data, error } = await supa
      .from('crm_contacts')
      .select('id, first_name, last_name, email, company, account_id, created_at')
      .eq('account_id', accountId)
      .order('created_at', { ascending: false })
      .limit(100);
    if (error) throw error;
    return (data || []).map((c: any) => ({
      id: c.id,
      name: [c.first_name, c.last_name].filter(Boolean).join(' ').trim() || null,
      email: c.email ?? null,
      company: c.company ?? null,
      role: null,
      account_id: c.account_id ?? null,
      created_at: c.created_at ?? null,
    }));
  } catch (e) {
    // Missing column (pre-migration) or transient error — never break the detail.
    console.warn('[accounts:contacts] crm_contacts read fell back to empty', (e as any)?.message);
    return [];
  }
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
        org_id,
        owner_id,
        name,
        domain,
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
   resolveCompanyId
   Tries users.company_id first (legacy hierarchy), then falls back
   to reps.company_id (Phase 1 identity bridge) so auth-first users
   (demo reps with no users row) can still create accounts.
----------------------------------------------------------------- */
async function resolveCompanyId(userId: string): Promise<string | null> {
  if (!userId) return null;

  const { data: userRow } = await supa
    .from('users')
    .select('company_id')
    .eq('id', userId)
    .maybeSingle();

  if (userRow?.company_id) return String(userRow.company_id);

  // Phase 1 bridge: check reps table for users with no users row
  const { data: repRow } = await supa
    .from('reps')
    .select('company_id')
    .eq('id', userId)
    .maybeSingle();

  return (repRow as any)?.company_id ? String((repRow as any).company_id) : null;
}

/* ----------------------------------------------------------------
   POST /v1/accounts
   Body: { name: string, domain?: string, owner_id?: string }
   - company_id is resolved server-side from the authenticated user
   - duplicate names within the same company are rejected (409)
----------------------------------------------------------------- */
router.post('/', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);
    if (!userId) {
      return res.status(401).json({ ok: false, error: 'missing_user' });
    }

    const companyId = await resolveCompanyId(userId);
    if (!companyId) {
      return res.status(403).json({ ok: false, error: 'missing_company_context' });
    }

    const name = String(req.body?.name ?? '').trim();
    if (!name) {
      return res.status(400).json({ ok: false, error: 'name_required' });
    }

    const domain  = req.body?.domain   ? String(req.body.domain).trim()   : null;
    const ownerId = req.body?.owner_id ? String(req.body.owner_id).trim() : userId;

    // Duplicate check: case-insensitive within the same company
    const { data: existing } = await supa
      .from('accounts')
      .select('id')
      .eq('org_id', companyId)
      .ilike('name', name)
      .maybeSingle();

    if (existing) {
      return res.status(409).json({
        ok: false,
        error: 'account_name_exists',
        detail: `An account named "${name}" already exists in this company.`,
      });
    }

    const { data: account, error } = await supa
      .from('accounts')
      .insert({ name, domain, org_id: companyId, owner_id: ownerId })
      .select('id, org_id, owner_id, name, domain, created_at, updated_at')
      .single();

    if (error) throw error;

    // Audit — fire-and-forget, never blocks the response
    void logAuditEvent({
      actorUserId: userId,
      action:      AUDIT_ACTIONS.CREATE_ACCOUNT,
      entityType:  "account",
      entityId:    account.id ?? null,
      metadata:    { name: account.name, company_id: companyId },
    });

    return res.status(201).json({ ok: true, account });
  } catch (e: any) {
    console.error('[accounts:create] failed', e);
    return res.status(500).json({ ok: false, error: e?.message || 'account_create_failed' });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/escalations
   MUST be registered before GET /:id — Express matches by definition order, and
   a generic /:id above this would capture the literal "escalations" segment (the
   Day-278 UUID guard would then reject it as an invalid id). Day 279.
----------------------------------------------------------------- */
router.get('/escalations', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);

    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({
        ok: false,
        error: 'missing_company_context',
      });
    }

    let escalationQuery = supa
      .from('account_escalations')
      .select(`
        id,
        account_id,
        severity,
        status,
        escalation_reason,
        intervention_required,
        assigned_manager_id,
        triggered_by,
        workflow_stage,
        metadata,
        created_at,
        updated_at
      `)
      // Day 279 — account_escalations is company-scoped via `company_id` (there is
      // no `org_id` column; the old name 500'd, but the route was unreachable so it
      // was never hit). Scope to the requester's own company_id — isolation intact.
      .eq('company_id', requester.company_id)
      .order('created_at', { ascending: false })
      .limit(100);

    const { data: escalations, error } =
      await escalationQuery;

    if (error) {
      throw error;
    }

    return res.json({
      ok: true,
      escalations: escalations || [],
    });
  } catch (e: any) {
    console.error('[accounts:get-escalations] failed', e);

    return res.status(500).json({
      ok: false,
      error:
        e?.message || 'account_escalations_fetch_failed',
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
        org_id,
        owner_id,
        name,
        domain,
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

    const [linkedContacts, callsRes] = await Promise.all([
      // Day 285 — canonical account contacts live in crm_contacts (by account_id).
      fetchAccountContacts(account.id),

      supa
        .from('calls')
        .select(`
          id,
          title,
          score:score_overall,
          duration_seconds,
          created_at,
          rep_id:user_id,
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

    // Day 284 — enrich the owner so the UI reflects assignment after refresh.
    const owner = await resolveOwnerRep(account.owner_id, requester.company_id);

    return res.json({
      ok: true,

      account: {
        ...account,

        owner,
        ownership_status: account.owner_id ? 'assigned' : 'unassigned',

        stats: {
          contacts: linkedContacts.length || 0,
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

      linked_contacts: linkedContacts,

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
   PATCH /v1/accounts/:id/owner   Body: { owner_id }
   Assign a same-company rep as the account owner. Company scope + owner
   membership are server-resolved; a foreign account 404s and a foreign/unknown
   owner is rejected without revealing existence.
----------------------------------------------------------------- */
router.patch('/:id/owner', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);
    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({ ok: false, error: 'missing_company_context' });
    }

    const accountId = String(req.params.id || '').trim();
    if (!accountId) {
      return res.status(400).json({ ok: false, error: 'invalid_account_id' });
    }

    let accountQuery = supa
      .from('accounts')
      .select('id, org_id, owner_id, name, domain')
      .eq('id', accountId)
      .limit(1);
    accountQuery = buildAccountVisibilityFilter(accountQuery, requester);

    const { data: accountRows, error: accountError } = await accountQuery;
    if (accountError) throw accountError;

    const account = accountRows?.[0];
    if (!account) {
      return res.status(404).json({ ok: false, error: 'account_not_found' });
    }

    const ownerId = req.body?.owner_id ? String(req.body.owner_id).trim() : '';
    if (!ownerId) {
      return res.status(400).json({ ok: false, error: 'invalid_owner' });
    }

    // The owner must be a rep in the SAME company. Rejects foreign/unknown owners
    // without leaking existence (uniform invalid_owner). Server is the authority —
    // the client cannot assign across company scope.
    const owner = await resolveOwnerRep(ownerId, requester.company_id);
    if (!owner) {
      return res.status(400).json({ ok: false, error: 'invalid_owner' });
    }

    const { data: updated, error: updateError } = await supa
      .from('accounts')
      .update({ owner_id: ownerId, updated_at: new Date().toISOString() })
      .eq('id', account.id)
      .eq('org_id', requester.company_id) // defense-in-depth: never cross company
      .select('id, org_id, owner_id, name, domain')
      .single();
    if (updateError) throw updateError;

    return res.json({
      ok: true,
      account: { ...updated, owner, ownership_status: 'assigned' },
    });
  } catch (e: any) {
    console.error('[accounts:assign-owner] failed', e);
    return res.status(500).json({ ok: false, error: e?.message || 'assign_owner_failed' });
  }
});

/* ----------------------------------------------------------------
   DELETE /v1/accounts/:id/owner
   Unassign the account owner (owner_id → null). Company-scoped; foreign 404s.
----------------------------------------------------------------- */
router.delete('/:id/owner', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);
    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({ ok: false, error: 'missing_company_context' });
    }

    const accountId = String(req.params.id || '').trim();
    if (!accountId) {
      return res.status(400).json({ ok: false, error: 'invalid_account_id' });
    }

    let accountQuery = supa
      .from('accounts')
      .select('id, org_id, owner_id')
      .eq('id', accountId)
      .limit(1);
    accountQuery = buildAccountVisibilityFilter(accountQuery, requester);

    const { data: accountRows, error: accountError } = await accountQuery;
    if (accountError) throw accountError;

    const account = accountRows?.[0];
    if (!account) {
      return res.status(404).json({ ok: false, error: 'account_not_found' });
    }

    const { data: updated, error: updateError } = await supa
      .from('accounts')
      .update({ owner_id: null, updated_at: new Date().toISOString() })
      .eq('id', account.id)
      .eq('org_id', requester.company_id)
      .select('id, org_id, owner_id, name, domain')
      .single();
    if (updateError) throw updateError;

    return res.json({
      ok: true,
      account: { ...updated, owner: null, ownership_status: 'unassigned' },
    });
  } catch (e: any) {
    console.error('[accounts:unassign-owner] failed', e);
    return res.status(500).json({ ok: false, error: e?.message || 'unassign_owner_failed' });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/:id/intelligence-timeline
----------------------------------------------------------------- */
router.get('/:id/intelligence-timeline', async (req: Request, res: Response) => {
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
        org_id,
        name,
        domain,
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

    const [callsRes, tasksRes, escalationsRes, summariesRes] =
      await Promise.all([
        supa
          .from('calls')
          .select(`
            id,
            title,
            score:score_overall,
            duration_seconds,
            rep_id:user_id,
            created_at
          `)
          .eq('account_id', account.id)
          .order('created_at', { ascending: false })
          .limit(50),

        supa
          .from('account_ai_tasks')
          .select(`
            id,
            title,
            description,
            category,
            urgency,
            status,
            assigned_to,
            escalation_source,
            due_at,
            completed_at,
            created_at,
            updated_at
          `)
          .eq('account_id', account.id)
          .order('created_at', { ascending: false })
          .limit(50),

        supa
          .from('account_escalations')
          .select(`
            id,
            severity,
            status,
            escalation_reason,
            intervention_required,
            assigned_manager_id,
            workflow_stage,
            created_at,
            updated_at
          `)
          .eq('account_id', account.id)
          .order('created_at', { ascending: false })
          .limit(50),

        supa
          .from('account_ai_summaries')
          .select(`
            id,
            summary,
            health_status,
            churn_risk,
            next_best_action,
            manager_notes,
            generated_by,
            created_at,
            updated_at
          `)
          .eq('account_id', account.id)
          .order('updated_at', { ascending: false })
          .limit(20),
      ]);

    if (callsRes.error) throw callsRes.error;
    if (tasksRes.error) throw tasksRes.error;
    if (escalationsRes.error) throw escalationsRes.error;
    if (summariesRes.error) throw summariesRes.error;

    const callEvents = (callsRes.data || []).map((call: any) => ({
      id: `call:${call.id}`,
      source_id: call.id,
      type: 'call',
      severity:
        Number(call.score || 0) < 60
          ? 'warning'
          : 'info',
      title: call.title || 'Sales call logged',
      description:
        Number(call.score || 0) > 0
          ? `Call score: ${call.score}`
          : 'Call activity recorded.',
      occurred_at: call.created_at,
      metadata: {
        score: call.score,
        duration_seconds: call.duration_seconds,
        rep_id: call.rep_id,
      },
    }));

    const taskEvents = (tasksRes.data || []).map((task: any) => ({
      id: `task:${task.id}`,
      source_id: task.id,
      type: 'ai_task',
      severity:
        task.urgency === 'critical'
          ? 'critical'
          : task.urgency === 'high'
            ? 'warning'
            : 'info',
      title: task.title || 'AI task created',
      description: task.description || 'AI-generated workflow task.',
      occurred_at: task.completed_at || task.updated_at || task.created_at,
      metadata: {
        category: task.category,
        urgency: task.urgency,
        status: task.status,
        assigned_to: task.assigned_to,
        escalation_source: task.escalation_source,
        due_at: task.due_at,
        completed_at: task.completed_at,
      },
    }));

    const escalationEvents = (escalationsRes.data || []).map((escalation: any) => ({
      id: `escalation:${escalation.id}`,
      source_id: escalation.id,
      type: 'escalation',
      severity:
        escalation.severity === 'critical'
          ? 'critical'
          : escalation.severity === 'high'
            ? 'warning'
            : 'info',
      title: 'Account escalation triggered',
      description:
        escalation.escalation_reason ||
        'AI escalation workflow triggered.',
      occurred_at: escalation.updated_at || escalation.created_at,
      metadata: {
        severity: escalation.severity,
        status: escalation.status,
        intervention_required: escalation.intervention_required,
        assigned_manager_id: escalation.assigned_manager_id,
        workflow_stage: escalation.workflow_stage,
      },
    }));

    const summaryEvents = (summariesRes.data || []).map((summary: any) => ({
      id: `summary:${summary.id}`,
      source_id: summary.id,
      type: 'ai_summary',
      severity:
        Number(summary.churn_risk || 0) >= 70
          ? 'critical'
          : Number(summary.churn_risk || 0) >= 40
            ? 'warning'
            : 'info',
      title: 'AI account memory updated',
      description:
        summary.summary ||
        summary.next_best_action ||
        'Persistent account intelligence updated.',
      occurred_at: summary.updated_at || summary.created_at,
      metadata: {
        health_status: summary.health_status,
        churn_risk: summary.churn_risk,
        next_best_action: summary.next_best_action,
        manager_notes: summary.manager_notes,
        generated_by: summary.generated_by,
      },
    }));

    const timeline = [
      ...callEvents,
      ...taskEvents,
      ...escalationEvents,
      ...summaryEvents,
    ]
      .filter((event) => !!event.occurred_at)
      .sort(
        (a, b) =>
          new Date(b.occurred_at).getTime() -
          new Date(a.occurred_at).getTime()
      );

    const criticalCount = timeline.filter(
      (event) => event.severity === 'critical'
    ).length;

    const warningCount = timeline.filter(
      (event) => event.severity === 'warning'
    ).length;

    return res.json({
      ok: true,
      account,
      timeline,
      summary: {
        total_events: timeline.length,
        critical_events: criticalCount,
        warning_events: warningCount,
        latest_event_at: timeline[0]?.occurred_at || null,
      },
    });
  } catch (e: any) {
    console.error('[accounts:intelligence-timeline] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_intelligence_timeline_failed',
    });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/:id/tasks
----------------------------------------------------------------- */
router.get('/:id/tasks', async (req: Request, res: Response) => {
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
        org_id,
        owner_id,
        name,
        domain
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

    const { data: tasks, error: tasksError } = await supa
      .from('account_ai_tasks')
      .select(`
        id,
        account_id,
        title,
        description,
        category,
        urgency,
        status,
        assigned_to,
        escalation_source,
        generated_by,
        due_at,
        completed_at,
        metadata,
        created_at,
        updated_at
      `)
      .eq('account_id', account.id)
      .order('created_at', { ascending: false })
      .limit(100);

    if (tasksError) {
      throw tasksError;
    }

    return res.json({
      ok: true,
      account,
      tasks: tasks || [],
    });
  } catch (e: any) {
    console.error('[accounts:get-tasks] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_tasks_fetch_failed',
    });
  }
});

/* ----------------------------------------------------------------
   POST /v1/accounts/:id/tasks/generate
----------------------------------------------------------------- */
router.post('/:id/tasks/generate', async (req: Request, res: Response) => {
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
        org_id,
        domain,
        owner_id
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

    const { data: calls } = await supa
      .from('calls')
      .select('id,score:score_overall,created_at')
      .eq('account_id', account.id)
      .order('created_at', { ascending: false })
      .limit(25);

    const callList = calls || [];

    const avgScore =
      callList.length > 0
        ? Math.round(
          callList.reduce(
            (acc: number, c: any) =>
              acc + Number(c.score || 0),
            0
          ) / callList.length
        )
        : 0;

    // Day 283 — assigned_to / generated_by REFERENCE public.users(id). Auth-first
    // identities (Admin Auth API + reps bridge) have no public.users row, so writing
    // their auth id violates the FK (500). Both columns are nullable, so resolve each
    // candidate to a real users.id or null. Legacy users-row identities resolve to self.
    const generatedById = await existingUserId(requester.id);
    const ownerAssignee = await existingUserId(
      account.owner_id || requester.id
    );

    const generatedTasks: any[] = [];

    if (!account.owner_id) {
      generatedTasks.push({
        account_id: account.id,
        title: 'Assign account owner',
        description:
          'This account currently has no accountable owner assigned.',
        category: 'ownership',
        urgency: 'high',
        status: 'open',
        assigned_to: generatedById,
        escalation_source: 'missing_owner',
        generated_by: generatedById,
        metadata: {
          trigger: 'owner_missing',
        },
      });
    }

    if (avgScore < 60) {
      generatedTasks.push({
        account_id: account.id,
        title: 'Run replay coaching',
        description:
          'Recent account call performance has deteriorated below acceptable thresholds.',
        category: 'coaching',
        urgency: 'critical',
        status: 'open',
        assigned_to: ownerAssignee,
        escalation_source: 'low_account_score',
        generated_by: generatedById,
        metadata: {
          avg_score: avgScore,
        },
      });
    }

    if (callList.length === 0) {
      generatedTasks.push({
        account_id: account.id,
        title: 'Re-engage inactive account',
        description:
          'No recent sales activity detected for this account.',
        category: 'engagement',
        urgency: 'high',
        status: 'open',
        assigned_to: ownerAssignee,
        escalation_source: 'account_inactivity',
        generated_by: generatedById,
        metadata: {
          inactivity_detected: true,
        },
      });
    }

    const now = Date.now();

    const insertPayload = generatedTasks.map((task) => ({
      ...task,
      due_at: new Date(
        now + 1000 * 60 * 60 * 24 * 3
      ).toISOString(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }));

    let insertedTasks: any[] = [];

    if (insertPayload.length > 0) {
      const { data, error } = await supa
        .from('account_ai_tasks')
        .insert(insertPayload)
        .select(`
          id,
          account_id,
          title,
          description,
          category,
          urgency,
          status,
          assigned_to,
          escalation_source,
          generated_by,
          due_at,
          completed_at,
          metadata,
          created_at,
          updated_at
        `);

      if (error) {
        throw error;
      }

      insertedTasks = data || [];
    }

    return res.json({
      ok: true,
      account,
      generated_tasks: insertedTasks,
      generated_count: insertedTasks.length,
    });
  } catch (e: any) {
    console.error('[accounts:generate-tasks] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_task_generation_failed',
    });
  }
});

/* ----------------------------------------------------------------
   PATCH /v1/accounts/tasks/:taskId
----------------------------------------------------------------- */
router.patch('/tasks/:taskId', async (req: Request, res: Response) => {
  try {
    const userId = getUserId(req);

    const requester = await getRequesterContext(userId);

    if (!requester?.company_id) {
      return res.status(403).json({
        ok: false,
        error: 'missing_company_context',
      });
    }

    const taskId = String(req.params.taskId || '').trim();

    if (!taskId) {
      return res.status(400).json({
        ok: false,
        error: 'invalid_task_id',
      });
    }

    const updates: any = {
      updated_at: new Date().toISOString(),
    };

    if (typeof req.body?.status === 'string') {
      updates.status = req.body.status.trim();
    }

    if (typeof req.body?.assigned_to === 'string') {
      updates.assigned_to = req.body.assigned_to.trim();
    }

    if (typeof req.body?.urgency === 'string') {
      updates.urgency = req.body.urgency.trim();
    }

    if (typeof req.body?.title === 'string') {
      updates.title = req.body.title.trim();
    }

    if (typeof req.body?.description === 'string') {
      updates.description = req.body.description.trim();
    }

    if (updates.status === 'completed') {
      updates.completed_at = new Date().toISOString();
    }

    const { data: updatedTask, error: updateError } = await supa
      .from('account_ai_tasks')
      .update(updates)
      .eq('id', taskId)
      .select(`
        id,
        account_id,
        title,
        description,
        category,
        urgency,
        status,
        assigned_to,
        escalation_source,
        generated_by,
        due_at,
        completed_at,
        metadata,
        created_at,
        updated_at
      `)
      .single();

    if (updateError) {
      throw updateError;
    }

    return res.json({
      ok: true,
      task: updatedTask,
    });
  } catch (e: any) {
    console.error('[accounts:update-task] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_task_update_failed',
    });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/:id/summary
----------------------------------------------------------------- */
router.get('/:id/summary', async (req: Request, res: Response) => {
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
        org_id,
        domain,
        owner_id
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

    const { data: summary, error: summaryError } = await supa
      .from('account_ai_summaries')
      .select(`
        id,
        account_id,
        summary,
        health_status,
        churn_risk,
        next_best_action,
        manager_notes,
        generated_by,
        created_at,
        updated_at
      `)
      .eq('account_id', account.id)
      .order('updated_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (summaryError) {
      throw summaryError;
    }

    return res.json({
      ok: true,
      account,
      summary: summary || null,
    });
  } catch (e: any) {
    console.error('[accounts:get-summary] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_summary_fetch_failed',
    });
  }
});

/* ----------------------------------------------------------------
   POST /v1/accounts/:id/summary
----------------------------------------------------------------- */
router.post('/:id/summary', async (req: Request, res: Response) => {
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
        org_id,
        domain,
        owner_id
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

    const summary = String(req.body?.summary || '').trim();

    const healthStatus = String(
      req.body?.health_status || 'stable'
    ).trim();

    const churnRisk = Number(req.body?.churn_risk || 0);

    const nextBestAction = String(
      req.body?.next_best_action || ''
    ).trim();

    const managerNotes = String(
      req.body?.manager_notes || ''
    ).trim();

    // Day 283 — generated_by REFERENCES public.users(id). Auth-first identities have
    // no public.users row, so writing their auth id violates the FK (500). The column
    // is nullable, so resolve the requester to a real users.id or null.
    const generatedBy = await existingUserId(requester.id);

    const payload = {
      account_id: account.id,
      summary,
      health_status: healthStatus,
      churn_risk: churnRisk,
      next_best_action: nextBestAction,
      manager_notes: managerNotes,
      generated_by: generatedBy,
      updated_at: new Date().toISOString(),
    };

    const { data: existing } = await supa
      .from('account_ai_summaries')
      .select('id')
      .eq('account_id', account.id)
      .limit(1)
      .maybeSingle();

    let writeResult: any = null;
    let writeError: any = null;

    if (existing?.id) {
      const result = await supa
        .from('account_ai_summaries')
        .update(payload)
        .eq('id', existing.id)
        .select(`
          id,
          account_id,
          summary,
          health_status,
          churn_risk,
          next_best_action,
          manager_notes,
          generated_by,
          created_at,
          updated_at
        `)
        .single();

      writeResult = result.data;
      writeError = result.error;
    } else {
      const result = await supa
        .from('account_ai_summaries')
        .insert({
          ...payload,
          created_at: new Date().toISOString(),
        })
        .select(`
          id,
          account_id,
          summary,
          health_status,
          churn_risk,
          next_best_action,
          manager_notes,
          generated_by,
          created_at,
          updated_at
        `)
        .single();

      writeResult = result.data;
      writeError = result.error;
    }

    if (writeError) {
      throw writeError;
    }

    return res.json({
      ok: true,
      account,
      summary: writeResult,
    });
  } catch (e: any) {
    console.error('[accounts:save-summary] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_summary_save_failed',
    });
  }
});

/* ----------------------------------------------------------------
   GET /v1/accounts/:id/coaching-actions
----------------------------------------------------------------- */
router.get('/:id/coaching-actions', async (req: Request, res: Response) => {
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
        org_id,
        owner_id,
        name,
        domain
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

    const { data: actions, error: actionsError } = await supa
      .from('account_coaching_actions')
      .select(`
        id,
        account_id,
        action_type,
        title,
        description,
        urgency,
        status,
        assigned_to,
        linked_escalation_id,
        linked_task_id,
        replay_call_id,
        sparring_scenario,
        manager_notes,
        due_at,
        completed_at,
        metadata,
        created_by,
        created_at,
        updated_at
      `)
      .eq('account_id', account.id)
      .order('created_at', { ascending: false })
      .limit(100);

    if (actionsError) {
      throw actionsError;
    }

    return res.json({
      ok: true,
      account,
      coaching_actions: actions || [],
    });
  } catch (e: any) {
    console.error('[accounts:get-coaching-actions] failed', e);

    return res.status(500).json({
      ok: false,
      error:
        e?.message || 'account_coaching_actions_fetch_failed',
    });
  }
});

/* ----------------------------------------------------------------
   POST /v1/accounts/:id/coaching-action
----------------------------------------------------------------- */
router.post('/:id/coaching-action', async (req: Request, res: Response) => {
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
        org_id,
        name,
        domain,
        owner_id
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

    const actionType = String(
      req.body?.action_type || 'coaching'
    ).trim();

    const urgency = String(
      req.body?.urgency || 'medium'
    ).trim();

    const title = String(
      req.body?.title || 'AI coaching action'
    ).trim();

    const description = String(
      req.body?.description || ''
    ).trim();

    // Day 280 — resolve user-FK columns to a real users.id or null (auth-first
    // identities have no users row; the columns are nullable and FK to users).
    const assignedTo = await existingUserId(
      req.body?.assigned_to || account.owner_id || requester.id
    );
    const createdBy = await existingUserId(requester.id);

    const managerNotes = String(
      req.body?.manager_notes || ''
    ).trim();

    const linkedEscalationId = req.body?.linked_escalation_id
      ? String(req.body.linked_escalation_id).trim()
      : null;

    const linkedTaskId = req.body?.linked_task_id
      ? String(req.body.linked_task_id).trim()
      : null;

    const replayCallId = req.body?.replay_call_id
      ? String(req.body.replay_call_id).trim()
      : null;

    const sparringScenario = req.body?.sparring_scenario
      ? String(req.body.sparring_scenario).trim()
      : null;

    const dueAt = req.body?.due_at
      ? new Date(req.body.due_at).toISOString()
      : new Date(
        Date.now() + 1000 * 60 * 60 * 24 * 3
      ).toISOString();

    const metadata = {
      source: 'account_coaching_engine',
      account_name: account.name,
      action_type: actionType,
      created_from: 'crm_intelligence_layer',
      ...(req.body?.metadata || {}),
    };

    const payload = {
      account_id: account.id,
      // Day 280 — account_coaching_actions is company-scoped via `company_id`
      // (there is no `org_id` column). Company ownership is resolved server-side
      // from the requester, never accepted from the client.
      company_id: requester.company_id,
      action_type: actionType,
      title,
      description,
      urgency,
      status: 'open',
      assigned_to: assignedTo,
      linked_escalation_id: linkedEscalationId,
      linked_task_id: linkedTaskId,
      replay_call_id: replayCallId,
      sparring_scenario: sparringScenario,
      manager_notes: managerNotes,
      due_at: dueAt,
      metadata,
      created_by: createdBy,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    const { data: coachingAction, error: coachingError } =
      await supa
        .from('account_coaching_actions')
        .insert(payload)
        .select(`
          id,
          account_id,
          action_type,
          title,
          description,
          urgency,
          status,
          assigned_to,
          linked_escalation_id,
          linked_task_id,
          replay_call_id,
          sparring_scenario,
          manager_notes,
          due_at,
          completed_at,
          metadata,
          created_by,
          created_at,
          updated_at
        `)
        .single();

    if (coachingError) {
      throw coachingError;
    }

    return res.json({
      ok: true,
      account,
      coaching_action: coachingAction,
    });
  } catch (e: any) {
    console.error('[accounts:create-coaching-action] failed', e);

    return res.status(500).json({
      ok: false,
      error:
        e?.message || 'account_coaching_action_failed',
    });
  }
});

/* GET /v1/accounts/escalations is registered ABOVE the generic GET /:id route
   (see near the top of this file) so the literal path is matched before /:id and
   is never captured by it. Day 279. */

/* ----------------------------------------------------------------
   POST /v1/accounts/:id/escalate
----------------------------------------------------------------- */
router.post('/:id/escalate', async (req: Request, res: Response) => {
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
        org_id,
        name,
        domain,
        owner_id
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

    const severity = String(
      req.body?.severity || 'high'
    ).trim();

    const escalationReason = String(
      req.body?.escalation_reason ||
      'AI detected elevated account risk.'
    ).trim();

    const workflowStage = String(
      req.body?.workflow_stage || 'intervention_required'
    ).trim();

    const interventionRequired =
      req.body?.intervention_required !== false;

    // Day 280 — resolve user-FK columns to a real users.id or null (auth-first
    // identities have no users row; these columns are nullable and FK to users).
    const assignedManagerId = await existingUserId(
      req.body?.assigned_manager_id || requester.manager_id || requester.id
    );
    const triggeredBy = await existingUserId(requester.id);

    const metadata = {
      source: 'ai_escalation_engine',
      account_name: account.name,
      triggered_by_user: requester.id,
      created_from: 'account_risk_workflow',
      ...(req.body?.metadata || {}),
    };

    const payload = {
      account_id: account.id,
      // Day 280 — account_escalations is company-scoped via `company_id` (there is
      // no `org_id` column). Company ownership is resolved server-side from the
      // requester, never accepted from the client.
      company_id: requester.company_id,
      severity,
      status: 'open',
      escalation_reason: escalationReason,
      intervention_required: interventionRequired,
      assigned_manager_id: assignedManagerId,
      triggered_by: triggeredBy,
      workflow_stage: workflowStage,
      metadata,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    const { data: escalation, error: escalationError } =
      await supa
        .from('account_escalations')
        .insert(payload)
        .select(`
          id,
          account_id,
          severity,
          status,
          escalation_reason,
          intervention_required,
          assigned_manager_id,
          triggered_by,
          workflow_stage,
          metadata,
          created_at,
          updated_at
        `)
        .single();

    if (escalationError) {
      throw escalationError;
    }

    return res.json({
      ok: true,
      account,
      escalation,
    });
  } catch (e: any) {
    console.error('[accounts:escalate] failed', e);

    return res.status(500).json({
      ok: false,
      error: e?.message || 'account_escalation_failed',
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
        org_id,
        owner_id,
        domain,
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
          score:score_overall,
          created_at,
          duration_seconds,
          rep_id:user_id
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
