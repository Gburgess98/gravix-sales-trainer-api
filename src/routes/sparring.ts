import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';

// Create a service-role Supabase client (write access)
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY!,
});

function getUserIdHeader(req: Request): string {
  const raw = (req.headers['x-user-id'] as string | undefined)?.toString().trim();
  if (!raw) {
    throw new Error('missing x-user-id header');
  }
  return raw;
}

const router = express.Router();

// List available sparring personas (global presets for now)
router.get("/personas", async (_req, res) => {
  return res.json({
    ok: true,
    personas: [
      {
        id: "price_sensitive",
        label: "Price Sensitive",
        traits: ["ROI-focused", "Budget restricted"],
        description:
          "Pushes back on price early and often. Fixated on ROI and alternatives.",
        difficulty_default: "normal",
      },
      {
        id: "angry",
        label: "Angry Buyer",
        traits: ["Short fuse", "Interrupts"],
        description:
          "Easily irritated, talks over you, demanding strong justification.",
        difficulty_default: "hard",
      },
      {
        id: "silent",
        label: "Ultra Silent Mode",
        traits: ["1–3 word answers", "Low engagement"],
        description:
          "Barely gives anything. You must carry the conversation and create momentum.",
        difficulty_default: "nightmare",
      },
      {
        id: "cfo",
        label: "The CFO",
        traits: ["Analytical", "Sceptical", "Risk averse"],
        description:
          "Wants numbers, risk minimisation, and clear upside justification.",
        difficulty_default: "hard",
      },
      {
        id: "procurement",
        label: "Procurement Wall",
        traits: ["Policy-driven", "Process-focused"],
        description:
          "Everything must go through a committee or existing vendor. Loves process.",
        difficulty_default: "normal",
      },
    ],
  });
});

// --------------------------------------------
// GET /v1/sparring/leaderboard/:personaId
// Returns win/loss stats across all users
// --------------------------------------------
router.get("/leaderboard/:personaId", async (req, res) => {
  const personaId = String(req.params.personaId);

  try {
    const { data, error } = await supa
      .from("sparring_sessions")
      .select("total_score")
      .eq("persona_id", personaId)
      .not("total_score", "is", null);

    if (error) {
      console.error("[sparring] leaderboard error", error);
      return res.status(400).json({ ok: false, error: error.message });
    }

    let wins = 0;
    let losses = 0;

    for (const row of data || []) {
      if (row.total_score >= 80) wins++;
      else losses++;
    }

    const total = wins + losses;
    const winRate = total > 0 ? Math.round((wins / total) * 100) : 0;

    return res.json({
      ok: true,
      personaId,
      wins,
      losses,
      winRate,
      total,
    });
  } catch (e: any) {
    console.error("[sparring] leaderboard unexpected error", e);
    return res
      .status(500)
      .json({ ok: false, error: e?.message || "leaderboard_failed" });
  }
});

/**
 * POST /v1/sparring/log
 * Body: { repId?: string, xp?: number, meta?: any, personaId?: string }
 * - Records a sparring session
 * - Adds an XP event for the rep (if provided)
 * - Attempts to increment the rep's total XP
 *
 * This endpoint is tolerant:
 * - If a table is missing, it returns ok:true with a warning.
 * - Writes are attempted independently, errors collected in warnings[].
 */
router.post('/log', express.json(), async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  const { repId, xp, meta, personaId } = req.body ?? {};
  const orgId = (req.header('x-org-id') || (req.body && req.body.orgId) || '').toString().trim();

  const award = Number.isFinite(xp) ? Number(xp) : 25;
  const rep_id: string | null = typeof repId === 'string' && repId.length ? repId : null;
  const persona_id: string = (typeof personaId === 'string' && personaId.trim().length) ? personaId.trim() : 'unknown';

  const session_id = uuidv4();
  const warnings: string[] = [];
  const nowIso = new Date().toISOString();

  // Ensure rep exists to satisfy FK on sparring_sessions.rep_id
  if (rep_id) {
    try {
      const { error: repUpsertErr } = await supa
        .from('reps')
        .upsert([{ id: rep_id, name: 'Rep' }], { onConflict: 'id', ignoreDuplicates: false });
      if (repUpsertErr) {
        // Non-fatal; continue but warn
        // (If table doesn't exist yet, this will be collected in warnings)
        // @ts-ignore
        warnings?.push?.(`reps upsert: ${repUpsertErr.message}`);
      }
    } catch (e: any) {
      // @ts-ignore
      warnings?.push?.(`reps upsert threw: ${e?.message ?? String(e)}`);
    }
  }

  // 1) Insert sparring session (if table exists)
  try {
    const { error } = await supa
      .from('sparring_sessions')
      .insert({
        id: session_id,
        rep_id,
        persona_id,       // <-- persona persisted
        xp_awarded: award,
        meta: meta ?? null,
        created_at: nowIso,
      } as any);
    if (error) {
      // Table may not exist yet — keep tolerant
      warnings.push(`sparring_sessions insert: ${error.message}`);
    }
  } catch (e: any) {
    warnings.push(`sparring_sessions insert threw: ${e?.message ?? String(e)}`);
  }

  // 2) Insert XP event (if table exists) — ties to rep
  if (rep_id) {
    try {
      const { error } = await supa
        .from('xp_events')
        .insert({
          id: uuidv4(),
          rep_id,                    // match actual schema column
          source: 'sparring',
          delta: award,
          amount: award,
          session_id,
          created_at: nowIso,
        } as any);
      if (error) warnings.push(`xp_events insert: ${error.message}`);
    } catch (e: any) {
      warnings.push(`xp_events insert threw: ${e?.message ?? String(e)}`);
    }

    // 3) Increment reps.xp (if column exists)
    try {
      const { error } = await supa.rpc('increment_rep_xp', { p_rep_id: rep_id, p_delta: award });
      if (error) {
        warnings.push(`increment_rep_xp rpc: ${error.message}`);
      }
    } catch (e: any) {
      warnings.push(`increment_rep_xp threw: ${e?.message ?? String(e)}`);
    }
  } else {
    warnings.push('No repId provided — recorded session without pinning XP to a rep.');
  }

  // 4) Activity log — only if explicitly enabled AND we have an org id.
  // Toggle with env var SPAR_ACTIVITY_LOG=1 to avoid type check constraint until schema includes 'sparring_session'.
  if (orgId && process.env.SPAR_ACTIVITY_LOG === '1') {
    try {
      const { error } = await supa
        .from('activities')
        .insert({
          id: uuidv4(),
          org_id: orgId,
          type: 'sparring_session', // add this to activities_type_check in DB before enabling
          actor_user_id: rep_id,
          call_id: null,
          payload: {
            session_id,
            xp_awarded: award,
            source: 'sparring',
          },
          created_at: nowIso,
        } as any);

      if (error) warnings.push(`activities insert: ${error.message}`);
    } catch (e: any) {
      warnings.push(`activities insert threw: ${e?.message ?? String(e)}`);
    }
  }

  return res.status(200).json({
    ok: true,
    session_id,
    rep_id,
    persona_id,
    xp_awarded: award,
    warnings,
  });
});


// --- Score endpoint ---
// Supports two modes:
// 1) Persist mode: { sessionId, total, meta } -> updates sparring_sessions.total_score and merges meta
// 2) Mock mode: { transcript, personaId } -> returns ephemeral mock scoring (no DB writes)
router.post('/score', express.json(), async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  // Mode 1: persist score if sessionId provided
  const { sessionId, total, meta } = req.body ?? {};
  if (typeof sessionId === 'string' && sessionId.trim().length) {
    try {
      // Get current meta to merge cleanly
      const { data: row, error: selErr } = await supa
        .from('sparring_sessions')
        .select('id, meta')
        .eq('id', sessionId)
        .single();

      if (selErr) {
        return res.status(400).json({ ok: false, error: `session select failed: ${selErr.message}` });
      }

      const existingMeta = (row?.meta && typeof row.meta === 'object') ? (row.meta as Record<string, any>) : {};
      const mergedMeta = { ...existingMeta, ...(meta || {}), total };

      const { data: upd, error: updErr } = await supa
        .from('sparring_sessions')
        .update({ total_score: total, meta: mergedMeta })
        .eq('id', sessionId)
        .select()
        .single();

      if (updErr) {
        return res.status(500).json({ ok: false, error: `score update failed: ${updErr.message}` });
      }

      return res.json({ ok: true, session: upd });
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message || 'score update exception' });
    }
  }

  // Mode 2: mock scoring (back-compat)
  const { transcript, personaId } = req.body ?? {};
  if (!transcript || typeof transcript !== 'string') {
    return res.status(400).json({ ok: false, error: 'transcript required (or provide { sessionId, total, meta } to persist score)' });
  }

  // temporary mocked scoring logic
  const base = Math.floor(Math.random() * 30) + 60;
  const scores = {
    tone: base + Math.floor(Math.random() * 10 - 5),
    objection: base + Math.floor(Math.random() * 10 - 5),
    close: base + Math.floor(Math.random() * 10 - 5),
  };
  const totalMock = Math.round((scores.tone + scores.objection + scores.close) / 3);

  return res.json({
    ok: true,
    personaId,
    scores,
    total: totalMock,
    xp_awarded: Math.floor(totalMock / 4),
  });
});

// ---------------------------
// POST /v1/sparring/sessions
// ---------------------------
// ---------------------------
// POST /v1/sparring/sessions
// ---------------------------
// Create a new sparring session for the current rep
// ---------------------------
// POST /v1/sparring/sessions
// ---------------------------
// Create a new sparring session for the current rep
router.post('/sessions', express.json(), async (req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');

  try {
    // 1) Work out who the rep is
    let repId: string | null = null;
    try {
      // Preferred: Supabase user id via proxy
      repId = getUserIdHeader(req);
    } catch {
      const bodyRep = (req.body as any)?.repId;
      if (typeof bodyRep === 'string' && bodyRep.trim().length) {
        repId = bodyRep.trim();
      }
    }

    const DEV_REP_ID =
      process.env.DEV_REP_ID || '11111111-1111-1111-8111-111111111111';

    // If we still don't have a repId (e.g. local curl), fall back to dev rep
    const effectiveRepId = repId || DEV_REP_ID;

    const { personaId, difficulty } = (req.body ?? {}) as {
      personaId?: string;
      difficulty?: string;
    };

    const sessionId = uuidv4();

    // 2) Make sure the rep exists to satisfy FK (reps.id)
    try {
      const { error: repUpsertErr } = await supa
        .from('reps')
        .upsert(
          [{ id: effectiveRepId, name: 'Rep' }],
          { onConflict: 'id', ignoreDuplicates: false }
        );
      if (repUpsertErr) {
        console.warn('[sparring/sessions POST] reps upsert warning', repUpsertErr);
        // non-fatal – session insert may still succeed if FK not enforced
      }
    } catch (e: any) {
      console.warn('[sparring/sessions POST] reps upsert threw', e?.message || e);
    }

    // 3) Insert the new session
    const payload: any = {
      id: sessionId,
      rep_id: effectiveRepId,
      persona_id: personaId || 'price_sensitive',
      difficulty: difficulty || 'normal',
      meta: {
        personaId: personaId || 'price_sensitive',
        difficulty: difficulty || 'normal',
      },
    };

    const { data, error } = await supa
      .from('sparring_sessions')
      .insert(payload)
      .select(
        'id, rep_id, persona_id, difficulty, xp_awarded, total_score, created_at, duration_ms, turns, summary, flags, meta'
      )
      .single();

    if (error || !data) {
      console.error('[sparring/sessions POST] insert error', error);
      throw error || new Error('insert_failed');
    }

    return res.json({ ok: true, session: data });
  } catch (err: any) {
    console.error('[sparring/sessions POST] unexpected error', err);
    return res
      .status(400)
      .json({ ok: false, error: err?.message || 'bad_request' });
  }
});

// -----------------------------------------
// POST /v1/sparring/sessions/:id/turns
// -----------------------------------------
// Append a user turn and generate an AI reply
router.post(
  '/sessions/:id/turns',
  express.json(),
  async (req: Request, res: Response) => {
    res.setHeader('Cache-Control', 'no-store');

    try {
      const id = String(req.params.id);
      const { text } = (req.body ?? {}) as { text?: string };

      if (!text || typeof text !== 'string' || !text.trim()) {
        return res
          .status(400)
          .json({ ok: false, error: 'text_required' });
      }

      let repId: string | null = null;
      let orgIdHeader: string | null = null;
      try {
        repId = getUserIdHeader(req);
      } catch {
        const bodyRep = (req.body as any)?.repId;
        if (typeof bodyRep === 'string' && bodyRep.trim().length) {
          repId = bodyRep.trim();
        }
      }
      orgIdHeader =
        (req.headers['x-org-id'] as string | undefined)?.toString().trim() ||
        null;

      // 1) Load session and basic access check
      const { data: session, error: sessErr } = await supa
        .from('sparring_sessions')
        .select('id, rep_id, persona_id')
        .eq('id', id)
        .single();

      if (sessErr || !session) {
        console.error('[sparring/turns] session not found', sessErr);
        return res.status(404).json({ ok: false, error: 'not_found' });
      }

      if (
        repId &&
        session.rep_id &&
        session.rep_id !== repId
      ) {
        return res.status(403).json({ ok: false, error: 'forbidden' });
      }

      // 2) Fetch existing turns to build conversation context
      const { data: existingTurns, error: turnsErr } = await supa
        .from('sparring_turns')
        .select('role, text')
        .eq('session_id', id)
        .order('created_at', { ascending: true });

      if (turnsErr) {
        console.error('[sparring/turns] load turns error', turnsErr);
        throw turnsErr;
      }

      const history = (existingTurns ?? []).map((t: any) => ({
        role: t.role === 'assistant' ? 'assistant' : 'user',
        content: t.text,
      })) as { role: 'user' | 'assistant'; content: string }[];

      // 3) Append the new user turn into the prompt
      history.push({ role: 'user', content: text });

      // 4) Call OpenAI for the AI reply
      let aiText = '';
      try {
        const completion = await openai.chat.completions.create({
          model: process.env.OPENAI_SPARRING_MODEL || 'gpt-4o-mini',
          messages: [
            {
              role: "system",
              content: `
You are a sales sparring persona. Your behaviour MUST change based on persona + difficulty.

Persona: ${session.persona_id}
Difficulty: ${session.meta?.difficulty || "normal"}

Rules:
- Stay in character 100%.
- Keep replies short (1–3 sentences) unless the persona explicitly tends to ramble.
- Never reveal you are an AI.
- Always behave like a real buyer/prospect with emotions, flaws, objections, and human inconsistency.

Persona Profiles:
- price_sensitive:
    Traits: hates high prices, compares alternatives, asks about ROI constantly.
    Tone: cautious, sceptical.
    Behaviour: frequently pushes back with cost objections.

- angry:
    Traits: frustrated, easily annoyed, snaps back quickly.
    Tone: sharp, impatient.
    Behaviour: interrupts, blames rep, demands justification.

- silent:
    Traits: disengaged, uninterested.
    Tone: flat, minimal replies.
    Behaviour: provides 1–4 word answers, slow to engage.

- friendly:
    Traits: warm, positive.
    Tone: helpful.
    Behaviour: open to conversation but still expects clarity and value.

Difficulty Modifiers:
- easy: fewer objections, more cooperative, gives opening for rep to win.
- normal: standard level objections, fair pushback.
- hard: tougher objections, shorter patience, challenges more aggressively.
- nightmare: interrupts, stacks objections, pulls up past bad experiences, very hard to close.

Apply persona + difficulty adjustments dynamically.
`
            },
            ...history,
          ],
          temperature: 0.7,
          max_tokens: 220,
        });

        aiText =
          completion.choices[0]?.message?.content?.trim() ||
          "I'm not convinced yet. Can you explain why this is worth the price?";
      } catch (llmErr: any) {
        console.error('[sparring/turns] OpenAI error', llmErr);
        aiText =
          "I'm still not sure about this. The price feels high compared to what I'm getting.";
      }

      // 5) Persist both turns
      const { data: insertedTurns, error: insertErr } = await supa
        .from('sparring_turns')
        .insert([
          {
            session_id: id,
            role: 'user',
            text,
          },
          {
            session_id: id,
            role: 'assistant',
            text: aiText,
          },
        ])
        .select('id, session_id, role, text, created_at');

      if (insertErr) {
        console.error('[sparring/turns] insert error', insertErr);
        throw insertErr;
      }

      return res.json({
        ok: true,
        turns: insertedTurns ?? [],
        ai: aiText,
      });
    } catch (err: any) {
      console.error(
        'POST /v1/sparring/sessions/:id/turns unexpected error',
        err
      );
      return res
        .status(400)
        .json({ ok: false, error: err?.message || 'bad_request' });
    }
  }
);

// GET /v1/sparring/sessions?repId=<uuid>&limit=5
router.get("/sessions", async (req: Request, res: Response) => {
  res.setHeader("Cache-Control", "no-store");

  try {
    // Prefer explicit ?repId= (manager views),
    // otherwise fall back to x-user-id header if present.
    let repId: string | null = null;

    if (typeof req.query.repId === "string" && req.query.repId.trim().length) {
      repId = req.query.repId.trim();
    } else {
      try {
        repId = getUserIdHeader(req);
      } catch {
        // no header – allowed for now (e.g. future manager/global view)
        repId = null;
      }
    }

    const limitRaw =
      typeof req.query.limit === "string"
        ? parseInt(req.query.limit as string, 10)
        : NaN;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 && limitRaw <= 50
        ? limitRaw
        : 20;

    let query = supa
      .from("sparring_sessions")
      .select(
        "id, rep_id, persona_id, difficulty, total_score, xp_awarded, created_at, duration_ms, turns, summary, flags, meta"
      )
      .order("created_at", { ascending: false })
      .limit(limit);

    // TEMP: manager/global view — return ALL sessions regardless of rep
    // If you want to scope per rep again later, re-enable this:
    // if (repId) {
    //   query = query.eq("rep_id", repId);
    // }

    const { data, error } = await query;

    if (error) {
      console.error("[sparring/sessions] Supabase error", error);
      return res
        .status(500)
        .json({ ok: false, error: "Failed to load sparring sessions" });
    }

    const sessions = (data || []).map((row: any) => {
      const personaId =
        row.persona_id ??
        row.meta?.personaId ??
        "unknown";

      const difficulty =
        row.difficulty ||
        row.meta?.difficulty ||
        "normal";

      const total =
        typeof row.total_score === "number"
          ? row.total_score
          : typeof row.meta?.total === "number"
            ? row.meta.total
            : null;

      const xp =
        typeof row.xp_awarded === "number"
          ? row.xp_awarded
          : row.meta?.xp ?? 0;

      const durationMs =
        typeof row.duration_ms === "number"
          ? row.duration_ms
          : row.meta?.duration_ms ?? null;

      const turns =
        typeof row.turns === "number"
          ? row.turns
          : Array.isArray(row.meta?.transcript)
            ? row.meta.transcript.length
            : null;

      const summary = row.summary ?? row.meta?.summary ?? null;
      const flags = row.flags ?? row.meta?.flags ?? null;

      return {
        id: row.id,
        rep_id: row.rep_id,
        persona_id: personaId,
        difficulty,
        total,
        xp_awarded: xp,
        created_at: row.created_at,
        duration_ms: durationMs,
        turns,
        summary,
        flags,
        meta: row.meta ?? null,
      };
    });

    return res.json({
      ok: true,
      sessions,
    });

  } catch (err: any) {
    console.error("[sparring/sessions] Unexpected error", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Unexpected error loading sparring sessions",
    });
  }
});

// GET /v1/sparring/sessions/:id
// Returns a single sparring session with metadata + full turn history
router.get('/sessions/:id', async (req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');

  try {
    const { id } = req.params;
    if (!id) {
      return res.status(400).json({ ok: false, error: 'id is required' });
    }

    const { data, error } = await supa
      .from('sparring_sessions')
      .select(
        'id, rep_id, persona_id, total_score, xp_awarded, created_at, duration_ms, turns, summary, flags, meta'
      )
      .eq('id', id)
      .single();

    if (error) {
      console.error('[sparring/sessions/:id] Supabase error', error);
      return res
        .status(500)
        .json({ ok: false, error: 'Failed to load sparring session' });
    }

    if (!data) {
      return res
        .status(404)
        .json({ ok: false, error: 'Sparring session not found' });
    }

    const personaId =
      (data as any).persona_id ??
      (data as any)?.meta?.personaId ??
      'unknown';

    const total =
      typeof (data as any).total_score === 'number'
        ? (data as any).total_score
        : typeof (data as any)?.meta?.total === 'number'
          ? (data as any).meta.total
          : null;

    const xp =
      typeof (data as any).xp_awarded === 'number'
        ? (data as any).xp_awarded
        : (data as any)?.meta?.xp ?? 0;

    const durationMs =
      typeof (data as any).duration_ms === 'number'
        ? (data as any).duration_ms
        : (data as any)?.meta?.duration_ms ?? null;

    const turnsCount =
      typeof (data as any).turns === 'number'
        ? (data as any).turns
        : Array.isArray((data as any)?.meta?.transcript)
          ? (data as any).meta.transcript.length
          : null;

    const summary =
      (data as any).summary ?? (data as any)?.meta?.summary ?? null;

    const flags =
      (data as any).flags ?? (data as any)?.meta?.flags ?? null;

    const session = {
      id: (data as any).id,
      rep_id: (data as any).rep_id,
      persona_id: personaId,
      difficulty: (data as any).difficulty || (data as any)?.meta?.difficulty || "normal",
      total,
      xp_awarded: xp,
      created_at: (data as any).created_at,
      duration_ms: durationMs,
      turns: turnsCount,
      summary,
      flags,
      meta: (data as any).meta ?? null,
    };

    // Fetch full turn history
    const { data: turns, error: turnsErr } = await supa
      .from('sparring_turns')
      .select('id, session_id, role, text, created_at')
      .eq('session_id', id)
      .order('created_at', { ascending: true });

    if (turnsErr) {
      console.error('[sparring/sessions/:id] load turns error', turnsErr);
      return res.status(500).json({
        ok: false,
        error: 'Failed to load sparring turns',
      });
    }

    return res.json({
      ok: true,
      session,
      turns: turns ?? [],
    });
  } catch (err: any) {
    console.error('[sparring/sessions/:id] Unexpected error', err);
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Unexpected error loading sparring session',
    });
  }
});

export default router;
