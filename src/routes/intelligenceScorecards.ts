import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";
import { requireManager } from "../middleware/requireManager";
import { logAuditEvent } from "../lib/audit";
import {
  GRAVIX_DEFAULT_RUBRIC,
  buildVersionSnapshot,
  defaultStageWeights,
  normaliseVersionPayload,
  validateForActivation,
} from "../lib/scorecardStudio";

// Intelligence Layer — Days 219B + 220: Scorecard Studio data layer +
// lifecycle completion.
//
// Managers define what a good call looks like per call type, WITHIN the
// fixed four-stage frame (SCORECARD_STUDIO_SPEC.md, WEB repo). Draft
// versions hold editable relational weight/criteria rows; activation
// validates (weights total 100, ≥1 criterion, call type or company
// default), stamps a deterministic immutable jsonb snapshot, supersedes
// the previous active version and writes an audit event. Nothing in the
// scoring runtime reads these tables yet.
//
// Day 220 lifecycle rules:
// - active/superseded versions are immutable — editing one means forking
//   it into a new draft version (POST …/versions/:versionId/fork);
// - cross-scorecard call-type conflicts are never replaced silently: the
//   manager must send { replace_conflicts: true } and the response names
//   every superseded version (the WEB dialog reads this);
// - archiving marks, never deletes — every version and snapshot survives;
//   archiving the company default is allowed because the Gravix Default
//   rubric is the guaranteed fallback (spec safety rule). No restore
//   endpoint yet; unarchiving is a later, explicit lane.
//
// Scope rules (same shape as routes/intelligence.ts, Day 218):
// - every endpoint requireManager-gated; reps 403, no identity 401;
// - company resolved from the requester via the users→reps identity
//   bridge, never from the request — cross-company ids answer 404;
// - no hard deletes anywhere: supersede/archive only.

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

function requesterIdFromHeaders(req: any): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

// Same users→reps identity bridge as routes/team.ts / intelligence.ts.
async function resolveCompanyId(userId: string): Promise<string | null> {
  if (!userId) return null;

  const { data: userRow } = await supa
    .from("users")
    .select("company_id")
    .eq("id", userId)
    .maybeSingle();
  if (userRow?.company_id) return String(userRow.company_id);

  const { data: repRow } = await supa
    .from("reps")
    .select("company_id")
    .eq("id", userId)
    .maybeSingle();
  return (repRow as any)?.company_id ? String((repRow as any).company_id) : null;
}

// Fail-soft until sql/20260714b_scorecard_studio.sql is applied.
function isMissingTableError(message: string | undefined): boolean {
  return /could not find the table/i.test(String(message ?? ""));
}
function tableError(res: any, message: string | undefined) {
  if (isMissingTableError(message)) {
    return res.status(503).json({
      ok: false,
      error: "scorecard_studio_not_migrated",
      detail: "Apply sql/20260714b_scorecard_studio.sql",
    });
  }
  return res.status(500).json({ ok: false, error: message || "server_error" });
}

const MAX_NAME_CHARS = 120;
const MAX_DESCRIPTION_CHARS = 2_000;

function serialiseScorecard(row: Record<string, any>) {
  return {
    id: String(row.id),
    name: String(row.name),
    description: row.description ?? null,
    status: String(row.status),
    is_company_default: Boolean(row.is_company_default),
    created_by: row.created_by ?? null,
    updated_by: row.updated_by ?? null,
    archived_at: row.archived_at ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
  };
}

function serialiseVersion(row: Record<string, any>, weights?: any[], criteria?: any[]) {
  const out: Record<string, unknown> = {
    id: String(row.id),
    scorecard_id: String(row.scorecard_id),
    version: Number(row.version) || 0,
    status: String(row.status),
    call_types: Array.isArray(row.call_types) ? row.call_types : [],
    origin: String(row.origin ?? "manual"),
    activation_note: row.activation_note ?? null,
    activated_by: row.activated_by ?? null,
    activated_at: row.activated_at ?? null,
    snapshot: row.snapshot ?? {},
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
  };
  if (weights) {
    out.stage_weights = weights.map((w) => ({
      stage: w.stage,
      weight: Number(w.weight) || 0,
      guidance: w.guidance ?? null,
    }));
  }
  if (criteria) {
    out.criteria = criteria.map((c) => ({
      id: String(c.id),
      stage: c.stage,
      label: c.label,
      description: c.description ?? null,
      scoring_guidance: c.scoring_guidance ?? null,
      good_example: c.good_example ?? null,
      weak_example: c.weak_example ?? null,
      coaching_prompt: c.coaching_prompt ?? null,
      pass_fail: Boolean(c.pass_fail),
      critical: Boolean(c.critical),
      emphasis: c.emphasis ?? "standard",
      sort_order: Number(c.sort_order) || 0,
    }));
  }
  return out;
}

async function fetchScorecard(companyId: string, id: string) {
  return supa
    .from("scorecards")
    .select("*")
    .eq("company_id", companyId)
    .eq("id", id)
    .maybeSingle();
}

async function fetchVersionRows(versionIds: string[]) {
  const [weightsRes, criteriaRes] = await Promise.all([
    supa
      .from("scorecard_stage_weights")
      .select("*")
      .in("scorecard_version_id", versionIds),
    supa
      .from("scorecard_criteria")
      .select("*")
      .in("scorecard_version_id", versionIds)
      .order("stage")
      .order("sort_order"),
  ]);
  return { weightsRes, criteriaRes };
}

function validName(res: any, name: unknown): string | null {
  const s = typeof name === "string" ? name.trim() : "";
  if (!s) {
    res.status(400).json({ ok: false, error: "name_required" });
    return null;
  }
  if (s.length > MAX_NAME_CHARS) {
    res.status(400).json({ ok: false, error: "name_too_long" });
    return null;
  }
  return s;
}

// ----- GET /v1/intelligence/scorecards ---------------------------------------
// Company scorecards + latest/active version summaries, plus the read-only
// Gravix default card (code, not a database row).
router.get("/", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: cards, error: cardsErr } = await supa
      .from("scorecards")
      .select("*")
      .eq("company_id", companyId)
      .order("created_at", { ascending: true });
    if (cardsErr) return tableError(res, cardsErr.message);

    const ids = ((cards ?? []) as any[]).map((c) => String(c.id));
    let versions: any[] = [];
    if (ids.length) {
      const { data: verRows, error: verErr } = await supa
        .from("scorecard_versions")
        .select("id, scorecard_id, version, status, call_types, origin, activated_at")
        .in("scorecard_id", ids)
        .order("version", { ascending: false });
      if (verErr) return tableError(res, verErr.message);
      versions = (verRows ?? []) as any[];
    }

    const items = ((cards ?? []) as any[]).map((card) => {
      const mine = versions.filter((v) => String(v.scorecard_id) === String(card.id));
      const latest = mine[0] ?? null;
      const active = mine.find((v) => v.status === "active") ?? null;
      return {
        ...serialiseScorecard(card),
        latest_version: latest ? serialiseVersion(latest) : null,
        active_version: active ? serialiseVersion(active) : null,
      };
    });

    return res.json({ ok: true, company_id: companyId, items, default_rubric: GRAVIX_DEFAULT_RUBRIC });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/scorecards --------------------------------------
// Create a draft scorecard with draft version 1: Gravix-default stage
// weights (25/25/25/25) and no criteria yet.
router.post("/", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const body = (req.body ?? {}) as Record<string, any>;
    const name = validName(res, body.name);
    if (!name) return;
    const description =
      typeof body.description === "string" && body.description.trim()
        ? body.description.trim().slice(0, MAX_DESCRIPTION_CHARS)
        : null;

    const { data: card, error: cardErr } = await supa
      .from("scorecards")
      .insert({
        company_id: companyId,
        name,
        description,
        status: "draft",
        is_company_default: Boolean(body.is_company_default),
        created_by: requesterId,
        updated_by: requesterId,
      })
      .select("*")
      .single();
    if (cardErr) {
      if (/duplicate key|uq_scorecards_company_name/i.test(cardErr.message)) {
        return res.status(409).json({ ok: false, error: "scorecard_name_taken" });
      }
      return tableError(res, cardErr.message);
    }

    const { data: version, error: verErr } = await supa
      .from("scorecard_versions")
      .insert({
        scorecard_id: card.id,
        company_id: companyId,
        version: 1,
        status: "draft",
        call_types: [],
        origin: "manual",
        created_by: requesterId,
      })
      .select("*")
      .single();
    if (verErr) return tableError(res, verErr.message);

    const { error: weightsErr } = await supa
      .from("scorecard_stage_weights")
      .insert(defaultStageWeights().map((w) => ({ ...w, scorecard_version_id: version.id })));
    if (weightsErr) return tableError(res, weightsErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "create_scorecard",
      entityType: "scorecard",
      entityId: String(card.id),
      metadata: { company_id: companyId, name },
    });

    const { weightsRes } = await fetchVersionRows([String(version.id)]);
    return res.status(201).json({
      ok: true,
      scorecard: serialiseScorecard(card),
      draft_version: serialiseVersion(version, (weightsRes.data ?? []) as any[], []),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- GET /v1/intelligence/scorecards/:id -----------------------------------
// Scorecard + every version with its stage weights and criteria.
router.get("/:id", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });

    const { data: verRows, error: verErr } = await supa
      .from("scorecard_versions")
      .select("*")
      .eq("scorecard_id", card.id)
      .order("version", { ascending: false });
    if (verErr) return tableError(res, verErr.message);

    const versionIds = ((verRows ?? []) as any[]).map((v) => String(v.id));
    let weights: any[] = [];
    let criteria: any[] = [];
    if (versionIds.length) {
      const { weightsRes, criteriaRes } = await fetchVersionRows(versionIds);
      if (weightsRes.error) return tableError(res, weightsRes.error.message);
      if (criteriaRes.error) return tableError(res, criteriaRes.error.message);
      weights = (weightsRes.data ?? []) as any[];
      criteria = (criteriaRes.data ?? []) as any[];
    }

    const versions = ((verRows ?? []) as any[]).map((v) =>
      serialiseVersion(
        v,
        weights.filter((w) => String(w.scorecard_version_id) === String(v.id)),
        criteria.filter((c) => String(c.scorecard_version_id) === String(v.id))
      )
    );

    return res.json({ ok: true, scorecard: serialiseScorecard(card), versions });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- PUT /v1/intelligence/scorecards/:id -----------------------------------
// Draft-safe metadata update: name / description / is_company_default.
// Archived scorecards are read-only history.
router.put("/:id", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });
    if (card.status === "archived") {
      return res.status(409).json({ ok: false, error: "scorecard_archived" });
    }

    const body = (req.body ?? {}) as Record<string, any>;
    const patch: Record<string, unknown> = {
      updated_by: requesterId,
      updated_at: new Date().toISOString(),
    };
    if (body.name !== undefined) {
      const name = validName(res, body.name);
      if (!name) return;
      patch.name = name;
    }
    if (body.description !== undefined) {
      patch.description =
        typeof body.description === "string" && body.description.trim()
          ? body.description.trim().slice(0, MAX_DESCRIPTION_CHARS)
          : null;
    }
    if (body.is_company_default !== undefined) {
      patch.is_company_default = Boolean(body.is_company_default);
    }

    const { data: updated, error: updErr } = await supa
      .from("scorecards")
      .update(patch)
      .eq("id", card.id)
      .eq("company_id", companyId)
      .select("*")
      .single();
    if (updErr) {
      if (/duplicate key|uq_scorecards_company_name/i.test(updErr.message)) {
        return res.status(409).json({ ok: false, error: "scorecard_name_taken" });
      }
      if (/uq_scorecards_company_default/i.test(updErr.message)) {
        return res.status(409).json({ ok: false, error: "company_default_taken" });
      }
      return tableError(res, updErr.message);
    }
    return res.json({ ok: true, scorecard: serialiseScorecard(updated) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- PUT /v1/intelligence/scorecards/:id/versions/:versionId ----------------
// Save the draft version's call types, stage weights and criteria. DRAFT
// ONLY — active/superseded versions are immutable (edits will fork a new
// draft version on the Day 220 lane). Weights-total-100 is deliberately
// not checked here: saving is never blocked, activation is.
router.put("/:id/versions/:versionId", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });
    if (card.status === "archived") {
      return res.status(409).json({ ok: false, error: "scorecard_archived" });
    }

    const { data: version, error: verErr } = await supa
      .from("scorecard_versions")
      .select("*")
      .eq("id", String(req.params.versionId))
      .eq("scorecard_id", card.id)
      .eq("company_id", companyId)
      .maybeSingle();
    if (verErr) return tableError(res, verErr.message);
    if (!version) return res.status(404).json({ ok: false, error: "not_found" });
    if (version.status !== "draft") {
      return res.status(409).json({ ok: false, error: "version_immutable" });
    }

    const parsed = normaliseVersionPayload(req.body ?? {});
    if (parsed.errors) {
      return res.status(400).json({ ok: false, error: parsed.errors[0].error, errors: parsed.errors });
    }
    const value = parsed.value!;

    // Replace the draft's working rows wholesale (draft editing, not history
    // — immutable versions are never touched by this path).
    const { error: delWErr } = await supa
      .from("scorecard_stage_weights")
      .delete()
      .eq("scorecard_version_id", version.id);
    if (delWErr) return tableError(res, delWErr.message);
    const { error: delCErr } = await supa
      .from("scorecard_criteria")
      .delete()
      .eq("scorecard_version_id", version.id);
    if (delCErr) return tableError(res, delCErr.message);

    const { error: insWErr } = await supa
      .from("scorecard_stage_weights")
      .insert(value.weights.map((w) => ({ ...w, scorecard_version_id: version.id })));
    if (insWErr) return tableError(res, insWErr.message);

    if (value.criteria.length) {
      const { error: insCErr } = await supa
        .from("scorecard_criteria")
        .insert(value.criteria.map((c) => ({ ...c, scorecard_version_id: version.id })));
      if (insCErr) return tableError(res, insCErr.message);
    }

    const { data: updatedVersion, error: updErr } = await supa
      .from("scorecard_versions")
      .update({ call_types: value.call_types, updated_at: new Date().toISOString() })
      .eq("id", version.id)
      .select("*")
      .single();
    if (updErr) return tableError(res, updErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "save_scorecard_draft",
      entityType: "scorecard_version",
      entityId: String(version.id),
      metadata: {
        company_id: companyId,
        scorecard_id: String(card.id),
        version: Number(version.version) || 0,
        criteria_count: value.criteria.length,
      },
    });

    const { weightsRes, criteriaRes } = await fetchVersionRows([String(version.id)]);
    return res.json({
      ok: true,
      version: serialiseVersion(
        updatedVersion,
        (weightsRes.data ?? []) as any[],
        (criteriaRes.data ?? []) as any[]
      ),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/scorecards/:id/versions/:versionId/fork ----------
// Day 220: "edit an active scorecard" = fork its immutable version into a
// new draft (version n+1) carrying the source's call types, weights,
// criteria and origin (provenance survives forks — an ai_draft stays
// visible as ai_draft in version history). Draft versions cannot be forked
// (they are already editable), and a scorecard can only hold one draft: if
// one exists the fork answers 409 draft_already_exists with its id rather
// than silently returning it — the existing draft may hold unrelated
// edits, so discarding or continuing it is the manager's call.
router.post("/:id/versions/:versionId/fork", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });
    if (card.status === "archived") {
      return res.status(409).json({ ok: false, error: "scorecard_archived" });
    }

    const { data: source, error: srcErr } = await supa
      .from("scorecard_versions")
      .select("*")
      .eq("id", String(req.params.versionId))
      .eq("scorecard_id", card.id)
      .eq("company_id", companyId)
      .maybeSingle();
    if (srcErr) return tableError(res, srcErr.message);
    if (!source) return res.status(404).json({ ok: false, error: "not_found" });
    if (source.status === "draft") {
      return res.status(400).json({ ok: false, error: "cannot_fork_draft" });
    }

    const { data: existingDraft, error: draftErr } = await supa
      .from("scorecard_versions")
      .select("id")
      .eq("scorecard_id", card.id)
      .eq("status", "draft")
      .maybeSingle();
    if (draftErr) return tableError(res, draftErr.message);
    if (existingDraft) {
      return res.status(409).json({
        ok: false,
        error: "draft_already_exists",
        draft_version_id: String(existingDraft.id),
      });
    }

    const { data: versionRows, error: verErr } = await supa
      .from("scorecard_versions")
      .select("version")
      .eq("scorecard_id", card.id)
      .order("version", { ascending: false })
      .limit(1);
    if (verErr) return tableError(res, verErr.message);
    const nextVersion = (Number(versionRows?.[0]?.version) || 0) + 1;

    const { data: fork, error: forkErr } = await supa
      .from("scorecard_versions")
      .insert({
        scorecard_id: card.id,
        company_id: companyId,
        version: nextVersion,
        status: "draft",
        call_types: Array.isArray(source.call_types) ? source.call_types : [],
        origin: source.origin ?? "manual",
        created_by: requesterId,
      })
      .select("*")
      .single();
    if (forkErr) return tableError(res, forkErr.message);

    // Copy the source's working rows (fresh ids, same content and order).
    const { weightsRes, criteriaRes } = await fetchVersionRows([String(source.id)]);
    if (weightsRes.error) return tableError(res, weightsRes.error.message);
    if (criteriaRes.error) return tableError(res, criteriaRes.error.message);
    const sourceWeights = (weightsRes.data ?? []) as any[];
    const sourceCriteria = (criteriaRes.data ?? []) as any[];

    if (sourceWeights.length) {
      const { error: insWErr } = await supa.from("scorecard_stage_weights").insert(
        sourceWeights.map((w) => ({
          scorecard_version_id: fork.id,
          stage: w.stage,
          weight: w.weight,
          guidance: w.guidance ?? null,
        }))
      );
      if (insWErr) return tableError(res, insWErr.message);
    }
    if (sourceCriteria.length) {
      const { error: insCErr } = await supa.from("scorecard_criteria").insert(
        sourceCriteria.map((c) => ({
          scorecard_version_id: fork.id,
          stage: c.stage,
          label: c.label,
          description: c.description ?? null,
          scoring_guidance: c.scoring_guidance ?? null,
          good_example: c.good_example ?? null,
          weak_example: c.weak_example ?? null,
          coaching_prompt: c.coaching_prompt ?? null,
          pass_fail: Boolean(c.pass_fail),
          critical: Boolean(c.critical),
          emphasis: c.emphasis ?? "standard",
          sort_order: Number(c.sort_order) || 0,
        }))
      );
      if (insCErr) return tableError(res, insCErr.message);
    }

    await logAuditEvent({
      actorUserId: requesterId,
      action: "fork_scorecard_version",
      entityType: "scorecard_version",
      entityId: String(fork.id),
      metadata: {
        company_id: companyId,
        scorecard_id: String(card.id),
        source_version_id: String(source.id),
        source_version: Number(source.version) || 0,
        version: nextVersion,
      },
    });

    const { weightsRes: forkW, criteriaRes: forkC } = await fetchVersionRows([String(fork.id)]);
    return res.status(201).json({
      ok: true,
      version: serialiseVersion(fork, (forkW.data ?? []) as any[], (forkC.data ?? []) as any[]),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/scorecards/:id/activate ---------------------------
// Explicit manager action, never automatic. Validates the activation rules
// (weights total 100, ≥1 criterion, call type or company default), rejects
// cross-scorecard call-type conflicts (409 — the replacement flow is the
// Day 220 dialog lane), stamps the immutable snapshot, supersedes this
// scorecard's previous active version and writes an audit event.
router.post("/:id/activate", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });
    if (card.status === "archived") {
      return res.status(409).json({ ok: false, error: "scorecard_archived" });
    }

    const { data: draft, error: draftErr } = await supa
      .from("scorecard_versions")
      .select("*")
      .eq("scorecard_id", card.id)
      .eq("status", "draft")
      .maybeSingle();
    if (draftErr) return tableError(res, draftErr.message);
    if (!draft) return res.status(400).json({ ok: false, error: "no_draft_version" });

    const { weightsRes, criteriaRes } = await fetchVersionRows([String(draft.id)]);
    if (weightsRes.error) return tableError(res, weightsRes.error.message);
    if (criteriaRes.error) return tableError(res, criteriaRes.error.message);
    const weights = (weightsRes.data ?? []) as any[];
    const criteria = (criteriaRes.data ?? []) as any[];

    const callTypes: string[] = Array.isArray(draft.call_types) ? draft.call_types : [];
    const activationErrors = validateForActivation({
      weights,
      criteriaCount: criteria.length,
      callTypes,
      isCompanyDefault: Boolean(card.is_company_default),
    });
    if (activationErrors.length) {
      return res.status(400).json({
        ok: false,
        error: activationErrors[0].error,
        errors: activationErrors,
      });
    }

    // One active scorecard per call type across the company (and one active
    // company default — the partial unique index backstops that one).
    // Day 220: conflicts are never replaced silently — without the explicit
    // { replace_conflicts: true } flag the activation answers 409 with the
    // full conflict list; with it, every conflicting active version is
    // superseded (whole versions — narrowing an immutable version's call
    // types would mutate it) and the response names each replacement.
    const replaceConflicts = Boolean((req.body ?? {}).replace_conflicts);

    type Conflict = {
      reason: "call_type" | "company_default";
      version_id: string;
      scorecard_id: string;
      version: number;
      call_types: string[];
    };
    const conflicts: Conflict[] = [];

    if (callTypes.length) {
      const { data: activeElsewhere, error: confErr } = await supa
        .from("scorecard_versions")
        .select("id, scorecard_id, version, call_types")
        .eq("company_id", companyId)
        .eq("status", "active")
        .neq("scorecard_id", card.id)
        .overlaps("call_types", callTypes);
      if (confErr) return tableError(res, confErr.message);
      for (const v of (activeElsewhere ?? []) as any[]) {
        conflicts.push({
          reason: "call_type",
          version_id: String(v.id),
          scorecard_id: String(v.scorecard_id),
          version: Number(v.version) || 0,
          call_types: (v.call_types ?? []).filter((t: string) => callTypes.includes(t)),
        });
      }
    }
    if (card.is_company_default) {
      const { data: otherDefaults, error: defErr } = await supa
        .from("scorecards")
        .select("id")
        .eq("company_id", companyId)
        .eq("is_company_default", true)
        .eq("status", "active")
        .neq("id", card.id);
      if (defErr) return tableError(res, defErr.message);
      for (const other of (otherDefaults ?? []) as any[]) {
        const { data: otherActive, error: otherErr } = await supa
          .from("scorecard_versions")
          .select("id, scorecard_id, version, call_types")
          .eq("scorecard_id", other.id)
          .eq("status", "active")
          .maybeSingle();
        if (otherErr) return tableError(res, otherErr.message);
        if (!otherActive) continue;
        if (conflicts.some((cf) => cf.version_id === String(otherActive.id))) continue;
        conflicts.push({
          reason: "company_default",
          version_id: String(otherActive.id),
          scorecard_id: String(other.id),
          version: Number(otherActive.version) || 0,
          call_types: Array.isArray(otherActive.call_types) ? otherActive.call_types : [],
        });
      }
    }

    if (conflicts.length && !replaceConflicts) {
      return res.status(409).json({
        ok: false,
        error: conflicts.some((cf) => cf.reason === "call_type")
          ? "call_type_conflict"
          : "company_default_conflict",
        conflicts,
        hint: "Re-send with { replace_conflicts: true } to supersede these versions.",
      });
    }

    const replaced: Conflict[] = [];
    if (conflicts.length && replaceConflicts) {
      for (const conflict of conflicts) {
        const { error: supErr } = await supa
          .from("scorecard_versions")
          .update({ status: "superseded", updated_at: new Date().toISOString() })
          .eq("id", conflict.version_id)
          .eq("company_id", companyId);
        if (supErr) return tableError(res, supErr.message);

        // The replaced scorecard no longer scores anything — drop it to
        // draft. Every version and snapshot survives untouched.
        const { error: dropErr } = await supa
          .from("scorecards")
          .update({ status: "draft", updated_by: requesterId, updated_at: new Date().toISOString() })
          .eq("id", conflict.scorecard_id)
          .eq("company_id", companyId)
          .eq("status", "active");
        if (dropErr) return tableError(res, dropErr.message);
        replaced.push(conflict);
      }
      await logAuditEvent({
        actorUserId: requesterId,
        action: "replace_scorecard_conflicts",
        entityType: "scorecard",
        entityId: String(card.id),
        metadata: { company_id: companyId, replaced },
      });
    }

    // Supersede this scorecard's previous active version (history kept).
    const { data: previousActive, error: prevErr } = await supa
      .from("scorecard_versions")
      .select("id")
      .eq("scorecard_id", card.id)
      .eq("status", "active")
      .maybeSingle();
    if (prevErr) return tableError(res, prevErr.message);
    if (previousActive) {
      const { error: supErr } = await supa
        .from("scorecard_versions")
        .update({ status: "superseded", updated_at: new Date().toISOString() })
        .eq("id", previousActive.id);
      if (supErr) return tableError(res, supErr.message);
    }

    const now = new Date().toISOString();
    const activationNote =
      typeof (req.body ?? {}).activation_note === "string" && (req.body.activation_note as string).trim()
        ? String(req.body.activation_note).trim().slice(0, MAX_DESCRIPTION_CHARS)
        : null;

    const { data: activated, error: actErr } = await supa
      .from("scorecard_versions")
      .update({
        status: "active",
        snapshot: buildVersionSnapshot({ weights, criteria }),
        activation_note: activationNote,
        activated_by: requesterId,
        activated_at: now,
        updated_at: now,
      })
      .eq("id", draft.id)
      .select("*")
      .single();
    if (actErr) {
      // Best-effort restore so the scorecard is not left with no active
      // version because the activation update failed after superseding.
      if (previousActive) {
        await supa
          .from("scorecard_versions")
          .update({ status: "active" })
          .eq("id", previousActive.id);
      }
      return tableError(res, actErr.message);
    }

    const { error: cardUpdErr } = await supa
      .from("scorecards")
      .update({ status: "active", updated_by: requesterId, updated_at: now })
      .eq("id", card.id)
      .eq("company_id", companyId);
    if (cardUpdErr) return tableError(res, cardUpdErr.message);

    await logAuditEvent({
      actorUserId: requesterId,
      action: "activate_scorecard_version",
      entityType: "scorecard_version",
      entityId: String(activated.id),
      metadata: {
        company_id: companyId,
        scorecard_id: String(card.id),
        version: Number(activated.version) || 0,
        call_types: callTypes,
      },
    });

    return res.json({
      ok: true,
      version: serialiseVersion(activated, weights, criteria),
      replaced,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/scorecards/:id/archive ----------------------------
// Day 220: archive marks, never deletes. The scorecard drops out of the
// live picture (status archived, active version superseded) but every
// version, snapshot and criteria row survives for history. Archiving the
// company default is allowed: the Gravix Default rubric is the guaranteed
// fallback (spec safety rule — the MVP must be deletable without breaking
// scoring). Archived scorecards are read-only; there is no restore
// endpoint yet (a later, explicit lane).
router.post("/:id/archive", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: card, error: cardErr } = await fetchScorecard(companyId, String(req.params.id));
    if (cardErr) return tableError(res, cardErr.message);
    if (!card) return res.status(404).json({ ok: false, error: "not_found" });
    if (card.status === "archived") {
      return res.status(409).json({ ok: false, error: "scorecard_archived" });
    }

    const now = new Date().toISOString();

    // Supersede the active version so nothing archived can be resolved as
    // live; drafts stay as rows (read-only behind the archived guard).
    const { data: active, error: activeErr } = await supa
      .from("scorecard_versions")
      .select("id, version")
      .eq("scorecard_id", card.id)
      .eq("status", "active")
      .maybeSingle();
    if (activeErr) return tableError(res, activeErr.message);
    if (active) {
      const { error: supErr } = await supa
        .from("scorecard_versions")
        .update({ status: "superseded", updated_at: now })
        .eq("id", active.id);
      if (supErr) return tableError(res, supErr.message);
    }

    const { data: archived, error: archErr } = await supa
      .from("scorecards")
      .update({ status: "archived", archived_at: now, updated_by: requesterId, updated_at: now })
      .eq("id", card.id)
      .eq("company_id", companyId)
      .select("*")
      .single();
    if (archErr) {
      // Best-effort restore so a failed archive does not leave the
      // scorecard active-but-versionless.
      if (active) {
        await supa.from("scorecard_versions").update({ status: "active" }).eq("id", active.id);
      }
      return tableError(res, archErr.message);
    }

    await logAuditEvent({
      actorUserId: requesterId,
      action: "archive_scorecard",
      entityType: "scorecard",
      entityId: String(card.id),
      metadata: {
        company_id: companyId,
        superseded_version: active ? Number(active.version) || 0 : null,
        was_company_default: Boolean(card.is_company_default),
      },
    });

    return res.json({ ok: true, scorecard: serialiseScorecard(archived) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

export default router;
