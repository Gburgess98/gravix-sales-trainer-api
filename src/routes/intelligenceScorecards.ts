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

// Intelligence Layer — Day 219B: Scorecard Studio data layer.
//
// Managers define what a good call looks like per call type, WITHIN the
// fixed four-stage frame (SCORECARD_STUDIO_SPEC.md, WEB repo). Draft
// versions hold editable relational weight/criteria rows; activation
// validates (weights total 100, ≥1 criterion, call type or company
// default), stamps a deterministic immutable jsonb snapshot, supersedes
// the previous active version and writes an audit event. Nothing in the
// scoring runtime reads these tables yet.
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
    if (callTypes.length) {
      const { data: activeElsewhere, error: confErr } = await supa
        .from("scorecard_versions")
        .select("id, scorecard_id, call_types")
        .eq("company_id", companyId)
        .eq("status", "active")
        .neq("scorecard_id", card.id)
        .overlaps("call_types", callTypes);
      if (confErr) return tableError(res, confErr.message);
      if (activeElsewhere?.length) {
        return res.status(409).json({
          ok: false,
          error: "call_type_conflict",
          conflicts: (activeElsewhere as any[]).map((v) => ({
            scorecard_id: String(v.scorecard_id),
            call_types: (v.call_types ?? []).filter((t: string) => callTypes.includes(t)),
          })),
        });
      }
    }
    if (card.is_company_default) {
      const { data: otherDefault, error: defErr } = await supa
        .from("scorecards")
        .select("id")
        .eq("company_id", companyId)
        .eq("is_company_default", true)
        .eq("status", "active")
        .neq("id", card.id)
        .limit(1);
      if (defErr) return tableError(res, defErr.message);
      if (otherDefault?.length) {
        return res.status(409).json({
          ok: false,
          error: "company_default_conflict",
          conflicts: [{ scorecard_id: String((otherDefault as any[])[0].id) }],
        });
      }
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
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

export default router;
