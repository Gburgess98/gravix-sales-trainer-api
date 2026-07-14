import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";
import { requireManager } from "../middleware/requireManager";
import { logAuditEvent } from "../lib/audit";
import { compileContextBlock, unknownContextKeys } from "../lib/contextEngine";

// Intelligence Layer — Day 218: Context Engine data layer.
//
// Manager-controlled company memory (CONTEXT_ENGINE_SPEC.md, WEB repo).
// Lifecycle: one draft row per company (working copy) + one published row
// (the only version runtime may later read) + archived history rows.
// Nothing in the scoring runtime reads this yet — publish never rescores,
// draft edits never touch the published snapshot.
//
// Scope rules (same shape as routes/team.ts, Day 212):
// - every endpoint is requireManager-gated — reps get 403, no identity 401.
//   (The rep read-only summary view is a WEB concern; a rep-readable subset
//   endpoint ships with that build, not before.)
// - company resolved from the requester via the users→reps identity bridge;
//   the company_id is never taken from the request, so cross-company
//   reads/writes are impossible by construction.
// - No destructive deletes: publish archives the previous version.

const router = Router();

const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const MAX_CONTEXT_JSON_CHARS = 64_000;

function requesterIdFromHeaders(req: any): string {
  return String(
    req.header("x-user-id") ||
    req.header("x-forwarded-user-id") ||
    req.header("x-gravix-user-id") ||
    ""
  ).trim();
}

// Same users→reps identity bridge as routes/team.ts resolveCompanyId.
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

// Fail-soft until sql/20260714_company_context.sql is applied: a missing
// table answers 503 with a precise error instead of a generic 500.
function isMissingTableError(message: string | undefined): boolean {
  return /could not find the table/i.test(String(message ?? ""));
}
function tableError(res: any, message: string | undefined) {
  if (isMissingTableError(message)) {
    return res.status(503).json({
      ok: false,
      error: "context_engine_not_migrated",
      detail: "Apply sql/20260714_company_context.sql",
    });
  }
  return res.status(500).json({ ok: false, error: message || "server_error" });
}

type ContextRow = Record<string, any>;

// Public row shape — internal columns only, no surprises for the WEB client.
function serialise(row: ContextRow | null) {
  if (!row) return null;
  return {
    id: String(row.id),
    status: String(row.status),
    version: Number(row.version) || 0,
    context: row.context ?? {},
    compiled_context: row.compiled_context ?? null,
    created_by: row.created_by ?? null,
    updated_by: row.updated_by ?? null,
    published_by: row.published_by ?? null,
    published_at: row.published_at ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
  };
}

async function fetchRow(companyId: string, status: "draft" | "published") {
  return supa
    .from("company_context")
    .select("*")
    .eq("company_id", companyId)
    .eq("status", status)
    .maybeSingle();
}

// ----- GET /v1/intelligence/context ------------------------------------------
// Current draft + latest published version for the caller's company.
router.get("/context", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: draft, error: draftErr } = await fetchRow(companyId, "draft");
    if (draftErr) return tableError(res, draftErr.message);
    const { data: published, error: pubErr } = await fetchRow(companyId, "published");
    if (pubErr) return tableError(res, pubErr.message);

    return res.json({
      ok: true,
      company_id: companyId,
      draft: serialise(draft),
      published: serialise(published),
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- PUT /v1/intelligence/context -------------------------------------------
// Save the draft only. Never touches the published row; publishing is an
// explicit separate action (no auto-publish, ever).
router.put("/context", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const context = (req.body ?? {}).context;
    if (!context || typeof context !== "object" || Array.isArray(context)) {
      return res.status(400).json({ ok: false, error: "context_must_be_object" });
    }
    const unknown = unknownContextKeys(context);
    if (unknown.length) {
      return res.status(400).json({ ok: false, error: "unknown_context_keys", keys: unknown });
    }
    if (JSON.stringify(context).length > MAX_CONTEXT_JSON_CHARS) {
      return res.status(400).json({ ok: false, error: "context_too_large" });
    }

    const { data: existing, error: readErr } = await fetchRow(companyId, "draft");
    if (readErr) return tableError(res, readErr.message);

    if (existing) {
      const { data: updated, error: updErr } = await supa
        .from("company_context")
        .update({ context, updated_by: requesterId, updated_at: new Date().toISOString() })
        .eq("id", existing.id)
        .eq("company_id", companyId)
        .select("*")
        .single();
      if (updErr) return tableError(res, updErr.message);
      return res.json({ ok: true, draft: serialise(updated) });
    }

    const { data: inserted, error: insErr } = await supa
      .from("company_context")
      .insert({
        company_id: companyId,
        status: "draft",
        version: 0,
        context,
        created_by: requesterId,
        updated_by: requesterId,
      })
      .select("*")
      .single();
    if (insErr) return tableError(res, insErr.message);
    return res.json({ ok: true, draft: serialise(inserted) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- POST /v1/intelligence/context/publish ----------------------------------
// Publish the current draft as a new immutable version:
//   1. next version = max version across the company's rows + 1 (monotonic);
//   2. previous published row → archived (history kept, nothing deleted);
//   3. new published row = draft snapshot + deterministic compiled block.
// The draft row survives as the working copy; later edits to it never
// change the published snapshot.
router.post("/context/publish", requireManager, async (req, res) => {
  try {
    const requesterId = requesterIdFromHeaders(req);
    const companyId = await resolveCompanyId(requesterId);
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const { data: draft, error: draftErr } = await fetchRow(companyId, "draft");
    if (draftErr) return tableError(res, draftErr.message);
    if (!draft) return res.status(400).json({ ok: false, error: "no_draft_to_publish" });

    const { data: versionRows, error: verErr } = await supa
      .from("company_context")
      .select("version")
      .eq("company_id", companyId)
      .order("version", { ascending: false })
      .limit(1);
    if (verErr) return tableError(res, verErr.message);
    const nextVersion = (Number(versionRows?.[0]?.version) || 0) + 1;

    const { data: previous, error: prevErr } = await fetchRow(companyId, "published");
    if (prevErr) return tableError(res, prevErr.message);
    if (previous) {
      const { error: archErr } = await supa
        .from("company_context")
        .update({ status: "archived", archived_at: new Date().toISOString() })
        .eq("id", previous.id)
        .eq("company_id", companyId);
      if (archErr) return tableError(res, archErr.message);
    }

    const now = new Date().toISOString();
    const { data: published, error: pubErr } = await supa
      .from("company_context")
      .insert({
        company_id: companyId,
        status: "published",
        version: nextVersion,
        context: draft.context ?? {},
        compiled_context: compileContextBlock(draft.context),
        created_by: draft.created_by ?? requesterId,
        updated_by: requesterId,
        published_by: requesterId,
        published_at: now,
      })
      .select("*")
      .single();
    if (pubErr) {
      // Best-effort restore so the company is not left without a published
      // version because the insert failed after the archive step.
      if (previous) {
        await supa
          .from("company_context")
          .update({ status: "published", archived_at: null })
          .eq("id", previous.id)
          .eq("company_id", companyId);
      }
      return tableError(res, pubErr.message);
    }

    await logAuditEvent({
      actorUserId: requesterId,
      action: "publish_company_context",
      entityType: "company_context",
      entityId: String(published.id),
      metadata: { company_id: companyId, version: nextVersion },
    });

    return res.json({ ok: true, published: serialise(published) });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

// ----- GET /v1/intelligence/context/compiled ----------------------------------
// Deterministic compiled-block preview. Draft compiles fresh (live preview);
// published returns the stored snapshot stamped at publish time so the
// published block never shifts under the caller.
router.get("/context/compiled", requireManager, async (req, res) => {
  try {
    const companyId = await resolveCompanyId(requesterIdFromHeaders(req));
    if (!companyId) return res.status(403).json({ ok: false, error: "no_company_scope" });

    const state = req.query.state === "published" ? "published" : "draft";
    const { data: row, error } = await fetchRow(companyId, state);
    if (error) return tableError(res, error.message);

    const compiled =
      state === "published"
        ? String(row?.compiled_context ?? "")
        : compileContextBlock(row?.context);

    return res.json({ ok: true, state, compiled });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "server_error" });
  }
});

export default router;
