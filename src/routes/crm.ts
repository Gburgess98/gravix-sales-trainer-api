import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";
import { postSlack } from "../lib/slack";
import { postCrmAutoAssignRunSlack } from "../lib/slack";

const router = Router();
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getUserIdHeader(req: any): string {
  const uid = req.header("x-user-id");
  if (!uid || !UUID_RE.test(uid)) throw new Error("Missing or invalid x-user-id");
  return uid;
}

function getOrgIdHeader(req: any): string {
  // Prefer request header; allow a safe dev/default fallback via env.
  const raw = String(req.header("x-org-id") || "").trim();
  if (raw && UUID_RE.test(raw)) return raw;

  const fallback = String(process.env.DEFAULT_ORG_ID || "").trim();
  if (fallback && UUID_RE.test(fallback)) return fallback;

  throw new Error("Missing or invalid x-org-id");
}

// ------------------- Contact Health Score (v1) -------------------
function healthClamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function healthDaysSince(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return null;
  const diffMs = Date.now() - t;
  return diffMs < 0 ? 0 : Math.floor(diffMs / (1000 * 60 * 60 * 24));
}

function computeContactHealthV1(args: {
  last_contacted_at?: string | null;
  open_actions: number;
  overdue_actions: number;
  has_notes?: boolean;
  has_recent_call?: boolean;
}) {
  const reasons: string[] = [];
  let score = 100;

  const ds = healthDaysSince(args.last_contacted_at);
  if (ds !== null && ds >= 14) {
    score -= 30;
    reasons.push(`No contact in ${ds} days`);
  }

  if ((args.overdue_actions ?? 0) > 0) {
    score -= 20;
    reasons.push(`${args.overdue_actions} overdue action${args.overdue_actions === 1 ? "" : "s"}`);
  }

  const openOver3 = Math.max(0, (args.open_actions ?? 0) - 3);
  if (openOver3 > 0) {
    score -= 10 * openOver3;
    reasons.push(`${args.open_actions} open actions`);
  }

  // Activity awareness (best-effort signals)
  if (args.has_recent_call) {
    score += 10;
    reasons.push("Recent call activity");
  }

  if (args.has_notes) {
    score += 5;
    reasons.push("Notes present");
  }

  score = healthClamp(score, 0, 100);

  let band: "healthy" | "watch" | "at_risk" = "healthy";
  if (score < 50) band = "at_risk";
  else if (score < 75) band = "watch";

  const health = {
    score,
    band,
    reasons,
    stats: {
      open_actions: args.open_actions ?? 0,
      overdue_actions: args.overdue_actions ?? 0,
      last_contacted_days: ds,
      has_notes: Boolean(args.has_notes),
      has_recent_call: Boolean(args.has_recent_call),
    },
  };

  const next_action = deriveNextAction(health);

  return {
    ...health,
    next_action,
  };
}

function deriveNextAction(health: {
  score: number;
  band: "healthy" | "watch" | "at_risk";
  reasons: string[];
  stats: {
    open_actions: number;
    overdue_actions: number;
    last_contacted_days: number | null;
    has_notes: boolean;
    has_recent_call: boolean;
  };
}) {
  const overdue = health?.stats?.overdue_actions ?? 0;
  const open = health?.stats?.open_actions ?? 0;
  const days = health?.stats?.last_contacted_days;

  if (overdue > 0) {
    return {
      type: "resolve_overdue" as const,
      label: "Resolve overdue actions",
      severity: "high" as const,
    };
  }

  if (typeof days === "number" && days > 14) {
    return {
      type: "follow_up" as const,
      label: "Schedule follow-up",
      severity: "medium" as const,
    };
  }

  if (open === 0) {
    return {
      type: "log_next_step" as const,
      label: "Log next step",
      severity: "low" as const,
    };
  }

  return {
    type: "none" as const,
    label: "Contact healthy",
    severity: "none" as const,
  };
}

// Org-scope guard (fail-closed)
// Requires a `reps.org_id` column. If missing, fail closed (500) so we don't leak cross-org.
async function assertRequesterInOrg(args: { requester: string; orgId: string }) {
  const { requester, orgId } = args;

  // Fail-closed: we require reps.id + reps.org_id. (No reps.user_id support.)
  const { data, error } = await supa
    .from("reps")
    .select("id, org_id")
    .eq("id", requester)
    .limit(1)
    .maybeSingle();

  if (error) {
    const msg = String((error as any)?.message ?? "");
    const lower = msg.toLowerCase();

    // Missing org_id column or reps table => fail closed
    if (
      (lower.includes("could not find") && lower.includes("org_id")) ||
      (lower.includes("column") && lower.includes("org_id") && lower.includes("does not exist")) ||
      (lower.includes("relation") && lower.includes("reps") && lower.includes("does not exist"))
    ) {
      throw new Error("org_scope_not_supported");
    }

    throw new Error(msg || "requester_lookup_failed");
  }

  if (!data) throw new Error("requester_not_found");

  const repOrg = (data as any).org_id ?? null;
  if (!repOrg) throw new Error("org_scope_not_supported");
  if (String(repOrg) !== orgId) throw new Error("forbidden_org_scope");
}

// Manager org scope helper (fail-closed by default)
// Requires:
// - x-user-id header (already enforced)
// - x-org-id header (UUID)
// - requester must belong to org (assertRequesterInOrg)
//
// Dev bypass:
// - If x-org-id is the all-zero UUID OR env ALLOW_ORG_BYPASS=1, we skip org membership checks.
// - This avoids blocking local smoke tests before org plumbing is fully wired.
async function requireManagerOrg(req: any): Promise<{ requester: string; orgId: string; bypassed: boolean }> {
  const requester = getUserIdHeader(req);

  // Header preferred; env fallback supported (DEFAULT_ORG_ID).
  const orgId = getOrgIdHeader(req);

  const allowBypass = String(process.env.ALLOW_ORG_BYPASS || "").trim() === "1";
  const isZeroOrg = orgId === "00000000-0000-0000-0000-000000000000";

  const headerProvided = !!String(req.header("x-org-id") || "").trim();

  if (allowBypass || isZeroOrg || !headerProvided) {
    // When header is missing we cannot safely validate membership; treat as bypassed (dev/local).
    return { requester, orgId, bypassed: true };
  }

  await assertRequesterInOrg({ requester, orgId });
  return { requester, orgId, bypassed: false };
}

// ------------------------------
// Cron auth helper (fail-closed)
// ------------------------------
function requireCronAuth(req: any) {
  const expected = String(process.env.CRON_SECRET || "").trim();
  if (!expected) throw new Error("cron_secret_not_configured");

  const provided = String(req.header("x-cron-secret") || "").trim();
  if (!provided || provided !== expected) throw new Error("invalid_cron_secret");
}

// Helper for safe contacts query (handles last_contacted_at column missing)
async function selectContactsSafe(args: {
  requester: string;
  query: string;
  limit: number;
}) {
  const { requester, query, limit } = args;

  const orFilter =
    `first_name.ilike.%${query}%` +
    `,last_name.ilike.%${query}%` +
    `,email.ilike.%${query}%` +
    `,company.ilike.%${query}%`;

  // Try with last_contacted_at first (if column exists)
  const attempt1 = await supa
    .from("crm_contacts")
    .select("id, first_name, last_name, email, company, last_contacted_at, created_at")
    .eq("user_id", requester)
    .or(orFilter)
    .order("created_at", { ascending: false })
    .limit(limit);

  if (!attempt1.error) return { rows: attempt1.data ?? [], hasLastContacted: true };

  // If the error is "column ... does not exist", retry without it
  const msg = String((attempt1.error as any)?.message ?? "");
  if (msg.toLowerCase().includes("last_contacted_at") && msg.toLowerCase().includes("does not exist")) {
    const attempt2 = await supa
      .from("crm_contacts")
      .select("id, first_name, last_name, email, company, created_at")
      .eq("user_id", requester)
      .or(orFilter)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (attempt2.error) throw attempt2.error;
    return { rows: attempt2.data ?? [], hasLastContacted: false };
  }

  // Any other error: surface it
  throw attempt1.error;
}

// GET /v1/crm/health
router.get("/health", async (_req, res) => {
  return res.json({ ok: true, service: "gravix-crm", ts: new Date().toISOString() });
});

/* ---------------------------------------------
   OPPORTUNITIES PIPELINE (Kanban v1)

   GET /v1/crm/opportunities/pipeline
   - Returns opportunities grouped by stage for the current requester
   - Schema tolerant: if crm_opportunities.stage missing, defaults to "new"

   PATCH /v1/crm/opportunities/:id/stage
   Body: { stage: string }
   - Updates the stage for a single opportunity owned by requester
   - Schema tolerant: if stage column missing, returns a clear error
---------------------------------------------- */

const PIPELINE_STAGES_DEFAULT = [
  "new",
  "qualified",
  "proposal",
  "negotiation",
  "won",
  "lost",
];

const MANAGER_ROLES = new Set(["Manager", "Owner"]);

async function isManagerUser(userId: string) {
  try {
    const { data, error } = await supa
      .from("reps")
      .select("tier")
      .eq("id", userId)
      .limit(1)
      .maybeSingle();

    if (error) return false;
    const tier = String((data as any)?.tier || "");
    return MANAGER_ROLES.has(tier);
  } catch {
    return false;
  }
}

async function listOrgRepIdsBestEffort(args: { orgId: string }) {
  const { orgId } = args;
  try {
    const { data, error } = await supa
      .from("reps")
      .select("id")
      .eq("org_id", orgId)
      .limit(500);

    if (error) return [];
    return ((data as any[]) ?? []).map((r) => String((r as any).id)).filter(Boolean);
  } catch {
    return [];
  }
}

type PipelineOpp = {
  id: string;
  name: string | null;
  stage: string;
  amount: number | null;
  close_date: string | null;
  contact_id: string | null;
  account_id: string | null;
  updated_at: string | null;
  created_at: string | null;
};

function normaliseStage(s: any) {
  const v = String(s ?? "").trim().toLowerCase();
  return v || "new";
}

router.get("/opportunities/pipeline", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const qAny = req.query as any;
    const limit = Math.min(Math.max(Number(qAny?.limit ?? 300), 1), 1000);

    // scope:
    // - mine (default): only my opps
    // - team: manager view across org (optionally filtered by repId)
    const scope = String(qAny?.scope ?? "mine").trim().toLowerCase();
    const repIdRaw = String(qAny?.repId ?? qAny?.rep_id ?? "").trim();

    const wantTeam = scope === "team";

    // Resolve allowed user_ids for query
    // - mine: use requester
    // - team: use repId (if provided) OR all reps in org
    let userIds: string[] | null = null; // null => use requester

    if (wantTeam) {
      const { orgId, bypassed } = await requireManagerOrg(req);

      // Require manager tier to view team pipeline
      const okManager = await isManagerUser(requester);
      if (!okManager) {
        return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
      }

      // If a repId is provided, ensure it's a UUID and (when not bypassed) belongs to the org.
      if (repIdRaw) {
        if (!UUID_RE.test(repIdRaw)) {
          return res.status(400).json({ ok: false, error: "invalid_rep_id" });
        }

        if (!bypassed) {
          // Confirm the target rep is in the same org (fail-closed)
          const { data: repRow, error: repErr } = await supa
            .from("reps")
            .select("id, org_id")
            .eq("id", repIdRaw)
            .limit(1)
            .maybeSingle();

          if (repErr) return res.status(500).json({ ok: false, error: repErr.message ?? "rep_lookup_failed" });
          if (!repRow) return res.status(404).json({ ok: false, error: "rep_not_found" });
          if (String((repRow as any).org_id ?? "") !== orgId) {
            return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
          }
        }

        userIds = [repIdRaw];
      } else {
        // Team-wide: pull rep ids for org (when possible). If we can't, fail closed.
        if (!bypassed) {
          const ids = await listOrgRepIdsBestEffort({ orgId });
          if (!ids.length) {
            return res.status(500).json({ ok: false, error: "org_scope_not_supported" });
          }
          userIds = ids;
        } else {
          // Dev bypass: fall back to requester only to avoid leaking cross-tenant data.
          userIds = [requester];
        }
      }
    }

    // Try a rich select first; strip missing columns if needed.
    const selectCandidates = [
      "id, name, stage, amount, close_date, contact_id, account_id, updated_at, created_at, user_id",
      "id, name, stage, amount, close_date, contact_id, account_id, created_at, user_id",
      "id, name, stage, amount, close_date, contact_id, account_id, created_at",
      "id, name, stage, amount, close_date, contact_id, account_id",
      "id, name, stage, created_at",
      "id, name, stage",
      "id, name",
      "id",
    ];

    let rows: any[] = [];
    let lastErr: any = null;

    for (const sel of selectCandidates) {
      let r;
      if (wantTeam) {
        r = await supa
          .from("crm_opportunities")
          .select(sel)
          .in("user_id", userIds ?? [requester])
          .order("created_at", { ascending: false })
          .limit(limit);
      } else {
        r = await supa
          .from("crm_opportunities")
          .select(sel)
          .eq("user_id", requester)
          .order("created_at", { ascending: false })
          .limit(limit);
      }

      if (!r.error) {
        rows = (r.data as any[]) ?? [];
        lastErr = null;
        break;
      }

      lastErr = r.error;

      const msg = String((r.error as any)?.message ?? "").toLowerCase();
      // Missing table / schema-cache miss => fail-open with empty pipeline
      if (
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find the table") && msg.includes("crm_opportunities")) ||
        (msg.includes("schema cache") && msg.includes("crm_opportunities"))
      ) {
        return res.json({ ok: true, stages: PIPELINE_STAGES_DEFAULT, columns: {}, items: [] });
      }

      // Missing column => try next candidate
      if (msg.includes("column") && msg.includes("does not exist")) continue;

      // Unknown error => stop
      break;
    }

    if (lastErr) {
      return res
        .status(500)
        .json({ ok: false, error: (lastErr as any)?.message ?? "pipeline_fetch_failed" });
    }

    const items: PipelineOpp[] = rows.map((r: any) => {
      const stage = normaliseStage(r.stage);
      return {
        id: String(r.id),
        name: (r as any).name ?? null,
        stage,
        amount: (r as any).amount ?? null,
        close_date: (r as any).close_date ?? null,
        contact_id: (r as any).contact_id ?? null,
        account_id: (r as any).account_id ?? null,
        updated_at: (r as any).updated_at ?? null,
        created_at: (r as any).created_at ?? null,
      };
    });

    // Derive stage list (stable default + any custom stages found)
    const foundStages = new Set<string>();
    for (const it of items) foundStages.add(normaliseStage(it.stage));

    const stages = Array.from(
      new Set([
        ...PIPELINE_STAGES_DEFAULT,
        ...Array.from(foundStages).filter(Boolean),
      ])
    );

    // Build columns map
    const columns: Record<string, string[]> = {};
    for (const s of stages) columns[s] = [];
    for (const it of items) {
      const s = normaliseStage(it.stage);
      if (!columns[s]) columns[s] = [];
      columns[s].push(it.id);
    }

    return res.json({ ok: true, stages, columns, items });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   GET /v1/crm/opportunities/pipeline/summary?scope=&repId=&limit=
   - Returns server-computed metrics for the Kanban summary bar.
   - Uses the SAME scope rules as /opportunities/pipeline:
     - scope=mine (default): only requester
     - scope=team: manager-only; optional repId filter; otherwise all reps in org
   - Schema-tolerant: if crm_opportunities table missing, returns empty summary.
---------------------------------------------- */
router.get("/opportunities/pipeline/summary", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const qAny = req.query as any;
    const limit = Math.min(Math.max(Number(qAny?.limit ?? 2000), 1), 5000);

    const scope = String(qAny?.scope ?? "mine").trim().toLowerCase();
    const repIdRaw = String(qAny?.repId ?? qAny?.rep_id ?? "").trim();
    const wantTeam = scope === "team";

    // Resolve allowed user_ids for query
    let userIds: string[] | null = null; // null => requester only

    if (wantTeam) {
      const { orgId, bypassed } = await requireManagerOrg(req);

      const okManager = await isManagerUser(requester);
      if (!okManager) {
        return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
      }

      if (repIdRaw) {
        if (!UUID_RE.test(repIdRaw)) {
          return res.status(400).json({ ok: false, error: "invalid_rep_id" });
        }

        if (!bypassed) {
          const { data: repRow, error: repErr } = await supa
            .from("reps")
            .select("id, org_id")
            .eq("id", repIdRaw)
            .limit(1)
            .maybeSingle();

          if (repErr) return res.status(500).json({ ok: false, error: repErr.message ?? "rep_lookup_failed" });
          if (!repRow) return res.status(404).json({ ok: false, error: "rep_not_found" });
          if (String((repRow as any).org_id ?? "") !== orgId) {
            return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
          }
        }

        userIds = [repIdRaw];
      } else {
        if (!bypassed) {
          const ids = await listOrgRepIdsBestEffort({ orgId });
          if (!ids.length) {
            return res.status(500).json({ ok: false, error: "org_scope_not_supported" });
          }
          userIds = ids;
        } else {
          // Dev bypass: safest behaviour is to scope to requester only.
          userIds = [requester];
        }
      }
    }

    // Schema-tolerant selects
    const selectCandidates = [
      "id, stage, amount, value, currency, close_date, updated_at, created_at, user_id",
      "id, stage, amount, value, close_date, updated_at, created_at, user_id",
      "id, stage, amount, value, close_date, created_at, user_id",
      "id, stage, amount, close_date, created_at, user_id",
      "id, stage, amount, close_date, created_at",
      "id, stage, amount, created_at",
      "id, stage",
      "id",
    ];

    let rows: any[] = [];
    let lastErr: any = null;

    for (const sel of selectCandidates) {
      let r;
      if (wantTeam) {
        r = await supa
          .from("crm_opportunities")
          .select(sel)
          .in("user_id", userIds ?? [requester])
          .order("created_at", { ascending: false })
          .limit(limit);
      } else {
        r = await supa
          .from("crm_opportunities")
          .select(sel)
          .eq("user_id", requester)
          .order("created_at", { ascending: false })
          .limit(limit);
      }

      if (!r.error) {
        rows = (r.data as any[]) ?? [];
        lastErr = null;
        break;
      }

      lastErr = r.error;

      const msg = String((r.error as any)?.message ?? "").toLowerCase();
      // Missing table / schema-cache miss => return empty summary (fail-open)
      if (
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find the table") && msg.includes("crm_opportunities")) ||
        (msg.includes("schema cache") && msg.includes("crm_opportunities"))
      ) {
        return res.json({
          ok: true,
          scope: wantTeam ? "team" : "mine",
          rep_id: wantTeam ? (repIdRaw || null) : null,
          stages: PIPELINE_STAGES_DEFAULT,
          counts_by_stage: Object.fromEntries(PIPELINE_STAGES_DEFAULT.map((s) => [s, 0])),
          total_count: 0,
          total_amount: 0,
          currency: null,
          forecast_amount: 0,
          forecast_count: 0,
        });
      }

      // Missing column => try next candidate
      if (msg.includes("column") && msg.includes("does not exist")) continue;

      break;
    }

    if (lastErr) {
      return res.status(500).json({ ok: false, error: (lastErr as any)?.message ?? "pipeline_summary_failed" });
    }

    const counts: Record<string, number> = {};
    for (const s of PIPELINE_STAGES_DEFAULT) counts[s] = 0;

    let total = 0;
    let forecastAmount = 0;
    let forecastCount = 0;

    for (const r of rows) {
      const stage = normaliseStage((r as any).stage);
      if (counts[stage] == null) counts[stage] = 0;
      counts[stage] += 1;

      const amtRaw = (r as any).amount ?? (r as any).value ?? null;
      const amt = typeof amtRaw === "number" ? amtRaw : Number(amtRaw);

      if (Number.isFinite(amt)) {
        total += amt;

        // MVP forecast = proposal + negotiation only (server-calculated)
        if (stage === "proposal" || stage === "negotiation") {
          forecastAmount += amt;
          forecastCount += 1;
        }
      } else {
        // still count forecast items even if amount missing
        if (stage === "proposal" || stage === "negotiation") {
          forecastCount += 1;
        }
      }
    }

    // Include any non-default stages that exist
    const stages = Array.from(new Set([...PIPELINE_STAGES_DEFAULT, ...Object.keys(counts)])).filter(Boolean);

    // Currency is not reliable when mixed; keep null for now.
    return res.json({
      ok: true,
      scope: wantTeam ? "team" : "mine",
      rep_id: wantTeam ? (repIdRaw || null) : null,
      stages,
      counts_by_stage: counts,
      total_count: rows.length,
      total_amount: total,
      forecast_amount: forecastAmount,
      forecast_count: forecastCount,
      currency: null,
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

const UpdateOpportunityStageSchema = z.object({
  stage: z.string().trim().min(1).max(60),
});

router.patch("/opportunities/:id/stage", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id_required" });

    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid_id" });
    }

    const body = UpdateOpportunityStageSchema.parse(req.body ?? {});
    const stage = normaliseStage(body.stage);

    // Default: only update my opp
    // Manager option: allow team update via ?scope=team&repId=... (or team-wide if omitted)
    const qAny = req.query as any;
    const scope = String(qAny?.scope ?? "mine").trim().toLowerCase();
    const wantTeam = scope === "team";
    const repIdRaw = String(qAny?.repId ?? qAny?.rep_id ?? "").trim();

    let repIdsForUpdate: string[] | null = null;

    if (wantTeam) {
      const { orgId, bypassed } = await requireManagerOrg(req);

      const okManager = await isManagerUser(requester);
      if (!okManager) {
        return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
      }

      if (repIdRaw) {
        if (!UUID_RE.test(repIdRaw)) {
          return res.status(400).json({ ok: false, error: "invalid_rep_id" });
        }

        if (!bypassed) {
          const { data: repRow, error: repErr } = await supa
            .from("reps")
            .select("id, org_id")
            .eq("id", repIdRaw)
            .limit(1)
            .maybeSingle();

          if (repErr) return res.status(500).json({ ok: false, error: repErr.message ?? "rep_lookup_failed" });
          if (!repRow) return res.status(404).json({ ok: false, error: "rep_not_found" });
          if (String((repRow as any).org_id ?? "") !== orgId) {
            return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
          }
        }

        repIdsForUpdate = [repIdRaw];
      } else {
        if (!bypassed) {
          const ids = await listOrgRepIdsBestEffort({ orgId });
          if (!ids.length) {
            return res.status(500).json({ ok: false, error: "org_scope_not_supported" });
          }
          repIdsForUpdate = ids;
        } else {
          repIdsForUpdate = [requester];
        }
      }
    }

    // Try update; if stage column missing, return clear error.
    let r;

    if (wantTeam && repIdsForUpdate) {
      r = await supa
        .from("crm_opportunities")
        .update({ stage })
        .eq("id", id)
        .in("user_id", repIdsForUpdate)
        .select("id, stage")
        .maybeSingle();
    } else {
      r = await supa
        .from("crm_opportunities")
        .update({ stage })
        .eq("id", id)
        .eq("user_id", requester)
        .select("id, stage")
        .maybeSingle();
    }

    if (r.error) {
      const msg = String((r.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }
      if (msg.includes("column") && msg.includes("stage") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_stage_missing" });
      }
      return res
        .status(500)
        .json({ ok: false, error: (r.error as any)?.message ?? "opportunity_stage_update_failed" });
    }

    if (!r.data) {
      return res.status(404).json({ ok: false, error: "not_found" });
    }

    return res.json({ ok: true, opportunity: { id, stage: (r.data as any).stage ?? stage } });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   OPPORTUNITY DETAIL (Drawer v1)

   GET /v1/crm/opportunities/:id
   - Returns a single opportunity (scoped like pipeline)

   PATCH /v1/crm/opportunities/:id
   - Allows editing: name, stage, amount, currency, close_date
---------------------------------------------- */

function parseScopeParams(qAny: any) {
  const scope = String(qAny?.scope ?? "mine").trim().toLowerCase();
  const repIdRaw = String(qAny?.repId ?? qAny?.rep_id ?? "").trim();
  const wantTeam = scope === "team";
  return { scope, repIdRaw, wantTeam };
}

async function resolveTeamUserIdsOrThrow(args: {
  req: any;
  requester: string;
  repIdRaw: string;
}): Promise<string[]> {
  const { req, requester, repIdRaw } = args;

  const { orgId, bypassed } = await requireManagerOrg(req);

  const okManager = await isManagerUser(requester);
  if (!okManager) {
    throw new Error("forbidden_not_manager");
  }

  if (repIdRaw) {
    if (!UUID_RE.test(repIdRaw)) {
      throw new Error("invalid_rep_id");
    }

    if (!bypassed) {
      const { data: repRow, error: repErr } = await supa
        .from("reps")
        .select("id, org_id")
        .eq("id", repIdRaw)
        .limit(1)
        .maybeSingle();

      if (repErr) throw new Error(repErr.message ?? "rep_lookup_failed");
      if (!repRow) throw new Error("rep_not_found");
      if (String((repRow as any).org_id ?? "") !== orgId) {
        throw new Error("forbidden_org_scope");
      }
    }

    return [repIdRaw];
  }

  if (!bypassed) {
    const ids = await listOrgRepIdsBestEffort({ orgId });
    if (!ids.length) throw new Error("org_scope_not_supported");
    return ids;
  }

  // Dev bypass: safest is requester-only.
  return [requester];
}

const OpportunityUpdateSchema = z
  .object({
    name: z.string().trim().min(1).max(200).optional(),
    stage: z.string().trim().min(1).max(60).optional(),
    amount: z.number().finite().optional().nullable(),
    currency: z.string().trim().min(1).max(10).optional().nullable(),
    close_date: z.string().trim().min(1).max(40).optional().nullable(),
  })
  .refine(
    (b) =>
      b.name !== undefined ||
      b.stage !== undefined ||
      b.amount !== undefined ||
      b.currency !== undefined ||
      b.close_date !== undefined,
    { message: "no_fields" }
  );

router.get("/opportunities/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();

    if (!id) return res.status(400).json({ ok: false, error: "id_required" });
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

    const qAny = req.query as any;
    const { repIdRaw, wantTeam } = parseScopeParams(qAny);

    let allowedUserIds: string[] = [requester];

    if (wantTeam) {
      try {
        allowedUserIds = await resolveTeamUserIdsOrThrow({ req, requester, repIdRaw });
      } catch (e: any) {
        const msg = String(e?.message ?? e);
        if (msg === "forbidden_not_manager") return res.status(403).json({ ok: false, error: msg });
        if (msg === "invalid_rep_id") return res.status(400).json({ ok: false, error: msg });
        if (msg === "rep_not_found") return res.status(404).json({ ok: false, error: msg });
        if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: msg });
        if (msg === "org_scope_not_supported") return res.status(500).json({ ok: false, error: msg });
        return res.status(500).json({ ok: false, error: msg || "team_scope_failed" });
      }
    }

    const selectCandidates = [
      "id, user_id, name, title, stage, amount, currency, close_date, account_id, contact_id, account_name, contact_email, created_at, updated_at",
      "id, user_id, name, title, stage, amount, currency, close_date, account_id, contact_id, created_at, updated_at",
      "id, user_id, name, title, stage, amount, currency, close_date, account_id, contact_id, created_at",
      "id, user_id, name, title, stage, amount, currency, close_date, created_at",
      "id, user_id, name, title, stage, amount, close_date, created_at",
      "id, user_id, name, title, stage, created_at",
      "id, user_id, name, stage, created_at",
      "id, user_id, name, stage",
      "id, name, stage",
      "id, name",
      "id",
    ];

    let row: any = null;
    let lastErr: any = null;

    for (const sel of selectCandidates) {
      const r = await supa
        .from("crm_opportunities")
        .select(sel)
        .eq("id", id)
        .in("user_id", allowedUserIds)
        .limit(1)
        .maybeSingle();

      if (!r.error) {
        row = r.data ?? null;
        lastErr = null;
        break;
      }

      lastErr = r.error;
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      if (
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find the table") && msg.includes("crm_opportunities")) ||
        (msg.includes("schema cache") && msg.includes("crm_opportunities"))
      ) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }

      if (msg.includes("column") && msg.includes("does not exist")) continue;
      break;
    }

    if (lastErr) {
      return res.status(500).json({ ok: false, error: (lastErr as any)?.message ?? "opportunity_fetch_failed" });
    }

    if (!row) return res.status(404).json({ ok: false, error: "not_found" });

    return res.json({
      ok: true,
      opportunity: {
        id: String((row as any).id),
        name: (row as any).name ?? (row as any).title ?? null,
        stage: normaliseStage((row as any).stage ?? "new"),
        amount: (row as any).amount ?? null,
        currency: (row as any).currency ?? null,
        close_date: (row as any).close_date ?? null,
        account_id: (row as any).account_id ?? null,
        contact_id: (row as any).contact_id ?? null,
        account_name: (row as any).account_name ?? null,
        contact_email: (row as any).contact_email ?? null,
        created_at: (row as any).created_at ?? null,
        updated_at: (row as any).updated_at ?? null,
      },
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

router.patch("/opportunities/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();

    if (!id) return res.status(400).json({ ok: false, error: "id_required" });
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

    const qAny = req.query as any;
    const { repIdRaw, wantTeam } = parseScopeParams(qAny);

    let allowedUserIds: string[] = [requester];

    if (wantTeam) {
      try {
        allowedUserIds = await resolveTeamUserIdsOrThrow({ req, requester, repIdRaw });
      } catch (e: any) {
        const msg = String(e?.message ?? e);
        if (msg === "forbidden_not_manager") return res.status(403).json({ ok: false, error: msg });
        if (msg === "invalid_rep_id") return res.status(400).json({ ok: false, error: msg });
        if (msg === "rep_not_found") return res.status(404).json({ ok: false, error: msg });
        if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: msg });
        if (msg === "org_scope_not_supported") return res.status(500).json({ ok: false, error: msg });
        return res.status(500).json({ ok: false, error: msg || "team_scope_failed" });
      }
    }

    const body = OpportunityUpdateSchema.parse(req.body ?? {});

    const patchBase: any = {};
    if (body.name !== undefined) patchBase.name = body.name;
    if (body.stage !== undefined) patchBase.stage = normaliseStage(body.stage);
    if (body.amount !== undefined) patchBase.amount = body.amount;
    if (body.currency !== undefined) patchBase.currency = body.currency;
    if (body.close_date !== undefined) patchBase.close_date = body.close_date;

    const attempts: any[] = [
      { ...patchBase },
      (() => {
        const p = { ...patchBase };
        delete p.close_date;
        return p;
      })(),
      (() => {
        const p = { ...patchBase };
        delete p.currency;
        return p;
      })(),
      (() => {
        const p = { ...patchBase };
        delete p.amount;
        return p;
      })(),
      (() => {
        const p = { ...patchBase };
        delete p.stage;
        return p;
      })(),
      (() => {
        const p = { ...patchBase };
        delete p.name;
        return p;
      })(),
    ].filter((p) => Object.keys(p).length > 0);

    let updated: any = null;
    let lastErr: any = null;

    for (const p of attempts) {
      const r = await supa
        .from("crm_opportunities")
        .update(p)
        .eq("id", id)
        .in("user_id", allowedUserIds)
        .select("id, name, title, stage, amount, currency, close_date, account_id, contact_id, account_name, contact_email, created_at, updated_at")
        .maybeSingle();

      if (!r.error) {
        updated = r.data ?? null;
        lastErr = null;
        break;
      }

      lastErr = r.error;
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      if (
        (msg.includes("relation") && msg.includes("does not exist")) ||
        (msg.includes("could not find the table") && msg.includes("crm_opportunities")) ||
        (msg.includes("schema cache") && msg.includes("crm_opportunities"))
      ) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }

      const isMissingCol =
        (msg.includes("column") && msg.includes("does not exist")) ||
        msg.includes("could not find") ||
        msg.includes("unknown column");

      if (isMissingCol) continue;
      break;
    }

    if (lastErr) {
      return res.status(500).json({ ok: false, error: (lastErr as any)?.message ?? "opportunity_update_failed" });
    }

    if (!updated) return res.status(404).json({ ok: false, error: "not_found" });

    return res.json({
      ok: true,
      opportunity: {
        id: String((updated as any).id),
        name: (updated as any).name ?? (updated as any).title ?? null,
        stage: normaliseStage((updated as any).stage ?? "new"),
        amount: (updated as any).amount ?? null,
        currency: (updated as any).currency ?? null,
        close_date: (updated as any).close_date ?? null,
        account_id: (updated as any).account_id ?? null,
        contact_id: (updated as any).contact_id ?? null,
        account_name: (updated as any).account_name ?? null,
        contact_email: (updated as any).contact_email ?? null,
        created_at: (updated as any).created_at ?? null,
        updated_at: (updated as any).updated_at ?? null,
      },
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg === "no_fields") {
      return res.status(400).json({ ok: false, error: "no_fields" });
    }
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/** ----------------------------------------------------------------
 * Demo contacts (until DB is populated)
 * ---------------------------------------------------------------- */
type DemoContact = {
  id: string;
  first_name: string;
  last_name: string;
  email: string;
  company?: string | null;
  last_contacted_at?: string | null;
};

const DEMO_CONTACTS: DemoContact[] = [
  { id: "c_anna", first_name: "Anna", last_name: "Rivera", email: "anna@demo.co", company: "Demo Co", last_contacted_at: null },
  { id: "c_bob", first_name: "Bob", last_name: "Trent", email: "bob@demo.co", company: "Demo Co", last_contacted_at: null },
  { id: "c_eric", first_name: "Eric", last_name: "Cole", email: "eric@demo.co", company: "Demo Co", last_contacted_at: null },
  { id: "c_lena", first_name: "Lena", last_name: "Yao", email: "lena@demo.co", company: "Demo Co", last_contacted_at: null },
  { id: "c_mark", first_name: "Mark", last_name: "Patel", email: "mark@demo.co", company: "Demo Co", last_contacted_at: null },
  { id: "c_olga", first_name: "Olga", last_name: "Smith", email: "olga@demo.co", company: "Demo Co", last_contacted_at: null },
];

function shapeDemoContact(c: DemoContact) {
  return {
    id: c.id,
    name: [c.first_name, c.last_name].filter(Boolean).join(" ").trim(),
    email: c.email ?? null,
    company: c.company ?? null,
    last_contacted_at: c.last_contacted_at ?? null,
  };
}


/* ---------------------------------------------
   GET /v1/crm/contacts?query=&limit=
   - Search contacts by name/email/company (case-insensitive)
   - Returns minimal fields for pickers/drawers
   - Safe: empty query => empty list (never returns demo)
---------------------------------------------- */
router.get("/contacts", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    // Express query params can be string | string[] | undefined.
    // Accept aliases to avoid silent empty-query bugs.
    const raw: unknown =
      (req.query.query as any) ??
      (req.query.q as any) ??
      (req.query.term as any) ??
      (req.query.search as any) ??
      "";

    const query =
      typeof raw === "string"
        ? raw.trim()
        : Array.isArray(raw)
          ? String(raw[0] ?? "").trim()
          : "";

    const limit = Math.min(Math.max(Number(req.query.limit ?? 10), 1), 50);

    // Empty query => empty list (never spam demo)
    if (!query) return res.json({ ok: true, items: [] });

    // 1) DB search first
    const { rows, hasLastContacted } = await selectContactsSafe({ requester, query, limit });

    if (rows.length) {
      const shaped = rows.map((c: any) => ({
        id: c.id,
        name: [c.first_name, c.last_name].filter(Boolean).join(" ").trim(),
        email: c.email ?? null,
        company: c.company ?? null,
        last_contacted_at: hasLastContacted ? (c.last_contacted_at ?? null) : null,
      }));

      return res.json({ ok: true, items: shaped, source: "db" });
    }

    // 2) No DB matches — if this user has ANY real contacts, return empty (no demo pollution)
    const { data: anyReal, error: anyErr } = await supa
      .from("crm_contacts")
      .select("id")
      .eq("user_id", requester)
      .limit(1);

    if (anyErr) {
      return res.status(500).json({ ok: false, error: anyErr.message ?? "contacts_count_failed" });
    }

    const hasRealContacts = (anyReal?.length ?? 0) > 0;
    if (hasRealContacts) {
      return res.json({ ok: true, items: [], source: "db_empty" });
    }

    // 3) Demo fallback ONLY when user has zero real contacts
    const q = query.toLowerCase();
    const demo = DEMO_CONTACTS.filter((c) => {
      const hay = `${c.first_name} ${c.last_name} ${c.email} ${c.company ?? ""}`.toLowerCase();
      return hay.includes(q);
    })
      .slice(0, limit)
      .map(shapeDemoContact);

    return res.json({ ok: true, items: demo, source: "demo" });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   GET /v1/crm/contacts/:id
   - Fetch a single contact (owned by requester)
   - Supports demo ids (c_*) until DB is populated
---------------------------------------------- */


router.get("/contacts/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String(req.params.id);

    // Demo id path
    if (id.startsWith("c_")) {
      const found = DEMO_CONTACTS.find((c) => c.id === id);
      if (!found) return res.status(404).json({ ok: false, error: "not_found" });

      const health = computeContactHealthV1({
        last_contacted_at: found.last_contacted_at ?? null,
        open_actions: 0,
        overdue_actions: 0,
        has_notes: false,
        has_recent_call: false,
      });

      return res.json({
        ok: true,
        contact: {
          id: found.id,
          first_name: found.first_name ?? null,
          last_name: found.last_name ?? null,
          email: found.email ?? null,
          company: found.company ?? null,
          last_contacted_at: found.last_contacted_at ?? null,
          created_at: null,
        },
        account: null,
        health,
      });
    }

    // Real DB UUID path
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

    let c: any = null;

    try {
      const a1 = await supa
        .from("crm_contacts")
        .select("id, first_name, last_name, email, company, last_contacted_at, created_at")
        .eq("user_id", requester)
        .eq("id", id)
        .maybeSingle();

      if (!a1.error && a1.data) c = a1.data;

      if (a1.error) {
        const msg = String((a1.error as any)?.message ?? "");
        if (msg.toLowerCase().includes("last_contacted_at") && msg.toLowerCase().includes("does not exist")) {
          const a2 = await supa
            .from("crm_contacts")
            .select("id, first_name, last_name, email, company, created_at")
            .eq("user_id", requester)
            .eq("id", id)
            .maybeSingle();

          if (a2.error || !a2.data) return res.status(404).json({ ok: false, error: "not_found" });
          c = { ...(a2.data as any), last_contacted_at: null };
        } else {
          return res.status(500).json({ ok: false, error: a1.error.message ?? "contact_query_failed" });
        }
      }
    } catch (e: any) {
      return res.status(500).json({ ok: false, error: e?.message ?? "contact_query_failed" });
    }

    if (!c) return res.status(404).json({ ok: false, error: "not_found" });

    // ---- Contact health (best-effort; schema tolerant)
    let open_actions = 0;
    let overdue_actions = 0;

    // ---- Activity awareness (best-effort): notes + recent calls
    let has_notes = false;
    let has_recent_call = false;
    let activityLastIso: string | null = null;

    const pickMaxIso = (a: string | null, b: string | null) => {
      if (!a) return b;
      if (!b) return a;
      const ta = new Date(a).getTime();
      const tb = new Date(b).getTime();
      if (!Number.isFinite(ta)) return b;
      if (!Number.isFinite(tb)) return a;
      return tb > ta ? b : a;
    };

    // Notes: crm_contact_notes(created_at)
    try {
      const rn = await supa
        .from("crm_contact_notes")
        .select("created_at")
        .eq("user_id", requester)
        .eq("contact_id", id)
        .order("created_at", { ascending: false })
        .limit(1);

      if (!rn.error) {
        const row = (rn.data as any[])?.[0] ?? null;
        const iso = row?.created_at ? String(row.created_at) : null;
        if (iso) {
          has_notes = true;
          activityLastIso = pickMaxIso(activityLastIso, iso);
        }
      } else {
        const msg = String((rn.error as any)?.message ?? "").toLowerCase();
        if (!(msg.includes("relation") && msg.includes("does not exist")) && !(msg.includes("does not exist") && msg.includes("column"))) {
          // ignore other errors (fail-soft)
        }
      }
    } catch {
      // fail-soft
    }

    // Calls: crm_call_links(contact_id -> call_id) + calls(created_at)
    try {
      // Link table may not exist
      const links = await supa
        .from("crm_call_links")
        .select("call_id")
        .eq("contact_id", id)
        .limit(25);

      if (!links.error) {
        const callIds = ((links.data as any[]) ?? []).map((r) => r?.call_id).filter(Boolean);
        if (callIds.length) {
          const rc = await supa
            .from("calls")
            .select("id, created_at")
            .eq("user_id", requester)
            .in("id", callIds)
            .order("created_at", { ascending: false })
            .limit(1);

          if (!rc.error) {
            const row = (rc.data as any[])?.[0] ?? null;
            const iso = row?.created_at ? String(row.created_at) : null;
            if (iso) {
              activityLastIso = pickMaxIso(activityLastIso, iso);
              const ds = healthDaysSince(iso);
              has_recent_call = ds !== null ? ds <= 14 : false;
            }
          }
        }
      } else {
        const msg = String((links.error as any)?.message ?? "").toLowerCase();
        if (msg.includes("relation") && msg.includes("does not exist")) {
          // ignore
        }
      }
    } catch {
      // fail-soft
    }

    try {
      const nowMs = Date.now();

      const selCandidates = [
        "id, due_at, completed_at, created_at",
        "id, due_at, completed_at",
        "id, due_at, created_at",
        "id, due_at",
        "id, completed_at",
        "id",
      ];

      let actions: any[] = [];

      for (const sel of selCandidates) {
        const r = await supa
          .from("crm_actions")
          .select(sel)
          .eq("user_id", requester)
          .eq("contact_id", id)
          .order("created_at", { ascending: false })
          .limit(200);

        if (!r.error) {
          actions = (r.data as any[]) ?? [];
          break;
        }

        const msg = String((r.error as any)?.message ?? "").toLowerCase();

        // Missing columns -> try next select
        if (msg.includes("column") && msg.includes("does not exist")) continue;

        // Missing table -> treat as no actions
        if (msg.includes("relation") && msg.includes("does not exist")) {
          actions = [];
          break;
        }

        // Unknown error -> stop trying
        break;
      }

      for (const a of actions) {
        const completedAt = (a as any)?.completed_at ?? null;
        const dueAt = (a as any)?.due_at ?? null;

        const isCompleted = !!completedAt;
        if (!isCompleted) open_actions++;

        if (!isCompleted && dueAt) {
          const dueMs = new Date(String(dueAt)).getTime();
          if (Number.isFinite(dueMs) && dueMs < nowMs) overdue_actions++;
        }
      }
    } catch {
      // fail-soft
    }

    const effectiveLastContactedAt = (c as any)?.last_contacted_at ?? activityLastIso ?? null;

    const health = computeContactHealthV1({
      last_contacted_at: effectiveLastContactedAt,
      open_actions,
      overdue_actions,
      has_notes,
      has_recent_call,
    });

    return res.json({
      ok: true,
      contact: {
        id: c.id,
        first_name: (c as any).first_name ?? null,
        last_name: (c as any).last_name ?? null,
        email: (c as any).email ?? null,
        company: (c as any).company ?? null,
        last_contacted_at: (c as any).last_contacted_at ?? null,
        created_at: (c as any).created_at ?? null,
      },
      account: null,
      health,
    });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// Best-effort contact touch helper (fail-soft)
// Reusable across notes, actions, etc.
//
// Behaviour:
// - Prefer ownership by `crm_contacts.user_id` (current canonical)
// - If that column is missing, fall back to `crm_contacts.requester_id`
// - Never throws; callers may ignore failures
async function touchContactBestEffort(opts: { requester: string; contactId: string }) {
  const { requester, contactId } = opts;
  const nowIso = new Date().toISOString();

  // Helper to detect "missing column" errors from PostgREST
  const isMissingCol = (msg: string, col: string) =>
    (msg.includes(col) && msg.includes("does not exist")) ||
    (msg.includes(col) && msg.includes("could not find"));

  const tryUpdate = async (ownerCol: "user_id" | "requester_id") => {
    // @ts-ignore dynamic column
    const r = await supa
      .from("crm_contacts")
      .update({ last_contacted_at: nowIso })
      .eq("id", contactId)
      // @ts-ignore dynamic column
      .eq(ownerCol, requester)
      .select("id")
      .maybeSingle();

    // Success when a row matched.
    if (!r.error && r.data) return { ok: true as const, last_contacted_at: nowIso };

    // No row matched (owned by someone else or not found)
    if (!r.error && !r.data) return { ok: false as const, reason: "not_found_or_not_owned" };

    const msg = String((r.error as any)?.message ?? "").toLowerCase();

    // Missing ownership column
    if (isMissingCol(msg, ownerCol)) return { ok: false as const, reason: "owner_col_missing" };

    // Missing last_contacted_at column
    if (isMissingCol(msg, "last_contacted_at")) return { ok: false as const, reason: "last_contacted_at_missing" };

    // Missing table
    if (msg.includes("relation") && msg.includes("does not exist")) {
      return { ok: false as const, reason: "crm_contacts_table_missing" };
    }

    return { ok: false as const, reason: "touch_failed", detail: msg };
  };

  // 1) Canonical: user_id
  const r1 = await tryUpdate("user_id");
  if (r1.ok) return r1;

  // If user_id column exists (i.e. failure wasn't "owner_col_missing"), don't bother trying requester_id.
  // This avoids noisy errors like "column crm_contacts.requester_id does not exist" in modern schemas.
  if (r1.reason !== "owner_col_missing") return r1;

  // 2) Legacy fallback: requester_id
  const r2 = await tryUpdate("requester_id");
  if (r2.ok) return r2;

  // If requester_id is also missing, just fail-soft.
  return r2.reason === "owner_col_missing" ? { ok: false as const, reason: "not_supported" } : r2;
}

// POST /v1/crm/contacts/:id/mark-contacted
// Sets crm_contacts.last_contacted_at to now (schema-tolerant ownership key).
router.post("/contacts/:id/mark-contacted", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String((req.params as any)?.id ?? "").trim();

    if (!contactId) {
      return res.status(400).json({ ok: false, error: "contact_id_required" });
    }

    // Demo contacts: just return ok (no DB write)
    if (contactId.startsWith("c_")) {
      return res.json({
        ok: true,
        contact_id: contactId,
        last_contacted_at: new Date().toISOString(),
        source: "demo",
      });
    }

    const UUID_RE =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    if (!UUID_RE.test(contactId)) {
      return res.status(400).json({ ok: false, error: "invalid_contact_id" });
    }

    // Use shared touch helper (fail-hard only for truly invalid ids)
    const touched = await touchContactBestEffort({ requester, contactId });

    if (touched.ok) {
      return res.json({
        ok: true,
        contact_id: contactId,
        last_contacted_at: touched.last_contacted_at,
      });
    }

    // If schema doesn't support this, surface a clear error (this endpoint is meant to be a hard signal)
    if (touched.reason === "last_contacted_at_missing") {
      return res.status(500).json({ ok: false, error: "crm_contacts_last_contacted_at_missing" });
    }

    if (touched.reason === "crm_contacts_table_missing") {
      return res.status(500).json({ ok: false, error: "crm_contacts_table_missing" });
    }

    return res.status(404).json({
      ok: false,
      error: "contact_not_found_or_not_owned",
      detail: (touched as any).detail ?? touched.reason,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: err.message || "mark_contacted_failed",
    });
  }
});

/** ----------------------------------------------------------------
 * Contact Assignments (read-only)
 * GET /v1/crm/contacts/:id/assignments
 * - Returns open + completed assignments tied to a contact.
 * - Safe: read-only; best-effort across schema variants.
 * ---------------------------------------------------------------- */
async function fetchContactAssignmentsBestEffort(args: {
  requester: string;
  contactId: string;
  limit: number;
}) {
  const { requester, contactId, limit } = args;

  // We’ve used a few table/shape variants over time. Try both without breaking.
  const candidateTables = ["assignments", "coach_assignments"]; // fallback order

  const out: any[] = [];
  const seen = new Set<string>();

  for (const table of candidateTables) {
    // 1) Direct column link: contact_id
    try {
      const { data, error } = await supa
        .from(table)
        .select("*")
        .eq("user_id", requester)
        .eq("contact_id", contactId)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (!error && data?.length) {
        for (const row of data) {
          const id = String((row as any).id ?? "");
          if (!id || seen.has(id)) continue;
          seen.add(id);
          out.push(row);
        }
      }
    } catch {
      // ignore
    }

    // 2) Meta link: meta.contact_id (json)
    try {
      const { data, error } = await supa
        .from(table)
        .select("*")
        .eq("user_id", requester)
        // Postgres JSON path filter (works when meta is jsonb)
        .filter("meta->>contact_id", "eq", contactId)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (!error && data?.length) {
        for (const row of data) {
          const id = String((row as any).id ?? "");
          if (!id || seen.has(id)) continue;
          seen.add(id);
          out.push(row);
        }
      }
    } catch {
      // ignore
    }

    // If we already found some rows, no need to hammer more tables.
    if (out.length) break;
  }

  // Normalise to a stable shape for the UI.
  const now = Date.now();
  const items = out.map((r) => {
    const dueAt = (r as any).due_at ?? (r as any).dueAt ?? null;
    const completedAt = (r as any).completed_at ?? (r as any).completedAt ?? null;

    const dueMs = dueAt ? new Date(String(dueAt)).getTime() : NaN;
    const isOverdue = !completedAt && Number.isFinite(dueMs) ? dueMs < now : false;

    return {
      id: (r as any).id,
      type: (r as any).type ?? (r as any).assignment_type ?? null,
      title:
        (r as any).title ??
        (r as any).label ??
        (r as any).name ??
        (r as any).task ??
        null,
      due_at: dueAt,
      completed_at: completedAt,
      created_at: (r as any).created_at ?? (r as any).createdAt ?? null,
      importance: (r as any).importance ?? null,
      meta: (r as any).meta ?? null,
      is_overdue: isOverdue,
    };
  });

  const open = items.filter((x) => !x.completed_at);
  const completed = items.filter((x) => !!x.completed_at);

  return { open, completed };
}

router.get("/contacts/:id/assignments", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    // Demo contacts: return empty (until we have real CRM data)
    if (contactId.startsWith("c_")) {
      return res.json({ ok: true, open: [], completed: [] });
    }

    // Real DB path: accept UUID or any non-empty id (meta-linked schemas may not be UUID)
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    const { open, completed } = await fetchContactAssignmentsBestEffort({ requester, contactId, limit });

    return res.json({ ok: true, open, completed });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// ----------------------------------------------------------------
// Contact Actions (crm_actions-backed)
// GET /v1/crm/contacts/:id/actions
// - This is what the WEB calls.
// - Best-effort: uses crm_actions if it exists; otherwise returns empty.
// ----------------------------------------------------------------
router.get("/contacts/:id/actions", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    // Demo contacts: no real actions yet
    if (contactId.startsWith("c_")) {
      return res.json({ ok: true, open: [], completed: [] });
    }

    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    // Prefer crm_actions stream (this is our stable action source of truth)
    const r = await fetchCrmActionsBestEffort({ requester, contactId, limit });

    // If crm_actions is missing or fails, fail-open with empties (don’t break contact page)
    if (!r.ok) {
      const msg = String((r as any)?.error?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.json({ ok: true, open: [], completed: [] });
      }
      // unknown error: still fail-open for UI
      return res.json({ ok: true, open: [], completed: [] });
    }

    return res.json({ ok: true, open: r.open ?? [], completed: r.completed ?? [] });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// ----------------------------------------------------------------
// GET /v1/crm/contacts/:id/activity
// Unified timeline feed (notes + actions + calls)
// ----------------------------------------------------------------
router.get("/contacts/:id/activity", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();

    if (!contactId) {
      return res.status(400).json({ ok: false, error: "invalid_contact_id" });
    }

    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);

    // Demo contacts
    if (contactId.startsWith("c_")) {
      return res.json({ ok: true, items: [] });
    }

    const items = await fetchContactActivityBestEffort({
      requester,
      contactId,
      limit,
    });

    return res.json({ ok: true, items });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------------------
   CRM ACTIVITIES / TASKS (Day 51)
   type = "task"
--------------------------------------------------------- */

const CreateActivitySchema = z.object({
  type: z.string().trim().min(1).max(40),
  title: z.string().trim().min(1).max(200),
  status: z.string().trim().min(1).max(40).optional().default("open"),
  due_at: z.string().optional().nullable(),
  opportunity_id: z.string().uuid().optional().nullable(),
  contact_id: z.string().uuid().optional().nullable(),
  account_id: z.string().uuid().optional().nullable(),
});

router.post("/activities", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const body = CreateActivitySchema.parse(req.body ?? {});

    const payload: any = {
      user_id: requester,
      type: body.type,
      title: body.title,
      status: body.status ?? "open",
      due_at: body.due_at ?? null,
      opportunity_id: body.opportunity_id ?? null,
      contact_id: body.contact_id ?? null,
      account_id: body.account_id ?? null,
    };

    const r = await supa
      .from("crm_activities")
      .insert(payload)
      .select("id,type,title,status,due_at,opportunity_id,contact_id,account_id,created_at")
      .maybeSingle();

    if (r.error) {
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_activities_table_missing" });
      }

      return res.status(500).json({ ok: false, error: r.error.message ?? "activity_create_failed" });
    }

    return res.json({ ok: true, activity: r.data });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

router.get("/activities", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const type = String(req.query.type ?? "task");
    const status = String(req.query.status ?? "").trim();

    let q = supa
      .from("crm_activities")
      .select("id,type,title,status,due_at,opportunity_id,contact_id,account_id,created_at")
      .eq("user_id", requester)
      .eq("type", type)
      .order("created_at", { ascending: false })
      .limit(200);

    if (status) q = q.eq("status", status);

    const r = await q;

    if (r.error) {
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.json({ ok: true, items: [] });
      }

      return res.status(500).json({ ok: false, error: r.error.message ?? "activities_fetch_failed" });
    }

    return res.json({ ok: true, items: r.data ?? [] });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

const UpdateActivitySchema = z.object({
  title: z.string().trim().min(1).max(200).optional(),
  status: z.string().trim().min(1).max(40).optional(),
  due_at: z.string().optional().nullable(),
});

router.patch("/activities/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String(req.params.id ?? "").trim();

    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid_id" });
    }

    const body = UpdateActivitySchema.parse(req.body ?? {});

    const patch: any = {};
    if (body.title !== undefined) patch.title = body.title;
    if (body.status !== undefined) patch.status = body.status;
    if (body.due_at !== undefined) patch.due_at = body.due_at;

    const r = await supa
      .from("crm_activities")
      .update(patch)
      .eq("id", id)
      .eq("user_id", requester)
      .select("id,type,title,status,due_at")
      .maybeSingle();

    if (r.error) {
      return res.status(500).json({ ok: false, error: r.error.message ?? "activity_update_failed" });
    }

    if (!r.data) return res.status(404).json({ ok: false, error: "not_found" });

    return res.json({ ok: true, activity: r.data });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

router.post("/activities/:id/complete", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String(req.params.id ?? "").trim();

    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid_id" });
    }

    const r = await supa
      .from("crm_activities")
      .update({ status: "done" })
      .eq("id", id)
      .eq("user_id", requester)
      .select("id,status")
      .maybeSingle();

    if (r.error) {
      return res.status(500).json({ ok: false, error: r.error.message ?? "activity_complete_failed" });
    }

    if (!r.data) return res.status(404).json({ ok: false, error: "not_found" });

    return res.json({ ok: true, activity: r.data });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// ---------------------------------------------------------
// DAY 52 — COACHING NUDGES (auto task creation)
// ---------------------------------------------------------

async function createCoachingTask(args: {
  userId: string;
  weakness: "weak_close" | "objection_handling";
  opportunityId?: string | null;
  contactId?: string | null;
  accountId?: string | null;
}) {
  const { userId, weakness, opportunityId, contactId, accountId } = args;

  let title = "";

  if (weakness === "weak_close") {
    title = "Review and strengthen close before next call";
  }

  if (weakness === "objection_handling") {
    title = "Practise objection handling before next follow‑up";
  }

  const payload: any = {
    user_id: userId,
    type: "task",
    title,
    status: "open",
    due_at: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(),
    opportunity_id: opportunityId ?? null,
    contact_id: contactId ?? null,
    account_id: accountId ?? null,
  };

  const r = await supa.from("crm_activities").insert(payload);

  if (r.error) {
    const msg = String((r.error as any)?.message ?? "").toLowerCase();

    if (msg.includes("relation") && msg.includes("does not exist")) {
      return { ok: false, reason: "crm_activities_table_missing" };
    }

    return { ok: false, reason: "activity_create_failed" };
  }

  return { ok: true };
}

const CoachingNudgeSchema = z.object({
  weakness: z.enum(["weak_close", "objection_handling"]),
  opportunity_id: z.string().uuid().optional().nullable(),
  contact_id: z.string().uuid().optional().nullable(),
  account_id: z.string().uuid().optional().nullable(),
});

// POST /v1/crm/coaching/nudges
// Manual endpoint for creating coaching tasks based on detected weaknesses
router.post("/coaching/nudges", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const body = CoachingNudgeSchema.parse(req.body ?? {});

    const r = await createCoachingTask({
      userId: requester,
      weakness: body.weakness,
      opportunityId: body.opportunity_id ?? null,
      contactId: body.contact_id ?? null,
      accountId: body.account_id ?? null,
    });

    if (!r.ok) {
      return res.status(500).json({ ok: false, error: r.reason });
    }

    return res.json({ ok: true, created: true });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// ---------------------------------------------------------
// DAY 52 — REP COACHING HISTORY (manager insight)
// ---------------------------------------------------------

// GET /v1/crm/reps/:id/coaching-history
// Returns lightweight coaching metrics for a rep
router.get("/reps/:id/coaching-history", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const repId = String(req.params.id ?? "").trim();

    if (!UUID_RE.test(repId)) {
      return res.status(400).json({ ok: false, error: "invalid_rep_id" });
    }

    // Manager guard (same pattern used in pipeline team scope)
    const okManager = await isManagerUser(requester);
    if (!okManager && requester !== repId) {
      return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
    }

    // -------------------------------------------------
    // Fetch tasks created for this rep
    // -------------------------------------------------

    const tasksRes = await supa
      .from("crm_activities")
      .select("id,status,title,created_at")
      .eq("user_id", repId)
      .eq("type", "task")
      .limit(500);

    const tasks = tasksRes.data ?? [];

    const coachingTasks = tasks.filter((t: any) =>
      String(t.title || "").toLowerCase().includes("close") ||
      String(t.title || "").toLowerCase().includes("objection")
    );

    const completedTasks = coachingTasks.filter((t: any) => t.status === "done");

    // -------------------------------------------------
    // Fetch call scoring trend (best-effort)
    // -------------------------------------------------

    let avgScore: number | null = null;
    let callCount = 0;

    try {
      const scores = await supa
        .from("call_scores")
        .select("total_score")
        .eq("user_id", repId)
        .limit(200);

      const rows = scores.data ?? [];

      if (rows.length) {
        callCount = rows.length;
        const sum = rows.reduce((acc: number, r: any) => acc + Number(r.total_score || 0), 0);
        avgScore = Math.round(sum / rows.length);
      }
    } catch {
      // fail-soft if table missing
    }

    // -------------------------------------------------
    // Weakness detection counts
    // -------------------------------------------------

    const weakCloseEvents = coachingTasks.filter((t: any) =>
      String(t.title || "").toLowerCase().includes("close")
    ).length;

    const objectionEvents = coachingTasks.filter((t: any) =>
      String(t.title || "").toLowerCase().includes("objection")
    ).length;

    return res.json({
      ok: true,
      rep_id: repId,
      calls_reviewed: callCount,
      avg_score: avgScore,
      coaching_tasks_created: coachingTasks.length,
      completed_tasks: completedTasks.length,
      weak_close_events: weakCloseEvents,
      objection_events: objectionEvents
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/** ----------------------------------------------------------------
 * Contact AI Brief (heuristic for now)
 * GET /v1/crm/contacts/:id/ai-brief
 * - Returns a short, actionable summary for reps based on:
 *   - contact fields
 *   - linked calls (if any)
 *   - latest call summary/flags (if available)
 *   - latest notes (if any)
 *
 * Safe-by-default:
 * - No external LLM calls required.
 * - If you later want GPT, add it behind an env flag.
 * ---------------------------------------------------------------- */
function fmtDateIso(d: string | null | undefined) {
  if (!d) return null;
  const dt = new Date(d);
  if (Number.isNaN(dt.getTime())) return null;
  return dt.toISOString();
}


function relativeTimeFromIso(iso: string | null | undefined) {
  if (!iso) return "Never";
  const dt = new Date(iso);
  const now = new Date();
  if (Number.isNaN(dt.getTime())) return "Unknown";
  const diffMs = now.getTime() - dt.getTime();
  if (diffMs < 0) return "In future";

  const mins = Math.floor(diffMs / (60 * 1000));
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 48) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 14) return `${days}d ago`;
  const weeks = Math.floor(days / 7);
  if (weeks < 10) return `${weeks}w ago`;
  const months = Math.floor(days / 30);
  return `${months}mo ago`;
}

// ------------------- Contact Health Helpers -------------------
function daysSince(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return null;
  const ms = Date.now() - dt.getTime();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

type ContactHealthStatus = "hot" | "warm" | "cold" | "stale";

function classifyHealth(args: {
  lastContactedDays: number | null;
  overdueCount: number;
  criticalNotesCount: number;
  importantNotesCount: number;
  lastCallScore: number | null;
}): { status: ContactHealthStatus; score: number; reasons: string[]; next_action: string } {
  const { lastContactedDays, overdueCount, criticalNotesCount, importantNotesCount, lastCallScore } = args;

  let score = 50;
  const reasons: string[] = [];

  if (lastContactedDays == null) {
    score -= 10;
    reasons.push("Never contacted");
  } else if (lastContactedDays <= 2) {
    score += 20;
    reasons.push("Contacted recently");
  } else if (lastContactedDays <= 7) {
    score += 10;
    reasons.push("Contacted within 7 days");
  } else if (lastContactedDays <= 14) {
    score -= 5;
    reasons.push("No contact in 2 weeks");
  } else if (lastContactedDays <= 30) {
    score -= 15;
    reasons.push("No contact in 30 days");
  } else {
    score -= 25;
    reasons.push("No contact in 30+ days");
  }

  if (overdueCount > 0) {
    score -= Math.min(25, overdueCount * 8);
    reasons.push(`${overdueCount} overdue assignment${overdueCount === 1 ? "" : "s"}`);
  }

  if (criticalNotesCount > 0) {
    score -= Math.min(20, criticalNotesCount * 10);
    reasons.push("Critical note(s) present");
  } else if (importantNotesCount > 0) {
    score -= Math.min(10, importantNotesCount * 5);
    reasons.push("Important note(s) present");
  }

  if (lastCallScore != null) {
    if (lastCallScore >= 80) {
      score += 10;
      reasons.push("Strong last call score");
    } else if (lastCallScore >= 60) {
      score += 3;
      reasons.push("Decent last call score");
    } else {
      score -= 8;
      reasons.push("Weak last call score");
    }
  }

  score = Math.max(0, Math.min(100, score));

  let status: ContactHealthStatus = "cold";
  if (overdueCount > 0 || (lastContactedDays != null && lastContactedDays > 14)) status = "stale";
  if ((lastContactedDays != null && lastContactedDays <= 7) && overdueCount === 0) status = "warm";
  if ((lastContactedDays != null && lastContactedDays <= 2) && overdueCount === 0 && criticalNotesCount === 0) status = "hot";

  let next_action = "Log a next step (time + outcome) and schedule follow-up.";
  if (overdueCount > 0) next_action = "Clear overdue assignment(s) before doing anything else.";
  else if (lastContactedDays == null) next_action = "Send intro + 2 discovery questions + book a time.";
  else if (lastContactedDays > 14) next_action = "Re-open with a short recap + clear next step + 2 time slots.";
  else if (criticalNotesCount > 0) next_action = "Review critical notes before contacting.";

  return { status, score, reasons, next_action };
}

function buildHeuristicBrief(args: {
  contact: any;
  notes: any[];
  calls: any[];
}) {
  const { contact, notes, calls } = args;

  const name = [contact?.first_name, contact?.last_name].filter(Boolean).join(" ").trim() || "Contact";
  const company = contact?.company || null;
  const lastContacted = contact?.last_contacted_at ? relativeTimeFromIso(contact.last_contacted_at) : "Never";

  const lastCall = calls?.[0] ?? null;
  const lastCallScore = lastCall?.score ?? null;
  const lastCallSummary = lastCall?.summary ?? null;
  const lastCallFlags = (lastCall?.flags as string[] | null) ?? null;

  const importantNotes = (notes ?? []).filter((n) => (n?.importance ?? "normal") !== "normal").slice(0, 3);
  const latestNote = (notes ?? [])[0] ?? null;

  const talkingPoints: string[] = [];
  if (company) talkingPoints.push(`Mention: ${company}`);
  if (lastCallScore != null) talkingPoints.push(`Last call score: ${lastCallScore}`);
  if (lastCallFlags?.length) talkingPoints.push(`Watch-outs: ${lastCallFlags.join(", ")}`);
  if (importantNotes.length) {
    talkingPoints.push(
      `Priority notes: ${importantNotes.map((n) => `${(n.importance ?? "normal").toUpperCase()}: ${String(n.body ?? "").slice(0, 80)}`).join(" | ")}`
    );
  }

  const nextMove: string[] = [];
  if (lastContacted === "Never") nextMove.push("Send a quick intro + value prop");
  else nextMove.push("Follow up with a clear next step + time slot");
  if (lastCallSummary) nextMove.push("Open with recap of last call");
  if (latestNote?.body) nextMove.push("Reference your latest note");

  return {
    title: `AI Brief: ${name}`,
    context: {
      name,
      email: contact?.email ?? null,
      company,
      last_contacted_at: fmtDateIso(contact?.last_contacted_at ?? null),
      last_contacted_relative: lastContacted,
      calls_count: (calls ?? []).length,
      notes_count: (notes ?? []).length,
    },
    summary:
      lastCallSummary ||
      (calls?.length ? "We have call history, but no summary yet." : "No calls linked yet — treat as a warm lead and qualify quickly."),
    talking_points: talkingPoints.length ? talkingPoints : ["Ask 2–3 discovery questions", "Confirm decision-maker + timeline"],
    next_move: nextMove.slice(0, 3),
  };
}

router.get("/contacts/:id/ai-brief", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    // 1) Resolve contact
    let contact: any = null;
    if (contactId.startsWith("c_")) {
      const found = DEMO_CONTACTS.find((c) => c.id === contactId);
      if (!found) return res.status(404).json({ ok: false, error: "not_found" });
      contact = {
        id: found.id,
        first_name: found.first_name ?? null,
        last_name: found.last_name ?? null,
        email: found.email ?? null,
        company: found.company ?? null,
        last_contacted_at: found.last_contacted_at ?? null,
      };
    } else {
      if (!UUID_RE.test(contactId)) return res.status(400).json({ ok: false, error: "invalid_id" });
      const { data: c, error: cErr } = await supa
        .from("crm_contacts")
        .select("id, first_name, last_name, email, company, last_contacted_at")
        .eq("user_id", requester)
        .eq("id", contactId)
        .maybeSingle();

      // Safe: maybeSingle() returns `data: null` when no rows match.
      if (cErr) return res.status(500).json({ ok: false, error: cErr.message ?? "contact_query_failed" });
      if (!c) return res.status(404).json({ ok: false, error: "not_found" });

      contact = c;
    }

    // 2) Pull latest notes
    const { data: notes, error: nErr } = await supa
      .from("crm_contact_notes")
      .select("id, body, created_at, author_id, author_name, importance")
      .eq("user_id", requester)
      .eq("contact_id", contactId)
      .order("created_at", { ascending: false })
      .limit(20);
    if (nErr) throw nErr;

    // 3) Pull linked calls (best-effort)
    // If you have a join table like `crm_call_links(contact_id, call_id)` we use it.
    // If not, this returns empty and the brief still works.
    let calls: any[] = [];
    try {
      const { data: links, error: lErr } = await supa
        .from("crm_call_links")
        .select("call_id")
        .eq("contact_id", contactId)
        .limit(25);
      if (lErr) throw lErr;

      const callIds = (links ?? []).map((r: any) => r.call_id).filter(Boolean);
      if (callIds.length) {
        // Pull lightweight fields from `calls`. If your schema differs, adjust select list.
        const { data: callRows, error: cErr } = await supa
          .from("calls")
          .select("id, created_at, summary, flags, duration_ms")
          .eq("user_id", requester)
          .in("id", callIds)
          .order("created_at", { ascending: false })
          .limit(10);
        if (cErr) throw cErr;

        // Pull latest score per call (best-effort)
        const { data: scoreRows } = await supa
          .from("call_scores")
          .select("call_id, total_score, created_at")
          .eq("user_id", requester)
          .in("call_id", callIds)
          .order("created_at", { ascending: false })
          .limit(50);

        const scoreByCall = new Map<string, number>();
        for (const s of scoreRows ?? []) {
          const cid = (s as any).call_id as string;
          if (!scoreByCall.has(cid)) scoreByCall.set(cid, Number((s as any).total_score));
        }

        calls = (callRows ?? []).map((r: any) => ({
          id: r.id,
          created_at: r.created_at,
          summary: r.summary ?? null,
          flags: r.flags ?? null,
          duration_ms: r.duration_ms ?? null,
          score: scoreByCall.get(r.id) ?? null,
        }));
      }
    } catch {
      calls = [];
    }

    const brief = buildHeuristicBrief({ contact, notes: notes ?? [], calls });

    return res.json({ ok: true, brief });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   POST /v1/crm/link-call
   Body (flexible):
     - callId | call_id (uuid) [required]
     - contact_id | contactId (uuid) OR email (string) [one required]
     - account_id | accountId (uuid) [optional]
     - opportunity_id | opportunityId (uuid) [optional]

   Behaviour (best-effort + schema tolerant):
     - Verify caller owns the call
     - Resolve contact (by contact_id if provided, else upsert by email scoped to requester)
     - Upsert link in crm_call_links (call_id -> contact_id)
     - Best-effort: update calls(contact_id/account_id/opportunity_id) if columns exist
     - Best-effort: insert crm_activities row (type=call) if table exists
---------------------------------------------- */
const LinkCallSchema = z
  .object({
    // NOTE: call ids in this system may be UUIDs OR demo/string ids (e.g. "demo-001").
    // So we accept any non-empty string here.
    callId: z.string().trim().min(1).optional(),
    call_id: z.string().trim().min(1).optional(),

    contact_id: z.string().uuid().optional(),
    contactId: z.string().uuid().optional(),
    email: z
      .string()
      .email()
      .transform((s) => s.toLowerCase().trim())
      .optional(),

    account_id: z.string().uuid().optional().nullable(),
    accountId: z.string().uuid().optional().nullable(),
    opportunity_id: z.string().uuid().optional().nullable(),
    opportunityId: z.string().uuid().optional().nullable(),
  })
  .refine((b) => Boolean(b.callId || b.call_id), { message: "call_id_required" })
  .refine((b) => Boolean(b.contact_id || b.contactId || b.email), { message: "contact_required" });

router.post("/link-call", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = LinkCallSchema.parse(req.body ?? {});

    const callId = (body.callId ?? body.call_id) as string;
    const accountId = (body.accountId ?? body.account_id) ?? null;
    const opportunityId = (body.opportunityId ?? body.opportunity_id) ?? null;

    const contactIdFromBody = (body.contactId ?? body.contact_id) ?? null;
    const email = body.email ?? null;

    // 1) Verify the call belongs to the requester
    // NOTE: In local/dev we may have demo call ids like "demo-001" that are not persisted.
    // Also: some schemas expect calls.id and crm_call_links.call_id to be UUID.
    // So we treat non-UUID call ids as “virtual/demo” and skip DB linkage writes.
    const callIdStr = String(callId);
    const isDemoCallId = /^demo-\d+$/i.test(callIdStr);
    const isUuidCallId = UUID_RE.test(callIdStr);
    const isVirtualCallId = isDemoCallId || !isUuidCallId;

    if (!isVirtualCallId) {
      const { data: call, error: callErr } = await supa
        .from("calls")
        .select("id, user_id")
        .eq("id", callId)
        .maybeSingle();

      if (callErr || !call) return res.status(404).json({ ok: false, error: "call_not_found" });
      if (String((call as any).user_id ?? "") !== requester) return res.status(403).json({ ok: false, error: "forbidden" });
    }

    // 2) Resolve contact
    let contactId: string | null = null;

    // 2a) If contact_id provided, validate ownership
    if (contactIdFromBody) {
      const { data: c, error: cErr } = await supa
        .from("crm_contacts")
        .select("id")
        .eq("id", contactIdFromBody)
        .eq("user_id", requester)
        .maybeSingle();

      if (cErr) return res.status(500).json({ ok: false, error: cErr.message ?? "contact_lookup_failed" });
      if (!c) return res.status(404).json({ ok: false, error: "contact_not_found" });
      contactId = String((c as any).id);
    } else {
      // 2b) Else resolve by email (scoped to requester)
      if (!email) return res.status(400).json({ ok: false, error: "email_required" });

      // Prefer selecting by (user_id + email) first
      const { data: existingScoped, error: exScopedErr } = await supa
        .from("crm_contacts")
        .select("id")
        .eq("user_id", requester)
        .eq("email", email)
        .maybeSingle();

      if (exScopedErr) return res.status(500).json({ ok: false, error: exScopedErr.message ?? "contact_lookup_failed" });

      if (existingScoped?.id) {
        contactId = String((existingScoped as any).id);
      } else {
        // Insert new contact owned by requester
        const insertAttempt = await supa
          .from("crm_contacts")
          .insert({ user_id: requester, email })
          .select("id")
          .single();

        if (!insertAttempt.error && insertAttempt.data) {
          contactId = String((insertAttempt.data as any).id);
        } else {
          // If email is globally unique, select by email and ensure ownership
          const { data: existingByEmail, error: exErr } = await supa
            .from("crm_contacts")
            .select("id, user_id")
            .eq("email", email)
            .maybeSingle();

          if (exErr) return res.status(500).json({ ok: false, error: exErr.message ?? "contact_lookup_failed" });
          if (!existingByEmail) {
            return res.status(500).json({ ok: false, error: (insertAttempt.error as any)?.message ?? "contact_upsert_failed" });
          }
          if (String((existingByEmail as any).user_id ?? "") !== requester) {
            return res.status(409).json({ ok: false, error: "email_owned_by_other_user" });
          }
          contactId = String((existingByEmail as any).id);
        }
      }
    }

    if (!contactId) return res.status(500).json({ ok: false, error: "contact_resolve_failed" });

    // 3) Link the call -> contact (best-effort)
    // Goals:
    // - Accept demo/string call ids (e.g. demo-001)
    // - Never attempt invalid UUID writes
    // - Do NOT depend on UNIQUE constraints / onConflict
    //
    // Behaviour:
    // - If callId is not a UUID (or is demo), we skip DB linkage writes and just return a stable response.
    // - If callId is a UUID, we do a simple "replace" write: delete existing links for call_id, then insert.
    if (isUuidCallId) {
      try {
        // crm_call_links may not exist yet
        // Replace semantics: one call_id should point to one contact_id
        await supa.from("crm_call_links").delete().eq("call_id", callId);

        const ins = await supa.from("crm_call_links").insert({ call_id: callId, contact_id: contactId });
        if (ins.error) {
          const msg = String((ins.error as any)?.message ?? "").toLowerCase();

          // Missing table is a hard error for link-call
          if (msg.includes("relation") && msg.includes("does not exist")) {
            return res.status(500).json({ ok: false, error: "crm_call_links_table_missing" });
          }

          // If the schema expects UUIDs and rejects (shouldn't happen because isUuidCallId), fail-soft and continue.
          if (msg.includes("invalid input syntax") && msg.includes("uuid")) {
            // no-op
          } else {
            return res.status(500).json({ ok: false, error: (ins.error as any)?.message ?? "link_call_failed" });
          }
        }
      } catch (e: any) {
        const emsg = String(e?.message ?? "").toLowerCase();
        if (emsg.includes("relation") && emsg.includes("does not exist")) {
          return res.status(500).json({ ok: false, error: "crm_call_links_table_missing" });
        }
        // Otherwise fail-soft (link table issues shouldn't block the rest)
      }
    }

    // 3b) Persist demo/virtual call links so GET /v1/crm/link can re-hydrate UI after refresh.
    // This is best-effort: if the table doesn't exist yet, we fail-soft.
    if (isVirtualCallId) {
      try {
        // Replace semantics: one (user_id, call_id) row
        await supa
          .from("crm_demo_call_links")
          .delete()
          .eq("user_id", requester)
          .eq("call_id", callIdStr);

        const ins = await supa.from("crm_demo_call_links").insert({
          user_id: requester,
          call_id: callIdStr,
          contact_id: contactId,
          account_id: accountId,
          opportunity_id: opportunityId,
        });

        if (ins.error) {
          const msg = String((ins.error as any)?.message ?? "").toLowerCase();
          // Table missing -> fail-soft (demo persistence not available)
          if (!(msg.includes("relation") && msg.includes("does not exist"))) {
            console.error("[crm/link-call] demo link insert failed:", ins.error);
          }
        }
      } catch (e: any) {
        const msg = String(e?.message ?? "").toLowerCase();
        // Table missing -> fail-soft
        if (!(msg.includes("relation") && msg.includes("does not exist"))) {
          console.error("[crm/link-call] demo link insert exception:", e);
        }
      }
    }

    // 4) Best-effort: update calls row with linkage columns (schema tolerant)
    // Virtual/demo call ids are not persisted, so skip updating `calls`.
    if (!isVirtualCallId) {
      try {
        const patchBase: any = {
          contact_id: contactId,
          ...(accountId ? { account_id: accountId } : {}),
          ...(opportunityId ? { opportunity_id: opportunityId } : {}),
        };

        const updateAttempts: any[] = [
          { ...patchBase },
          (() => {
            const p = { ...patchBase };
            delete p.opportunity_id;
            return p;
          })(),
          (() => {
            const p = { ...patchBase };
            delete p.account_id;
            return p;
          })(),
          (() => {
            const p = { ...patchBase };
            delete p.account_id;
            delete p.opportunity_id;
            return p;
          })(),
        ].filter((p) => Object.keys(p).length > 0);

        for (const p of updateAttempts) {
          const r = await supa
            .from("calls")
            .update(p)
            .eq("id", callId)
            .eq("user_id", requester)
            .select("id")
            .maybeSingle();

          if (!r.error) break;

          const msg = String((r.error as any)?.message ?? "").toLowerCase();
          const isMissingCol =
            (msg.includes("column") && msg.includes("does not exist")) ||
            msg.includes("could not find") ||
            msg.includes("unknown column");

          if (isMissingCol) continue;
          break;
        }
      } catch {
        // fail-soft
      }
    }

    // 5) Best-effort: insert an activity row (timeline)
    // If call_id is not a UUID, many schemas will reject it; skip activity insert.
    if (!isUuidCallId) {
      // still continue (we’ll return ok below)
    } else {
      try {
        const activityBase: any = {
          user_id: requester,
          type: "call",
          contact_id: contactId,
          call_id: callId,
          ...(accountId ? { account_id: accountId } : {}),
          ...(opportunityId ? { opportunity_id: opportunityId } : {}),
          meta: { source: "link_call_v1" },
        };

        const activityAttempts: any[] = [
          { ...activityBase },
          (() => {
            const p = { ...activityBase };
            delete p.meta;
            return p;
          })(),
          (() => {
            const p = { ...activityBase };
            delete p.call_id;
            return p;
          })(),
          (() => {
            const p = { ...activityBase };
            delete p.account_id;
            delete p.opportunity_id;
            return p;
          })(),
          { user_id: requester, type: "call", contact_id: contactId },
        ];

        for (const payload of activityAttempts) {
          const r = await supa.from("crm_activities").insert(payload);
          if (!r.error) break;

          const msg = String((r.error as any)?.message ?? "").toLowerCase();
          if (msg.includes("relation") && msg.includes("does not exist")) break;

          const isMissingCol =
            (msg.includes("column") && msg.includes("does not exist")) ||
            msg.includes("could not find") ||
            msg.includes("unknown column");

          if (isMissingCol) continue;
          break;
        }
      } catch {
        // fail-soft
      }
    }

    // 6) Touch contact (best-effort)
    try {
      await touchContactBestEffort({ requester, contactId });
    } catch {
      // fail-soft
    }

    return res.json({
      ok: true,
      link: {
        call_id: callId,
        contact_id: contactId,
        account_id: accountId,
        opportunity_id: opportunityId,
        demo_call: isDemoCallId,
        virtual_call: isVirtualCallId,
      },
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   POST /v1/crm/unlink
   Body:
     - callId (string) [required] (UUID or demo-###)
     - target: 'contact' | 'account' [required]

   Behaviour:
     - For UUID calls: verify caller owns the call via `calls` table, then update/delete from `crm_call_links`.
     - For demo/virtual calls: update/delete from `crm_demo_call_links` (scoped by user_id).
     - Best-effort schema tolerance: if account_id column missing, fallback to deleting the link row.
---------------------------------------------- */
const UnlinkSchema = z.object({
  callId: z.string().trim().min(1),
  target: z.enum(["contact", "account"]),
});

// POST /v1/crm/unlink
// Body: { callId | call_id, target?: "contact"|"account"|"opportunity"|"all" }
// Supports demo/string call IDs by using `crm_demo_call_links` for non-UUID ids.
router.post("/unlink", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const body = (req.body ?? {}) as any;
    const callId = String(body.callId ?? body.call_id ?? "").trim();
    const target = String(body.target ?? "all").trim().toLowerCase();

    if (!callId) {
      return res.status(400).json({ ok: false, error: "callId_required" });
    }

    const callIdStr = String(callId);
    const isDemoCallId = /^demo-\d+$/i.test(callIdStr);
    const isUuidCallId = UUID_RE.test(callIdStr);
    const isVirtualCallId = isDemoCallId || !isUuidCallId;



    // ----------------------------
    // A) DEMO/VIRTUAL call ids
    // ----------------------------
    if (isVirtualCallId) {
      try {
        // IMPORTANT:
        // GET /v1/crm/link for demo ids hydrates from crm_demo_call_links.
        // So unlinking the CONTACT must delete the row, otherwise it will “come back”.
        if (target === "all" || target === "contact") {
          const del = await supa
            .from("crm_demo_call_links")
            .delete()
            .eq("user_id", requester)
            .eq("call_id", callIdStr);

          if (del.error) {
            const msg = String((del.error as any)?.message ?? "").toLowerCase();
            // Table missing -> fail-soft
            if (!(msg.includes("relation") && msg.includes("does not exist"))) {
              console.error("[crm/unlink] demo delete failed:", del.error);
            }
          }

          return res.json({ ok: true, call_id: callIdStr, unlinked: target });
        }

        // For demo ids, we only support clearing account/opportunity via update.
        const patch: any = {};
        if (target === "account") {
          patch.account_id = null;
          patch.opportunity_id = null;
        } else if (target === "opportunity") {
          patch.opportunity_id = null;
        } else {
          // unknown target: fail-soft but don't delete the link
          patch.opportunity_id = null;
        }

        const attempts: any[] = [
          { ...patch },
          (() => {
            const p = { ...patch };
            delete p.opportunity_id;
            return p;
          })(),
          (() => {
            const p = { ...patch };
            delete p.account_id;
            return p;
          })(),
        ].filter((p) => Object.keys(p).length > 0);

        for (const p of attempts) {
          const r = await supa
            .from("crm_demo_call_links")
            .update(p)
            .eq("user_id", requester)
            .eq("call_id", callIdStr);

          if (!r.error) break;

          const msg = String((r.error as any)?.message ?? "").toLowerCase();
          const isMissingCol =
            (msg.includes("column") && msg.includes("does not exist")) ||
            msg.includes("could not find") ||
            msg.includes("unknown column");

          if (msg.includes("relation") && msg.includes("does not exist")) break; // fail-soft
          if (isMissingCol) continue;
          break;
        }

        return res.json({ ok: true, call_id: callIdStr, unlinked: target });
      } catch {
        // fail-soft
        return res.json({ ok: true, call_id: callIdStr, unlinked: target });
      }
    }

    // 2) Best-effort: clear linkage columns on calls (schema tolerant)
    {
      const patchBase: any = {};

      if (target === "contact") {
        patchBase.contact_id = null;
        patchBase.opportunity_id = null;
      } else if (target === "account") {
        patchBase.account_id = null;
        patchBase.opportunity_id = null;
      } else if (target === "opportunity") {
        patchBase.opportunity_id = null;
      } else {
        patchBase.contact_id = null;
        patchBase.account_id = null;
        patchBase.opportunity_id = null;
      }

      const attempts: any[] = [
        { ...patchBase },
        (() => {
          const p = { ...patchBase };
          delete p.opportunity_id;
          return p;
        })(),
        (() => {
          const p = { ...patchBase };
          delete p.account_id;
          return p;
        })(),
        (() => {
          const p = { ...patchBase };
          delete p.contact_id;
          return p;
        })(),
      ].filter((p) => Object.keys(p).length > 0);

      for (const p of attempts) {
        const r = await supa
          .from("calls")
          .update(p)
          .eq("id", callIdStr)
          .eq("user_id", requester)
          .select("id")
          .maybeSingle();

        if (!r.error) break;

        const msg = String((r.error as any)?.message ?? "").toLowerCase();
        const isMissingCol =
          (msg.includes("column") && msg.includes("does not exist")) ||
          msg.includes("could not find") ||
          msg.includes("unknown column");

        if (isMissingCol) continue;
        break;
      }
    }

    return res.json({ ok: true, call_id: callIdStr, unlinked: target });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// GET /v1/crm/link?callId=...
// Returns CRM link info for a call.
// Notes:
// - Demo ids like "demo-001" are allowed (returns a stable empty link unless your schema supports text call_ids).
// - We do NOT depend on crm_call_links.user_id existing.
// - Related entities (contact/account/opportunity) are always scoped to requester via user_id.
router.get("/link", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const callId = String((req.query as any)?.callId ?? "").trim();

    if (!callId) {
      return res.status(400).json({ ok: false, error: "callId_required" });
    }

    const isDemoCallId = /^demo-\d+$/i.test(callId);
    const isUuidCallId = UUID_RE.test(callId);

    // If it's not a UUID, the main link table may be UUID-typed.
    // For demo/virtual calls we persist links in crm_demo_call_links (text call_id).
    if (!isUuidCallId) {
      try {
        const dl = await supa
          .from("crm_demo_call_links")
          .select("call_id, contact_id, account_id, opportunity_id")
          .eq("user_id", requester)
          .eq("call_id", callId)
          .maybeSingle();

        if (!dl.error && dl.data) {
          const linkRow: any = {
            call_id: (dl.data as any).call_id,
            contact_id: (dl.data as any).contact_id,
            account_id: (dl.data as any).account_id ?? null,
            opportunity_id: (dl.data as any).opportunity_id ?? null,
            demo_call: isDemoCallId,
            virtual_call: true,
          };

          let contact: any = null;
          let account: any = null;
          let opportunity: any = null;

          if (linkRow.contact_id) {
            const r = await supa
              .from("crm_contacts")
              .select("id, first_name, last_name, email, company")
              .eq("id", linkRow.contact_id)
              .eq("user_id", requester)
              .maybeSingle();
            if (!r.error) contact = r.data ?? null;
          }

          if (linkRow.account_id) {
            const r = await supa
              .from("crm_accounts")
              .select("id, name")
              .eq("id", linkRow.account_id)
              .eq("user_id", requester)
              .maybeSingle();
            if (!r.error) account = r.data ?? null;
          }

          if (linkRow.opportunity_id) {
            const r = await supa
              .from("crm_opportunities")
              .select("id, name, stage")
              .eq("id", linkRow.opportunity_id)
              .eq("user_id", requester)
              .maybeSingle();
            if (!r.error) opportunity = r.data ?? null;
          }

          return res.json({ ok: true, link: linkRow, contact, account, opportunity });
        }

        // If demo table missing, fail-soft and fall through to stable empty response
        if (dl.error) {
          const msg = String((dl.error as any)?.message ?? "").toLowerCase();
          if (!(msg.includes("relation") && msg.includes("does not exist"))) {
            console.error("[crm/link] demo link fetch failed:", dl.error);
          }
        }
      } catch {
        // ignore
      }

      return res.json({
        ok: true,
        link: isDemoCallId
          ? {
            call_id: callId,
            contact_id: null,
            account_id: null,
            opportunity_id: null,
            demo_call: true,
            virtual_call: true,
          }
          : null,
        contact: null,
        account: null,
        opportunity: null,
      });
    }

    // Find link row (schema-tolerant: do NOT assume user_id/account_id/opportunity_id columns exist)
    let linkRow: any = null;
    try {
      const r = await supa
        .from("crm_call_links")
        .select("*")
        .eq("call_id", callId)
        .maybeSingle();

      if (r.error) {
        const msg = String((r.error as any)?.message ?? "").toLowerCase();

        // Missing table => fail-open (stable empty link)
        if (msg.includes("relation") && msg.includes("does not exist")) {
          return res.json({ ok: true, link: null, contact: null, account: null, opportunity: null });
        }

        // Any uuid parsing mismatch => treat as not linked
        if (msg.includes("invalid input") && msg.includes("uuid")) {
          return res.json({ ok: true, link: null, contact: null, account: null, opportunity: null });
        }

        throw r.error;
      }

      linkRow = r.data ?? null;
    } catch (err: any) {
      const msg = String(err?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.json({ ok: true, link: null, contact: null, account: null, opportunity: null });
      }
      if (msg.includes("invalid input") && msg.includes("uuid")) {
        return res.json({ ok: true, link: null, contact: null, account: null, opportunity: null });
      }
      throw err;
    }

    if (!linkRow) {
      return res.json({ ok: true, link: null, contact: null, account: null, opportunity: null });
    }

    let contact: any = null;
    let account: any = null;
    let opportunity: any = null;

    if ((linkRow as any).contact_id) {
      const r = await supa
        .from("crm_contacts")
        .select("id, first_name, last_name, email, company")
        .eq("id", (linkRow as any).contact_id)
        .eq("user_id", requester)
        .maybeSingle();

      if (!r.error) contact = r.data ?? null;
    }

    // account_id/opportunity_id are optional and may not exist on older link schemas.
    if ((linkRow as any).account_id) {
      const r = await supa
        .from("crm_accounts")
        .select("id, name")
        .eq("id", (linkRow as any).account_id)
        .eq("user_id", requester)
        .maybeSingle();

      if (!r.error) account = r.data ?? null;
    }

    if ((linkRow as any).opportunity_id) {
      const r = await supa
        .from("crm_opportunities")
        .select("id, name, stage")
        .eq("id", (linkRow as any).opportunity_id)
        .eq("user_id", requester)
        .maybeSingle();

      if (!r.error) opportunity = r.data ?? null;
    }

    return res.json({
      ok: true,
      link: linkRow,
      contact,
      account,
      opportunity,
    });
  } catch (e: any) {
    return res.status(500).json({
      ok: false,
      error: e?.message ?? "link_fetch_failed",
    });
  }
});

/* ---------------------------------------------
   CONTACT NOTES
   - Simple rep notes per contact
---------------------------------------------- */
const NoteImportanceSchema = z.enum(["normal", "important", "critical"]);
const CreateNoteSchema = z.object({
  body: z.string().trim().min(1),
  // Optional client-provided display name (denormalised). Safe because it’s just text.
  author_name: z.string().trim().min(1).max(120).optional(),
  importance: NoteImportanceSchema.optional(),
});

/* GET /v1/crm/contacts/:id/notes */
router.get("/contacts/:id/notes", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    const { data, error } = await supa
      .from("crm_contact_notes")
      .select("id, body, created_at, author_id, author_name, importance")
      .eq("contact_id", contactId)
      .eq("user_id", requester)
      .order("created_at", { ascending: false });

    if (error) throw error;

    return res.json({ ok: true, notes: data ?? [] });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* POST /v1/crm/contacts/:id/notes
   Body: { body: string, importance?: 'normal'|'important'|'critical' }
*/
router.post("/contacts/:id/notes", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    const parsed = CreateNoteSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({ ok: false, error: "invalid_body" });
    }

    const body = parsed.data.body;
    const importance = parsed.data.importance ?? "normal";

    // author (server-truth)
    const authorId = requester;

    // best-effort rep name lookup (safe if it fails)
    let authorName: string | null = null;
    try {
      const { data: rep } = await supa
        .from("reps")
        .select("name, full_name, first_name, last_name, email, user_id, id")
        .or(`user_id.eq.${authorId},id.eq.${authorId}`)
        .limit(1)
        .maybeSingle();

      if (rep) {
        authorName =
          (rep as any).full_name ||
          (rep as any).name ||
          [(rep as any).first_name, (rep as any).last_name].filter(Boolean).join(" ") ||
          (rep as any).email ||
          null;
      }
    } catch {
      // ignore lookup errors
    }

    // after the rep lookup try/catch
    authorName = authorName ?? parsed.data.author_name ?? "Rep";

    const { data, error } = await supa
      .from("crm_contact_notes")
      .insert({
        contact_id: contactId,
        user_id: requester,
        body,
        author_id: authorId,
        author_name: authorName,
        importance,
      })
      .select("id, body, created_at, author_id, author_name, importance")
      .single();

    if (error) throw error;

    // best-effort: touching contact improves health accuracy; never block note creation
    try {
      await touchContactBestEffort({ requester, contactId });
    } catch {
      // fail-soft
    }

    return res.json({ ok: true, note: data });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   CONTACT IMPORT (Bulk Upload v1)
   POST /v1/crm/contacts/import
   Body: { rows: [{ email, first_name?, last_name?, company?, phone? }], dry_run?: boolean }

   Notes:
   - Uses `supa` service-role client.
   - Best-effort safety: if crm_contacts has UNIQUE(email) globally, we will NOT overwrite a different user’s contact.
---------------------------------------------- */
const ImportContactsSchema = z.object({
  dry_run: z.boolean().optional(),
  rows: z.array(
    z.object({
      email: z.string().email().transform((s) => s.toLowerCase().trim()),
      first_name: z.string().optional().nullable(),
      last_name: z.string().optional().nullable(),
      company: z.string().optional().nullable(),
    })
  ),
});

router.post("/contacts/import", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    // Body: { rows: [{ email, first_name?, last_name?, company? }], dry_run?: boolean }
    const parsed = ImportContactsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({ ok: false, error: "invalid_body" });
    }

    const { rows, dry_run = false } = parsed.data;

    // Deduplicate by email (last write wins)
    const byEmail = new Map<string, any>();
    for (const r of rows) byEmail.set(r.email, r);
    const uniqueRows = [...byEmail.values()];

    let inserted = 0;
    let updated = 0;
    let skipped = 0;

    const preview: any[] = [];
    const errors: any[] = [];

    for (const r of uniqueRows) {
      // If table has globally-unique email, ensure we don’t overwrite another user’s record.
      const { data: existing, error: exErr } = await supa
        .from("crm_contacts")
        .select("id, user_id")
        .eq("email", r.email)
        .maybeSingle();

      if (exErr) {
        skipped++;
        errors.push({ email: r.email, error: exErr.message });
        continue;
      }

      if (dry_run) {
        if (!existing) preview.push({ email: r.email, action: "insert" });
        else if ((existing as any).user_id === requester) preview.push({ email: r.email, action: "update" });
        else preview.push({ email: r.email, action: "skip_other_user" });
        continue;
      }

      if (!existing) {
        // Insert brand new row owned by this requester
        const { error } = await supa.from("crm_contacts").insert({
          user_id: requester,
          email: r.email,
          first_name: r.first_name ?? null,
          last_name: r.last_name ?? null,
          company: r.company ?? null,
        });

        if (error) {
          skipped++;
          errors.push({ email: r.email, error: error.message });
        } else {
          inserted++;
        }
        continue;
      }

      // Existing row
      const existingUserId = (existing as any).user_id ?? null;

      // If this is a legacy row with NULL user_id, claim it safely for this requester.
      // (We never overwrite another user's contact.)
      if (existingUserId === null) {
        const { error } = await supa
          .from("crm_contacts")
          .update({
            user_id: requester,
            first_name: r.first_name ?? null,
            last_name: r.last_name ?? null,
            company: r.company ?? null,
          })
          .eq("id", (existing as any).id);

        if (error) {
          skipped++;
          errors.push({ email: r.email, error: error.message });
        } else {
          updated++;
        }
        continue;
      }

      // If owned by someone else, skip
      if (existingUserId !== requester) {
        skipped++;
        errors.push({ email: r.email, error: "email_owned_by_other_user" });
        continue;
      }

      // Owned by requester: update fields
      const { error } = await supa
        .from("crm_contacts")
        .update({
          first_name: r.first_name ?? null,
          last_name: r.last_name ?? null,
          company: r.company ?? null,
        })
        .eq("id", (existing as any).id)
        .eq("user_id", requester);

      if (error) {
        skipped++;
        errors.push({ email: r.email, error: error.message });
      } else {
        updated++;
      }
    }

    return res.json({
      ok: true,
      dry_run,
      summary: { inserted, updated, skipped },
      preview: dry_run ? preview : undefined,
      errors,
    });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message ?? "server_error" });
  }
});

// ------------------- CRM Actions (stable fallback) -------------------
// Purpose: provide a stable, rep-visible action stream that does NOT depend on the `assignments` schema.
// If the `crm_actions` table doesn't exist yet, all helpers fail-open.

// Helper: Safe select for crm_actions (handles missing columns/filters/orders)
async function selectCrmActionsSafe(args: {
  requester: string;
  repId?: string | null;
  contactId?: string | null;
  status?: string | null;
  limit: number;
  orderByDue?: boolean;
}) {
  const { requester, repId, contactId, status, limit, orderByDue } = args;

  // Prefer a rich select, but tolerate missing columns.
  let cols = [
    "id",
    "contact_id",
    "type",
    "title",
    "label",
    "due_at",
    "created_at",
    "completed_at",
    "status",
    "importance",
    "source",
    "meta",
  ];

  // Schema-tolerant status filter
  let statusFilterOn = true;

  // Build a query factory so we can re-run after stripping columns/filters.
  const run = async (useRepFilter: boolean, selectCols: string[]) => {
    let q = supa.from("crm_actions").select(selectCols.join(", ")).eq("user_id", requester);

    if (useRepFilter && repId) q = q.eq("rep_id", repId);
    if (contactId) q = q.eq("contact_id", contactId);

    // Schema-tolerant status filter
    if (status && statusFilterOn) q = q.eq("status", status);

    if (orderByDue) {
      q = q.order("due_at", { ascending: true });
    } else {
      q = q.order("created_at", { ascending: false });
    }

    q = q.limit(limit);
    return await q;
  };

  const stripFromMsg = (msg: string): string | null => {
    const m1 = msg.match(/column crm_actions\.([a-zA-Z0-9_]+) does not exist/i);
    if (m1?.[1]) return m1[1];
    const m2 = msg.match(/could not find the '([^']+)' column/i);
    if (m2?.[1]) return m2[1];
    return null;
  };

  // Attempt with rep_id filter first if provided.
  for (const useRepFilter of [true, false]) {
    // If no repId, don't bother with rep filter.
    if (!repId && useRepFilter) continue;

    let selectCols = [...cols];

    for (let i = 0; i < 10; i++) {
      const r = await run(useRepFilter, selectCols);
      if (!r.error) return { ok: true as const, data: r.data ?? [], usedRepFilter: useRepFilter, selectCols };

      const msg = String((r.error as any)?.message ?? "");
      const lower = msg.toLowerCase();

      // If status column is missing, disable status filter AND remove it from select list, then retry.
      // (Otherwise we'll keep failing if the SELECT includes `status`.)
      if (
        (lower.includes("crm_actions.status") || (lower.includes("status") && (lower.includes("column") || lower.includes("could not find")))) &&
        (lower.includes("does not exist") || lower.includes("could not find"))
      ) {
        statusFilterOn = false;
        if (selectCols.includes("status")) selectCols = selectCols.filter((c) => c !== "status");
        continue;
      }

      // Table missing → fail fast.
      if (lower.includes("relation") && lower.includes("does not exist")) {
        return { ok: false as const, error: r.error };
      }

      // If the rep_id filter is the problem, fall through to the no-rep-filter pass.
      if (useRepFilter && (lower.includes("rep_id") && (lower.includes("does not exist") || lower.includes("could not find")))) {
        break;
      }

      // If ORDER BY due_at / created_at is failing, strip that column and retry.
      if ((lower.includes("due_at") || lower.includes("created_at")) && lower.includes("does not exist")) {
        const bad = stripFromMsg(msg);
        if (bad) {
          selectCols = selectCols.filter((c) => c !== bad);
          // If ordering by the missing col, switch to created_at (or no order if both missing).
          // We'll naturally retry; if created_at also missing, we'll eventually strip it too.
          continue;
        }
      }

      const badCol = stripFromMsg(msg);
      if (badCol && selectCols.includes(badCol)) {
        selectCols = selectCols.filter((c) => c !== badCol);
        continue;
      }

      // Unknown error → stop.
      return { ok: false as const, error: r.error };
    }
  }

  return { ok: false as const, error: new Error("crm_actions_select_failed") };
}

async function insertCrmActionBestEffort(args: {
  requester: string;
  repId?: string | null;
  contactId: string;
  type: string;
  title: string;
  due_at: string | null;
  importance: "normal" | "important" | "critical";
  meta: any;
}) {
  const { requester, repId, contactId, type, title, due_at, importance, meta } = args;

  // Best-effort: if table/columns don't exist, do not block.
  // We also try to be compatible with older schemas by:
  // - including rep_id/manager_id when present
  // - stripping missing columns iteratively (not just once)
  // - falling back to a minimal payload if constraints bite
  try {
    const base: any = {
      user_id: requester,
      rep_id: repId ?? requester,
      manager_id: requester,
      contact_id: contactId,
      type,
      title,
      due_at,
      importance,
      status: "open",
      source: "health_auto_assign_v1",
      meta: meta ?? {},
    };

    const candidates: any[] = [
      { ...base },
      // Some schemas use `label` instead of `title`
      { ...base, label: title },
      // Some schemas don't have meta/source/status
      {
        user_id: requester,
        rep_id: repId ?? requester,
        manager_id: requester,
        contact_id: contactId,
        type,
        title,
        due_at,
        importance,
      },
      // Ultra-minimal but still satisfies NOT NULL type
      { user_id: requester, contact_id: contactId, type, title },
      { user_id: requester, contact_id: contactId, type, label: title },
    ];

    let lastErr: any = null;

    for (const seed of candidates) {
      let payload: any = { ...seed };

      // Up to 6 retries where we progressively strip missing columns.
      for (let i = 0; i < 6; i++) {
        const { error } = await supa.from("crm_actions").insert(payload);
        if (!error) return { ok: true as const };

        lastErr = error;
        const msg = String((error as any)?.message ?? "").toLowerCase();

        // Table missing -> bail immediately
        if (msg.includes("relation") && msg.includes("does not exist")) {
          return { ok: false as const, error };
        }

        // Schema cache missing column: strip it and retry
        const m = msg.match(/could not find the '([^']+)' column/);
        const missingCol = m?.[1];
        if (missingCol && missingCol in payload) {
          // If title missing, try label (and vice versa) before dropping
          if (missingCol === "title" && payload.label == null) payload.label = title;
          if (missingCol === "label" && payload.title == null) payload.title = title;

          delete payload[missingCol];
          continue;
        }

        // PostgREST column missing variant
        if (msg.includes("column") && msg.includes("does not exist")) {
          // Strip common optional fields
          delete payload.rep_id;
          delete payload.manager_id;
          delete payload.status;
          delete payload.source;
          delete payload.meta;
          delete payload.importance;

          // Keep the essentials
          payload.user_id = requester;
          payload.contact_id = contactId;
          payload.type = type;
          payload.title = payload.title ?? title;

          continue;
        }

        // Not-null constraint errors: populate required fields and/or drop to minimal
        if (msg.includes("violates not-null constraint")) {
          if (msg.includes("rep_id")) payload.rep_id = requester;
          if (msg.includes("manager_id")) payload.manager_id = requester;
          if (msg.includes("contact_id")) payload.contact_id = contactId;
          if (msg.includes("user_id")) payload.user_id = requester;
          if (msg.includes("type")) payload.type = payload.type ?? type;

          // If still failing, drop to minimal essentials (including type)
          if (i >= 2) {
            payload = { user_id: requester, contact_id: contactId, type, title };
          }
          continue;
        }

        // Any other error: stop retrying this seed payload.
        break;
      }
    }

    return { ok: false as const, error: lastErr };
  } catch (e: any) {
    return { ok: false as const, error: e };
  }
}

async function fetchCrmActionsBestEffort(args: { requester: string; contactId: string; limit: number; }) {
  const { requester, contactId, limit } = args;

  try {
    const r = await selectCrmActionsSafe({
      requester,
      contactId,
      status: null,
      repId: null,
      limit,
      orderByDue: false,
    });

    if (!r.ok) return { ok: false as const, open: [], completed: [], error: (r as any).error };

    const rows = (r.data ?? []).map((row: any) => {
      const title = row.title ?? row.label ?? null;
      const completedAt = row.completed_at ?? null;
      const status = row.status ?? null;

      return {
        id: row.id,
        type: row.type ?? row.source ?? "crm_action",
        title,
        due_at: row.due_at ?? null,
        completed_at: completedAt,
        created_at: row.created_at ?? null,
        importance: row.importance ?? null,
        meta: row.meta ?? null,
        status,
      };
    });

    // (fetchContactActivityBestEffort moved to top-level scope)

    const open = rows.filter((x) => !x.completed_at && String(x.status ?? "open").toLowerCase() !== "done");
    const completed = rows.filter((x) => !!x.completed_at || String(x.status ?? "").toLowerCase() === "done");

    return { ok: true as const, open, completed };
  } catch (e: any) {
    return { ok: false as const, open: [], completed: [], error: e };
  }
}

// ----------------------------------------------------------------
// Contact Activity (Timeline Feed)
// Best-effort merge: notes + crm_actions + calls
// ----------------------------------------------------------------
async function fetchContactActivityBestEffort(args: {
  requester: string;
  contactId: string;
  limit: number;
}) {
  const { requester, contactId, limit } = args;

  const items: any[] = [];

  // ---- NOTES ----
  try {
    const { data } = await supa
      .from("crm_contact_notes")
      .select("id, body, created_at, importance, author_name")
      .eq("user_id", requester)
      .eq("contact_id", contactId)
      .order("created_at", { ascending: false })
      .limit(limit);

    for (const n of data ?? []) {
      items.push({
        id: `note_${(n as any).id}`,
        type: "note",
        created_at: (n as any).created_at,
        title: (n as any).body?.slice(0, 120) ?? "Note",
        importance: (n as any).importance ?? "normal",
        meta: {
          author: (n as any).author_name ?? null,
        },
      });
    }
  } catch {
    // fail-soft
  }

  // ---- ACTIONS ----
  try {
    const r = await fetchCrmActionsBestEffort({ requester, contactId, limit });

    if (r.ok) {
      const all = [...((r as any).open ?? []), ...((r as any).completed ?? [])];

      for (const a of all) {
        items.push({
          id: `action_${(a as any).id}`,
          type: "action",
          created_at: (a as any).created_at ?? (a as any).due_at ?? null,
          title: (a as any).title ?? "CRM Action",
          importance: (a as any).importance ?? null,
          meta: {
            completed: !!(a as any).completed_at,
            due_at: (a as any).due_at ?? null,
          },
        });
      }
    }
  } catch {
    // fail-soft
  }

  // ---- CALLS ----
  try {
    const { data: links } = await supa
      .from("crm_call_links")
      .select("call_id")
      .eq("contact_id", contactId)
      .limit(limit);

    const callIds = (links ?? []).map((l: any) => l.call_id).filter(Boolean);

    if (callIds.length) {
      const { data: calls } = await supa
        .from("calls")
        .select("id, created_at, summary, duration_ms")
        .eq("user_id", requester)
        .in("id", callIds)
        .order("created_at", { ascending: false })
        .limit(limit);

      for (const c of calls ?? []) {
        items.push({
          id: `call_${(c as any).id}`,
          type: "call",
          created_at: (c as any).created_at,
          title: (c as any).summary ?? "Sales Call",
          importance: null,
          meta: {
            duration_ms: (c as any).duration_ms ?? null,
          },
        });
      }
    }
  } catch {
    // fail-soft
  }

  // ---- SORT newest first ----
  items.sort((a, b) => {
    const ta = new Date(a.created_at ?? 0).getTime();
    const tb = new Date(b.created_at ?? 0).getTime();
    return tb - ta;
  });

  return items.slice(0, limit);
}

// GET /v1/crm/actions/today
// Rep home: stable actions list (does NOT depend on crm_actions.rep_id existing)
router.get("/actions/today", async (req, res) => {
  {
    try {
      const requester = getUserIdHeader(req);

      const repIdRaw = String((req.query as any).repId ?? "").trim();
      const repId = repIdRaw || requester;

      const limit = Math.min(Math.max(Number((req.query as any).limit ?? 10), 1), 25);

      const r = await selectCrmActionsSafe({
        requester,
        repId,
        status: "open",
        limit,
        orderByDue: true,
      });

      if (!r.ok) {
        const err = (r as any).error;
        const msg = String((err as any)?.message ?? err ?? "actions_today_failed");
        console.error("[crm/actions/today]", msg);
        return res.status(500).json({ ok: false, error: msg });
      }

      return res.json({
        ok: true,
        items: r.data ?? [],
        source: r.usedRepFilter ? "crm_actions_rep_id" : "crm_actions_user_only",
        select_cols: r.selectCols,
      });
    } catch (err: any) {
      console.error("[crm/actions/today]", err);
      return res.status(500).json({ ok: false, error: err.message || "actions_today_failed" });
    }
  }
});

// GET /v1/crm/actions
// Stable actions list for manager/rep filtering screens.
// Supports best-effort repId + status filtering without requiring crm_actions.status to exist.
router.get("/actions", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const repIdRaw = String((req.query as any).repId ?? (req.query as any).rep_id ?? "").trim();
    const repId = repIdRaw || null;

    const statusRaw = String((req.query as any).status ?? "").trim().toLowerCase();
    const status = statusRaw || null; // open | completed | overdue | (null)

    const limit = Math.min(Math.max(Number((req.query as any).limit ?? 200), 1), 500);

    // Pull a broad set (best-effort, schema tolerant) then filter in-process.
    const r = await selectCrmActionsSafe({
      requester,
      repId,
      contactId: null,
      status: null,
      limit,
      orderByDue: true,
    });

    if (!r.ok) {
      const err = (r as any).error;
      const msg = String((err as any)?.message ?? err ?? "actions_list_failed");
      const lower = msg.toLowerCase();

      // Missing table => return empty list (fail-open for UI)
      if (lower.includes("relation") && lower.includes("does not exist")) {
        return res.json({ ok: true, actions: [], source: "missing_table" });
      }

      console.error("[crm/actions]", msg);
      return res.status(500).json({ ok: false, error: msg });
    }

    const now = Date.now();

    const shaped = (r.data ?? []).map((row: any) => {
      const title = row.title ?? row.label ?? null;
      const completedAt = row.completed_at ?? null;
      const dueAt = row.due_at ?? null;

      const dueMs = dueAt ? new Date(String(dueAt)).getTime() : NaN;
      const isOverdue = !completedAt && Number.isFinite(dueMs) ? dueMs < now : false;

      return {
        id: row.id,
        contact_id: row.contact_id ?? null,
        rep_id: row.rep_id ?? null,
        type: row.type ?? row.source ?? "crm_action",
        title,
        due_at: dueAt,
        created_at: row.created_at ?? null,
        completed_at: completedAt,
        importance: row.importance ?? null,
        status: row.status ?? null,
        meta: row.meta ?? null,
        is_overdue: isOverdue,
      };
    });



    const filtered = (() => {
      if (status === "completed") return shaped.filter((x) => !!x.completed_at);
      if (status === "overdue") return shaped.filter((x) => x.is_overdue === true);
      if (status === "open") return shaped.filter((x) => !x.completed_at);
      return shaped;
    })();

    return res.json({
      ok: true,
      actions: filtered,
      source: r.usedRepFilter ? "crm_actions_rep_id" : "crm_actions_user_only",
      select_cols: r.selectCols,
    });
  } catch (err: any) {
    console.error("[crm/actions]", err);
    return res.status(500).json({ ok: false, error: err.message || "actions_list_failed" });
  }
});

// POST /v1/crm/actions
// Create a CRM action (manager/rep use). Schema-tolerant insert.
// Body: { contact_id, type?, title?, due_at?, importance?, meta?, rep_id? }
router.post("/actions", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const body = (req.body ?? {}) as any;

    const contactIdRaw = String(body.contact_id ?? body.contactId ?? "").trim();
    if (!contactIdRaw) {
      return res.status(400).json({ ok: false, error: "contact_id_required" });
    }

    const type = String(body.type ?? "follow_up").trim() || "follow_up";
    const title = String(body.title ?? "Follow up").trim() || "Follow up";

    const dueAt = body.due_at ?? body.dueAt ?? null;
    const importance = String(body.importance ?? "normal").trim() || "normal";
    const meta = body.meta ?? { source: "manager_contacts_table" };

    // Optional: allow passing rep_id (future manager flows). Default to requester.
    const repId = String(body.rep_id ?? body.repId ?? requester).trim() || requester;

    // We’ll try a “full” insert first, then retry if optional columns don’t exist.
    // IMPORTANT:
    // - user_id is ALWAYS the owner (requester)
    // - rep_id is the assignee (may equal requester)
    const basePayload: any = {
      user_id: requester,        // OWNER (always requester)
      rep_id: repId,             // ASSIGNEE
      contact_id: contactIdRaw,
      type,
      title,
      due_at: dueAt,
      importance,
      meta,
      status: "open",
      source: "manager_inline_create_v1",
    };

    const tryInsert = async (payload: any) => {
      return await supa.from("crm_actions").insert(payload).select("*").single();
    };

    // Retry ladder for schema tolerance (missing columns)
    const attempts: any[] = [
      // Full payload
      { ...basePayload },

      // Strip optional columns progressively
      (() => {
        const p = { ...basePayload };
        delete p.status;
        return p;
      })(),

      (() => {
        const p = { ...basePayload };
        delete p.importance;
        return p;
      })(),

      (() => {
        const p = { ...basePayload };
        delete p.meta;
        return p;
      })(),

      (() => {
        const p = { ...basePayload };
        delete p.source;
        return p;
      })(),

      (() => {
        const p = { ...basePayload };
        delete p.due_at;
        return p;
      })(),

      // Minimal but still correctly owned
      {
        user_id: requester,       // NEVER repId (owner must stay requester)
        rep_id: repId,
        contact_id: contactIdRaw,
        type,
        title,
      },

      // Absolute minimum fallback
      {
        user_id: requester,
        contact_id: contactIdRaw,
        type,
        title,
      },
    ];

    let lastErr: any = null;

    for (const payload of attempts) {
      const r = await tryInsert(payload);
      if (!r.error) {
        // best-effort: creating an action indicates contact engagement
        try {
          await touchContactBestEffort({ requester, contactId: contactIdRaw });
        } catch {
          // fail-soft
        }

        return res.json({ ok: true, action: r.data });
      }

      lastErr = r.error;
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      // Missing table should be loud (not silent), because manager ops depend on it
      if (msg.includes("relation") && msg.includes("does not exist")) {
        console.error("[crm/actions/create] missing_table", r.error);
        return res.status(500).json({ ok: false, error: "crm_actions_table_missing" });
      }

      // If it’s a missing column error, continue trying next payload
      if (msg.includes("column") && msg.includes("does not exist")) {
        continue;
      }

      // Otherwise stop retrying
      break;
    }

    return res.status(500).json({
      ok: false,
      error: (lastErr as any)?.message ?? "action_create_failed",
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: err.message || "action_create_failed",
    });
  }
});


/* ---------------------------------------------
   OPPORTUNITIES (Pipeline / Kanban v1)
   - Minimal endpoints to power Day 50
---------------------------------------------- */

const StageSchema = z.object({
  stage: z.string().trim().min(1),
});

// Best-effort select helper (schema tolerant) for crm_opportunities
async function selectCrmOpportunitiesSafe(args: {
  requester: string;
  stage?: string | null;
  limit: number;
}) {
  const { requester, stage, limit } = args;

  const candidateSelects = [
    "id, name, stage, amount, close_date, account_id, contact_id, created_at, updated_at",
    "id, name, stage, amount, close_date, account_id, contact_id, created_at",
    "id, name, stage, amount, close_date, account_id, contact_id",
    "id, name, stage, account_id, contact_id",
    "id, name, stage",
    "id, stage",
    "id",
  ];

  for (const sel of candidateSelects) {
    const q = supa
      .from("crm_opportunities")
      .select(sel)
      .eq("user_id", requester)
      .order("created_at", { ascending: false })
      .limit(limit);

    const r = stage ? await q.eq("stage", stage) : await q;

    if (!r.error) {
      return { ok: true as const, rows: (r.data as any[]) ?? [], select: sel };
    }

    const msg = String((r.error as any)?.message ?? "").toLowerCase();

    // Table missing: hard fail (pipeline depends on this table)
    if (msg.includes("relation") && msg.includes("does not exist")) {
      return { ok: false as const, error: r.error };
    }

    // Missing column: try the next select candidate
    if (msg.includes("column") && msg.includes("does not exist")) {
      continue;
    }

    // Unknown error: stop
    return { ok: false as const, error: r.error };
  }

  return { ok: false as const, error: new Error("crm_opportunities_select_failed") };
}

/* ---------------------------------------------
   POST /v1/crm/opportunities
   Body:
     - name | title (string) [required]
     - stage (string) [optional, default "new"]
     - amount | value (number) [optional]
     - currency (string) [optional]
     - account_name | accountName (string) [optional]
     - contact_email | contactEmail | email (string) [optional]

   Behaviour (MVP):
     - Inserts into crm_opportunities (schema tolerant)
     - Scoped to requester (user_id=requester)
---------------------------------------------- */

const CreateOpportunitySchema = z.object({
  name: z.string().trim().min(1).optional(),
  title: z.string().trim().min(1).optional(),
  stage: z.string().trim().min(1).optional(),

  amount: z.number().finite().nonnegative().optional().nullable(),
  value: z.number().finite().nonnegative().optional().nullable(),
  currency: z.string().trim().min(1).max(12).optional().nullable(),

  account_name: z.string().trim().min(1).max(180).optional().nullable(),
  accountName: z.string().trim().min(1).max(180).optional().nullable(),

  contact_email: z.string().email().transform((s) => s.toLowerCase().trim()).optional().nullable(),
  contactEmail: z.string().email().transform((s) => s.toLowerCase().trim()).optional().nullable(),
  email: z.string().email().transform((s) => s.toLowerCase().trim()).optional().nullable(),
});

router.post("/opportunities", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const body = CreateOpportunitySchema.parse(req.body ?? {});
    const name = String(body.name ?? body.title ?? "").trim();
    if (!name) return res.status(400).json({ ok: false, error: "name_required" });

    const stage = String(body.stage ?? "new").trim() || "new";

    const accountName = String(body.account_name ?? body.accountName ?? "").trim() || null;
    const contactEmail = (body.contact_email ?? body.contactEmail ?? body.email) ?? null;

    const amount = (body.amount ?? body.value ?? null) as number | null;
    const currency = String(body.currency ?? "").trim() || null;

    // --------------------------------------------
    // AUTO-CREATE / LINK CONTACT + ACCOUNT (optional)
    // --------------------------------------------
    let contactId: string | null = null;
    let accountId: string | null = null;

    // 1) Resolve or create contact by email
    if (contactEmail) {
      const existingContact = await supa
        .from("crm_contacts")
        .select("id, user_id")
        .eq("user_id", requester)
        .eq("email", contactEmail)
        .maybeSingle();

      if (existingContact.error) {
        const msg = String((existingContact.error as any)?.message ?? "").toLowerCase();
        if (msg.includes("relation") && msg.includes("does not exist")) {
          return res.status(500).json({ ok: false, error: "crm_contacts_table_missing" });
        }
        return res.status(500).json({ ok: false, error: "contact_lookup_failed" });
      }

      if (existingContact.data) {
        contactId = String((existingContact.data as any).id);
      } else {
        const ins = await supa
          .from("crm_contacts")
          .insert({
            user_id: requester,
            email: contactEmail,
          })
          .select("id")
          .maybeSingle();

        if (ins.error) {
          return res.status(500).json({
            ok: false,
            error: (ins.error as any)?.message ?? "contact_create_failed",
          });
        }

        contactId = String((ins.data as any).id);
      }
    }

    // 2) Resolve or create account by name
    if (accountName) {
      const existingAccount = await supa
        .from("crm_accounts")
        .select("id, user_id")
        .eq("user_id", requester)
        .eq("name", accountName)
        .maybeSingle();

      if (existingAccount.error) {
        const msg = String((existingAccount.error as any)?.message ?? "").toLowerCase();
        if (msg.includes("relation") && msg.includes("does not exist")) {
          return res.status(500).json({ ok: false, error: "crm_accounts_table_missing" });
        }
        return res.status(500).json({ ok: false, error: "account_lookup_failed" });
      }

      if (existingAccount.data) {
        accountId = String((existingAccount.data as any).id);
      } else {
        const ins = await supa
          .from("crm_accounts")
          .insert({
            user_id: requester,
            name: accountName,
          })
          .select("id")
          .maybeSingle();

        if (ins.error) {
          return res.status(500).json({
            ok: false,
            error: (ins.error as any)?.message ?? "account_create_failed",
          });
        }

        accountId = String((ins.data as any).id);
      }
    }

    // Insert (schema tolerant: try richer payload then fall back if cols missing)
    const payloadBase: any = {
      user_id: requester,
      name,
      title: name,
      stage,
      ...(amount !== null ? { amount, value: amount } : {}),
      ...(currency ? { currency } : {}),
      ...(accountId ? { account_id: accountId } : {}),
      ...(contactId ? { contact_id: contactId } : {}),
      ...(accountName ? { account_name: accountName } : {}),
      ...(contactEmail ? { contact_email: contactEmail } : {}),
    };

    const attempts: any[] = [
      { ...payloadBase },
      (() => {
        const p = { ...payloadBase };
        delete p.value;
        return p;
      })(),
      (() => {
        const p = { ...payloadBase };
        delete p.amount;
        delete p.value;
        return p;
      })(),
      (() => {
        const p = { ...payloadBase };
        delete p.account_name;
        return p;
      })(),
      (() => {
        const p = { ...payloadBase };
        delete p.contact_email;
        return p;
      })(),
      { user_id: requester, name, title: name, stage },
    ];

    let created: any = null;
    let lastErr: any = null;

    for (const p of attempts) {
      const ins = await supa.from("crm_opportunities").insert(p).select("*").maybeSingle();

      if (!ins.error) {
        created = ins.data;
        break;
      }

      lastErr = ins.error;
      const msg = String((ins.error as any)?.message ?? "").toLowerCase();

      // loud if table missing
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }

      const isMissingCol =
        (msg.includes("column") && msg.includes("does not exist")) ||
        msg.includes("could not find") ||
        msg.includes("unknown column");

      if (isMissingCol) continue;

      // otherwise stop retrying and return error
      break;
    }

    if (!created) {
      return res.status(500).json({
        ok: false,
        error: (lastErr as any)?.message ?? "opportunity_create_failed",
      });
    }

    return res.json({ ok: true, opportunity: created });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// GET /v1/crm/opportunities/pipeline
// Returns opportunities grouped by stage (kanban-ready)
router.get("/opportunities/pipeline", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    const limit = Math.min(Math.max(Number((req.query as any)?.limit ?? 500), 1), 2000);
    const stage = String((req.query as any)?.stage ?? "").trim() || null;

    const r = await selectCrmOpportunitiesSafe({ requester, stage, limit });
    if (!r.ok) {
      const msg = String(((r as any).error as any)?.message ?? (r as any).error ?? "pipeline_fetch_failed");
      return res.status(500).json({ ok: false, error: msg });
    }

    const rows = (r.rows ?? []).map((o: any) => ({
      id: o.id,
      name: o.name ?? null,
      stage: o.stage ?? null,
      amount: (o as any).amount ?? null,
      close_date: (o as any).close_date ?? null,
      account_id: (o as any).account_id ?? null,
      contact_id: (o as any).contact_id ?? null,
      created_at: (o as any).created_at ?? null,
      updated_at: (o as any).updated_at ?? null,
    }));

    const byStage: Record<string, any[]> = {};
    for (const o of rows) {
      const s = String(o.stage ?? "Unstaged").trim() || "Unstaged";
      (byStage[s] ||= []).push(o);
    }

    // Deterministic column order (alpha), but keep common stages first if present
    const preferred = ["Lead", "Qualified", "Proposal", "Negotiation", "Won", "Lost"];
    const stageSet = new Set(Object.keys(byStage));
    const orderedStages = [
      ...preferred.filter((s) => stageSet.has(s)),
      ...[...stageSet].filter((s) => !preferred.includes(s)).sort((a, b) => a.localeCompare(b)),
    ];

    return res.json({
      ok: true,
      stages: orderedStages,
      by_stage: byStage,
      opportunities: rows,
      meta: { select: r.select, limit, stage },
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// GET /v1/crm/opportunities/stages
// Returns distinct stage values for the requester.
router.get("/opportunities/stages", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);

    // Best-effort: select just stage; tolerate schemas without created_at
    const r = await supa
      .from("crm_opportunities")
      .select("stage")
      .eq("user_id", requester)
      .limit(2000);

    if (r.error) {
      const msg = String((r.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }
      // fail-open with defaults
      return res.json({
        ok: true,
        stages: ["Lead", "Qualified", "Proposal", "Negotiation", "Won", "Lost"],
        source: "defaults",
      });
    }

    const stages = Array.from(
      new Set(
        ((r.data as any[]) ?? [])
          .map((x) => String((x as any)?.stage ?? "").trim())
          .filter(Boolean)
      )
    ).sort((a, b) => a.localeCompare(b));

    const out = stages.length
      ? stages
      : ["Lead", "Qualified", "Proposal", "Negotiation", "Won", "Lost"];

    return res.json({ ok: true, stages: out, source: stages.length ? "db" : "defaults" });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

// PATCH /v1/crm/opportunities/:id/stage
// Body: { stage: string }
router.patch("/opportunities/:id/stage", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "opportunity_id_required" });

    const body = StageSchema.parse(req.body ?? {});

    // Best-effort: update stage (and updated_at if present)
    const candidates: any[] = [
      { stage: body.stage, updated_at: new Date().toISOString() },
      { stage: body.stage },
    ];

    let lastErr: any = null;

    for (const payload of candidates) {
      const r = await supa
        .from("crm_opportunities")
        .update(payload)
        .eq("id", id)
        .eq("user_id", requester)
        .select("id, name, stage")
        .maybeSingle();

      if (!r.error) {
        if (!r.data) return res.status(404).json({ ok: false, error: "not_found" });
        return res.json({ ok: true, opportunity: r.data });
      }

      lastErr = r.error;
      const msg = String((r.error as any)?.message ?? "").toLowerCase();

      // Missing column updated_at -> try next payload
      if (msg.includes("column") && msg.includes("updated_at") && msg.includes("does not exist")) {
        continue;
      }

      // Missing table
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_opportunities_table_missing" });
      }

      break;
    }

    return res.status(500).json({ ok: false, error: (lastErr as any)?.message ?? "stage_update_failed" });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   GET /v1/crm/opportunities/:id
   Query:
     - scope=mine|team (default mine)

   Behaviour:
     - mine (default): requester can fetch only their own opportunities
     - team: manager-only, can fetch an opp owned by a rep in the same org

   Returns minimal fields for the Opportunity Drawer.
---------------------------------------------- */
router.get("/opportunities/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();
    if (!id || !UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

    const scopeRaw = String((req.query as any)?.scope ?? "mine").trim().toLowerCase();
    const scope = scopeRaw === "team" ? "team" : "mine";

    const selectCols =
      "id,user_id,org_id,name,title,stage,amount,value,currency,close_date,account_id,contact_id,account_name,contact_email,created_at,updated_at";

    if (scope === "mine") {
      const { data, error } = await supa
        .from("crm_opportunities")
        .select(selectCols)
        .eq("id", id)
        .eq("user_id", requester)
        .maybeSingle();

      if (error) throw error;
      if (!data) return res.status(404).json({ ok: false, error: "not_found" });

      return res.json({
        ok: true,
        opportunity: {
          id: (data as any).id,
          name: (data as any).name ?? (data as any).title ?? null,
          stage: (data as any).stage ?? null,
          amount: (data as any).amount ?? (data as any).value ?? null,
          currency: (data as any).currency ?? null,
          close_date: (data as any).close_date ?? null,
          account_id: (data as any).account_id ?? null,
          contact_id: (data as any).contact_id ?? null,
          account_name: (data as any).account_name ?? null,
          contact_email: (data as any).contact_email ?? null,
          created_at: (data as any).created_at ?? null,
          updated_at: (data as any).updated_at ?? null,
        },
      });
    }

    // team scope => manager-only + org guard
    const mgr = await requireManagerOrg(req);
    const orgId = String((mgr as any)?.orgId ?? "").trim();

    const { data, error } = await supa.from("crm_opportunities").select(selectCols).eq("id", id).maybeSingle();
    if (error) throw error;
    if (!data) return res.status(404).json({ ok: false, error: "not_found" });

    const oppOrgId = String((data as any).org_id ?? "").trim();
    const ownerId = String((data as any).user_id ?? "").trim();

    // Validate owner is in org. Prefer explicit opp.org_id; otherwise validate via reps table.
    if (orgId) {
      if (oppOrgId) {
        if (oppOrgId !== orgId) return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
      } else if (ownerId) {
        const { data: repRow, error: repErr } = await supa.from("reps").select("id,org_id").eq("id", ownerId).maybeSingle();
        if (repErr) throw repErr;
        const repOrg = String((repRow as any)?.org_id ?? "").trim();
        if (!repOrg || repOrg !== orgId) return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
      }
    }

    return res.json({
      ok: true,
      opportunity: {
        id: (data as any).id,
        name: (data as any).name ?? (data as any).title ?? null,
        stage: (data as any).stage ?? null,
        amount: (data as any).amount ?? (data as any).value ?? null,
        currency: (data as any).currency ?? null,
        close_date: (data as any).close_date ?? null,
        account_id: (data as any).account_id ?? null,
        contact_id: (data as any).contact_id ?? null,
        account_name: (data as any).account_name ?? null,
        contact_email: (data as any).contact_email ?? null,
        created_at: (data as any).created_at ?? null,
        updated_at: (data as any).updated_at ?? null,
      },
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
    if (msg === "forbidden_not_manager") return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
    return res.status(500).json({ ok: false, error: e?.message ?? "opportunity_fetch_failed" });
  }
});

/* ---------------------------------------------
   PATCH /v1/crm/opportunities/:id
   Query:
     - scope=mine|team (default mine)

   Allows editing v1:
     - name, stage, amount, currency, close_date
---------------------------------------------- */
const PatchOpportunitySchema = z.object({
  name: z.string().trim().min(1).max(200).optional(),
  stage: z.string().trim().min(1).max(80).optional(),
  amount: z.number().finite().nonnegative().optional(),
  currency: z.string().trim().min(1).max(8).optional(),
  close_date: z.string().trim().min(1).nullable().optional(),
});

router.patch("/opportunities/:id", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String((req.params as any)?.id ?? "").trim();
    if (!id || !UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid_id" });

    const scopeRaw = String((req.query as any)?.scope ?? "mine").trim().toLowerCase();
    const scope = scopeRaw === "team" ? "team" : "mine";

    const body = PatchOpportunitySchema.parse(req.body ?? {});
    const patch: any = {};

    if (typeof body.name === "string") {
      patch.name = body.name;
      patch.title = body.name; // keep compat
    }
    if (typeof body.stage === "string") patch.stage = body.stage;
    if (typeof body.amount === "number") {
      patch.amount = body.amount;
      patch.value = body.amount;
    }
    if (typeof body.currency === "string") patch.currency = body.currency;
    if (body.close_date === null) patch.close_date = null;
    if (typeof body.close_date === "string") patch.close_date = body.close_date;

    if (Object.keys(patch).length === 0) {
      return res.status(400).json({ ok: false, error: "no_changes" });
    }

    const selectCols =
      "id,user_id,org_id,name,title,stage,amount,value,currency,close_date,account_id,contact_id,account_name,contact_email,created_at,updated_at";

    if (scope === "mine") {
      const { data, error } = await supa
        .from("crm_opportunities")
        .update(patch)
        .eq("id", id)
        .eq("user_id", requester)
        .select(selectCols)
        .maybeSingle();

      if (error) throw error;
      if (!data) return res.status(404).json({ ok: false, error: "not_found" });

      return res.json({
        ok: true,
        opportunity: {
          id: (data as any).id,
          name: (data as any).name ?? (data as any).title ?? null,
          stage: (data as any).stage ?? null,
          amount: (data as any).amount ?? (data as any).value ?? null,
          currency: (data as any).currency ?? null,
          close_date: (data as any).close_date ?? null,
          account_id: (data as any).account_id ?? null,
          contact_id: (data as any).contact_id ?? null,
          account_name: (data as any).account_name ?? null,
          contact_email: (data as any).contact_email ?? null,
          created_at: (data as any).created_at ?? null,
          updated_at: (data as any).updated_at ?? null,
        },
      });
    }

    // team scope => manager-only + org guard
    const mgr = await requireManagerOrg(req);
    const orgId = String((mgr as any)?.orgId ?? "").trim();

    // validate opp belongs to org
    const { data: existing, error: exErr } = await supa.from("crm_opportunities").select("id,user_id,org_id").eq("id", id).maybeSingle();
    if (exErr) throw exErr;
    if (!existing) return res.status(404).json({ ok: false, error: "not_found" });

    const oppOrgId = String((existing as any).org_id ?? "").trim();
    const ownerId = String((existing as any).user_id ?? "").trim();

    if (orgId) {
      if (oppOrgId) {
        if (oppOrgId !== orgId) return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
      } else if (ownerId) {
        const { data: repRow, error: repErr } = await supa.from("reps").select("id,org_id").eq("id", ownerId).maybeSingle();
        if (repErr) throw repErr;
        const repOrg = String((repRow as any)?.org_id ?? "").trim();
        if (!repOrg || repOrg !== orgId) return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
      }
    }

    const { data, error } = await supa.from("crm_opportunities").update(patch).eq("id", id).select(selectCols).maybeSingle();
    if (error) throw error;

    return res.json({
      ok: true,
      opportunity: {
        id: (data as any)?.id ?? id,
        name: (data as any)?.name ?? (data as any)?.title ?? null,
        stage: (data as any)?.stage ?? null,
        amount: (data as any)?.amount ?? (data as any)?.value ?? null,
        currency: (data as any)?.currency ?? null,
        close_date: (data as any)?.close_date ?? null,
        account_id: (data as any)?.account_id ?? null,
        contact_id: (data as any)?.contact_id ?? null,
        account_name: (data as any)?.account_name ?? null,
        contact_email: (data as any)?.contact_email ?? null,
        created_at: (data as any)?.created_at ?? null,
        updated_at: (data as any)?.updated_at ?? null,
      },
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg === "forbidden_org_scope") return res.status(403).json({ ok: false, error: "forbidden_org_scope" });
    if (msg === "forbidden_not_manager") return res.status(403).json({ ok: false, error: "forbidden_not_manager" });
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});


/* ---------------------------------------------
   LIGHTWEIGHT CREATE ENDPOINTS (v1)

   POST /v1/crm/contacts
   Body: { email: string, first_name?: string, last_name?: string, company?: string }
   - Creates or finds a contact scoped to requester by email.

   POST /v1/crm/accounts
   Body: { name: string }
   - Creates or finds an account scoped to requester by name.

   Notes:
   - Minimal for MVP quick-create flows.
   - Clear errors when tables missing.
---------------------------------------------- */

const CreateContactSchema = z.object({
  email: z.string().email().transform((s) => s.toLowerCase().trim()),
  first_name: z.string().trim().min(1).max(120).optional(),
  last_name: z.string().trim().min(1).max(120).optional(),
  company: z.string().trim().min(1).max(200).optional(),
});

router.post("/contacts", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = CreateContactSchema.parse(req.body ?? {});

    // 1) Find existing by (user_id + email)
    const ex = await supa
      .from("crm_contacts")
      .select("id, first_name, last_name, email, company, created_at")
      .eq("user_id", requester)
      .eq("email", body.email)
      .limit(1)
      .maybeSingle();

    if (ex.error) {
      const msg = String((ex.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_contacts_table_missing" });
      }
      return res.status(500).json({ ok: false, error: (ex.error as any)?.message ?? "contact_lookup_failed" });
    }

    if (ex.data) {
      return res.json({ ok: true, contact: ex.data, created: false });
    }

    // 2) Insert new contact
    const payload: any = {
      user_id: requester,
      email: body.email,
      ...(body.first_name ? { first_name: body.first_name } : {}),
      ...(body.last_name ? { last_name: body.last_name } : {}),
      ...(body.company ? { company: body.company } : {}),
    };

    const ins = await supa
      .from("crm_contacts")
      .insert(payload)
      .select("id, first_name, last_name, email, company, created_at")
      .maybeSingle();

    if (ins.error) {
      const msg = String((ins.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_contacts_table_missing" });
      }
      return res.status(500).json({ ok: false, error: (ins.error as any)?.message ?? "contact_create_failed" });
    }

    return res.json({ ok: true, contact: ins.data, created: true });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

const CreateAccountSchema = z.object({
  name: z.string().trim().min(1).max(200),
});

router.post("/accounts", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = CreateAccountSchema.parse(req.body ?? {});
    const nameNorm = body.name.trim();

    // 1) Find existing by (user_id + name)
    const ex = await supa
      .from("crm_accounts")
      .select("id, name, created_at")
      .eq("user_id", requester)
      .eq("name", nameNorm)
      .limit(1)
      .maybeSingle();

    if (ex.error) {
      const msg = String((ex.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_accounts_table_missing" });
      }
      return res.status(500).json({ ok: false, error: (ex.error as any)?.message ?? "account_lookup_failed" });
    }

    if (ex.data) {
      return res.json({ ok: true, account: ex.data, created: false });
    }

    // 2) Insert new account
    const ins = await supa
      .from("crm_accounts")
      .insert({ user_id: requester, name: nameNorm })
      .select("id, name, created_at")
      .maybeSingle();

    if (ins.error) {
      const msg = String((ins.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_accounts_table_missing" });
      }
      return res.status(500).json({ ok: false, error: (ins.error as any)?.message ?? "account_create_failed" });
    }

    return res.json({ ok: true, account: ins.data, created: true });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   QUICK CREATE (v1)
   - Contact / Account
   - Re-usable from anywhere (call page, pipeline, etc.)
---------------------------------------------- */

const CreateContactSchemaV2 = z.object({
  email: z.string().email().transform((s) => s.toLowerCase().trim()),
  first_name: z.string().trim().min(1).max(120).optional().nullable(),
  last_name: z.string().trim().min(1).max(120).optional().nullable(),
  company: z.string().trim().min(1).max(160).optional().nullable(),
});

// POST /v1/crm/contacts/create
router.post("/contacts/create", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = CreateContactSchemaV2.parse(req.body ?? {});

    const email = body.email;

    // 1) Try scoped lookup (user_id + email)
    const scoped = await supa
      .from("crm_contacts")
      .select("id, user_id, email, first_name, last_name, company")
      .eq("user_id", requester)
      .eq("email", email)
      .maybeSingle();

    if (!scoped.error && scoped.data) {
      return res.json({ ok: true, contact: scoped.data, created: false });
    }

    // 2) Insert new
    const ins = await supa
      .from("crm_contacts")
      .insert({
        user_id: requester,
        email,
        first_name: body.first_name ?? null,
        last_name: body.last_name ?? null,
        company: body.company ?? null,
      })
      .select("id, user_id, email, first_name, last_name, company")
      .single();

    if (!ins.error && ins.data) {
      return res.json({ ok: true, contact: ins.data, created: true });
    }

    // 3) Fallback: email may be globally unique — fetch by email and validate ownership
    const ex = await supa
      .from("crm_contacts")
      .select("id, user_id, email, first_name, last_name, company")
      .eq("email", email)
      .maybeSingle();

    if (ex.error) {
      return res.status(500).json({ ok: false, error: (ex.error as any)?.message ?? "contact_lookup_failed" });
    }

    if (!ex.data) {
      return res.status(500).json({ ok: false, error: (ins.error as any)?.message ?? "contact_create_failed" });
    }

    if (String((ex.data as any).user_id ?? "") !== requester) {
      return res.status(409).json({ ok: false, error: "email_owned_by_other_user" });
    }

    return res.json({ ok: true, contact: ex.data, created: false });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

const CreateAccountSchemaV2 = z.object({
  name: z.string().trim().min(1).max(160),
  domain: z.string().trim().min(1).max(160).optional().nullable(),
});

// POST /v1/crm/accounts/create
router.post("/accounts/create", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = CreateAccountSchemaV2.parse(req.body ?? {});

    // Best-effort: if you later add unique (user_id, domain) or (user_id, name), we can upsert.
    // For now: create + return.
    const ins = await supa
      .from("crm_accounts")
      .insert({
        user_id: requester,
        name: body.name,
        domain: body.domain ?? null,
      })
      .select("id, name, domain")
      .single();

    if (ins.error) {
      const msg = String((ins.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) {
        return res.status(500).json({ ok: false, error: "crm_accounts_table_missing" });
      }
      return res.status(500).json({ ok: false, error: (ins.error as any)?.message ?? "account_create_failed" });
    }

    return res.json({ ok: true, account: ins.data, created: true });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request" });
  }
});

export default router;