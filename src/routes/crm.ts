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
   - Search contacts by name/email (case-insensitive)
   - Returns minimal fields for the drawer
---------------------------------------------- */
router.get("/contacts", async (req, res) => {
  // PROBE: if you don't see these headers/logs, you're not hitting this handler.
  const marker = "contacts_handler_v4";
  res.setHeader("x-crm-contacts-marker", marker);
  res.setHeader("x-crm-contacts-marker-src", "routes/crm.ts");
  console.log("[CRM] /contacts hit", { marker, q: (req.query as any)?.query ?? (req.query as any)?.q, limit: (req.query as any)?.limit });

  try {
    const requester = getUserIdHeader(req);

    // Express query params can be string | string[] | undefined.
    // Accept a few aliases to avoid silent empty-query bugs.
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

    const limit = Math.min(Math.max(Number(req.query.limit ?? 12), 1), 50);

    // Marker so we can confirm which handler is live.
    const marker = "contacts_handler_v4";

    // Empty query => empty list (never spam demo)
    if (!query) return res.json({ ok: true, items: [], marker });

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

      return res.json({ ok: true, items: shaped, marker, source: "db" });
    }

    // 2) No DB matches — if this user has ANY real contacts, return empty (no demo pollution)
    const { data: anyReal, error: anyErr } = await supa
      .from("crm_contacts")
      .select("id")
      .eq("user_id", requester)
      .limit(1);

    // If this query errors, fail closed (empty) rather than showing demo noise.
    if (anyErr) {
      return res.status(500).json({ ok: false, error: anyErr.message ?? "contacts_count_failed", marker });
    }

    const hasRealContacts = (anyReal?.length ?? 0) > 0;
    if (hasRealContacts) {
      return res.json({ ok: true, items: [], marker, source: "db_empty" });
    }

    // 3) Demo fallback ONLY when user has zero real contacts
    const q = query.toLowerCase();
    const demo = DEMO_CONTACTS.filter((c) => {
      const hay = `${c.first_name} ${c.last_name} ${c.email} ${c.company ?? ""}`.toLowerCase();
      return hay.includes(q);
    })
      .slice(0, limit)
      .map(shapeDemoContact);

    return res.json({ ok: true, items: demo, marker, source: "demo" });
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
   Body: { callId: uuid, email: string }
   - Ensure the caller owns the call
   - Upsert contact by email (for this user)
   - Link call → contact in crm_call_links
---------------------------------------------- */
const LinkCallSchema = z.object({
  callId: z.string().uuid(),
  email: z.string().email().toLowerCase(),
});

router.post("/link-call", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = LinkCallSchema.parse(req.body);
    const { callId, email } = body;

    // Verify the call belongs to the requester
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id, user_id")
      .eq("id", callId)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "call_not_found" });
    if ((call as any).user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    // Upsert contact by email for this user (note: your crm_contacts has UNIQUE(email) currently)
    // If email is globally unique, this will fail across users. For now, we keep it simple:
    // - attempt insert
    // - if conflict, select existing by email
    const insertAttempt = await supa
      .from("crm_contacts")
      .insert({ user_id: requester, email })
      .select("id")
      .single();

    let contactId: string | null = null;

    if (!insertAttempt.error && insertAttempt.data) {
      contactId = (insertAttempt.data as any).id;
    } else {
      const { data: existing, error: exErr } = await supa
        .from("crm_contacts")
        .select("id")
        .eq("email", email)
        .single();
      if (exErr || !existing) throw insertAttempt.error ?? exErr;
      contactId = (existing as any).id;
    }

    const { error: linkErr } = await supa
      .from("crm_call_links")
      .upsert({ call_id: callId, contact_id: contactId }, { onConflict: "call_id" });

    if (linkErr) throw linkErr;

    // best-effort: linking a call is a "touch"
    try {
      await touchContactBestEffort({ requester, contactId });
    } catch {
      // fail-soft
    }

    res.json({ ok: true, link: { contact_id: contactId } });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
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
    const basePayload: any = {
      user_id: requester,      // OWNER (always requester)
      rep_id: repId,           // ASSIGNEE (optional)
      contact_id: contactIdRaw,
      type,
      title,
      due_at: dueAt,
      importance,
      meta,
      status: "open",
    };

    const tryInsert = async (payload: any) => {
      return await supa.from("crm_actions").insert(payload).select("*").single();
    };

    // Retry ladder for schema tolerance (missing columns)
    const attempts: any[] = [
      { ...basePayload },
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
        delete p.due_at;
        return p;
      })(),
      (() => {
        const p = { ...basePayload };
        delete p.status;
        delete p.importance;
        delete p.meta;
        delete p.due_at;
        return p;
      })(),
      // absolute minimum
      { user_id: repId, contact_id: contactIdRaw, type, title },
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

      // If it’s a missing column error, continue retry ladder
      const isMissingCol =
        (msg.includes("column") && msg.includes("does not exist")) ||
        msg.includes("could not find") ||
        msg.includes("unknown column");

      if (isMissingCol) continue;

      // Unknown error -> fail closed
      console.error("[crm/actions/create]", r.error);
      return res
        .status(500)
        .json({ ok: false, error: (r.error as any)?.message ?? "actions_create_failed" });
    }

    console.error("[crm/actions/create] exhausted_attempts", lastErr);
    return res
      .status(500)
      .json({ ok: false, error: (lastErr as any)?.message ?? "actions_create_failed" });
  } catch (err: any) {
    console.error("[crm/actions/create]", err);
    return res
      .status(500)
      .json({ ok: false, error: err.message || "actions_create_failed" });
  }
});

// POST /v1/crm/actions/:id/complete
router.post("/actions/:id/complete", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const actionId = String(req.params.id ?? "").trim();
    if (!actionId) {
      return res.status(400).json({ ok: false, error: "action_id_required" });
    }

    const nowIso = new Date().toISOString();

    const { data, error } = await supa
      .from("crm_actions")
      .update({
        completed_at: nowIso,
        status: "done",
      })
      .eq("id", actionId)
      .eq("user_id", requester)
      .select("id, contact_id")
      .single();

    if (error || !data) {
      return res.status(404).json({ ok: false, error: "action_not_found" });
    }

    // 🔥 Touch the related contact so health stays honest (fail-soft)
    try {
      const contactId = (data as any)?.contact_id;
      if (contactId) {
        await touchContactBestEffort({
          requester,
          contactId,
        });
      }
    } catch (err) {
      console.error("touch_on_action_complete_failed", err);
    }

    return res.json({ ok: true });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: err.message || "action_complete_failed",
    });
  }
});

// ------------------------------
// ------------------------------
// MANAGER CONTACTS + NUDGES helpers (v1)
// ------------------------------
async function fetchManagerContactsEnrichedV1(args: { requester: string; limit: number }) {
  const { requester, limit } = args;

  // ------------------------------
  // Contacts fetch (schema tolerant)
  // ------------------------------
  const selectContactsBestEffort = async () => {
    const ownerCols: Array<"user_id" | "requester_id"> = ["user_id", "requester_id"];
    const selectColsCandidates = [
      "id, first_name, last_name, email, company, last_contacted_at, created_at",
      "id, first_name, last_name, email, company, created_at",
    ];

    for (const ownerCol of ownerCols) {
      for (const sel of selectColsCandidates) {
        const r = await supa
          .from("crm_contacts")
          .select(sel)
          // @ts-ignore - dynamic column name
          .eq(ownerCol, requester)
          .order("created_at", { ascending: false })
          .limit(limit);

        if (!r.error) {
          const rows = (r.data as any[]) ?? [];
          const hasLastContacted = sel.includes("last_contacted_at");
          return { rows, hasLastContacted };
        }

        const msg = String((r.error as any)?.message ?? "").toLowerCase();

        // Missing table -> return empty
        if (msg.includes("relation") && msg.includes("does not exist")) {
          return { rows: [] as any[], hasLastContacted: false };
        }

        // Missing column -> try next candidate
        if ((msg.includes("column") && msg.includes("does not exist")) || msg.includes("could not find")) {
          continue;
        }

        // Unknown error -> surface
        throw r.error;
      }
    }

    return { rows: [] as any[], hasLastContacted: false };
  };

  const { rows, hasLastContacted } = await selectContactsBestEffort();

  // ------------------------------
  // Enrich per contact (best-effort)
  // ------------------------------
  const nowMs = Date.now();

  const enriched = await Promise.all(
    (rows ?? []).map(async (c: any) => {
      const contactId = String(c?.id ?? "");

      // Action counts (best-effort)
      let open = 0;
      let completed = 0;
      let overdue = 0;

      try {
        const r = await fetchCrmActionsBestEffort({ requester, contactId, limit: 250 });
        if (r.ok) {
          const openItems = (r.open ?? []) as any[];
          const completedItems = (r.completed ?? []) as any[];

          open = openItems.length;
          completed = completedItems.length;

          overdue = openItems.reduce((acc, a) => {
            const dueAt = (a as any)?.due_at ?? null;
            if (!dueAt) return acc;
            const dueMs = new Date(String(dueAt)).getTime();
            if (Number.isFinite(dueMs) && dueMs < nowMs) return acc + 1;
            return acc;
          }, 0);
        }
      } catch {
        // fail-soft
      }

      const lastContactedAt = hasLastContacted ? (c?.last_contacted_at ?? null) : null;
      const health = computeContactHealthV1({
        last_contacted_at: lastContactedAt,
        open_actions: open,
        overdue_actions: overdue,
        has_notes: false,
        has_recent_call: false,
      });

      return {
        id: contactId,
        first_name: c?.first_name ?? null,
        last_name: c?.last_name ?? null,
        email: c?.email ?? null,
        company: c?.company ?? null,
        last_contacted_at: hasLastContacted ? (c?.last_contacted_at ?? null) : null,
        created_at: c?.created_at ?? null,
        health,
        action_counts: {
          open,
          overdue,
          completed,
        },
      };
    })
  );

  return enriched;
}

function nudgePriorityScoreV1(c: any) {
  const overdue = Number(c?.action_counts?.overdue ?? 0);
  const open = Number(c?.action_counts?.open ?? 0);

  const healthScore = Number(c?.health?.score ?? 100);
  const band = String(c?.health?.band ?? "");

  const daysRaw = c?.health?.stats?.last_contacted_days;
  const days = typeof daysRaw === "number" && Number.isFinite(daysRaw) ? daysRaw : null;

  const next = (c?.health as any)?.next_action ?? null;
  const severity = String((next as any)?.severity ?? "none");

  // --- Weights (tuned for “feels right” sorting) ---
  // 1) Overdue dominates (manager urgency)
  // 2) Next-action severity matters (high/medium/low)
  // 3) Staleness ramps after 7 days, ramps harder after 14+
  // 4) Backlog (open actions) matters but should not eclipse overdue
  // 5) Poor health nudges up, good health nudges down slightly

  const bandW = band === "at_risk" ? 300 : band === "watch" ? 120 : 0;

  const sevW =
    severity === "high"
      ? 400
      : severity === "medium"
        ? 180
        : severity === "low"
          ? 60
          : 0;

  const staleW = (() => {
    // If never contacted/unknown, treat as stale.
    if (days === null) return 240;
    if (days <= 3) return 0;
    if (days <= 7) return (days - 3) * 15; // 4..7 => 15..60
    if (days <= 14) return 60 + (days - 7) * 25; // 8..14 => 85..235
    return 235 + (days - 14) * 35; // 15+ ramps hard
  })();

  // Health delta: bad health increases priority, good health slightly decreases
  const healthW = (100 - Math.max(0, Math.min(100, healthScore))) * 4;

  // Backlog grows but with diminishing returns
  const backlogW = Math.min(240, open * 35);

  // Overdue is king
  const overdueW = overdue * 900;

  // Small boost if there's literally anything to do (prevents lots of ties)
  const activityW = open > 0 ? 25 : 0;

  const priority = overdueW + bandW + sevW + staleW + backlogW + healthW + activityW;

  // Stable integer score for consistent sorts
  return Math.max(0, Math.round(priority));
}

function shapeManagerNudgeV1(c: any) {
  const contactId = String(c?.id ?? c?.contact_id ?? "").trim();

  const name =
    String(c?.name ?? "").trim() ||
    [c?.first_name, c?.last_name].filter(Boolean).join(" ").trim() ||
    "(no name)";

  const email = c?.email ?? null;
  const company = c?.company ?? null;

  const actionCounts = {
    open: Number(c?.action_counts?.open ?? 0),
    overdue: Number(c?.action_counts?.overdue ?? 0),
    completed: Number(c?.action_counts?.completed ?? 0),
  };

  // Always have a full, known-good health object to fall back to.
  const fallbackHealth = computeContactHealthV1({
    last_contacted_at: (c?.last_contacted_at ?? null) as any,
    open_actions: actionCounts.open,
    overdue_actions: actionCounts.overdue,
    has_notes: Boolean(c?.health?.stats?.has_notes ?? c?.has_notes ?? false),
    has_recent_call: Boolean(c?.health?.stats?.has_recent_call ?? c?.has_recent_call ?? false),
  });

  // Prefer enriched health if it looks like ours; otherwise use fallback.
  const h =
    c?.health &&
    typeof c.health === "object" &&
    (typeof (c.health as any).score === "number" || typeof (c.health as any).band === "string")
      ? c.health
      : fallbackHealth;

  // Normalise next_action to strings everywhere (older code sometimes used an object).
  const nextAny = (h as any)?.next_action ?? null;

  const nextLabelRaw =
    nextAny && typeof nextAny === "object"
      ? String((nextAny as any).label ?? "")
      : String(nextAny ?? "");

  const nextTypeRaw =
    nextAny && typeof nextAny === "object"
      ? String((nextAny as any).type ?? "none")
      : "none";

  const nextSeverityRaw =
    nextAny && typeof nextAny === "object"
      ? String((nextAny as any).severity ?? "none")
      : "none";

  // If label is blank, fall back to our derived default label.
  const fallbackNext: any = (fallbackHealth as any)?.next_action ?? null;
  const next_action =
    nextLabelRaw.trim() ||
    String(fallbackNext?.label ?? "") ||
    "Contact healthy";

  const next_action_type =
    nextTypeRaw.trim() ||
    String(fallbackNext?.type ?? "none") ||
    "none";

  const next_action_severity =
    nextSeverityRaw.trim() ||
    String(fallbackNext?.severity ?? "none") ||
    "none";

  const score =
    typeof (h as any)?.score === "number" && Number.isFinite((h as any).score)
      ? (h as any).score
      : fallbackHealth.score;

  const band =
    String((h as any)?.band ?? "").trim() ||
    String(fallbackHealth.band ?? "").trim() ||
    "healthy";

  const reasons = Array.isArray((h as any)?.reasons)
    ? (h as any).reasons
    : fallbackHealth.reasons;

  const stats =
    (h as any)?.stats && typeof (h as any).stats === "object"
      ? (h as any).stats
      : fallbackHealth.stats;

  const health = {
    score,
    band,
    reasons,
    stats,
    next_action,
    next_action_type,
    next_action_severity,
  };

  const lastContactedAt = c?.last_contacted_at ?? null;
  const createdAt = c?.created_at ?? null;

  const priority = Number(c?.priority ?? c?.priority_score ?? nudgePriorityScoreV1(c) ?? 0);

  return {
    contact_id: contactId,
    name,
    email,
    company,
    priority,
    health,
    action_counts: actionCounts,
    last_contacted_at: lastContactedAt,
    created_at: createdAt,
  };
}

// ------------------------------
// MANAGER CONTACTS (v1)
// GET /v1/crm/manager/contacts?filter=at-risk&limit=50
// - Org-scoped (fail-closed via requireManagerOrg)
// - Returns a list of contacts enriched with health + action counts
// - Schema tolerant: crm_contacts may lack last_contacted_at; ownership may be user_id or requester_id
// ------------------------------
router.get("/manager/contacts", async (req, res) => {
  try {
    const { requester } = await requireManagerOrg(req);

    const rawFilter = String((req.query as any)?.filter ?? "").trim().toLowerCase();
    const filter = rawFilter || null; // 'at-risk' | null

    const limit = Math.min(Math.max(Number((req.query as any)?.limit ?? 50), 1), 200);

    const enriched = await fetchManagerContactsEnrichedV1({ requester, limit });

    // ... rest of handler ...
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request", contacts: [] });
  }
});

// ------------------------------
// MANAGER NUDGES (v1)
// GET /v1/crm/manager/nudges?limit=25
// - Org-scoped by default (fail-closed), with local-dev bypass when x-org-id is all-zero UUID.
// - Reuses manager contacts list, computes priority score, sorts, and returns a shaped list.
// ------------------------------
router.get("/manager/nudges", async (req, res) => {
  try {
    const { requester, orgId, bypassed } = await requireManagerOrg(req);

    const limit = Math.min(Math.max(Number((req.query as any)?.limit ?? 25), 1), 200);

    const enriched = await fetchManagerContactsEnrichedV1({ requester, limit });

    const nudges = (enriched ?? [])
      .map((c: any) => {
        const shaped = shapeManagerNudgeV1(c);

        // FINAL GUARD: never allow null health out of this endpoint.
        // If anything upstream mutates health, we still return a real object.
        if (!shaped.health || typeof shaped.health !== "object") {
          const fallback = computeContactHealthV1({
            last_contacted_at: shaped.last_contacted_at ?? null,
            open_actions: Number(shaped.action_counts?.open ?? 0),
            overdue_actions: Number(shaped.action_counts?.overdue ?? 0),
            has_notes: false,
            has_recent_call: false,
          });

          shaped.health = {
            score: fallback.score,
            band: fallback.band,
            reasons: fallback.reasons,
            stats: fallback.stats,
            next_action: String((fallback as any)?.next_action?.label ?? "Contact healthy"),
            next_action_type: String((fallback as any)?.next_action?.type ?? "none"),
            next_action_severity: String((fallback as any)?.next_action?.severity ?? "none"),
          } as any;
        }

        return shaped;
      })
      .sort((a: any, b: any) => Number(b?.priority ?? 0) - Number(a?.priority ?? 0))
      .slice(0, limit);

    return res.json({
      ok: true,
      org: { id: orgId, bypassed },
      nudges,
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e?.message ?? "bad_request", nudges: [] });
  }
});

export default router;