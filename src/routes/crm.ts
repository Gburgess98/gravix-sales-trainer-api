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
  const oid = req.header("x-org-id");
  if (!oid || !UUID_RE.test(oid)) throw new Error("Missing or invalid x-org-id");
  return oid;
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

// Manager org scope helper (fail-closed)
// Requires:
// - x-user-id header (already enforced)
// - x-org-id header (UUID)
// - requester must belong to org (assertRequesterInOrg)
async function requireManagerOrg(req: any): Promise<{ requester: string; orgId: string }> {
  const requester = getUserIdHeader(req);
  const orgId = getOrgIdHeader(req);
  await assertRequesterInOrg({ requester, orgId });
  return { requester, orgId };
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
        .single();

      if (!a1.error && a1.data) c = a1.data;

      if (a1.error) {
        const msg = String((a1.error as any)?.message ?? "");
        if (msg.toLowerCase().includes("last_contacted_at") && msg.toLowerCase().includes("does not exist")) {
          const a2 = await supa
            .from("crm_contacts")
            .select("id, first_name, last_name, email, company, created_at")
            .eq("user_id", requester)
            .eq("id", id)
            .single();

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
    });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
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
        .single();
      if (cErr || !c) return res.status(404).json({ ok: false, error: "not_found" });
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

  // Build a query factory so we can re-run after stripping columns/filters.
  const run = async (useRepFilter: boolean, selectCols: string[]) => {
    let q = supa.from("crm_actions").select(selectCols.join(", ")).eq("user_id", requester);

    if (useRepFilter && repId) q = q.eq("rep_id", repId);
    if (contactId) q = q.eq("contact_id", contactId);
    if (status) q = q.eq("status", status);

    if (orderByDue) {
      // If due_at column doesn't exist, we'll strip it and retry.
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

    const open = rows.filter((x) => !x.completed_at && String(x.status ?? "open").toLowerCase() !== "done");
    const completed = rows.filter((x) => !!x.completed_at || String(x.status ?? "").toLowerCase() === "done");

    return { ok: true as const, open, completed };
  } catch (e: any) {
    return { ok: false as const, open: [], completed: [], error: e };
  }
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
// ------------------------------
// MANAGER OVERVIEW (v1)
// GET /v1/crm/manager/overview
// ------------------------------
router.get("/manager/overview", async (req, res) => {
  try {
    const { requester, orgId } = await requireManagerOrg(req);

    const limit = Math.min(Math.max(Number(req.query.limit ?? 25), 1), 200);

    // Resolve reps within org scope (fail-closed inside resolver)
    const reps = await resolveManagerScopedReps(requester, orgId);

    // Detect rep_id column on crm_actions
    let hasRepId = false;
    try {
      const probe = await supa.from("crm_actions").select("rep_id").limit(1);
      if (!probe.error) hasRepId = true;
    } catch {
      hasRepId = false;
    }

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);
    const startIso = startOfDay.toISOString();

    const items: any[] = [];

    for (const rep of reps.slice(0, limit)) {
      const base = supa
        .from("crm_actions")
        .select("id, due_at, completed_at, created_at, status")
        .eq("user_id", requester);

      const scoped = hasRepId ? base.eq("rep_id", rep.rep_id) : base;

      const { data, error } = await scoped
        .order("created_at", { ascending: false })
        .limit(500);

      if (error) throw error;

      const rows = data ?? [];
      const open = rows.filter(
        (r: any) => !r.completed_at && (r.status ?? "open") !== "done"
      );
      const overdue = open.filter(
        (r: any) => r.due_at && new Date(r.due_at).getTime() < Date.now()
      );
      const completedToday = rows.filter(
        (r: any) =>
          r.completed_at &&
          new Date(r.completed_at).toISOString() >= startIso
      );

      items.push({
        rep_id: rep.rep_id,
        rep_name: rep.rep_name ?? null,
        counts: {
          open: open.length,
          overdue: overdue.length,
          completed_today: completedToday.length,
        },
        meta: {
          mode: hasRepId ? "rep_id" : "user_only",
        },
      });
    }

    // Sort by overdue DESC, then open DESC
    items.sort((a, b) => {
      const od = (b.counts.overdue ?? 0) - (a.counts.overdue ?? 0);
      if (od !== 0) return od;
      return (b.counts.open ?? 0) - (a.counts.open ?? 0);
    });

    return res.json({
      ok: true,
      items,
      mode: hasRepId ? "rep_id" : "user_only",
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

// ------------------------------
// Helper: Resolve manager-scoped reps (fail-closed, schema-tolerant)
async function resolveManagerScopedReps(requester: string, orgId: string): Promise<Array<{ rep_id: string; rep_name?: string | null }>> {
  // Fail-closed default
  let fallback: Array<{ rep_id: string; rep_name?: string | null }> = [
    { rep_id: requester, rep_name: "You" },
  ];

  // Try to resolve reps that are managed by requester. Different deployments use different columns.
  // We attempt a small set of candidate filters and accept the first one that works.
  const buildName = (r: any) =>
    r.full_name ||
    r.name ||
    [r.first_name, r.last_name].filter(Boolean).join(" ") ||
    r.email ||
    "Rep";

  const mapRows = (rows: any[]) =>
    (rows ?? []).map((r: any) => ({
      rep_id: String(r.id ?? requester),
      rep_name: buildName(r),
    }));

  const baseSelect = "id, org_id, name, full_name, first_name, last_name, email";

  // Candidate filters in priority order.
  // - manager_id.eq.<requester>
  // - owner_id.eq.<requester>
  // - lead_id.eq.<requester>
  // - id.eq.<requester> (single-user fallback)
  const candidates: Array<() => Promise<any>> = [
    async () =>
      await supa
        .from("reps")
        .select(`${baseSelect}, manager_id`)
        .eq("org_id", orgId)
        .eq("manager_id", requester)
        .limit(500),
    async () =>
      await supa
        .from("reps")
        .select(`${baseSelect}, owner_id`)
        .eq("org_id", orgId)
        .eq("owner_id", requester)
        .limit(500),
    async () =>
      await supa
        .from("reps")
        .select(`${baseSelect}, lead_id`)
        .eq("org_id", orgId)
        .eq("lead_id", requester)
        .limit(500),
    async () =>
      await supa
        .from("reps")
        .select(baseSelect)
        .eq("org_id", orgId)
        .eq("id", requester)
        .limit(500),
  ];

  for (const run of candidates) {
    try {
      const { data, error } = await run();
      if (error) {
        const msg = String((error as any)?.message ?? "").toLowerCase();
        //
        // Missing org_id -> fail closed (do NOT fall back to cross-org data)
        if (
          (msg.includes("could not find") && msg.includes("org_id")) ||
          (msg.includes("column") && msg.includes("org_id") && msg.includes("does not exist"))
        ) {
          return fallback;
        }
        //
        // Other missing columns/tables -> try next candidate
        if (
          msg.includes("could not find") ||
          (msg.includes("column") && msg.includes("does not exist")) ||
          (msg.includes("relation") && msg.includes("does not exist"))
        ) {
          continue;
        }
        // Other errors: fail closed
        return fallback;
      }

      if (data?.length) {
        const reps = mapRows(data);
        // Always include requester as a final safety.
        const hasRequester = reps.some((x) => x.rep_id === requester);
        return hasRequester ? reps : [{ rep_id: requester, rep_name: "You" }, ...reps];
      }
    } catch {
      // Try next candidate
      continue;
    }
  }

  return fallback;
}
// ------------------------------
// MANAGER RUNNER (v1)
// POST /v1/crm/manager/runner
// - Triggers auto-assign runs for reps or a single rep
// - Safe, best-effort, never blocks the UI
// Body: { rep_id?: string, dry_run?: boolean }
// ------------------------------
router.post("/manager/runner", async (req, res) => {
  try {
    const { requester, orgId } = await requireManagerOrg(req);
    const body: any = req.body ?? {};

    const repIdRaw = typeof body.rep_id === "string" ? body.rep_id.trim() : "";
    const rep_id = repIdRaw || null;

    // Accept boolean OR string "true"/"false" for backwards clients
    const dryRunRaw = (body as any).dry_run;
    const dry_run = dryRunRaw === true || dryRunRaw === "true";
    const mode: "dry_run" | "execute" = dry_run ? "dry_run" : "execute";

    const startedAt = new Date();
    const run_id = (globalThis.crypto as any)?.randomUUID?.() ?? `run_${Date.now()}`;

    // Resolve reps (single or all) within requester scope. Fail-closed to requester.
    let reps = await resolveManagerScopedReps(requester, orgId);

    // If a specific rep is requested, ensure it is in scope.
    if (rep_id) {
      const hit = reps.find((r) => r.rep_id === rep_id);
      if (!hit) {
        return res.status(403).json({ ok: false, error: "forbidden_rep_scope" });
      }
      reps = [{ rep_id: hit.rep_id, rep_name: hit.rep_name ?? null }];
    }

    const result: {
      ok: true;
      run_id: string;
      deprecated: true;
      hint: string;
      mode: "dry_run" | "execute";
      started_at: string;
      finished_at: string;
      totals: {
        reps_considered: number;
        contacts_considered: number;
        actions_created: number;
        skipped_dedupe: number;
        errors: number;
      };
      reps: Array<{
        rep_id: string;
        rep_name?: string | null;
        created: number;
        skipped_dedupe: number;
        contacts_considered: number;
        errors_sample?: Array<{ contact_id: string; error: string }>;
        error?: string;
      }>;
      // Legacy v1 shape preserved for older callers
      dry_run: boolean;
      results: Array<{
        rep_id: string;
        contacts_processed: number;
        created: number;
        skipped: number;
        error?: string;
      }>;
    } = {
      ok: true,
      run_id,
      deprecated: true,
      hint: "Use POST /v1/crm/manager/auto-assign/run instead.",
      mode,
      started_at: startedAt.toISOString(),
      finished_at: "",
      totals: {
        reps_considered: reps.length,
        contacts_considered: 0,
        actions_created: 0,
        skipped_dedupe: 0,
        errors: 0,
      },
      reps: [],
      dry_run,
      results: [],
    };

    // Match v2 safety caps
    const CONTACTS_PER_REP = 50;
    const MAX_TOTAL_CONTACTS = 200;
    let totalContactsConsidered = 0;

    const dry_run_flag = mode === "dry_run";

    for (const rep of reps) {
      try {
        let fetchContactsMs = 0;

        const fetchT0 = Date.now();
        const { data: contacts, error: cErr } = await supa
          .from("crm_contacts")
          .select("id")
          .eq("user_id", requester)
          .order("created_at", { ascending: false })
          .limit(CONTACTS_PER_REP);
        fetchContactsMs = Date.now() - fetchT0;

        if (cErr) throw cErr;

        let created = 0;
        let skipped_dedupe = 0;
        let contacts_considered = 0;
        const errors_sample: Array<{ contact_id: string; error: string }> = [];

        const remaining = Math.max(0, MAX_TOTAL_CONTACTS - totalContactsConsidered);
        const list = (contacts ?? []).slice(0, Math.min(CONTACTS_PER_REP, remaining));

        const procT0 = Date.now();
        for (const c of list) {
          const contactId = String((c as any).id ?? "").trim();
          if (!contactId) continue;

          try {
            const out = await runContactAutoAssignCore({
              requester,
              repId: rep.rep_id,
              contactId,
              dry_run: dry_run_flag,
            });

            if (out?.created === true) created++;
            if (out?.duplicate === true) skipped_dedupe++;
          } catch (e: any) {
            result.totals.errors++;
            if (errors_sample.length < 3) {
              errors_sample.push({
                contact_id: contactId,
                error: String(e?.message ?? "contact_auto_assign_failed"),
              });
            }
          } finally {
            contacts_considered++;
            totalContactsConsidered++;
            result.totals.contacts_considered = totalContactsConsidered;
          }
        }

        result.totals.actions_created += created;
        result.totals.skipped_dedupe += skipped_dedupe;

        result.reps.push({
          rep_id: rep.rep_id,
          rep_name: rep.rep_name ?? null,
          created,
          skipped_dedupe,
          contacts_considered,
          errors_sample: errors_sample.length ? errors_sample : undefined,
        });

        // Legacy v1 mapping
        result.results.push({
          rep_id: rep.rep_id,
          contacts_processed: contacts_considered,
          created,
          skipped: skipped_dedupe,
        });
      } catch (e: any) {
        result.totals.errors++;

        result.reps.push({
          rep_id: rep.rep_id,
          rep_name: rep.rep_name ?? null,
          created: 0,
          skipped_dedupe: 0,
          contacts_considered: 0,
          error: String(e?.message ?? "rep_runner_failed"),
        });

        result.results.push({
          rep_id: rep.rep_id,
          contacts_processed: 0,
          created: 0,
          skipped: 0,
          error: String(e?.message ?? "rep_runner_failed"),
        });
      }

      // Stop early if we've hit the global cap
      if (totalContactsConsidered >= MAX_TOTAL_CONTACTS) break;
    }

    result.finished_at = new Date().toISOString();
    return res.json(result);
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_rep_scope") || msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

// ------------------------------
// Step F — Slack alert on bad runs (fail-open)
// Triggers when:
// - totals.errors > 0
// - OR actions_created >= threshold (safety)
async function notifyCrmAutoAssignRunSlack(args: {
  run_id: string;
  requester: string;
  mode: "dry_run" | "execute";
  totals: {
    reps_considered: number;
    contacts_considered: number;
    actions_created: number;
    skipped_dedupe: number;
    errors: number;
  };
  reps: Array<{
    rep_id: string;
    rep_name?: string | null;
    created: number;
    skipped_dedupe: number;
    contacts_considered: number;
    errors_sample?: Array<{ contact_id: string; error: string }>;
    error?: string;
  }>;
}) {
  const { run_id, requester, mode, totals, reps } = args;

  const ACTIONS_CREATED_ALERT_THRESHOLD = 25;
  const shouldAlert = (totals.errors ?? 0) > 0 || (totals.actions_created ?? 0) >= ACTIONS_CREATED_ALERT_THRESHOLD;
  if (!shouldAlert) return;

  // Pull a tiny error sample (if any)
  let sample: string | null = null;
  for (const r of reps ?? []) {
    if (r.error) {
      sample = `rep ${r.rep_name ?? r.rep_id}: ${r.error}`;
      break;
    }
    const s = (r.errors_sample ?? [])[0];
    if (s) {
      sample = `rep ${r.rep_name ?? r.rep_id}: contact ${s.contact_id} — ${s.error}`;
      break;
    }
  }

  const icon = (totals.errors ?? 0) > 0 ? "⚠️" : "👀";
  const title = `${icon} CRM auto-assign run ${mode.toUpperCase()} — ${run_id}`;
  const lines = [
    title,
    `requester: ${requester}`,
    `reps_considered: ${totals.reps_considered ?? 0}`,
    `contacts_considered: ${totals.contacts_considered ?? 0}`,
    `actions_created: ${totals.actions_created ?? 0}`,
    `skipped_dedupe: ${totals.skipped_dedupe ?? 0}`,
    `errors: ${totals.errors ?? 0}`,
  ];

  if (sample) lines.push(`sample: ${sample}`);

  // Fail-open: never block the API response.
  try {
    await postSlack({ text: lines.join("\n") });
  } catch (e: any) {
    console.error("[crm/manager/auto-assign/run] slack_alert_failed", String(e?.message ?? e));
  }
}

// ------------------------------
// Best-effort run logging (fail-open)
// Table target: crm_auto_assign_runs
// If the table/columns don't exist, ignore errors and continue.
async function logCrmAutoAssignRunBestEffort(args: {
  phase: "start" | "finish";
  run_id: string;
  requester: string;
  mode: "dry_run" | "execute";
  started_at: string;
  finished_at?: string;
  totals?: any;
  reps?: any;
}) {
  const { phase, run_id, requester, mode, started_at, finished_at, totals, reps } = args;

  // Common payload shapes across possible schemas.
  const baseInsertCandidates: any[] = [
    {
      run_id,
      user_id: requester,
      requester_id: requester,
      mode,
      started_at,
      finished_at: finished_at ?? null,
      totals: totals ?? null,
      reps: reps ?? null,
    },
    {
      run_id,
      user_id: requester,
      mode,
      started_at,
      finished_at: finished_at ?? null,
      totals: totals ?? null,
      reps: reps ?? null,
    },
    {
      run_id,
      user_id: requester,
      mode,
      started_at,
    },
  ];

  const baseUpdateCandidates: any[] = [
    {
      finished_at: finished_at ?? new Date().toISOString(),
      totals: totals ?? null,
      reps: reps ?? null,
    },
    {
      finished_at: finished_at ?? new Date().toISOString(),
      totals: totals ?? null,
    },
    {
      finished_at: finished_at ?? new Date().toISOString(),
    },
  ];

  const isMissingSchema = (msg: string) => {
    const m = msg.toLowerCase();
    return (
      (m.includes("relation") && m.includes("does not exist")) ||
      (m.includes("does not exist") && m.includes("crm_auto_assign_runs"))
    );
  };

  const stripMissingCol = (msg: string, payload: any) => {
    const lower = msg.toLowerCase();
    const m1 = lower.match(/could not find the '([^']+)' column/);
    const m2 = lower.match(/column crm_auto_assign_runs\.([a-zA-Z0-9_]+) does not exist/);
    const missing = (m1?.[1] ?? m2?.[1]) as string | undefined;
    if (missing && missing in payload) {
      const next = { ...payload };
      delete next[missing];
      return next;
    }
    return null;
  };

  try {
    if (phase === "start") {
      for (const seed of baseInsertCandidates) {
        let payload = { ...seed };
        for (let i = 0; i < 6; i++) {
          const { error } = await supa.from("crm_auto_assign_runs").insert(payload);
          if (!error) return;

          const msg = String((error as any)?.message ?? "");
          if (isMissingSchema(msg)) return;

          const stripped = stripMissingCol(msg, payload);
          if (stripped) {
            payload = stripped;
            continue;
          }

          // Unknown error: stop trying this seed.
          break;
        }
      }
      return;
    }

    // finish
    for (const patch0 of baseUpdateCandidates) {
      let patch = { ...patch0 };
      for (let i = 0; i < 6; i++) {
        const { error } = await supa
          .from("crm_auto_assign_runs")
          .update(patch)
          .eq("run_id", run_id)
          .eq("user_id", requester);

        if (!error) return;

        const msg = String((error as any)?.message ?? "");
        if (isMissingSchema(msg)) return;

        const stripped = stripMissingCol(msg, patch);
        if (stripped) {
          patch = stripped;
          continue;
        }

        break;
      }
    }
  } catch {
    // Fail-open: never block the runner.
    return;
  }
}
// ------------------------------
// MANAGER AUTO-ASSIGN PREVIEW (v1)
// POST /v1/crm/manager/auto-assign/preview
// - Alias to dry_run with richer diagnostics for managers
// - NEVER mutates data
// ------------------------------
router.post("/manager/auto-assign/preview", async (req, res) => {
  try {
    const { requester, orgId } = await requireManagerOrg(req);

    const parsed = ManagerAutoAssignRunSchema.safeParse({
      ...(req.body ?? {}),
      mode: "dry_run",
    });

    if (!parsed.success) {
      return res.status(400).json({ ok: false, error: "invalid_body" });
    }

    const { limit_reps, contacts_per_rep, max_total_contacts } = parsed.data;

    // Reuse the existing runner logic safely via in-process call
    // IMPORTANT: do NOT spread `req` into a plain object (it loses req.header()).
    // We temporarily override req.body and call the real handler with the real req instance.
    const startedAt = new Date();

    // Call the real runner handler directly
    // NOTE: safe because we force dry_run mode.
    const runner = (router as any).stack.find(
      (r: any) =>
        r.route &&
        r.route.path === "/manager/auto-assign/run" &&
        r.route.methods.post
    );

    if (!runner) {
      return res.status(500).json({ ok: false, error: "runner_not_found" });
    }

    // Capture the JSON output from the runner
    let payload: any = null;
    const fakeRes: any = {
      json: (x: any) => {
        payload = x;
        return x;
      },
      status: (_c: number) => fakeRes,
    };

    const prevBody = (req as any).body;
    try {
      (req as any).body = {
        mode: "dry_run",
        limit_reps,
        contacts_per_rep,
        max_total_contacts,
      };

      await runner.route.stack[0].handle(req, fakeRes);
    } finally {
      (req as any).body = prevBody;
    }

    if (!payload?.ok) {
      const upstreamErr = String(payload?.error ?? "preview_failed");
      return res.status(500).json({ ok: false, error: "preview_failed", detail: upstreamErr });
    }

    // --- PATCH: Best-effort: finish preview run with label (fail-open)
    // Best-effort: label this run as a preview in the run history (fail-open)
    try {
      await supa
        .from("crm_auto_assign_runs")
        .update({ source: "preview" })
        .eq("run_id", payload.run_id)
        .eq("user_id", requester);
    } catch {
      // fail-open
    }
    await logCrmAutoAssignRunBestEffort({
      phase: "finish",
      run_id: payload.run_id,
      requester,
      mode: "dry_run",
      started_at: startedAt.toISOString(),
      finished_at: new Date().toISOString(),
      totals: payload.totals,
      reps: payload.reps,
    });

    // Enrich with preview-only metadata and clear label for downstream UI
    return res.json({
      ok: true,
      preview: true,
      source: "preview",
      generated_at: startedAt.toISOString(),
      run_id: payload.run_id,
      totals: payload.totals,
      reps: payload.reps,
      // ✅ Forward v2 meta (timings/caps). Never return null.
      meta: {
        ...(payload && typeof (payload as any).meta === "object" && (payload as any).meta ? (payload as any).meta : {}),
        // Always make meta an object for consumers (never null)
        preview: true,
        forwarded: Boolean(payload && (payload as any).meta),
        generated_at: startedAt.toISOString(),
      },
      hint: "Preview only — no actions were created. Execute explicitly to apply.",
    });

  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_rep_scope") || msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error", detail: msg || "server_error" });
  }
});

// ------------------------------
// MANAGER AUTO-ASSIGN EXECUTE FROM PREVIEW (v1)
// POST /v1/crm/manager/auto-assign/execute-from-preview
// Body: { preview_run_id: uuid }
// - Executes a previously completed dry_run
// - Fails closed if the run does not exist or was not a dry_run
// ------------------------------
router.post("/manager/auto-assign/execute-from-preview", async (req, res) => {
  try {
    const { requester, orgId } = await requireManagerOrg(req);

    const __t0 = Date.now();

    // Explicit org safety (defensive, even if DB schema changes)
    if (!orgId) {
      return res.status(500).json({ ok: false, error: "org_scope_required" });
    }

    const previewRunId = String((req.body as any)?.preview_run_id ?? "").trim();

    if (!UUID_RE.test(previewRunId)) {
      return res.status(400).json({ ok: false, error: "invalid_preview_run_id" });
    }

    // Fetch the preview run (tolerate missing source column)
    const { data: rows, error } = await supa
      .from("crm_auto_assign_runs")
      .select("run_id, mode, totals")
      .eq("user_id", requester)
      .eq("run_id", previewRunId)
      .limit(1);

    if (error) throw error;

    const row = (rows ?? [])[0];
    if (!row) {
      return res.status(404).json({ ok: false, error: "preview_run_not_found" });
    }

    if (String(row.mode) !== "dry_run") {
      return res.status(409).json({ ok: false, error: "preview_run_not_dry_run" });
    }

    // Defensive: tolerate missing 'source' column; no-op if not present.
    // (Preview validation now relies only on: run exists and mode === "dry_run")

    // If preview produced zero actions, executing it is a valid no-op.
    // Return ok=true with meta so the Manager UI can still show timings/why nothing happened.
    const previewActionsCreated = Number((row as any)?.totals?.actions_created ?? 0);
    if (previewActionsCreated === 0) {
      return res.json({
        ok: true,
        executed_from_preview: previewRunId,
        run_id: null,
        totals: {
          reps_considered: 0,
          contacts_considered: 0,
          actions_created: 0,
          skipped_dedupe: 0,
          errors: 0,
        },
        reps: [],
        meta: {
          version: "v2",
          total_ms: Date.now() - __t0,
          time_budget_ms: 10000,
          aborted_reason: "preview_has_no_actions",
          timings: { per_rep: [] },
          caps: { contacts_per_rep: 0, max_total_contacts: 0 },
          executed_from_preview: true,
          preview_run_id: previewRunId,
        },
        hint: "Nothing to execute — preview had no actions.",
      });
    }

    // Fail closed if this preview has already been executed
    const { data: executedCheck } = await supa
      .from("crm_auto_assign_runs")
      .select("run_id, executed_from_preview_run_id")
      .eq("executed_from_preview_run_id", previewRunId)
      .limit(1);

    if (executedCheck && executedCheck.length > 0) {
      return res.status(409).json({ ok: false, error: "preview_already_executed" });
    }

    // Execute with the same safety defaults (no re-preview)
    // IMPORTANT: do NOT spread `req` into a plain object (it loses req.header()).
    const runner = (router as any).stack.find(
      (r: any) =>
        r.route &&
        r.route.path === "/manager/auto-assign/run" &&
        r.route.methods.post
    );

    if (!runner) {
      return res.status(500).json({ ok: false, error: "runner_not_found" });
    }

    let payload: any = null;
    const fakeRes = {
      json: (x: any) => {
        payload = x;
        return x;
      },
      status: (_c: number) => fakeRes,
    };

    const prevBody = (req as any).body;
    try {
      (req as any).body = { mode: "execute" };
      await runner.route.stack[0].handle(req, fakeRes);
    } finally {
      (req as any).body = prevBody;
    }

    if (!payload?.ok) {
      return res.status(500).json({ ok: false, error: "execute_failed" });
    }

    // Best-effort audit link: mark execution as derived from preview
    try {
      await supa
        .from("crm_auto_assign_runs")
        .update({
          executed_from_preview_run_id: previewRunId,
          executed_by_user_id: requester,
          executed_at: new Date().toISOString(),
        })
        .eq("run_id", payload.run_id)
        .eq("user_id", requester);
    } catch {
      // fail-open: audit should never block execution
    }

    const meta =
      payload && typeof (payload as any).meta === "object" && (payload as any).meta
        ? (payload as any).meta
        : {};

    return res.json({
      ok: true,
      executed_from_preview: previewRunId,
      run_id: payload.run_id,
      totals: payload.totals,
      reps: payload.reps,
      // ✅ propagate v2 timings/caps when available
      meta: {
        total_ms: typeof (meta as any)?.total_ms === "number" ? (meta as any).total_ms : Date.now() - __t0,
        ...meta,
        executed_from_preview: true,
        preview_run_id: previewRunId,
      },
    });

  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("invalid_preview_run_id")) {
      return res.status(400).json({ ok: false, error: msg });
    }
    if (
      msg.includes("preview_run_not_found") ||
      msg.includes("preview_already_executed") ||
      msg.includes("preview_run_not_preview") ||
      msg.includes("preview_run_not_dry_run")
    ) {
      return res.status(409).json({ ok: false, error: msg });
    }
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_rep_scope") || msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});
// ------------------------------
// MANAGER AUTO-ASSIGN RUNNER (v2)
// POST /v1/crm/manager/auto-assign/run
// Body: { mode: 'dry_run'|'execute', limit_reps?: number }
// - Safe-by-default: dry_run supported.
// - Per-rep isolation: one rep failure never aborts the whole run.
// - Returns a structured, scheduler-safe summary.
// ------------------------------
const ManagerAutoAssignRunSchema = z.object({
  mode: z.enum(["dry_run", "execute"]),
  limit_reps: z.number().int().min(1).max(200).optional(),

  // Optional safety caps for scheduled runs
  contacts_per_rep: z.number().int().min(1).max(200).optional(),
  max_total_contacts: z.number().int().min(1).max(5000).optional(),
});

router.post("/manager/auto-assign/run", async (req, res) => {
  try {
    const { requester, orgId } = await requireManagerOrg(req);

    // ------------------------------
    // Day 39 Step 1: Timing instrumentation
    // ------------------------------
    const t0 = Date.now();
    const TIME_BUDGET_MS = 10000;

    const perRepTimings: Array<{
      rep_id: string;
      fetch_contacts_ms: number;
      processing_ms: number;
      total_ms: number;
    }> = [];

    let aborted_reason: "max_contacts" | "time_budget" | null = null;

    const parsed = ManagerAutoAssignRunSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({ ok: false, error: "invalid_body" });
    }

    const { mode, limit_reps, contacts_per_rep, max_total_contacts } = parsed.data;
    const dry_run = mode === "dry_run";

    const startedAt = new Date();
    const run_id = (globalThis.crypto as any)?.randomUUID?.() ?? `run_${Date.now()}`;

    // Best-effort: create a run record (fail-open)
    await logCrmAutoAssignRunBestEffort({
      phase: "start",
      run_id,
      requester,
      mode,
      started_at: startedAt.toISOString(),
    });

    // Resolve reps within requester scope. Fail-closed to requester.
    let reps = await resolveManagerScopedReps(requester, orgId);

    if (typeof limit_reps === "number") {
      reps = reps.slice(0, limit_reps);
    }

    const result: {
      ok: true;
      run_id: string;
      mode: "dry_run" | "execute";
      started_at: string;
      finished_at: string;
      totals: {
        reps_considered: number;
        contacts_considered: number;
        actions_created: number;
        skipped_dedupe: number;
        errors: number;
      };
      reps: Array<{
        rep_id: string;
        rep_name?: string | null;
        created: number;
        skipped_dedupe: number;
        contacts_considered: number;
        errors_sample?: Array<{ contact_id: string; error: string }>;
        error?: string;
        // --- Day 38 Step C: skipped reason diagnostics ---
        skipped_by_reason?: Record<string, number>;
        skipped_samples?: Array<{ contact_id: string; reason: string }>;
      }>;
    } = {
      ok: true,
      run_id,
      mode,
      started_at: startedAt.toISOString(),
      finished_at: "",
      totals: {
        reps_considered: reps.length,
        contacts_considered: 0,
        actions_created: 0,
        skipped_dedupe: 0,
        errors: 0,
      },
      reps: [],
    };

    // Safety: cap contacts per rep per run to prevent blow-ups.
    const CONTACTS_PER_REP = contacts_per_rep ?? 50;
    // Safety: cap TOTAL contacts across all reps for a single run.
    const MAX_TOTAL_CONTACTS = max_total_contacts ?? 200;
    let totalContactsConsidered = 0;

    for (const rep of reps) {
      // Global safety guards
      if (Date.now() - t0 > TIME_BUDGET_MS) {
        aborted_reason = "time_budget";
        break;
      }

      // If we aborted early (time budget / max contacts), surface it as an error signal.
      // This keeps the run visible in dashboards/alerts without throwing.
      if (aborted_reason) {
        result.totals.errors += 1;
      }

      if (totalContactsConsidered >= MAX_TOTAL_CONTACTS) {
        aborted_reason = "max_contacts";
        break;
      }

      const repT0 = Date.now();
      let fetchContactsMs = 0;
      let processingMs = 0;
      try {
        const fetchT0 = Date.now();
        const { data: contacts, error: cErr } = await supa
          .from("crm_contacts")
          .select("id")
          .eq("user_id", requester)
          .order("created_at", { ascending: false })
          .limit(CONTACTS_PER_REP);
        fetchContactsMs = Date.now() - fetchT0;

        if (cErr) throw cErr;

        let created = 0;
        let skipped_dedupe = 0;
        let contacts_considered = 0;
        const errors_sample: Array<{ contact_id: string; error: string }> = [];
        // --- Step 2: skipped reason diagnostics ---
        const skipped_by_reason: Record<string, number> = {};
        const skipped_samples: Array<{ contact_id: string; reason: string }> = [];

        const remaining = Math.max(0, MAX_TOTAL_CONTACTS - totalContactsConsidered);
        const list = (contacts ?? []).slice(0, Math.min(CONTACTS_PER_REP, remaining));

        const procT0 = Date.now();
        for (const c of list) {
          const contactId = String((c as any).id ?? "").trim();
          if (!contactId) continue;
          try {
            const out = await runContactAutoAssignCore({
              requester,
              repId: rep.rep_id,
              contactId,
              dry_run,
            });

            // --- Step 3: capture skip reasons ---
            const createdFlag = out?.created === true;
            const dupFlag = out?.duplicate === true;

            if (createdFlag) created++;
            if (dupFlag) skipped_dedupe++;

                        // If core doesn't emit a reason but also didn't create/duplicate,
            // count it as a skip with a best-effort reason.
            if (!createdFlag && !dupFlag) {
              const reasonRaw =
                (out as any)?.skip_reason ??
                (out as any)?.reason ??
                (out as any)?.skipped_reason ??
                (out as any)?.skipped ??
                null;

              const reason = String(reasonRaw ?? "no_next_action").trim() || "no_next_action";
              skipped_by_reason[reason] = (skipped_by_reason[reason] || 0) + 1;
              if (skipped_samples.length < 3) {
                skipped_samples.push({ contact_id: contactId, reason });
              }
            }
          } catch (e: any) {
            result.totals.errors++;
            if (errors_sample.length < 3) {
              errors_sample.push({
                contact_id: contactId,
                error: String(e?.message ?? "contact_auto_assign_failed"),
              });
            }
          } finally {
            contacts_considered++;
            totalContactsConsidered++;
            result.totals.contacts_considered = totalContactsConsidered;

            // Time budget guard inside tight loop
            if (Date.now() - t0 > TIME_BUDGET_MS) {
              aborted_reason = "time_budget";
              break;
            }

            if (totalContactsConsidered >= MAX_TOTAL_CONTACTS) {
              aborted_reason = "max_contacts";
              break;
            }
          }
        }
        processingMs = Date.now() - procT0;

        result.totals.actions_created += created;
        result.totals.skipped_dedupe += skipped_dedupe;

        perRepTimings.push({
          rep_id: rep.rep_id,
          fetch_contacts_ms: fetchContactsMs,
          processing_ms: processingMs,
          total_ms: Date.now() - repT0,
        });

        result.reps.push({
          rep_id: rep.rep_id,
          rep_name: rep.rep_name ?? null,
          created,
          skipped_dedupe,
          contacts_considered,
          errors_sample: errors_sample.length ? errors_sample : undefined,
          skipped_by_reason: Object.keys(skipped_by_reason).length ? skipped_by_reason : undefined,
          skipped_samples: skipped_samples.length ? skipped_samples : undefined,
        });
      } catch (e: any) {
        result.totals.errors++;

        perRepTimings.push({
          rep_id: rep.rep_id,
          fetch_contacts_ms: fetchContactsMs,
          processing_ms: processingMs,
          total_ms: Date.now() - repT0,
        });

        result.reps.push({
          rep_id: rep.rep_id,
          rep_name: rep.rep_name ?? null,
          created: 0,
          skipped_dedupe: 0,
          contacts_considered: 0,
          error: String(e?.message ?? "rep_runner_failed"),
        });
      }

      if (aborted_reason) break;
    }

    result.finished_at = new Date().toISOString();

    // If we aborted early, surface it as a soft error signal
    if (aborted_reason) result.totals.errors += 1;

    // Best-effort: run logging + Slack alert (fail-open)
    try {
      await logCrmAutoAssignRunBestEffort({
        phase: "finish",
        run_id,
        requester,
        mode,
        started_at: result.started_at,
        finished_at: result.finished_at,
        totals: result.totals,
        reps: result.reps,
      });
    } catch {}

    try {
      await notifyCrmAutoAssignRunSlack({
        run_id,
        requester,
        mode,
        totals: result.totals,
        reps: result.reps,
      });
    } catch {}

    const meta = {
      version: "v2" as const,
      total_ms: Date.now() - t0,
      time_budget_ms: TIME_BUDGET_MS,
      aborted_reason,
      timings: { per_rep: perRepTimings },
      caps: {
        contacts_per_rep: CONTACTS_PER_REP,
        max_total_contacts: MAX_TOTAL_CONTACTS,
      },
      preview: dry_run,
      generated_at: startedAt.toISOString(),
    };

    return res.json({ ...result, meta });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_rep_scope") || msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

// ------------------------------
// MANAGER RUN HISTORY
// GET /v1/crm/manager/auto-assign/runs
// ------------------------------
router.get("/manager/auto-assign/runs", async (req, res) => {
  try {
    const { requester } = await requireManagerOrg(req);

    const limit = Math.min(Math.max(Number(req.query.limit ?? 10), 1), 50);

    const isMissingTable = (msg: string) => {
      const m = msg.toLowerCase();
      return (
        (m.includes("relation") && m.includes("does not exist")) ||
        (m.includes("does not exist") && m.includes("crm_auto_assign_runs"))
      );
    };

    const { data, error } = await supa
      .from("crm_auto_assign_runs")
      .select(
        "run_id, mode, started_at, finished_at, totals, reps, source, is_preview, executed_from_preview_run_id, executed_by_user_id, executed_at"
      )
      .eq("user_id", requester)
      .order("started_at", { ascending: false })
      .limit(limit);

    if (error) {
      const msg = String((error as any)?.message ?? "");
      if (isMissingTable(msg)) return res.json({ ok: true, items: [], source: "missing_table" });
      return res.status(400).json({ ok: false, error: msg || "query_failed" });
    }

    const items = (data ?? [])
      .map((row: any) => ({
        run_id: String(row.run_id ?? "").trim(),
        mode: row.mode ?? null,
        source: row.source ?? null,
        is_preview: Boolean(row.is_preview ?? row.preview ?? false),
        executed_from_preview_run_id: row.executed_from_preview_run_id ?? null,
        executed_by_user_id: row.executed_by_user_id ?? null,
        executed_at: row.executed_at ?? null,
        started_at: row.started_at ?? null,
        finished_at: row.finished_at ?? null,
        totals: row.totals ?? null,
      }))
      .filter((x: any) => x.run_id);

    return res.json({ ok: true, items });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

// GET /v1/crm/manager/auto-assign/runs/:run_id
router.get("/manager/auto-assign/runs/:run_id", async (req, res) => {
  try {
    const { requester } = await requireManagerOrg(req);
    const runId = String(req.params.run_id ?? "").trim();
    if (!runId) return res.status(400).json({ ok: false, error: "invalid_run_id" });

    const isMissingTable = (msg: string) => {
      const m = msg.toLowerCase();
      return (
        (m.includes("relation") && m.includes("does not exist")) ||
        (m.includes("does not exist") && m.includes("crm_auto_assign_runs"))
      );
    };

    const { data, error } = await supa
      .from("crm_auto_assign_runs")
      .select(
        "run_id, mode, started_at, finished_at, totals, reps, source, is_preview, executed_from_preview_run_id, executed_by_user_id, executed_at"
      )
      .eq("user_id", requester)
      .eq("run_id", runId)
      .limit(1);

    if (error) {
      const msg = String((error as any)?.message ?? "");
      if (isMissingTable(msg)) return res.json({ ok: true, item: null, source: "missing_table" });
      return res.status(400).json({ ok: false, error: msg || "query_failed" });
    }

    const row = (data ?? [])[0] as any;
    if (!row) return res.json({ ok: true, item: null });

    return res.json({
      ok: true,
      item: {
        run_id: String(row.run_id ?? "").trim(),
        mode: row.mode ?? null,
        source: row.source ?? null,
        is_preview: Boolean(row.is_preview ?? row.preview ?? false),
        executed_from_preview_run_id: row.executed_from_preview_run_id ?? null,
        executed_by_user_id: row.executed_by_user_id ?? null,
        executed_at: row.executed_at ?? null,
        started_at: row.started_at ?? null,
        finished_at: row.finished_at ?? null,
        totals: row.totals ?? null,
        reps: row.reps ?? null,
      },
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

// GET /v1/crm/manager/auto-assign/runs/latest
router.get("/manager/auto-assign/runs/latest", async (req, res) => {
  try {
    const { requester } = await requireManagerOrg(req);

    const isMissingTable = (msg: string) => {
      const m = msg.toLowerCase();
      return (
        (m.includes("relation") && m.includes("does not exist")) ||
        (m.includes("does not exist") && m.includes("crm_auto_assign_runs"))
      );
    };

    const { data, error } = await supa
      .from("crm_auto_assign_runs")
      .select("run_id, mode, started_at, finished_at, totals, reps")
      .eq("user_id", requester)
      .order("started_at", { ascending: false })
      .limit(1);

    if (error) {
      const msg = String((error as any)?.message ?? "");
      if (isMissingTable(msg)) return res.json({ ok: true, item: null, source: "missing_table" });
      return res.status(400).json({ ok: false, error: msg || "query_failed" });
    }

    const row = (data ?? [])[0] as any;
    if (!row) return res.json({ ok: true, item: null });

    return res.json({
      ok: true,
      item: {
        run_id: String(row.run_id ?? "").trim(),
        mode: String(row.mode ?? "").trim(),
        started_at: row.started_at ?? null,
        finished_at: row.finished_at ?? null,
        totals: row.totals ?? null,
        reps: row.reps ?? null,
      },
    });
  } catch (e: any) {
    const msg = String(e?.message ?? "");
    if (msg.includes("Missing or invalid x-user-id") || msg.includes("Missing or invalid x-org-id")) {
      return res.status(401).json({ ok: false, error: msg });
    }
    if (msg.includes("forbidden_org_scope")) {
      return res.status(403).json({ ok: false, error: msg });
    }
    return res.status(500).json({ ok: false, error: msg || "server_error" });
  }
});

export default router;