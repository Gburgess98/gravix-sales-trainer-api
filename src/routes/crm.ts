import { Router } from "express";
import { z } from "zod";
import { createClient } from "@supabase/supabase-js";

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
  contactId: string;
  type: string;
  title: string;
  due_at: string | null;
  importance: "normal" | "important" | "critical";
  meta: any;
}) {
  const { requester, contactId, type, title, due_at, importance, meta } = args;

  // Best-effort: if table/columns don't exist, do not block.
  // We also try to be compatible with older schemas by:
  // - including rep_id/manager_id when present
  // - stripping missing columns iteratively (not just once)
  // - falling back to a minimal payload if constraints bite
  try {
    const base: any = {
      user_id: requester,
      rep_id: requester,
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
        rep_id: requester,
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
    const requester = getUserIdHeader(req);

    const limit = Math.min(Math.max(Number(req.query.limit ?? 25), 1), 200);

    // Default single-rep fallback
    let reps: Array<{ rep_id: string; name: string | null }> = [
      { rep_id: requester, name: "You" },
    ];

    // Best-effort reps lookup
    try {
      const { data: repRows, error: repErr } = await supa
        .from("reps")
        .select("id, name, full_name, first_name, last_name, email, user_id")
        .or(`user_id.eq.${requester},id.eq.${requester}`)
        .limit(50);

      if (!repErr && repRows?.length) {
        reps = repRows.map((r: any) => ({
          rep_id: String(r.id ?? r.user_id ?? requester),
          name:
            r.full_name ||
            r.name ||
            [r.first_name, r.last_name].filter(Boolean).join(" ") ||
            r.email ||
            "Rep",
        }));
      }
    } catch {
      // ignore
    }

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
        rep_name: rep.name,
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
    return res
      .status(400)
      .json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// ------------------------------
// MANAGER RUNNER (v1)
// POST /v1/crm/manager/runner
// - Triggers auto-assign runs for reps or a single rep
// - Safe, best-effort, never blocks the UI
// Body: { rep_id?: string, dry_run?: boolean }
// ------------------------------
router.post("/manager/runner", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const body = req.body ?? {};
    const repId = typeof body.rep_id === "string" ? body.rep_id : null;
    const dryRun = Boolean(body.dry_run);

    // Resolve reps (single or all)
    let reps: Array<{ rep_id: string }> = [];

    if (repId) {
      reps = [{ rep_id: repId }];
    } else {
      try {
        const { data } = await supa
          .from("reps")
          .select("id, user_id")
          .or(`user_id.eq.${requester},id.eq.${requester}`)
          .limit(50);

        if (data?.length) {
          reps = data.map((r: any) => ({
            rep_id: String(r.id ?? r.user_id),
          }));
        }
      } catch {
        reps = [{ rep_id: requester }];
      }
    }

    const results: any[] = [];

    for (const rep of reps) {
      try {
        // Pull contacts for this rep (best-effort: user-scoped)
        const { data: contacts } = await supa
          .from("crm_contacts")
          .select("id")
          .eq("user_id", requester)
          .limit(200);

        let created = 0;
        let skipped = 0;

        for (const c of contacts ?? []) {
          if (dryRun) {
            skipped++;
            continue;
          }

          const r = await insertCrmActionBestEffort({
            requester,
            contactId: String(c.id),
            type: "follow_up",
            title: "Review contact & set next step",
            due_at: new Date(Date.now() + 2 * 86400000).toISOString(),
            importance: "normal",
            meta: { source: "manager_runner_v1" },
          });

          if (r.ok) created++;
          else skipped++;
        }

        results.push({
          rep_id: rep.rep_id,
          contacts_processed: contacts?.length ?? 0,
          created,
          skipped,
        });
      } catch (e: any) {
        results.push({
          rep_id: rep.rep_id,
          error: e?.message ?? "rep_runner_failed",
        });
      }
    }

    return res.json({
      ok: true,
      dry_run: dryRun,
      results,
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// ------------------------------
// MANAGER AUTO-ASSIGN RUNNER (v1)
// POST /v1/crm/manager/auto-assign/run
// Body: { dry_run?: boolean, limit?: number }
// - Runs the SAME core logic as /contacts/:id/auto-assign for the latest contacts
// ------------------------------
router.post("/manager/auto-assign/run", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const dry_run = Boolean((req.body as any)?.dry_run);

    const limitRaw = Number((req.body as any)?.limit ?? 10);
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 10;

    const { data: contacts, error: cErr } = await supa
      .from("crm_contacts")
      .select("id")
      .eq("user_id", requester)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (cErr) return res.status(400).json({ ok: false, error: cErr.message ?? "contacts_query_failed" });

    const results: any[] = [];
    const errors: any[] = [];

    for (const c of contacts ?? []) {
      const contactId = String((c as any).id ?? "").trim();
      if (!contactId) continue;

      try {
        const out = await runContactAutoAssignCore({ requester, contactId, dry_run });
        results.push(out);
      } catch (e: any) {
        errors.push({ contact_id: contactId, error: String(e?.message ?? "auto_assign_failed") });
      }
    }

    const created = results.filter((r) => r?.created === true).length;
    const duplicates = results.filter((r) => r?.duplicate === true).length;

    return res.json({
      ok: true,
      dry_run,
      limit,
      processed: results.length + errors.length,
      created,
      duplicates,
      results,
      errors,
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// GET /v1/crm/contacts/:id/actions
// Stable action stream for reps (preferred over assignments when schema drifts)
router.get("/contacts/:id/actions", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    // Demo contacts: empty
    if (contactId.startsWith("c_")) {
      return res.json({ ok: true, open: [], completed: [] });
    }

    const limit = Math.min(Math.max(Number(req.query.limit ?? 50), 1), 200);
    const r = await fetchCrmActionsBestEffort({ requester, contactId, limit });

    if (!r.ok) {
      // Fail-open: return empty rather than breaking reps.
      return res.json({ ok: true, open: [], completed: [], source: "crm_actions_missing_or_unavailable" });
    }

    return res.json({ ok: true, open: r.open, completed: r.completed, source: "crm_actions" });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// POST /v1/crm/actions/:id/complete
// Marks an action as completed (idempotent). Uses crm_actions (stable fallback table).
router.post("/actions/:id/complete", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String(req.params.id ?? "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "invalid_id" });

    // Load action (owned by requester)
    const { data: row, error: e1 } = await supa
      .from("crm_actions")
      .select("*")
      .eq("user_id", requester)
      .eq("id", id)
      .maybeSingle();

    if (e1) throw e1;
    if (!row) return res.status(404).json({ ok: false, error: "not_found" });

    // Idempotent: already completed
    if ((row as any).completed_at || String((row as any).status ?? "").toLowerCase() === "done") {
      return res.json({ ok: true, action: row, already_completed: true });
    }

    // Best-effort update: set completed_at + status (if columns exist)
    const fullPatch: any = {
      completed_at: new Date().toISOString(),
      status: "done",
    };

    const tryUpdate = async (patch: any) => {
      return await supa
        .from("crm_actions")
        .update(patch)
        .eq("user_id", requester)
        .eq("id", id)
        .select("*")
        .maybeSingle();
    };

    let updated: any = null;
    let lastErr: any = null;

    // Try up to 5 times, stripping missing columns.
    let patch: any = { ...fullPatch };
    for (let i = 0; i < 5; i++) {
      const r2 = await tryUpdate(patch);
      if (!r2.error) {
        updated = r2.data ?? null;
        lastErr = null;
        break;
      }

      lastErr = r2.error;
      const msg = String((r2.error as any)?.message ?? "");
      const lower = msg.toLowerCase();

      const m1 = lower.match(/column crm_actions\.([a-zA-Z0-9_]+) does not exist/);
      const m2 = lower.match(/could not find the '([^']+)' column/);
      const missingCol = (m1?.[1] ?? m2?.[1]) as string | undefined;

      if (missingCol && missingCol in patch) {
        delete patch[missingCol];
        continue;
      }

      // Unknown error → stop.
      break;
    }

    if (lastErr) throw lastErr;

    // If select is blocked by RLS/schema, fall back to re-fetch
    if (!updated) {
      const r3 = await supa
        .from("crm_actions")
        .select("*")
        .eq("user_id", requester)
        .eq("id", id)
        .maybeSingle();
      if (r3.error) throw r3.error;
      updated = r3.data ?? null;
    }

    return res.json({ ok: true, action: updated, already_completed: false });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// ------------------- Auto-assign helpers -------------------
async function computeContactHealthFor(args: {
  requester: string;
  contactId: string;
}): Promise<{
  contact_id: string;
  status: ContactHealthStatus;
  score: number;
  reasons: string[];
  next_action: string;
  signals: {
    last_contacted_at: string | null;
    last_contacted_days: number | null;
    overdue_assignments: number;
    critical_notes: number;
    important_notes: number;
    last_call_score: number | null;
  };
}> {
  const { requester, contactId } = args;

  let contact: any = null;

  if (contactId.startsWith("c_")) {
    const found = DEMO_CONTACTS.find((c) => c.id === contactId);
    if (!found) throw new Error("not_found");
    contact = { id: found.id, last_contacted_at: found.last_contacted_at ?? null };
  } else {
    if (!UUID_RE.test(contactId)) throw new Error("invalid_id");
    const { data: c, error: cErr } = await supa
      .from("crm_contacts")
      .select("id, last_contacted_at")
      .eq("user_id", requester)
      .eq("id", contactId)
      .single();
    if (cErr || !c) throw new Error("not_found");
    contact = c;
  }

  const { data: notes } = await supa
    .from("crm_contact_notes")
    .select("importance")
    .eq("user_id", requester)
    .eq("contact_id", contactId);

  const criticalNotesCount = (notes ?? []).filter((n: any) => n.importance === "critical").length;
  const importantNotesCount = (notes ?? []).filter((n: any) => n.importance === "important").length;

  let overdueCount = 0;
  try {
    const { open } = await fetchContactAssignmentsBestEffort({ requester, contactId, limit: 200 });
    overdueCount = open.filter((x: any) => x.is_overdue).length;
  } catch {
    overdueCount = 0;
  }

  let lastCallScore: number | null = null;
  try {
    const { data: links } = await supa
      .from("crm_call_links")
      .select("call_id")
      .eq("contact_id", contactId)
      .limit(25);

    const callIds = (links ?? []).map((r: any) => r.call_id).filter(Boolean);
    if (callIds.length) {
      const { data: scores } = await supa
        .from("call_scores")
        .select("total_score")
        .eq("user_id", requester)
        .in("call_id", callIds)
        .order("created_at", { ascending: false })
        .limit(1);
      if (scores?.length) lastCallScore = Number((scores[0] as any).total_score);
    }
  } catch {
    lastCallScore = null;
  }

  const lastContactedDays = daysSince(contact?.last_contacted_at ?? null);

  const result = classifyHealth({
    lastContactedDays,
    overdueCount,
    criticalNotesCount,
    importantNotesCount,
    lastCallScore,
  });

  return {
    contact_id: contactId,
    status: result.status,
    score: result.score,
    reasons: result.reasons,
    next_action: result.next_action,
    signals: {
      last_contacted_at: contact?.last_contacted_at ?? null,
      last_contacted_days: lastContactedDays,
      overdue_assignments: overdueCount,
      critical_notes: criticalNotesCount,
      important_notes: importantNotesCount,
      last_call_score: lastCallScore,
    },
  };
}

function buildAutoAssignSuggestion(h: {
  status: ContactHealthStatus;
  score: number;
  reasons: string[];
  next_action: string;
}): { type: string; title: string; due_at: string; importance: "normal" | "important" | "critical"; meta: any } {
  const now = Date.now();

  // Default: 2-day due
  let dueDays = 2;
  let importance: "normal" | "important" | "critical" = "normal";

  if (h.status === "hot") {
    dueDays = 1;
    importance = "important";
  } else if (h.status === "warm") {
    dueDays = 2;
    importance = "normal";
  } else if (h.status === "stale") {
    dueDays = 0;
    importance = "critical";
  } else {
    // cold
    dueDays = 3;
    importance = "normal";
  }

  // If critical notes mentioned, bump importance
  if (h.reasons.some((r) => String(r).toLowerCase().includes("critical"))) {
    importance = "critical";
    dueDays = 0;
  }

  const due_at = new Date(now + dueDays * 24 * 60 * 60 * 1000).toISOString();

  const title = h.next_action;

  return {
    // IMPORTANT: `assignments.type` is constrained by `assignments_type_check`.
    // Use a safe, common value and keep intent in meta.
    type: "follow_up",
    title,
    due_at,
    importance,
    meta: {
      source: "health_auto_assign_v1",
      score: h.score,
      status: h.status,
      reasons: h.reasons,
      intent: "next_action",
    },
  };
}

async function insertAssignmentBestEffort(args: {
  requester: string;
  contactId: string;
  type: string;
  title: string;
  due_at: string;
  importance: "normal" | "important" | "critical";
  meta: any;
}) {
  const { requester, contactId, type, title, due_at, importance, meta } = args;

  // We have seen multiple schema variants across projects:
  // - assignments(contact_id, ...)
  // - assignments(target_type, target_id, ...)
  // - assignments(meta json with contact_id)
  // and sometimes the table is `coach_assignments`.
  // This function tries a small set of payload shapes in order.

  // IMPORTANT: `coach_assignments` in this repo is used for call coaching and can require
  // non-null fields like `call_id`. Auto-assign is contact-driven and must not write there.
  // So we only write to `assignments` here.
  const tables = ["assignments"];

  const baseMeta = meta ?? {};

  const payloads: any[] = [
    // Variant A: direct contact_id + title
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      contact_id: contactId,
      type,
      title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant A0: direct contact_id + assignment_type + title (some schemas use assignment_type)
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      contact_id: contactId,
      assignment_type: type,
      title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant A0b: direct contact_id + assignment_type + label
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      contact_id: contactId,
      assignment_type: type,
      label: title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant A2: direct contact_id + label (some schemas use label instead of title)
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      contact_id: contactId,
      type,
      label: title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant B: target_type/target_id + title
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      target_type: "contact",
      target_id: contactId,
      type,
      title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant B2: target_type/target_id + label
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      target_type: "contact",
      target_id: contactId,
      type,
      label: title,
      due_at,
      completed_at: null,
      importance,
      meta: baseMeta,
    },
    // Variant C: meta-only link (contact_id inside meta) + title
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      type,
      title,
      due_at,
      completed_at: null,
      importance,
      meta: { ...baseMeta, contact_id: contactId },
    },
    // Variant C2: meta-only link + label
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      type,
      label: title,
      due_at,
      completed_at: null,
      importance,
      meta: { ...baseMeta, contact_id: contactId },
    },
    // Variant D: minimal (in case importance/title/meta columns are missing)
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      type,
      due_at,
      completed_at: null,
      meta: { contact_id: contactId, title },
    },
    // Variant E: coach_assignments minimal (no title/label/meta/contact fields)
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      assignment_type: type,
      due_at,
      completed_at: null,
    },
    // Variant E2: ultra-minimal
    {
      user_id: requester,
      rep_id: requester,
      manager_id: requester,
      assignment_type: type,
    },
  ];

  let lastErr: any = null;

  for (const table of tables) {
    for (const p0 of payloads) {
      // Attempt the payload, and if a column is missing, strip it and retry a couple times.
      let p: any = { ...p0 };

      // IMPORTANT: some deployments use `coach_assignments` with a smaller column set.
      // We normalise early AND avoid sending columns that often don't exist (e.g. meta/title/type).
      const allowMeta = table !== "coach_assignments";

      for (let i = 0; i < 3; i++) {
        const { error } = await supa.from(table).insert(p);
        if (!error) return;

        const msg = String((error as any)?.message ?? "").toLowerCase();
        lastErr = error;

        // If we hit the `assignments_type_check`, fall back to other allowed-ish types.
        // We don't know the exact enum across deployments, so try a small safe set.
        if (table === "assignments" && msg.includes("violates check constraint") && msg.includes("assignments_type_check")) {
          const fallbacks = ["follow_up", "task", "todo", "action", "crm_follow_up"];
          const cur = String((p as any).type ?? (p as any).assignment_type ?? "");
          for (const t of fallbacks) {
            if (!t || t === cur) continue;
            const p2: any = { ...p };
            if ("type" in p2) p2.type = t;
            if ("assignment_type" in p2) p2.assignment_type = t;
            if (p2.meta && typeof p2.meta === "object") p2.meta = { ...p2.meta, type_fallback_from: cur, type_fallback_to: t };
            const { error: e2 } = await supa.from(table).insert(p2);
            if (!e2) return;
          }
        }

        // Table doesn't exist? break to next table.
        if (msg.includes("does not exist") || (msg.includes("relation") && msg.includes("does not exist"))) {
          break;
        }

        // Supabase schema cache missing column errors look like:
        // "Could not find the 'contact_id' column of 'assignments' in the schema cache"
        // We parse the column name and drop it.
        const m = msg.match(/could not find the '([^']+)' column/);
        const missingCol = m?.[1];
        if (missingCol && missingCol in p) {
          // If the schema uses label instead of title (or vice-versa), map across before dropping.
          if (missingCol === "title" && (p as any).label == null && typeof title === "string") {
            (p as any).label = title;
          }
          if (missingCol === "label" && (p as any).title == null && typeof title === "string") {
            (p as any).title = title;
          }

          // Some schemas use assignment_type instead of type.
          if (missingCol === "type") {
            if (table === "coach_assignments") {
              // Do not attempt to map to assignment_type (it may not exist).
              delete (p as any).type;
              continue;
            }

            if ((p as any).assignment_type == null) {
              (p as any).assignment_type = type;
            }

            if (allowMeta) {
              if ((p as any).meta && typeof (p as any).meta === "object") {
                (p as any).meta = { ...(p as any).meta, type };
              } else {
                (p as any).meta = { type };
              }
            }

            delete (p as any).type;
            continue;
          }
          if (missingCol === "assignment_type") {
            // For coach_assignments, we keep ultra-minimal and avoid adding type back in.
            if (table !== "coach_assignments") {
              if ((p as any).type == null) (p as any).type = type;
            }
          }

          // Special-case: handle rep_id column missing
          if (missingCol === "rep_id") {
            delete (p as any).rep_id;
            continue;
          }
          // Special-case: handle manager_id column missing
          if (missingCol === "manager_id") {
            delete (p as any).manager_id;
            continue;
          }

          delete (p as any)[missingCol];
          continue;
        }

        // PostgREST column missing variants
        if (msg.includes("column") && msg.includes("does not exist")) {
          // Best effort: drop common optional fields when any column error occurs.
          delete p.contact_id;
          delete p.target_type;
          delete p.target_id;
          delete p.type;
          delete p.rep_id;
          delete p.manager_id;

          if (allowMeta) {
            if (p.meta && typeof p.meta === "object") {
              p.meta = { ...(p.meta ?? {}), contact_id: contactId };
            } else {
              p.meta = { contact_id: contactId };
            }
          } else {
            delete p.meta;
          }

          delete p.importance;
          continue;
        }

        // Any other error: stop retrying this payload.
        break;
      }
    }
  }

  throw lastErr ?? new Error("assignment_insert_failed");
}

// ------------------- Contact Health Route -------------------
router.get("/contacts/:id/health", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    let contact: any = null;

    if (contactId.startsWith("c_")) {
      const found = DEMO_CONTACTS.find((c) => c.id === contactId);
      if (!found) return res.status(404).json({ ok: false, error: "not_found" });
      contact = { id: found.id, last_contacted_at: found.last_contacted_at ?? null };
    } else {
      if (!UUID_RE.test(contactId)) return res.status(400).json({ ok: false, error: "invalid_id" });
      const { data: c, error: cErr } = await supa
        .from("crm_contacts")
        .select("id, last_contacted_at")
        .eq("user_id", requester)
        .eq("id", contactId)
        .single();
      if (cErr || !c) return res.status(404).json({ ok: false, error: "not_found" });
      contact = c;
    }

    const { data: notes } = await supa
      .from("crm_contact_notes")
      .select("importance")
      .eq("user_id", requester)
      .eq("contact_id", contactId);

    const criticalNotesCount = (notes ?? []).filter((n: any) => n.importance === "critical").length;
    const importantNotesCount = (notes ?? []).filter((n: any) => n.importance === "important").length;

    let overdueCount = 0;
    try {
      const { open } = await fetchContactAssignmentsBestEffort({ requester, contactId, limit: 200 });
      overdueCount = open.filter((x: any) => x.is_overdue).length;
    } catch { }

    let lastCallScore: number | null = null;
    try {
      const { data: links } = await supa
        .from("crm_call_links")
        .select("call_id")
        .eq("contact_id", contactId)
        .limit(25);

      const callIds = (links ?? []).map((r: any) => r.call_id).filter(Boolean);
      if (callIds.length) {
        const { data: scores } = await supa
          .from("call_scores")
          .select("total_score")
          .eq("user_id", requester)
          .in("call_id", callIds)
          .order("created_at", { ascending: false })
          .limit(1);
        if (scores?.length) lastCallScore = Number((scores[0] as any).total_score);
      }
    } catch { }

    const lastContactedDays = daysSince(contact?.last_contacted_at ?? null);

    const result = classifyHealth({
      lastContactedDays,
      overdueCount,
      criticalNotesCount,
      importantNotesCount,
      lastCallScore,
    });

    return res.json({
      ok: true,
      health: {
        contact_id: contactId,
        status: result.status,
        score: result.score,
        reasons: result.reasons,
        next_action: result.next_action,
        signals: {
          last_contacted_at: contact?.last_contacted_at ?? null,
          last_contacted_days: lastContactedDays,
          overdue_assignments: overdueCount,
          critical_notes: criticalNotesCount,
          important_notes: importantNotesCount,
          last_call_score: lastCallScore,
        },
      },
    });
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

// POST /v1/crm/contacts/:id/auto-assign (core)
// Body: { dry_run?: boolean }
// - Uses Contact Health to suggest a single "next_action" action.
// - Dedupe within 24h to avoid spam.
// - Writes to assignments if possible; always falls back to crm_actions; final fallback is an AUTO note.
async function runContactAutoAssignCore(args: {
  requester: string;
  contactId: string;
  dry_run: boolean;
}) {
  const { requester, contactId, dry_run } = args;

  // Compute health (shared logic)
  const health = await computeContactHealthFor({ requester, contactId });
  const suggestion = buildAutoAssignSuggestion({
    status: health.status,
    score: health.score,
    reasons: health.reasons,
    next_action: health.next_action,
  });

  // ---------------------------------------------------------
  // DEDUPE: prevent spamming auto-assign creations (24h window)
  // ---------------------------------------------------------
  const sinceIso = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  // 1) Notes dedupe (reliable table)
  try {
    const { data: recentNotes, error: nErr } = await supa
      .from("crm_contact_notes")
      .select("id, created_at, body")
      .eq("user_id", requester)
      .eq("contact_id", contactId)
      .gte("created_at", sinceIso)
      .order("created_at", { ascending: false })
      .limit(25);

    if (!nErr && (recentNotes?.length ?? 0) > 0) {
      const hit = (recentNotes ?? []).find((n: any) => {
        const body = String(n?.body ?? "");
        return body.includes("[AUTO]") && body.includes("health_auto_assign_v1");
      });

      if (hit) {
        return {
          ok: true,
          dry_run: false,
          contact_id: contactId,
          health,
          created: false,
          duplicate: true,
          created_via: "dedupe_recent_auto_note",
        };
      }
    }
  } catch {
    // fail-open
  }

  // 1b) CRM Actions dedupe (stable fallback table)
  try {
    const r = await fetchCrmActionsBestEffort({ requester, contactId, limit: 50 });
    if (r.ok) {
      const dup = (r.open ?? []).some((a: any) => {
        const ca = a?.created_at ? new Date(String(a.created_at)).toISOString() : null;
        const isRecent = ca ? ca >= sinceIso : false;
        const meta = a?.meta ?? {};
        return (
          isRecent &&
          (String(a?.type ?? "").includes("health_auto_assign_v1") ||
            String(meta?.source ?? "").includes("health_auto_assign_v1"))
        );
      });

      if (dup) {
        return {
          ok: true,
          dry_run: false,
          contact_id: contactId,
          health,
          created: false,
          duplicate: true,
          created_via: "dedupe_recent_crm_action",
        };
      }
    }
  } catch {
    // ignore
  }

  // 2) Assignments dedupe (best-effort)
  try {
    const { open } = await fetchContactAssignmentsBestEffort({
      requester,
      contactId,
      limit: 50,
    });

    const dup = (open ?? []).some((a: any) => {
      const ca = a?.created_at ? new Date(String(a.created_at)).toISOString() : null;
      const isRecent = ca ? ca >= sinceIso : false;
      const meta = a?.meta ?? {};
      return isRecent && String(meta?.source ?? "").includes("health_auto_assign_v1");
    });

    if (dup) {
      return {
        ok: true,
        dry_run: false,
        contact_id: contactId,
        health,
        created: false,
        duplicate: true,
        created_via: "dedupe_recent_assignment",
      };
    }
  } catch {
    // ignore
  }

  if (dry_run) {
    return { ok: true, dry_run: true, contact_id: contactId, health, suggestion };
  }

  // Insert assignment (best-effort across table variants)
  try {
    await insertAssignmentBestEffort({
      requester,
      contactId,
      type: suggestion.type,
      title: suggestion.title,
      due_at: suggestion.due_at,
      importance: suggestion.importance,
      meta: suggestion.meta,
    });

    // ALSO write to CRM Actions best-effort so reps always see it in a stable list.
    // This must never block success.
    await insertCrmActionBestEffort({
      requester,
      contactId,
      type: suggestion.type,
      title: suggestion.title,
      due_at: suggestion.due_at,
      importance: suggestion.importance,
      meta: suggestion.meta,
    });

    return {
      ok: true,
      dry_run: false,
      contact_id: contactId,
      health,
      created: true,
      created_via: "assignments",
    };
  } catch {
    // If assignments insert fails for ANY reason, fall back to CRM Actions.
    const r = await insertCrmActionBestEffort({
      requester,
      contactId,
      type: suggestion.type,
      title: suggestion.title,
      due_at: suggestion.due_at,
      importance: suggestion.importance,
      meta: suggestion.meta,
    });

    if (r.ok) {
      return {
        ok: true,
        dry_run: false,
        contact_id: contactId,
        health,
        created: true,
        created_via: "crm_actions_fallback",
      };
    }

    // Final fallback: write an AUTO note so the endpoint never blocks.
    const importance =
      suggestion.importance === "critical"
        ? "critical"
        : suggestion.importance === "important"
          ? "important"
          : "normal";

    const noteBody =
      `[AUTO] Next action: ${suggestion.title}\n` +
      `Due: ${suggestion.due_at}\n` +
      `Health: ${health.status} (${health.score})\n` +
      `Reasons: ${(health.reasons ?? []).join("; ")}`;

    const { error: nErr } = await supa.from("crm_contact_notes").insert({
      user_id: requester,
      contact_id: contactId,
      body: noteBody,
      author_id: requester,
      author_name: "System",
      importance,
    });

    if (nErr) {
      throw new Error(`assignment_blocked_and_fallback_failed: ${nErr.message}`);
    }

    return {
      ok: true,
      dry_run: false,
      contact_id: contactId,
      health,
      created: true,
      created_via: "crm_contact_notes_fallback",
    };
  }
}

// ------------------- Contact Auto-Assign Route -------------------
// POST /v1/crm/contacts/:id/auto-assign
// Body: { dry_run?: boolean }
// - Uses Contact Health to suggest a single "next_action" assignment.
// - If dry_run=true, returns suggestion only.
router.post("/contacts/:id/auto-assign", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const contactId = String(req.params.id ?? "").trim();
    if (!contactId) return res.status(400).json({ ok: false, error: "invalid_contact_id" });

    const dry_run = Boolean((req.body as any)?.dry_run);

    const out = await runContactAutoAssignCore({ requester, contactId, dry_run });
    return res.json(out);
  } catch (e: any) {
    return res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

export default router;