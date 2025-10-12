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

/* ---------------------------------------------
   GET /v1/crm/contacts?query=&limit=
   - Search contacts by name/email (case-insensitive)
   - Returns minimal fields for the drawer
---------------------------------------------- */
router.get("/contacts", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const query = String(req.query.query ?? "").trim();
    const limit = Math.min(Math.max(Number(req.query.limit ?? 12), 1), 50);

    if (!query) return res.json({ ok: true, items: [] });

    // Assuming a 'contacts' table with:
    // id (uuid), first_name, last_name, email (unique per tenant), account_id (uuid, nullable), user_id (owner/tenant)
    // Optional: an 'accounts' table with id, name, domain.
    const { data: contacts, error } = await supa
      .from("contacts")
      .select("id, first_name, last_name, email, account_id")
      .eq("user_id", requester)
      .or([
        `first_name.ilike.%${query}%`,
        `last_name.ilike.%${query}%`,
        `email.ilike.%${query}%`,
      ].join(","))
      .order("first_name", { ascending: true })
      .limit(limit);

    if (error) throw error;
    const items = contacts ?? [];

    // get account names in one query if needed
    const accountIds = Array.from(
      new Set(items.map(x => x.account_id).filter(Boolean) as string[])
    );
    let accountMap: Record<string, { name: string | null; domain: string | null }> = {};
    if (accountIds.length) {
      const { data: accounts, error: accErr } = await supa
        .from("accounts")
        .select("id, name, domain")
        .in("id", accountIds);
      if (accErr) throw accErr;
      accountMap = Object.fromEntries((accounts ?? []).map(a => [a.id, { name: a.name ?? null, domain: a.domain ?? null }]));
    }

    const shaped = items.map(c => ({
      id: c.id,
      name: [c.first_name, c.last_name].filter(Boolean).join(" ").trim(),
      email: (c as any).email ?? null,
      company: c.account_id ? (accountMap[c.account_id]?.name ?? accountMap[c.account_id]?.domain ?? null) : null,
    }));

    res.json({ ok: true, items: shaped });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   POST /v1/crm/link-call
   Body: { callId: uuid, email: string }
   - Ensure the caller owns the call
   - Upsert contact by email (for this user)
   - Upsert account by email domain (optional)
   - Link call → contact (+account) in call_links
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
      .select("id, user_id, filename")
      .eq("id", callId)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "call_not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    const domain = email.split("@")[1]?.toLowerCase() ?? null;

    // 1) Upsert account by domain (optional; skip if you don't want auto-account)
    let accountId: string | null = null;
    if (domain) {
      // assumes 'accounts' has unique constraint on (user_id, domain) or (user_id, name)
      const { data: acc, error: accErr } = await supa
        .from("accounts")
        .upsert(
          { user_id: requester, name: null, domain },
          { onConflict: "user_id,domain" }
        )
        .select()
        .single();
      if (accErr) throw accErr;
      accountId = acc?.id ?? null;
    }

    // 2) Upsert contact by email for this user
    const { data: contact, error: contactErr } = await supa
      .from("contacts")
      .upsert(
        { user_id: requester, email, account_id: accountId },
        { onConflict: "user_id,email" }
      )
      .select()
      .single();

    if (contactErr) throw contactErr;

    // 3) Link the call → contact/account (assumes call_links table with unique (call_id))
    // call_links: id, call_id (unique), contact_id, account_id, created_at
    const linkRow: any = {
      call_id: callId,
      contact_id: contact.id,
      account_id: accountId,
    };

    const { data: link, error: linkErr } = await supa
      .from("call_links")
      .upsert(linkRow, { onConflict: "call_id" })
      .select()
      .single();

    if (linkErr) throw linkErr;

    res.json({ ok: true, link: { contact_id: contact.id, account_id: accountId } });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

/* ---------------------------------------------
   GET /v1/crm/calls/:id/link
   - Read linked contact/account for a call
---------------------------------------------- */
router.get("/calls/:id/link", async (req, res) => {
  try {
    const requester = getUserIdHeader(req);
    const id = String(req.params.id);
    if (!UUID_RE.test(id)) return res.status(400).json({ ok: false, error: "invalid id" });

    // verify call ownership
    const { data: call, error: callErr } = await supa
      .from("calls")
      .select("id, user_id")
      .eq("id", id)
      .single();

    if (callErr || !call) return res.status(404).json({ ok: false, error: "call_not_found" });
    if (call.user_id !== requester) return res.status(403).json({ ok: false, error: "forbidden" });

    // get link
    const { data: link, error: linkErr } = await supa
      .from("call_links")
      .select("contact_id, account_id")
      .eq("call_id", id)
      .single();

    if (linkErr || !link) return res.json({ ok: true, link: null });

    // fetch contact/account details (optional)
    const [contact, account] = await Promise.all([
      link.contact_id
        ? supa.from("contacts").select("id, first_name, last_name, email").eq("id", link.contact_id).single()
        : Promise.resolve({ data: null, error: null } as any),
      link.account_id
        ? supa.from("accounts").select("id, name, domain").eq("id", link.account_id).single()
        : Promise.resolve({ data: null, error: null } as any),
    ]);

    const shaped = {
      contact: contact?.data
        ? {
          id: contact.data.id,
          first_name: (contact.data as any).first_name ?? null,
          last_name: (contact.data as any).last_name ?? null,
          email: (contact.data as any).email ?? null,
        }
        : null,
      account: account?.data
        ? {
          id: account.data.id,
          name: (account.data as any).name ?? null,
          domain: (account.data as any).domain ?? null,
        }
        : null,
      opportunity: null, // reserved for future
    };

    res.json({ ok: true, link: shaped });
  } catch (e: any) {
    res.status(400).json({ ok: false, error: e.message ?? "bad_request" });
  }
});

export default router;