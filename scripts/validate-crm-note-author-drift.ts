/**
 * validate-crm-note-author-drift.ts
 *
 * Day 246 — locks in the crm.ts reps author-name fix.
 *
 * POST /v1/crm/contacts/:id/notes stamps the note with the author's name,
 * resolved from the reps table. That lookup selected `full_name` and
 * `user_id` and filtered on `user_id` — none of which exist (reps has
 * `name`, keyed by `id`). The select 42703'd, the whole try/catch threw,
 * and every note was authored with the "Rep" fallback instead of the real
 * person.
 *
 * Coverage:
 *   ✓ POST returns 200 and keeps its { ok, note } shape
 *   ✓ the note's author_name is the rep's real name, not "Rep"
 *   ✓ author_name is never a raw UUID
 *   ✓ a client-supplied author_name never overrides the resolved rep name
 *   ✓ when the author has no rep row, it falls back safely (no 500)
 *
 * Self-cleaning: creates its own org, rep and contact and removes them,
 * along with the notes it writes. Never touches the demo companies.
 *
 * Requirements: server running (npm run dev).
 * Usage: npm run validate:crm-note-author-drift
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const supa = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(name: string): string {
  const h = crypto.createHash("sha256").update(`DAY246::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function hit(method: string, path: string, userId: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json", "x-user-id": userId };
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(30_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

const ORG = uid("org");
const REP = uid("rep");
const NO_REP_USER = uid("no-rep-user");
const CONTACT = uid("contact");
const REP_NAME = "Day246 Author Rep";

async function seedFixtures() {
  const { error: orgErr } = await supa.from("orgs").upsert(
    [{ id: ORG, name: "Day246 Org (validator)" }], { onConflict: "id" }
  );
  if (orgErr) throw new Error(`fixture org upsert failed: ${orgErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [{ id: REP, name: REP_NAME, tier: "SalesRep", org_id: ORG }], { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture rep upsert failed: ${repErr.message}`);

  const { error: contactErr } = await supa.from("contacts").upsert(
    [{ id: CONTACT, org_id: ORG, first_name: "Day246", last_name: "Contact" }], { onConflict: "id" }
  );
  if (contactErr) throw new Error(`fixture contact upsert failed: ${contactErr.message}`);
}

async function cleanup() {
  await supa.from("crm_contact_notes").delete().eq("contact_id", CONTACT);
  await supa.from("contacts").delete().eq("id", CONTACT);
  await supa.from("reps").delete().eq("id", REP);
  await supa.from("orgs").delete().eq("id", ORG);
  console.log("\n  Cleanup: removed validator org, rep, contact and notes.");
}

const notePath = `/v1/crm/contacts/${CONTACT}/notes`;

async function main() {
  console.log("\nDay 246 — crm.ts reps author-name drift\n");

  await seedFixtures();

  // A client-supplied author_name is included to prove the resolved rep name
  // wins over it — the drift made the fallback take that value instead.
  const post = await hit("POST", notePath, REP, {
    body: "Day246 note body",
    author_name: "Client Supplied Name",
  });
  c("POST note returns 200", post.status === 200, `got ${post.status} ${JSON.stringify(post.data)?.slice(0, 140)}`);

  c("POST note keeps its { ok, note } shape",
    post.data?.ok === true && post.data?.note && typeof post.data.note === "object" && "author_name" in post.data.note,
    `got ${JSON.stringify(post.data)?.slice(0, 140)}`);

  const authorName = String(post.data?.note?.author_name ?? "");
  c("note author_name is the rep's real name (not the 'Rep' fallback)",
    authorName === REP_NAME, `got ${JSON.stringify(authorName)}`);

  c("note author_name is not a raw UUID",
    !UUID_RE.test(authorName), `got ${JSON.stringify(authorName)}`);

  c("resolved rep name wins over a client-supplied author_name",
    authorName !== "Client Supplied Name", `got ${JSON.stringify(authorName)}`);

  // Author with no rep row: must not 500, and must fall back rather than
  // resolve a name.
  const noRep = await hit("POST", notePath, NO_REP_USER, {
    body: "Day246 no-rep note",
    author_name: "Fallback Name",
  });
  c("author with no rep row still returns 200 (safe fallback)",
    noRep.status === 200, `got ${noRep.status} ${JSON.stringify(noRep.data)?.slice(0, 140)}`);

  c("no-rep author falls back to the supplied name, not a rep lookup",
    String(noRep.data?.note?.author_name ?? "") === "Fallback Name",
    `got ${JSON.stringify(noRep.data?.note?.author_name)}`);
}

main()
  .catch((e) => {
    console.error("\n  Validator crashed:", e?.message || e);
    c("validator ran to completion", false, String(e?.message || e));
  })
  .finally(async () => {
    await cleanup();
    const passed = checks.filter((x) => x.passed).length;
    console.log(`\n  ${passed}/${checks.length} checks passed`);
    console.log(
      passed === checks.length
        ? "  CRM note author drift validation PASSED\n"
        : "  CRM note author drift validation FAILED\n"
    );
    process.exit(passed === checks.length ? 0 : 1);
  });
