// Coach-assignment object-level authorisation guard (network-free, no DB, no LLM).
//
// Proves, against an in-memory store that stands in for coach_assignments + reps:
//   DELETE  1 manager deletes an assignment for a rep they manage          5 malformed / missing / unknown ids fail safely
//           2 rep cannot delete their own assignment                       6 spoofed x-org-id (HTTP level) does not change scope
//           3 rep cannot delete another rep's assignment                   7 existing authorised manager behaviour (response shape) still works
//           4 manager cannot delete outside their tenant                   8 the row remains after EVERY denied attempt
//   PATCH   assignee / in-scope manager may complete+reopen; strangers, other tenants, unknown ids may not
//   CREATE  POST /v1/coach/assign refuses an assignee outside the caller's scope
//   WIRING  server.ts routes call the guarded handlers and never touch coach_assignments by bare id
//   NON-VACUITY  the same scenarios run against the OLD behaviour (identity-only delete) must FAIL
//
// Run: npm run validate:coach-assignment-authz   (tsx)

import * as fs from "node:fs";
import * as path from "node:path";
import http from "node:http";
import express from "express";
import type { AssignmentRow, CoachDeps as Deps, NoteRow } from "../src/lib/coachAssignmentAccess";
import type { RepLookups, RepScope } from "../src/lib/repAccess";

// repAccess builds a Supabase client at import time. Dummy, unroutable values: this guard never opens a connection
// (it swaps in an in-memory store), and it must never read a real .env.
process.env.SUPABASE_URL = "http://127.0.0.1:9";
process.env.SUPABASE_SERVICE_ROLE_KEY = "validator-dummy-key";
let deleteCoachAssignment: typeof import("../src/lib/coachAssignmentAccess").deleteCoachAssignment;
let updateCoachAssignmentStatus: typeof import("../src/lib/coachAssignmentAccess").updateCoachAssignmentStatus;
let canAssignTo: typeof import("../src/lib/coachAssignmentAccess").canAssignTo;
let listCoachAssignments: typeof import("../src/lib/coachAssignmentAccess").listCoachAssignments;
let listAssignmentsByEntity: typeof import("../src/lib/coachAssignmentAccess").listAssignmentsByEntity;
let getCoachNote: typeof import("../src/lib/coachAssignmentAccess").getCoachNote;
let saveCoachNote: typeof import("../src/lib/coachAssignmentAccess").saveCoachNote;

let failures = 0;
const check = (label: string, ok: boolean, detail = "") => {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
};

// ── fixture ────────────────────────────────────────────────────────────────────────────────────────────────
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const O_A = id(9001), O_B = id(9002), C_A = id(9101), C_B = id(9102), C_P = id(9103);
const REPS: RepScope[] = [
  { id: id(1), tier: "Manager", company_id: C_A, org_id: O_A }, // managerA
  { id: id(2), tier: "SalesRep", company_id: C_A, org_id: O_A }, // repA1
  { id: id(3), tier: "SalesRep", company_id: C_A, org_id: O_A }, // repA2
  { id: id(4), tier: "Manager", company_id: C_B, org_id: O_B }, // managerB
  { id: id(5), tier: "SalesRep", company_id: C_B, org_id: O_B }, // repB1
  { id: id(6), tier: "PartnerAdmin", company_id: C_P, org_id: O_A }, // partner admin whose tenant key is another company
  { id: id(7), tier: "Owner", company_id: C_A, org_id: O_A }, // ownerA
  { id: id(8), tier: "Manager", company_id: null, org_id: null }, // manager with no tenant at all
];
const [MA, RA1, RA2, MB, RB1, PA, OWN, MNONE] = REPS.map((r) => r.id);
const A_RA1 = id(101), A_RA2 = id(102), A_RB1 = id(103), A_NULL = id(104), A_MISMATCH = id(105);
const seed = (): AssignmentRow[] => [
  { id: A_RA1, assignee_user_id: RA1, org_id: O_A, call_id: id(201), drill_id: "d", status: "open" },
  { id: A_RA2, assignee_user_id: RA2, org_id: O_A, call_id: id(202), drill_id: "d", status: "open" },
  { id: A_RB1, assignee_user_id: RB1, org_id: O_B, call_id: id(203), drill_id: "d", status: "open" },
  { id: A_NULL, assignee_user_id: null, org_id: O_A, call_id: id(204), drill_id: "d", status: "open" },
  { id: A_MISMATCH, assignee_user_id: RA1, org_id: O_B, call_id: id(205), drill_id: "d", status: "open" },
];

// Calls: account X is deliberately shared across both tenants (an id an attacker can name); contacts are per call.
const ACC_X = id(301), K1 = id(311), K3 = id(313);
const CALLS = [
  { id: id(201), user_id: RA1, org_id: O_A, filename: "ra1-call.mp3", account_id: ACC_X, contact_id: K1 },
  { id: id(202), user_id: RA2, org_id: O_A, filename: "ra2-call.mp3", account_id: ACC_X, contact_id: id(312) },
  { id: id(203), user_id: RB1, org_id: O_B, filename: "rb1-secret-call.mp3", account_id: ACC_X, contact_id: K3 },
  { id: id(204), user_id: RA1, org_id: O_A, filename: "ra1-other.mp3", account_id: null, contact_id: null },
  { id: id(205), user_id: RA1, org_id: O_A, filename: "ra1-x.mp3", account_id: null, contact_id: null },
];
const seedNotes = (): NoteRow[] => [
  { id: id(401), call_id: id(201), notes: "ra1 private coaching note", updated_at: "2026-01-01T00:00:00Z" },
  { id: id(403), call_id: id(203), notes: "tenant B note", updated_at: "2026-01-01T00:00:00Z" },
];

function world() {
  const rows = new Map(seed().map((r) => [r.id, r]));
  const notes = new Map(seedNotes().map((n) => [n.call_id, n]));
  const lookups: RepLookups = {
    getRep: async (i) => REPS.find((r) => r.id === i) ?? null,
    listTenantRepIds: async (s) => REPS.filter((r) => (s.company_id ? r.company_id === s.company_id : r.org_id === s.org_id)).map((r) => r.id),
  };
  const deps: Deps = {
    lookups,
    store: {
      getAssignment: async (i) => rows.get(i) ?? null,
      deleteAssignment: async (i) => void rows.delete(i),
      updateAssignment: async (i, patch) => {
        const r = rows.get(i);
        if (!r) return null;
        Object.assign(r, patch);
        return r;
      },
      listAssignments: async ({ callIds, assignee, status, max }) =>
        [...rows.values()].filter((r) => (!callIds || callIds.includes(String(r.call_id))) && (!assignee || r.assignee_user_id === assignee) && (!status || r.status === status)).slice(0, max),
      callIdsForEntity: async ({ accountId, contactId }) => CALLS.filter((c) => (accountId ? c.account_id === accountId : c.contact_id === contactId)).map((c) => c.id),
      getCalls: async (ids) => CALLS.filter((c) => ids.includes(c.id)),
      getNote: async (cid) => notes.get(cid) ?? null,
      upsertNote: async (cid, text) => {
        const n: NoteRow = { id: notes.get(cid)?.id ?? id(900 + notes.size), call_id: cid, notes: text, updated_at: "now" };
        notes.set(cid, n);
        return n;
      },
    },
  };
  return { rows, deps, lookups, notes };
}
const snapshot = (rows: Map<string, AssignmentRow>) => JSON.stringify([...rows.entries()].sort());

type Del = (requester: string, rawId: unknown) => Promise<{ status: number; body: any }>;

// Scenarios are written once and run against the real handler and against the pre-fix behaviour.
async function deleteScenarios(mk: () => { rows: Map<string, AssignmentRow>; del: Del }) {
  const out: Record<string, boolean> = {};
  const denied = async (name: string, requester: string, rawId: unknown, want: number[]) => {
    const w = mk();
    const before = snapshot(w.rows);
    const r = await w.del(requester, rawId);
    out[name] = want.includes(r.status) && r.body?.ok === false && snapshot(w.rows) === before; // denied AND row untouched
  };
  {
    const w = mk();
    const r = await w.del(MA, A_RA1);
    out["1 manager deletes an assignment for a rep they manage"] = r.status === 200 && r.body?.ok === true && !w.rows.has(A_RA1) && w.rows.has(A_RA2) && w.rows.has(A_RB1);
  }
  {
    const w = mk();
    const r = await w.del(OWN, A_RA2);
    out["7 owner tier in the same tenant still deletes (existing authorised behaviour, same response shape)"] = r.status === 200 && JSON.stringify(r.body) === '{"ok":true}' && !w.rows.has(A_RA2);
  }
  await denied("2 rep cannot delete their own assignment", RA1, A_RA1, [403]);
  await denied("3 rep cannot delete another rep's assignment (same tenant)", RA2, A_RA1, [403]);
  await denied("3b rep cannot delete another tenant's assignment", RA1, A_RB1, [403]);
  await denied("4 manager cannot delete another tenant's assignment", MA, A_RB1, [404]);
  await denied("4b other-tenant manager cannot delete tenant A's assignment", MB, A_RA1, [404]);
  await denied("4c partner admin with a different tenant key cannot delete", PA, A_RA1, [404]);
  await denied("4d manager with no tenant cannot delete anything", MNONE, A_RA1, [403, 404]);
  await denied("4e assignment whose org_id disagrees with the manager's org is refused", MA, A_MISMATCH, [404]);
  await denied("4f assignee-less assignment is only reachable inside its own org (other org manager refused)", MB, A_NULL, [404]);
  await denied("5a malformed id", MA, "not-a-uuid", [400]);
  await denied("5b SQL-ish / wildcard id", MA, "' or 1=1 --", [400]);
  await denied("5c missing id", MA, undefined, [400]);
  await denied("5d unknown (well-formed) id", MA, id(999), [404]);
  await denied("5e unknown caller (no reps row)", id(777), A_RA1, [403]);
  await denied("5f no caller identity", "", A_RA1, [401]);
  return out;
}

async function main() {
  ({ deleteCoachAssignment, updateCoachAssignmentStatus, canAssignTo, listCoachAssignments, listAssignmentsByEntity, getCoachNote, saveCoachNote } = await import("../src/lib/coachAssignmentAccess"));
  console.log("Coach-assignment authorisation guard (network-free)\n");

  // ── real handler ────────────────────────────────────────────────────────────────────────────────────────
  const real = await deleteScenarios(() => {
    const w = world();
    return { rows: w.rows, del: (u, i) => deleteCoachAssignment(w.deps, u, i) };
  });
  for (const [k, v] of Object.entries(real)) check(`DELETE ${k}`, v);

  // 5 no information leak: unknown vs out-of-scope are byte-identical for a manager
  {
    const w = world();
    const unknown = await deleteCoachAssignment(w.deps, MA, id(999));
    const foreign = await deleteCoachAssignment(w.deps, MA, A_RB1);
    check("DELETE 5g unknown and out-of-scope ids answer identically (no existence oracle)", JSON.stringify(unknown) === JSON.stringify(foreign) && unknown.status === 404);
    const repForeign = await deleteCoachAssignment(w.deps, RA1, A_RB1);
    const repUnknown = await deleteCoachAssignment(w.deps, RA1, id(999));
    check("DELETE 5h a rep gets the same answer for existing and unknown ids", JSON.stringify(repForeign) === JSON.stringify(repUnknown));
  }
  // database errors do not leak raw messages
  {
    const w = world();
    const broken: Deps = { ...w.deps, store: { ...w.deps.store, getAssignment: async () => { throw new Error('relation "coach_assignments" secret detail'); } } };
    const r = await deleteCoachAssignment(broken, MA, A_RA1);
    check("DELETE 5i a database error is a calm 500 with no raw database text", r.status === 500 && !JSON.stringify(r.body).includes("secret detail"));
  }

  // ── PATCH ───────────────────────────────────────────────────────────────────────────────────────────────
  {
    const w = world();
    const own = await updateCoachAssignmentStatus(w.deps, RA1, A_RA1, "completed");
    check("PATCH assignee can complete their own assignment (shape kept: ok + assignment)", own.status === 200 && (own.body as any).assignment?.status === "completed" && !!(own.body as any).assignment?.completed_at);
    const reopen = await updateCoachAssignmentStatus(w.deps, RA1, A_RA1, "open");
    check("PATCH assignee can reopen (completed_at cleared)", reopen.status === 200 && (reopen.body as any).assignment?.completed_at === null);
    const mgr = await updateCoachAssignmentStatus(w.deps, MA, A_RA2, "completed");
    check("PATCH manager completes an in-tenant rep's assignment", mgr.status === 200 && w.rows.get(A_RA2)!.status === "completed");
  }
  {
    const cases: [string, string, string, unknown, number][] = [
      ["another rep, same tenant", RA2, A_RA1, "completed", 404],
      ["rep from another tenant", RB1, A_RA1, "completed", 404],
      ["manager from another tenant", MB, A_RA1, "completed", 404],
      ["partner admin with another tenant key", PA, A_RA1, "completed", 404],
      ["unknown id", MA, id(999), "completed", 404],
      ["malformed id", MA, "nope", "completed", 400],
      ["bad status", RA1, A_RA1, "deleted", 400],
      ["no identity", "", A_RA1, "completed", 401],
    ];
    for (const [name, who, aid, st, want] of cases) {
      const w = world();
      const before = snapshot(w.rows);
      const r = await updateCoachAssignmentStatus(w.deps, who, aid, st);
      check(`PATCH denied: ${name} → ${want}, row unchanged`, r.status === want && (r.body as any).ok === false && snapshot(w.rows) === before, JSON.stringify(r));
    }
  }

  // ── CREATE (assignee scope) ─────────────────────────────────────────────────────────────────────────────
  {
    const { lookups } = world();
    check("CREATE a rep may assign to themself", await canAssignTo(RA1, RA1, lookups));
    check("CREATE a manager may assign to a rep in their tenant", await canAssignTo(MA, RA2, lookups));
    check("CREATE a rep may not assign to another rep", !(await canAssignTo(RA1, RA2, lookups)));
    check("CREATE nobody may assign across tenants", !(await canAssignTo(MA, RB1, lookups)) && !(await canAssignTo(RA1, RB1, lookups)));
    check("CREATE an unknown assignee is refused", !(await canAssignTo(MA, id(999), lookups)));
  }

  // ── 6 HTTP level: x-org-id / body / query cannot change scope ───────────────────────────────────────────
  {
    const w = world();
    const app = express();
    app.use(express.json());
    // Same shape as server.ts: identity comes from the verified context (here a stub that reads x-test-user), never from x-org-id.
    app.use((req, _res, next) => { (req as any).userId = String(req.header("x-test-user") || ""); next(); });
    app.delete("/v1/coach/assignments/:id", async (req, res) => { const r = await deleteCoachAssignment(w.deps, (req as any).userId, req.params.id); res.status(r.status).json(r.body); });
    app.patch("/v1/coach/assignments/:id", async (req, res) => { const r = await updateCoachAssignmentStatus(w.deps, (req as any).userId, req.params.id, (req.body || {}).status); res.status(r.status).json(r.body); });
    const server = http.createServer(app);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const port = (server.address() as any).port;
    const call = (method: string, p: string, headers: Record<string, string>, body?: unknown) =>
      fetch(`http://127.0.0.1:${port}${p}`, { method, headers: { "content-type": "application/json", ...headers }, body: body ? JSON.stringify(body) : undefined }).then(async (r) => ({ status: r.status, body: await r.json() }));
    const before = snapshot(w.rows);
    const spoof = await call("DELETE", `/v1/coach/assignments/${A_RB1}?orgId=${O_A}&org_id=${O_A}`, { "x-test-user": MA, "x-org-id": O_B });
    check("DELETE 6a manager of A spoofing x-org-id/orgId of B still cannot delete B's assignment", spoof.status === 404 && w.rows.has(A_RB1));
    const spoof2 = await call("DELETE", `/v1/coach/assignments/${A_RA1}`, { "x-test-user": RA1, "x-org-id": O_A, "x-user-role": "Manager", "x-tier": "Owner" });
    check("DELETE 6b a rep claiming a manager role/org via headers is still a rep", spoof2.status === 403 && w.rows.has(A_RA1));
    const spoof3 = await call("DELETE", `/v1/coach/assignments/${A_RA1}`, { "x-test-user": RA2, "x-org-id": O_B });
    check("DELETE 6c a rep spoofing another org header is refused", spoof3.status === 403 && w.rows.has(A_RA1));
    const ok = await call("DELETE", `/v1/coach/assignments/${A_RA1}`, { "x-test-user": MA, "x-org-id": O_B });
    check("DELETE 6d the legitimate manager still succeeds even when the (ignored) header is wrong", ok.status === 200 && ok.body.ok === true && !w.rows.has(A_RA1));
    check("DELETE 6e denied spoof attempts left every other row untouched", w.rows.has(A_RB1) && w.rows.has(A_RA2) && w.rows.size === 4 && before !== snapshot(w.rows));
    const patchSpoof = await call("PATCH", `/v1/coach/assignments/${A_RB1}`, { "x-test-user": MA, "x-org-id": O_B }, { status: "completed" });
    check("PATCH 6f spoofed x-org-id cannot complete another tenant's assignment", patchSpoof.status === 404 && w.rows.get(A_RB1)!.status === "open");
    await new Promise<void>((r) => server.close(() => r()));
  }


  // ── READS: assignments ──────────────────────────────────────────────────────────────────────────────────
  const idsOf = (r: { body: any }) => (r.body.items || []).map((x: any) => x.id).sort();
  const w0 = world();
  const byCall = (u: string, callId: string, extra: Record<string, unknown> = {}) => listCoachAssignments(w0.deps, u, { callId, ...extra });
  {
    const rep = await listCoachAssignments(w0.deps, RA1, { assigneeUserId: RA1 });
    check("READ rep sees only their own assignments (assignee=self)", rep.status === 200 && JSON.stringify(idsOf(rep)) === JSON.stringify([A_RA1, A_MISMATCH].sort()));
    const repOther = await listCoachAssignments(w0.deps, RA1, { assigneeUserId: RA2 });
    const repNone = await listCoachAssignments(w0.deps, RA1, { assigneeUserId: id(999) });
    check("READ rep asking for another rep's assignments gets nothing, identical to an unknown rep", repOther.status === 200 && repOther.body.items.length === 0 && JSON.stringify(repOther) === JSON.stringify(repNone));
    const repCall = await byCall(RA1, id(202));
    check("READ rep cannot read assignments on a teammate's call (by callId)", repCall.status === 200 && repCall.body.items.length === 0);
    const mgr = await listCoachAssignments(w0.deps, MA, { assigneeUserId: RA2 });
    check("READ manager sees an in-tenant rep's assignments", mgr.status === 200 && idsOf(mgr).includes(A_RA2));
    const mgrForeign = await listCoachAssignments(w0.deps, MA, { assigneeUserId: RB1 });
    check("READ manager cannot read another tenant's rep (empty, not an error)", mgrForeign.status === 200 && mgrForeign.body.items.length === 0);
    const mgrCall = await byCall(MA, id(203));
    const mgrUnknown = await byCall(MA, id(999));
    check("READ manager on a foreign-tenant call and on an unknown call get byte-identical answers", JSON.stringify(mgrCall) === JSON.stringify(mgrUnknown) && mgrCall.body.items.length === 0);
    const mgrOwn = await byCall(MA, id(202));
    check("READ manager sees assignments on an in-tenant call (response shape kept)", mgrOwn.status === 200 && Object.keys(mgrOwn.body.items[0]).sort().join() === "assignee_user_id,call_id,call_label,completed_at,created_at,drill_id,id,notes,status" && mgrOwn.body.items[0].call_label === "ra2-call.mp3");
    const spoof = await byCall(RA1, id(203), { orgId: O_B, org_id: O_B, repId: RB1, assigneeUserId: RB1, role: "Manager" });
    check("READ spoofed orgId / role / rep identifiers do not expand a rep's results", spoof.status === 200 && spoof.body.items.length === 0);
    const spoof2 = await listCoachAssignments(w0.deps, RA1, { assigneeUserId: RA1, orgId: O_B });
    check("READ a foreign orgId does not shrink or grow a legitimate read either (ignored)", JSON.stringify(idsOf(spoof2)) === JSON.stringify(idsOf(rep)));
    const mSpoof = await listCoachAssignments(w0.deps, MA, { accountId: ACC_X, orgId: O_B });
    check("READ manager of A asking for shared account X with orgId=B still sees only tenant A rows", mSpoof.status === 200 && idsOf(mSpoof).every((x: string) => [A_RA1, A_RA2, A_MISMATCH].includes(x)) && !idsOf(mSpoof).includes(A_RB1));
    check("READ a foreign call's filename is never leaked in labels", !JSON.stringify([mSpoof, mgrCall, spoof]).includes("secret-call"));
    check("READ malformed ids → 400; missing selector → 400", (await listCoachAssignments(w0.deps, MA, { callId: "x'--" })).status === 400 && (await listCoachAssignments(w0.deps, MA, {})).status === 400);
    check("READ no identity → 401", (await listCoachAssignments(w0.deps, "", { callId: id(202) })).status === 401);
  }
  // by-entity
  {
    const m = await listAssignmentsByEntity(w0.deps, MA, { accountId: ACC_X });
    check("BY-ENTITY manager sees only tenant A assignments for a shared account (original slim shape)", m.status === 200 && !idsOf(m).includes(A_RB1) && idsOf(m).includes(A_RA1) && Object.keys(m.body.items[0]).sort().join() === "call_id,call_label,created_at,drill_id,id,notes");
    const x = await listAssignmentsByEntity(w0.deps, MB, { accountId: ACC_X });
    check("BY-ENTITY the other tenant's manager sees only their own (cross-tenant read fails safe)", idsOf(x).join() === [A_RB1].join());
    const r = await listAssignmentsByEntity(w0.deps, RA2, { accountId: ACC_X });
    check("BY-ENTITY a rep sees only their own assignments on that account", idsOf(r).join() === [A_RA2].join());
    const k = await listAssignmentsByEntity(w0.deps, RA1, { contactId: K3 });
    const unk = await listAssignmentsByEntity(w0.deps, RA1, { contactId: id(998) });
    check("BY-ENTITY a foreign contact and an unknown contact answer identically", JSON.stringify(k) === JSON.stringify(unk) && k.body.items.length === 0);
    check("BY-ENTITY client-supplied owner/org/rep params change nothing", JSON.stringify(await listAssignmentsByEntity(w0.deps, RA1, { accountId: ACC_X, orgId: O_B, repId: RB1, assigneeUserId: RB1 } as any)) === JSON.stringify(await listAssignmentsByEntity(w0.deps, RA1, { accountId: ACC_X })));
    check("BY-ENTITY needs a selector; malformed id → 400", (await listAssignmentsByEntity(w0.deps, MA, {})).status === 400 && (await listAssignmentsByEntity(w0.deps, MA, { accountId: "bad" })).status === 400);
  }

  // ── NOTES ───────────────────────────────────────────────────────────────────────────────────────────────
  {
    const w = world();
    const own = await getCoachNote(w.deps, RA1, id(201));
    check("NOTES GET call owner reads their note (shape: ok + note{id,call_id,notes,updated_at})", own.status === 200 && (own.body as any).note?.notes === "ra1 private coaching note" && Object.keys((own.body as any).note).sort().join() === "call_id,id,notes,updated_at");
    const mgr = await getCoachNote(w.deps, MA, id(201));
    check("NOTES GET manager in the owner's tenant reads it", mgr.status === 200 && (mgr.body as any).note?.id === id(401));
    const none = await getCoachNote(w.deps, RA1, id(204));
    check("NOTES GET an authorised caller with no note yet gets note:null", none.status === 200 && (none.body as any).note === null);
    const denied = [
      ["another rep, same tenant", RA2, id(201)],
      ["another tenant's rep", RB1, id(201)],
      ["another tenant's manager", MB, id(201)],
      ["manager reading the other tenant", MA, id(203)],
    ] as const;
    const unknown = await getCoachNote(w.deps, RA1, id(999));
    for (const [name, who, cid] of denied) {
      const r = await getCoachNote(w.deps, who, cid);
      check(`NOTES GET denied: ${name} → 404 identical to unknown call (no existence/author leak)`, r.status === 404 && JSON.stringify(r) === JSON.stringify(unknown) && !JSON.stringify(r).includes("private"));
    }
    check("NOTES GET malformed / missing callId", (await getCoachNote(w.deps, RA1, "zzz")).status === 400 && (await getCoachNote(w.deps, RA1, "")).status === 400);

    const ok = await saveCoachNote(w.deps, RA1, id(204), "my first note");
    check("NOTES POST call owner writes (shape ok + note) and it is stored", ok.status === 200 && (ok.body as any).note?.notes === "my first note" && w.notes.get(id(204))?.notes === "my first note");
    const mok = await saveCoachNote(w.deps, MA, id(202), "manager note on a managed rep's call");
    check("NOTES POST manager writes inside the tenant", mok.status === 200 && w.notes.get(id(202))?.notes === "manager note on a managed rep's call");
    const snap = JSON.stringify([...w.notes.entries()].sort());
    const writes = [
      ["another rep, same tenant", RA2, id(201)],
      ["another tenant's rep", RB1, id(201)],
      ["another tenant's manager", MB, id(201)],
      ["manager attaching to a foreign-tenant call", MA, id(203)],
      ["any caller attaching to an unknown call id", MA, id(999)],
    ] as const;
    for (const [name, who, cid] of writes) {
      const r = await saveCoachNote(w.deps, who, cid, "INJECTED");
      check(`NOTES POST denied: ${name} → 404 and no note is created or changed`, r.status === 404 && JSON.stringify(r) === JSON.stringify(await saveCoachNote(w.deps, who, id(997), "INJECTED")) && JSON.stringify([...w.notes.entries()].sort()) === snap);
    }
    check("NOTES POST malformed callId / non-text notes / no identity", (await saveCoachNote(w.deps, RA1, "x", "t")).status === 400 && (await saveCoachNote(w.deps, RA1, id(204), { a: 1 })).status === 400 && (await saveCoachNote(w.deps, "", id(204), "t")).status === 401 && JSON.stringify([...w.notes.entries()].sort()) === snap);
  }

  // ── wiring: the real routes use the guarded handlers ────────────────────────────────────────────────────
  const root = path.resolve(__dirname, "..");
  const server = fs.readFileSync(path.join(root, "src/server.ts"), "utf8");
  const coach = fs.readFileSync(path.join(root, "src/routes/coach.ts"), "utf8");
  const delBlock = /app\.delete\("\/v1\/coach\/assignments\/:id"[\s\S]*?\n\}\);/.exec(server)?.[0] ?? "";
  const patchBlock = /app\.patch\("\/v1\/coach\/assignments\/:id"[\s\S]*?\n\}\);/.exec(server)?.[0] ?? "";
  check("WIRING DELETE route calls deleteCoachAssignment with the verified req.userId", /deleteCoachAssignment\(coachAssignmentDeps,\s*\(req as any\)\.userId,\s*req\.params\.id\)/.test(delBlock));
  check("WIRING PATCH route calls updateCoachAssignmentStatus with the verified req.userId", /updateCoachAssignmentStatus\(coachAssignmentDeps,\s*\(req as any\)\.userId,\s*req\.params\.id/.test(patchBlock));
  check("WIRING neither route reads headers, query orgId or touches coach_assignments directly", !/req\.(header|headers|query)|\.from\(["']coach_assignments["']\)/.test(delBlock + patchBlock));
  check("WIRING the only mutating coach_assignments access in server.ts is inside the guarded store", (server.match(/\.from\("coach_assignments"\)\s*\.(delete|update)\(/g) || []).length === 2 && /const coachAssignmentDeps/.test(server));
  const routeBlock = (re: RegExp) => re.exec(server)?.[0] ?? "";
  const reads = [
    ["GET /v1/coach/assignments", /app\.get\("\/v1\/coach\/assignments", requireIdentity[\s\S]*?\n\}\);/, /listCoachAssignments\(coachAssignmentDeps,\s*\(req as any\)\.userId/],
    ["GET /v1/coach/assignments/by-entity", /app\.get\('\/v1\/coach\/assignments\/by-entity'[\s\S]*?\n\}\);/, /listAssignmentsByEntity\(coachAssignmentDeps,\s*\(req as any\)\.userId/],
    ["GET /v1/coach/notes", /app\.get\("\/v1\/coach\/notes"[\s\S]*?\n\}\);/, /getCoachNote\(coachAssignmentDeps,\s*\(req as any\)\.userId/],
    ["POST /v1/coach/notes", /app\.post\("\/v1\/coach\/notes"[\s\S]*?\n\}\);/, /saveCoachNote\(coachAssignmentDeps,\s*\(req as any\)\.userId/],
  ] as const;
  for (const [name, re, call] of reads) {
    const b = routeBlock(re);
    check(`WIRING ${name} delegates to the guarded handler and never reads headers/orgId or the tables itself`, call.test(b) && !/req\.(header|headers)|orgId|\.from\(/.test(b));
  }
  {
    // Every /v1/coach route registered in server.ts or routes/coach.ts is either public-static, delegated, or has an explicit check.
    const coachRoutes = [...server.matchAll(/app\.(get|post|put|patch|delete)\(\s*["'`](\/v1\/coach[^"'`]*)["'`][^\n]*/g)].map((m) => `${m[1].toUpperCase()} ${m[2]}`);
    const known = new Set(["GET /v1/coach/assignments", "GET /v1/coach/assignments/by-entity", "DELETE /v1/coach/assignments/:id", "PATCH /v1/coach/assignments/:id", "GET /v1/coach/notes", "POST /v1/coach/notes", "GET /v1/coach/drills"]);
    check("WIRING no unreviewed /v1/coach route has appeared in server.ts", coachRoutes.every((r) => known.has(r)) && coachRoutes.length === known.size, coachRoutes.join(" | "));
    const coachFile = [...coach.matchAll(/\br\.(get|post|put|patch|delete)\(\s*["'`]([^"'`]*)["'`]/g)].map((m) => `${m[1].toUpperCase()} ${m[2]}`);
    check("WIRING routes/coach.ts exposes only POST /assign (owner + assignee-scope checked)", coachFile.join() === "POST /assign", coachFile.join());
    const ident = (server.match(/app\.(?:get|post|put|patch|delete)\(\s*["'`]\/v1\/coach[^\n]*requireIdentity/g) || []).length;
    check("WIRING every requireIdentity-only coach route in server.ts is one of the six delegated above", ident === 6, String(ident));
  }
  check("WIRING POST /v1/coach/assign checks the assignee scope before writing", /canAssignTo\(requester, body\.assigneeUserId, repLookups\)/.test(coach) && coach.indexOf("canAssignTo(requester") < coach.indexOf(".insert(assignmentPayload)"));

  // ── non-vacuity: the pre-fix behaviour must fail the same scenarios ─────────────────────────────────────
  const legacy = await deleteScenarios(() => {
    const w = world();
    // Old handler: any non-empty identity deletes by bare id.
    return { rows: w.rows, del: async (u, i) => { if (!u) return { status: 401, body: { ok: false } }; if (!i) return { status: 400, body: { ok: false } }; w.rows.delete(String(i)); return { status: 200, body: { ok: true } }; } };
  });
  const legacyFailed = Object.entries(legacy).filter(([, v]) => !v).map(([k]) => k);
  check("NON-VACUITY the identity-only handler fails the rep / cross-tenant / spoof scenarios", ["2 ", "3 ", "3b", "4 ", "4b"].every((p) => legacyFailed.some((k) => k.startsWith(p))), `failed: ${legacyFailed.join(" | ")}`);

  // Pre-fix behaviour, re-implemented from the old handlers (identity only; scope = whatever the client sent) and run through the
  // SAME planted scenarios. Each must violate the policy, proving the scenarios above can detect the vulnerability.
  {
    const w = world();
    const store = w.deps.store;
    const legacyList = async (_u: string, q: { callId?: string; assigneeUserId?: string; orgId?: string }) => {
      const rows = await store.listAssignments({ callIds: q.callId ? [q.callId] : undefined, assignee: q.assigneeUserId, max: 100 });
      return rows.filter((r) => !q.orgId || r.org_id === q.orgId);
    };
    const legacyNoteGet = async (_u: string, callId: string) => store.getNote(callId);
    const legacyNotePost = async (_u: string, callId: string, notes: string) => store.upsertNote(callId, notes);
    const foreign = await legacyList(MA, { callId: id(203) });
    check("NON-VACUITY legacy list leaks a foreign tenant's assignments by callId", foreign.length === 1 && foreign[0].assignee_user_id === RB1);
    const viaOrg = await legacyList(RA1, { assigneeUserId: RB1, orgId: O_B });
    check("NON-VACUITY legacy list honours a spoofed orgId/assignee for a rep", viaOrg.length === 1);
    check("NON-VACUITY legacy notes GET returns another rep's / tenant's note", (await legacyNoteGet(RA2, id(201)))?.notes === "ra1 private coaching note" && (await legacyNoteGet(MA, id(203)))?.notes === "tenant B note");
    await legacyNotePost(MA, id(203), "INJECTED");
    await legacyNotePost(RA1, id(999), "ORPHAN");
    check("NON-VACUITY legacy notes POST overwrites a foreign note and attaches to an unknown call", w.notes.get(id(203))?.notes === "INJECTED" && w.notes.has(id(999)));
    const real = world();
    const g1 = await listCoachAssignments(real.deps, MA, { callId: id(203) });
    const g2 = await listCoachAssignments(real.deps, RA1, { assigneeUserId: RB1, orgId: O_B });
    const g3 = await getCoachNote(real.deps, RA2, id(201));
    await saveCoachNote(real.deps, MA, id(203), "INJECTED");
    await saveCoachNote(real.deps, RA1, id(999), "ORPHAN");
    check("NON-VACUITY the guarded handlers refuse every one of those operations", g1.body.items.length === 0 && g2.body.items.length === 0 && g3.status === 404 && real.notes.get(id(203))?.notes === "tenant B note" && !real.notes.has(id(999)));
  }

  console.log(failures ? `\n${failures} check(s) FAILED` : "\nAll coach-assignment authorisation checks PASSED");
  process.exit(failures ? 1 : 0);
}
main().catch((e) => { console.error(e); process.exit(1); });
