/**
 * verify-coach-authz-staging.ts — LIVE, STAGING-ONLY proof of the coach-assignment / notes authorisation boundary.
 *
 * Creates temporary, clearly-labelled fixtures in the synthetic staging tenant(s) (two companies, a manager and reps in
 * each, calls, assignments and notes), signs the temporary users in through Supabase Auth to obtain real bearer tokens,
 * exercises the DEPLOYED staging API over HTTP, checks the database rows with the service role after every denied
 * attempt, then removes everything it created (also on failure).
 *
 * STATUS: written alongside the fix; NOT YET RUN. Run it only after the API fix has been approved and deployed to staging.
 *
 * SAFETY
 *   • Refuses unless APP_ENV=staging, SUPABASE_URL's project ref == EXPECTED_STAGING_SUPABASE_REF, and (if given) != PROD_SUPABASE_REF.
 *   • API_BASE must be the staging API (https://*staging*.up.railway.app) and its /v1/version must report app_env "staging".
 *   • Never prints passwords, tokens or keys. Passwords are random, in memory only. All ids/emails are synthetic (*.invalid).
 *
 * ENV: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_ANON_KEY, EXPECTED_STAGING_SUPABASE_REF, APP_ENV=staging, API_BASE
 * Run: APP_ENV=staging API_BASE=https://… npx tsx scripts/verify-coach-authz-staging.ts   (env from .env.staging.local)
 */
import "dotenv/config";
import crypto from "node:crypto";
import { createClient } from "@supabase/supabase-js";

const need = (k: string) => { const v = process.env[k]; if (!v) { console.error(`missing ${k}`); process.exit(2); } return v; };
const URL_ = need("SUPABASE_URL"), SERVICE = process.env.SUPABASE_SERVICE_ROLE_KEY || need("SUPABASE_SERVICE_KEY"), ANON = need("SUPABASE_ANON_KEY");
const API = need("API_BASE").replace(/\/$/, "");
const refOf = (u: string) => (/^https?:\/\/([a-z0-9]+)\.supabase\./i.exec(u)?.[1] || "").toLowerCase();
if (process.env.APP_ENV !== "staging") { console.error("APP_ENV must be staging"); process.exit(2); }
if (refOf(URL_) !== (process.env.EXPECTED_STAGING_SUPABASE_REF || "").toLowerCase() || !refOf(URL_)) { console.error("SUPABASE_URL is not the expected staging project"); process.exit(2); }
if (process.env.PROD_SUPABASE_REF && refOf(URL_) === process.env.PROD_SUPABASE_REF.toLowerCase()) { console.error("refusing: production project"); process.exit(2); }
if (!/staging/i.test(API)) { console.error("API_BASE must be the staging API"); process.exit(2); }

const admin = createClient(URL_, SERVICE, { auth: { persistSession: false, autoRefreshToken: false } });
const RUN = crypto.randomBytes(3).toString("hex");
const uuid = () => crypto.randomUUID();
let fails = 0;
const check = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) fails++; };

type U = { key: string; id: string; email: string; password: string; tier: string; company: string; org: string; token: string };
const TENANTS = {
  A: { org: "00000000-2711-0000-0000-000000000002", company: "00000000-2711-0000-0000-000000000003" },
  B: { org: "00000000-2711-0000-0000-0000000000b2", company: "00000000-2711-0000-0000-0000000000b3" },
};
const created = { users: [] as string[], calls: [] as string[], assigns: [] as string[], notes: [] as string[], orgB: false, companyB: false };

async function http(user: U | null, method: string, path: string, body?: unknown, extra: Record<string, string> = {}) {
  const r = await fetch(`${API}${path}`, { method, headers: { "content-type": "application/json", ...(user ? { authorization: `Bearer ${user.token}` } : {}), ...extra }, body: body === undefined ? undefined : JSON.stringify(body) });
  let j: any = null; try { j = await r.json(); } catch { /* non-json */ }
  return { status: r.status, body: j };
}

async function mkUser(key: string, tier: string, t: "A" | "B"): Promise<U> {
  const email = `coachauthz-${RUN}-${key}@gravix.invalid`;
  const password = crypto.randomBytes(18).toString("base64url");
  const { data, error } = await admin.auth.admin.createUser({ email, password, email_confirm: true });
  if (error || !data.user) throw new Error(`createUser ${key} failed`);
  created.users.push(data.user.id);
  const { error: rErr } = await admin.from("reps").upsert({ id: data.user.id, org_id: TENANTS[t].org, company_id: TENANTS[t].company, tier, email });
  if (rErr) throw new Error(`reps upsert ${key}: ${rErr.message}`);
  const anon = createClient(URL_, ANON, { auth: { persistSession: false, autoRefreshToken: false } });
  const s = await anon.auth.signInWithPassword({ email, password });
  if (s.error || !s.data.session) throw new Error(`sign-in ${key} failed`);
  return { key, id: data.user.id, email, password, tier, company: TENANTS[t].company, org: TENANTS[t].org, token: s.data.session.access_token };
}
async function mkCall(owner: U) {
  const id = uuid();
  const { error } = await admin.from("calls").insert({ id, user_id: owner.id, org_id: owner.org, storage_path: `coachauthz/${RUN}/${id}.wav`, audio_path: `coachauthz/${RUN}/${id}.wav`, filename: `coachauthz-${RUN}-${owner.key}.wav`, status: "scored" });
  if (error) throw new Error(`calls insert: ${error.message}`);
  created.calls.push(id);
  return id;
}
async function mkAssign(call: string, assignee: U) {
  const { data, error } = await admin.from("coach_assignments").insert({ call_id: call, assignee_user_id: assignee.id, drill_id: "intro-basics", notes: `coachauthz ${RUN}`, org_id: assignee.org, status: "open" }).select("id").single();
  if (error || !data) throw new Error(`assignment insert: ${error?.message}`);
  created.assigns.push(data.id);
  return data.id as string;
}
const assignRow = async (id: string) => (await admin.from("coach_assignments").select("id,status,completed_at").eq("id", id).maybeSingle()).data;
const noteRow = async (callId: string) => (await admin.from("coach_notes").select("id,notes").eq("call_id", callId).maybeSingle()).data;

async function main() {
  const v = await http(null, "GET", "/v1/version");
  check("target is the staging API", v.body?.app_env === "staging", JSON.stringify(v.body)?.slice(0, 80));
  console.log(`      API version ${v.body?.version}`);

  // tenant B rows (only created if missing)
  if (!(await admin.from("orgs").select("id").eq("id", TENANTS.B.org).maybeSingle()).data) { await admin.from("orgs").insert({ id: TENANTS.B.org, name: `coachauthz-${RUN}-B` }); created.orgB = true; }
  if (!(await admin.from("companies").select("id").eq("id", TENANTS.B.company).maybeSingle()).data) {
    const tmc = (await admin.from("companies").select("tmc_id").eq("id", TENANTS.A.company).maybeSingle()).data?.tmc_id;
    await admin.from("companies").insert({ id: TENANTS.B.company, tmc_id: tmc, name: `coachauthz-${RUN}-B`, slug: `coachauthz-${RUN}-b` }); created.companyB = true;
  }

  const mgrA = await mkUser("mgrA", "Manager", "A"), repA1 = await mkUser("repA1", "SalesRep", "A"), repA2 = await mkUser("repA2", "SalesRep", "A");
  const mgrB = await mkUser("mgrB", "Manager", "B"), repB1 = await mkUser("repB1", "SalesRep", "B");
  const cA1 = await mkCall(repA1), cA2 = await mkCall(repA2), cB1 = await mkCall(repB1);
  const aA1 = await mkAssign(cA1, repA1), aA2 = await mkAssign(cA2, repA2), aB1 = await mkAssign(cB1, repB1);
  await admin.from("coach_notes").insert([{ call_id: cA1, notes: "A1 note" }, { call_id: cB1, notes: "B1 note" }]); created.notes.push(cA1, cB1);

  const ids = (r: { body: any }) => (r.body?.items || []).map((x: any) => x.id);
  const tamper = { "x-org-id": TENANTS.B.org, "x-user-role": "Manager", "x-user-id": mgrB.id };

  // ── allowed workflows ──
  const l1 = await http(repA1, "GET", `/v1/coach/assignments?callId=${cA1}`);
  check("rep lists their own assignment on their call", l1.status === 200 && ids(l1).includes(aA1));
  const l2 = await http(mgrA, "GET", `/v1/coach/assignments?callId=${cA2}`);
  check("manager lists an in-tenant rep's assignment", l2.status === 200 && ids(l2).includes(aA2));
  const p1 = await http(repA1, "PATCH", `/v1/coach/assignments/${aA1}`, { status: "completed" });
  check("rep completes their own assignment", p1.status === 200 && (await assignRow(aA1))?.status === "completed");
  const p2 = await http(mgrA, "PATCH", `/v1/coach/assignments/${aA1}`, { status: "open" });
  check("manager reopens it", p2.status === 200 && (await assignRow(aA1))?.status === "open");
  const n1 = await http(repA1, "GET", `/v1/coach/notes?callId=${cA1}`);
  check("rep reads their call's note", n1.status === 200 && n1.body?.note?.notes === "A1 note");
  const n2 = await http(mgrA, "POST", "/v1/coach/notes", { callId: cA1, notes: "manager edit" });
  check("manager writes a note in their tenant", n2.status === 200 && (await noteRow(cA1))?.notes === "manager edit");
  const asg = await http(repA1, "POST", "/v1/coach/assign", { callId: cA1, assigneeUserId: repA1.id, drillId: "discovery-5qs", notes: `coachauthz ${RUN}` });
  if (asg.body?.item?.id) created.assigns.push(asg.body.item.id);
  check("rep assigns a drill to themself on their own call", asg.status === 200 && asg.body?.ok === true, `${asg.status}`);

  // ── denied reads ──
  const d1 = await http(repA1, "GET", `/v1/coach/assignments?callId=${cB1}`, undefined, tamper);
  check("rep + header tampering cannot read tenant B's assignments by call", d1.status === 200 && ids(d1).length === 0);
  const d2 = await http(mgrA, "GET", `/v1/coach/assignments?callId=${cB1}&orgId=${TENANTS.B.org}`, undefined, tamper);
  const d2u = await http(mgrA, "GET", `/v1/coach/assignments?callId=${uuid()}&orgId=${TENANTS.B.org}`);
  check("manager A + orgId=B cannot read B; foreign and unknown calls look identical", d2.status === 200 && ids(d2).length === 0 && JSON.stringify(d2.body) === JSON.stringify(d2u.body));
  const d3 = await http(repA1, "GET", `/v1/coach/assignments?assigneeUserId=${repA2.id}`);
  check("rep cannot read a teammate's assignments", d3.status === 200 && ids(d3).length === 0);
  const d4 = await http(mgrA, "GET", `/v1/coach/assignments/by-entity?accountId=${uuid()}`);
  check("by-entity with an unknown account is an empty list", d4.status === 200 && ids(d4).length === 0);
  const nd = [await http(repA2, "GET", `/v1/coach/notes?callId=${cA1}`), await http(repB1, "GET", `/v1/coach/notes?callId=${cA1}`), await http(mgrB, "GET", `/v1/coach/notes?callId=${cA1}`), await http(mgrA, "GET", `/v1/coach/notes?callId=${cB1}`, undefined, tamper)];
  const unk = await http(repA1, "GET", `/v1/coach/notes?callId=${uuid()}`);
  check("notes GET denied across rep and tenant boundaries, identical to unknown", nd.every((r) => r.status === 404 && JSON.stringify(r.body) === JSON.stringify(unk.body)));

  // ── denied mutations: rows unchanged ──
  const before = { a: await assignRow(aA2), b: await assignRow(aB1), nA: await noteRow(cA1), nB: await noteRow(cB1) };
  const dm = [
    await http(repA2, "DELETE", `/v1/coach/assignments/${aA2}`),
    await http(repA1, "DELETE", `/v1/coach/assignments/${aA2}`, undefined, tamper),
    await http(mgrA, "DELETE", `/v1/coach/assignments/${aB1}`, undefined, tamper),
    await http(mgrB, "DELETE", `/v1/coach/assignments/${aA2}`),
    await http(mgrA, "DELETE", `/v1/coach/assignments/not-a-uuid`),
    await http(mgrA, "DELETE", `/v1/coach/assignments/${uuid()}`),
    await http(repA1, "PATCH", `/v1/coach/assignments/${aA2}`, { status: "completed" }),
    await http(mgrB, "PATCH", `/v1/coach/assignments/${aA2}`, { status: "completed" }),
    await http(repA1, "POST", "/v1/coach/notes", { callId: cB1, notes: "INJECTED" }, tamper),
    await http(mgrA, "POST", "/v1/coach/notes", { callId: cB1, notes: "INJECTED" }),
    await http(repA2, "POST", "/v1/coach/notes", { callId: cA1, notes: "INJECTED" }),
    await http(repA1, "POST", "/v1/coach/notes", { callId: uuid(), notes: "INJECTED" }),
  ];
  check("every denied mutation is refused (4xx)", dm.every((r) => r.status >= 400 && r.status < 500), dm.map((r) => r.status).join(","));
  const after = { a: await assignRow(aA2), b: await assignRow(aB1), nA: await noteRow(cA1), nB: await noteRow(cB1) };
  check("the database rows are unchanged after every denied attempt", JSON.stringify(before) === JSON.stringify(after));
  const orphan = await admin.from("coach_notes").select("id").eq("notes", "INJECTED");
  check("no INJECTED note was created anywhere", (orphan.data || []).length === 0);

  // ── allowed delete last ──
  const del = await http(mgrA, "DELETE", `/v1/coach/assignments/${aA2}`);
  check("manager deletes an in-tenant rep's assignment", del.status === 200 && del.body?.ok === true && (await assignRow(aA2)) === null);
  check("tenant B's assignment survived all of the above", (await assignRow(aB1)) !== null);
}

async function cleanup() {
  const e = async (label: string, p: PromiseLike<{ error: any }>) => { const r = await p; if (r.error) console.log(`cleanup ${label}: ${r.error.message}`); };
  if (created.notes.length) await e("notes", admin.from("coach_notes").delete().in("call_id", created.notes));
  if (created.calls.length) { await e("notes(calls)", admin.from("coach_notes").delete().in("call_id", created.calls)); await e("assignments(calls)", admin.from("coach_assignments").delete().in("call_id", created.calls)); }
  if (created.assigns.length) await e("assignments", admin.from("coach_assignments").delete().in("id", created.assigns));
  if (created.calls.length) await e("calls", admin.from("calls").delete().in("id", created.calls));
  if (created.users.length) { await e("reps", admin.from("reps").delete().in("id", created.users)); for (const id of created.users) await admin.auth.admin.deleteUser(id); }
  if (created.companyB) await e("company", admin.from("companies").delete().eq("id", TENANTS.B.company));
  if (created.orgB) await e("org", admin.from("orgs").delete().eq("id", TENANTS.B.org));
  console.log("      cleanup complete (temporary users, reps, calls, assignments, notes removed)");
}

main().catch((e) => { console.error("ERROR", e?.message || e); fails++; }).finally(async () => { await cleanup(); console.log(fails ? `\n${fails} check(s) FAILED` : "\nStaging authorisation boundary verified"); process.exit(fails ? 1 : 0); });
