// Go Live Day 39 — F-38-1 (upload hierarchy) + F-38-2 (score activity). Network-free.
//
// Boots the REAL app in a child process against a loopback PostgREST/storage stub
// (no real DB/storage/provider; empty cwd; placeholder keys) and proves:
//
//   F-38-1  every upload path stamps the caller's CANONICAL org/company/office:
//             reps-only caller (the Day-38 shape, no `users` row) is stamped; legacy users-only
//             caller is stamped; reps wins over users; manager with no office is stamped
//             (office optional); client-supplied hierarchy/user fields are ignored;
//             missing company is REFUSED before any call/job row, and the already-PUT object is
//             removed (bounded, one attempt; removal failure is reported, never hidden);
//             lookup errors are 503, not "unscoped"; direct upload refuses before storing anything;
//             POST /v1/calls uses the same resolver
//   F-38-2  a successful score job posts exactly ONE schema-valid `score_posted` activity
//             (never `call_scored`); a database error on that insert is OBSERVABLE (job result
//             `activity.logged=false` + stable code) while the call stays `scored`
//   unit    the activity helper and resolver with fake clients (errors-as-data, exceptions, org)
// Non-vacuity: each source mutation of a COPY of src/ MUST fail the named check.
//
// Run: npm run validate:upload-hierarchy   (tsx; child process + loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import * as crypto from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
type Check = { id: string; pass: boolean; detail?: string };

const U = (n: number) => `aaaaaaaa-0000-4000-8000-${String(n).padStart(12, "0")}`;
const ORG_A = "dddddddd-0000-4000-8000-00000000000a", CO_A = "dddddddd-0000-4000-8000-00000000000b", OFF_A = "dddddddd-0000-4000-8000-00000000000c";
const ORG_B = "eeeeeeee-0000-4000-8000-00000000000a", CO_B = "eeeeeeee-0000-4000-8000-00000000000b";
const DEFAULT_ORG = "99999999-0000-4000-8000-0000000000de"; // planted global default — must NOT be stamped over a caller's own org
const PROXY_SECRET = "planted-proxy-secret-d39";
const WAV = (() => { const n = 8000, d = Buffer.alloc(n * 2), h = Buffer.alloc(44); h.write("RIFF", 0); h.writeUInt32LE(36 + d.length, 4); h.write("WAVEfmt ", 8); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22); h.writeUInt32LE(8000, 24); h.writeUInt32LE(16000, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34); h.write("data", 36); h.writeUInt32LE(d.length, 40); return Buffer.concat([h, d]); })();

// ───────────── controllable loopback stub ─────────────
type Row = Record<string, unknown> | null;
type World = {
  reps: Record<string, Row>; users: Record<string, Row>;
  repsError: boolean; activitiesError: boolean; storageRemoveError: boolean;
  calls: Record<string, Record<string, unknown>>; // inserted calls by id
  posts: Array<{ table: string; body: any }>; patches: Array<{ table: string; body: any }>;
  storage: string[]; // "METHOD path"
};
const world = (): World => ({ reps: {}, users: {}, repsError: false, activitiesError: false, storageRemoveError: false, calls: {}, posts: [], patches: [], storage: [] });
function startStub(w: { cur: World }): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const chunks: Buffer[] = []; req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const W = w.cur; const u = new URL(req.url || "/", "http://stub"); const raw = Buffer.concat(chunks);
      const wantsObject = String(req.headers["accept"] || "").includes("vnd.pgrst.object");
      const send = (c: number, b?: unknown) => { res.writeHead(c, { "content-type": "application/json" }); res.end(b === undefined ? undefined : JSON.stringify(b)); };
      if (u.pathname === "/storage/v1/bucket") return send(200, []);
      if (u.pathname.startsWith("/storage/")) {
        W.storage.push(`${req.method} ${u.pathname}`);
        if (u.pathname.startsWith("/storage/v1/object/sign/")) return send(200, { signedURL: "/object/sign/x?token=t" });
        if (req.method === "GET") { res.writeHead(200, { "content-type": "audio/wav" }); return res.end(WAV); }
        if (req.method === "DELETE") return W.storageRemoveError ? send(500, { message: "planted storage failure" }) : send(200, []);
        return send(200, { Key: "stub" });
      }
      if (u.pathname.startsWith("/auth/")) return send(400, { error: "invalid_grant" });
      const table = u.pathname.replace("/rest/v1/", "");
      const idq = (u.searchParams.get("id") || "").replace(/^eq\./, "");
      const body = (() => { try { return JSON.parse(raw.toString("utf8")); } catch { return null; } })();
      if (req.method === "POST") {
        W.posts.push({ table, body });
        if (table === "calls" && body?.id) W.calls[body.id] = body;
        if (table === "activities" && W.activitiesError) return send(400, { code: "23514", message: "new row violates check constraint \"activities_type_check\"", details: "planted", hint: null });
        if (String(req.headers["prefer"] || "").includes("return=representation") && body) return send(201, wantsObject ? body : [body]);
        res.writeHead(201); return res.end();
      }
      if (req.method === "PATCH") { W.patches.push({ table, body }); res.writeHead(204); return res.end(); }
      // reads
      const pick = (map: Record<string, Row>) => { const r = map[idq]; return r ? (wantsObject ? r : [r]) : (wantsObject ? null : []); };
      if (table === "reps" && idq) { if (W.repsError) return send(500, { message: "planted reps failure" }); const o = pick(W.reps); return o === null ? send(406, { code: "PGRST116", message: "none", details: "", hint: null }) : send(200, o); }
      if (table === "users" && idq) { const o = pick(W.users); return o === null ? send(406, { code: "PGRST116", message: "none", details: "", hint: null }) : send(200, o); }
      if (table === "calls" && idq) { const c = W.calls[idq]; if (!c) return wantsObject ? send(406, { code: "PGRST116", message: "none", details: "", hint: null }) : send(200, []); return send(200, wantsObject ? c : [c]); }
      return wantsObject ? send(406, { code: "PGRST116", message: "none", details: "", hint: null }) : send(200, []);
    });
  });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}
async function serve(root: string, stubUrl: string): Promise<{ base: string; kill: () => void; child: ChildProcess }> {
  const env: Record<string, string> = { PATH: process.env.PATH || "", HOME: process.env.HOME || "", NODE_ENV: "test", GRAVIX_API_NO_LISTEN: "1", SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder", OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder", DEFAULT_ORG_ID: DEFAULT_ORG, SCORING_PROVIDER: "stub", SKIP_SCORING_SIDE_EFFECTS: "1", TRANSCRIPTION_PROVIDER: "stub", PROXY_SHARED_SECRET: PROXY_SECRET, SUPABASE_STORAGE_BUCKET: "d39-bucket" };
  const child = spawn(path.join(ROOT, "node_modules/.bin/tsx"), [__filename, "--serve", root], { env, cwd: EMPTY_CWD, stdio: ["ignore", "pipe", "pipe"] });
  const port: number = await new Promise((resolve, reject) => {
    let buf = ""; const t = setTimeout(() => reject(new Error("child server did not start: " + buf.slice(-200))), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); const m = buf.match(/SERVING_PORT=(\d+)/); if (m) { clearTimeout(t); resolve(Number(m[1])); } });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", (c) => { clearTimeout(t); reject(new Error(`child exited ${c}: ${buf.slice(-200)}`)); });
  });
  return { base: `http://127.0.0.1:${port}`, child, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } } };
}
async function call(base: string, method: string, p: string, uid: string | null, body?: unknown, form?: FormData) {
  const headers: Record<string, string> = { ...(uid ? { "x-user-id": uid, "x-proxy-secret": PROXY_SECRET } : {}), ...(body !== undefined ? { "content-type": "application/json" } : {}) };
  const ctl = new AbortController(); const timer = setTimeout(() => ctl.abort(), 8000);
  try { const r = await fetch(base + p, { method, headers, body: form ?? (body !== undefined ? JSON.stringify(body) : undefined), signal: ctl.signal }); let j: any = null; try { j = await r.json(); } catch { /* none */ } return { status: r.status, body: j }; }
  catch { return { status: 0, body: null }; } finally { clearTimeout(timer); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const callsPosted = (W: World, uid?: string) => W.posts.filter((p) => p.table === "calls" && (!uid || p.body?.user_id === uid));
const mine = (W: World, table: string, uid: string) => W.posts.filter((p) => p.table === table && (p.body?.user_id === uid || p.body?.actor_user_id === uid)).map((p) => p.body);
const finalizeBody = (uid: string, extra: Record<string, unknown> = {}) => ({ path: `${uid}/${crypto.randomUUID()}.wav`, filename: "d39.wav", mime: "audio/wav", size: WAV.length, sha256: crypto.createHash("sha256").update(WAV).digest("hex"), ...extra });

// ───────────── unit: helpers with fake clients ─────────────
async function unitSuite(root: string): Promise<Check[]> {
  const sa = await import(path.join(root, "src/lib/scoreActivity.ts"));
  const ch = await import(path.join(root, "src/lib/callerHierarchy.ts"));
  const args = { callId: "c1", jobId: "j1", orgId: ORG_A, actorUserId: U(1), overall: 71.4, stages: { intro: 70, discovery: 74, objection: 68, close: 72 }, model: "stub:v1" };
  const mk = (insert: () => Promise<{ error: unknown }>) => ({ from: () => ({ insert }) }) as any;
  const ok = await sa.recordScorePostedActivity(mk(async () => ({ error: null })), args);
  const errData = await sa.recordScorePostedActivity(mk(async () => ({ error: { code: "23514", message: "check violation" } })), args);
  const thrown = await sa.recordScorePostedActivity(mk(async () => { throw new Error("boom"); }), args);
  const noOrg = await sa.recordScorePostedActivity(mk(async () => ({ error: null })), { ...args, orgId: null });
  let inserted: any = null; await sa.recordScorePostedActivity(mk(async () => ({ error: null })) , args); const cap = { from: () => ({ insert: async (r: any) => { inserted = r; return { error: null }; } }) } as any; await sa.recordScorePostedActivity(cap, args);
  const fake = (rep: any, usr: any, repErr = false) => ({ from: (t: string) => ({ select: () => ({ eq: () => ({ maybeSingle: async () => (t === "reps" ? { data: rep, error: repErr ? { message: "x" } : null } : { data: usr, error: null }) }) }) }) }) as any;
  const r1 = await ch.resolveCallerHierarchy(fake({ company_id: CO_A, org_id: ORG_A, office_id: null }, null), U(1));
  const r2 = await ch.resolveCallerHierarchy(fake(null, { company_id: CO_B, org_id: ORG_B, office_id: OFF_A }), U(1));
  const r3 = await ch.resolveCallerHierarchy(fake({ company_id: CO_A, org_id: null, office_id: null }, null), U(1), { defaultOrgId: DEFAULT_ORG });
  const r4 = await ch.resolveCallerHierarchy(fake({ company_id: null, org_id: ORG_A }, null), U(1));
  const r5 = await ch.resolveCallerHierarchy(fake(null, null, true), U(1));
  const r6 = await ch.resolveCallerHierarchy(fake(null, null), "");
  return [
    { id: "unit-activity-success", pass: ok.logged === true && inserted?.type === "score_posted" && inserted?.payload?.job_id === "j1" && inserted?.call_id === "c1" && inserted?.org_id === ORG_A, detail: JSON.stringify(inserted)?.slice(0, 140) },
    { id: "unit-activity-error-as-data-is-not-success", pass: errData.logged === false && errData.error_code === "23514", detail: JSON.stringify(errData) },
    { id: "unit-activity-exception-is-not-success", pass: thrown.logged === false && thrown.error_code === "exception", detail: JSON.stringify(thrown) },
    { id: "unit-activity-missing-org-is-recorded-failure", pass: noOrg.logged === false && noOrg.error_code === "missing_org", detail: JSON.stringify(noOrg) },
    { id: "unit-hierarchy-reps-only", pass: r1.ok && r1.hierarchy.company_id === CO_A && r1.hierarchy.office_id === null, detail: JSON.stringify(r1) },
    { id: "unit-hierarchy-users-fallback", pass: r2.ok && r2.hierarchy.company_id === CO_B && r2.hierarchy.office_id === OFF_A, detail: JSON.stringify(r2) },
    { id: "unit-hierarchy-default-org-only-fills-missing-org", pass: r3.ok && r3.hierarchy.org_id === DEFAULT_ORG && r3.hierarchy.company_id === CO_A, detail: JSON.stringify(r3) },
    { id: "unit-hierarchy-missing-company-refused", pass: !r4.ok && r4.error === "caller_hierarchy_incomplete" && (r4 as any).missing.includes("company_id"), detail: JSON.stringify(r4) },
    { id: "unit-hierarchy-lookup-error-is-not-missing", pass: !r5.ok && r5.error === "hierarchy_lookup_failed", detail: JSON.stringify(r5) },
    { id: "unit-hierarchy-empty-caller-refused", pass: !r6.ok, detail: JSON.stringify(r6) },
  ];
}

// ───────────── e2e: the real app ─────────────
async function e2eSuite(root: string, stubUrl: string, w: { cur: World }): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Awaited<ReturnType<typeof serve>> | null = null;
  try {
    app = await serve(root, stubUrl);
    const A = app.base;
    const fresh = () => { w.cur = world(); return w.cur; };

    // control
    let W = fresh();
    const anon = await call(A, "POST", "/v1/upload/finalize", null, finalizeBody(U(1)));
    add("e2e-control-anonymous-finalize-401", anon.status === 401, `status=${anon.status}`);

    // H1: Day-38 shape — canonical reps row, NO users row
    W = fresh(); W.reps[U(1)] = { id: U(1), org_id: ORG_A, company_id: CO_A, office_id: null };
    let r = await call(A, "POST", "/v1/upload/finalize", U(1), finalizeBody(U(1)));
    let row = callsPosted(W, U(1))[0]?.body;
    add("h1-finalize-reps-only-caller-is-company-stamped", r.status === 200 && row?.company_id === CO_A && row?.org_id === ORG_A && row?.user_id === U(1), `status=${r.status} company=${row?.company_id === CO_A} org=${row?.org_id === ORG_A}`);
    add("h1-global-default-org-not-stamped-over-callers-org", !!row && row.org_id !== DEFAULT_ORG, `org=${row?.org_id}`);
    add("h1-office-optional-omitted-when-caller-has-none", !!row && !("office_id" in row), `keys=${row ? Object.keys(row).filter((k) => k.endsWith("_id")).join(",") : ""}`);

    // H2: legacy users-only caller
    W = fresh(); W.users[U(2)] = { id: U(2), org_id: ORG_B, company_id: CO_B, office_id: OFF_A };
    r = await call(A, "POST", "/v1/upload/finalize", U(2), finalizeBody(U(2))); row = callsPosted(W, U(2))[0]?.body;
    add("h2-finalize-legacy-users-only-caller-is-stamped", r.status === 200 && row?.company_id === CO_B && row?.office_id === OFF_A && row?.org_id === ORG_B, `status=${r.status}`);

    // H3: reps wins over users
    W = fresh(); W.reps[U(3)] = { id: U(3), org_id: ORG_A, company_id: CO_A, office_id: OFF_A }; W.users[U(3)] = { id: U(3), org_id: ORG_B, company_id: CO_B, office_id: null };
    r = await call(A, "POST", "/v1/upload/finalize", U(3), finalizeBody(U(3))); row = callsPosted(W, U(3))[0]?.body;
    add("h3-reps-takes-precedence-over-legacy-users", r.status === 200 && row?.company_id === CO_A && row?.org_id === ORG_A && row?.office_id === OFF_A, `company=${row?.company_id === CO_A}`);

    // H4: manager with no office is stamped (office optional)
    W = fresh(); W.reps[U(4)] = { id: U(4), org_id: ORG_A, company_id: CO_A, office_id: null, tier: "Manager" };
    r = await call(A, "POST", "/v1/upload/finalize", U(4), finalizeBody(U(4))); row = callsPosted(W, U(4))[0]?.body;
    add("h4-manager-without-office-is-company-stamped", r.status === 200 && row?.company_id === CO_A && !("office_id" in row), `status=${r.status}`);

    // H5: spoofed hierarchy / ownership ignored
    W = fresh(); W.reps[U(5)] = { id: U(5), org_id: ORG_A, company_id: CO_A, office_id: null };
    r = await call(A, "POST", "/v1/upload/finalize", U(5), finalizeBody(U(5), { company_id: CO_B, org_id: ORG_B, office_id: OFF_A, user_id: U(99) })); row = callsPosted(W, U(5))[0]?.body;
    add("h5-client-supplied-hierarchy-and-owner-ignored", r.status === 200 && row?.company_id === CO_A && row?.org_id === ORG_A && !("office_id" in row) && row?.user_id === U(5), `company=${row?.company_id === CO_A} owner=${row?.user_id === U(5)}`);

    // H6: missing company -> refused BEFORE any call/job, object removed
    W = fresh(); W.reps[U(6)] = { id: U(6), org_id: ORG_A, company_id: null, office_id: null };
    const fb6 = finalizeBody(U(6)); r = await call(A, "POST", "/v1/upload/finalize", U(6), fb6);
    add("h6-missing-company-refused-422-stable-error", r.status === 422 && r.body?.error === "upload_hierarchy_incomplete", `status=${r.status} ${JSON.stringify(r.body)}`);
    add("h6-no-call-or-job-row-created-when-refused", callsPosted(W, U(6)).length === 0 && mine(W, "jobs", U(6)).length === 0, `posts=${W.posts.filter((p) => p.body?.user_id === U(6)).map((p) => p.table).join(",")}`);
    add("h6-uploaded-object-removed-bounded-cleanup", r.body?.object_removed === true && W.storage.filter((s) => s.startsWith("DELETE")).length === 1, `removed=${r.body?.object_removed} deletes=${W.storage.filter((s) => s.startsWith("DELETE")).length}`);
    add("h6-refusal-body-does-not-leak-internals", !JSON.stringify(r.body).match(/company_id|org_id|reps|users|d39-bucket|supabase/i), JSON.stringify(r.body));

    // H7: cleanup failure is reported, never hidden, and still creates nothing
    W = fresh(); W.reps[U(7)] = { id: U(7), org_id: ORG_A, company_id: null }; W.storageRemoveError = true;
    r = await call(A, "POST", "/v1/upload/finalize", U(7), finalizeBody(U(7)));
    add("h7-cleanup-failure-reported-not-hidden", r.status === 422 && r.body?.object_removed === false && callsPosted(W, U(7)).length === 0, `status=${r.status} removed=${r.body?.object_removed}`);

    // H8: lookup error != unscoped
    W = fresh(); W.repsError = true;
    r = await call(A, "POST", "/v1/upload/finalize", U(8), finalizeBody(U(8)));
    add("h8-lookup-error-is-503-creates-nothing-and-keeps-the-object-for-retry", r.status === 503 && r.body?.error === "hierarchy_lookup_failed" && callsPosted(W, U(8)).length === 0 && !W.storage.some((x) => x.startsWith("DELETE")), `status=${r.status} ${JSON.stringify(r.body)}`);

    // H9: direct upload path
    W = fresh(); W.reps[U(9)] = { id: U(9), org_id: ORG_A, company_id: CO_A, office_id: OFF_A };
    const form = () => { const fd = new FormData(); fd.append("file", new Blob([new Uint8Array(WAV)], { type: "audio/wav" }), "d39-direct.wav"); return fd; };
    r = await call(A, "POST", "/v1/upload", U(9), undefined, form()); row = callsPosted(W, U(9))[0]?.body;
    add("h9-direct-upload-is-company-stamped", r.status === 200 && row?.company_id === CO_A && row?.office_id === OFF_A && row?.org_id === ORG_A && row?.user_id === U(9), `status=${r.status}`);
    W = fresh(); W.reps[U(10)] = { id: U(10), org_id: ORG_A, company_id: null };
    r = await call(A, "POST", "/v1/upload", U(10), undefined, form());
    add("h9-direct-upload-refused-before-storing-anything", r.status === 422 && r.body?.error === "upload_hierarchy_incomplete" && callsPosted(W, U(10)).length === 0 && !W.storage.some((s) => s.startsWith("POST") || s.startsWith("PUT")), `status=${r.status} storage=${W.storage.join("|")}`);

    // H10: POST /v1/calls uses the same resolver
    W = fresh(); W.reps[U(11)] = { id: U(11), org_id: ORG_A, company_id: CO_A, office_id: null };
    r = await call(A, "POST", "/v1/calls", U(11), { userId: U(11), userEmail: "d39@example.com", fileName: "d39.wav", storagePath: `${U(11)}/${crypto.randomUUID()}.wav`, sizeBytes: WAV.length }); row = callsPosted(W, U(11))[0]?.body;
    add("h10-post-calls-auth-first-rep-no-longer-refused-and-stamped", r.status === 200 && row?.company_id === CO_A && row?.org_id === ORG_A, `status=${r.status} ${JSON.stringify(r.body)?.slice(0, 100)}`);
    W = fresh(); W.reps[U(12)] = { id: U(12), org_id: ORG_A, company_id: null };
    r = await call(A, "POST", "/v1/calls", U(12), { userId: U(12), userEmail: "d39@example.com", fileName: "d39.wav", storagePath: `${U(12)}/${crypto.randomUUID()}.wav`, sizeBytes: 1 });
    add("h10-post-calls-missing-company-refused", r.status === 400 && r.body?.error === "user_missing_company" && callsPosted(W, U(12)).length === 0, `status=${r.status} ${JSON.stringify(r.body)}`);

    // F-38-2: score job -> exactly one score_posted
    W = fresh(); W.reps[U(13)] = { id: U(13), org_id: ORG_A, company_id: CO_A, office_id: null };
    r = await call(A, "POST", "/v1/upload/finalize", U(13), finalizeBody(U(13)));
    for (let i = 0; i < 40 && !W.patches.some((p) => p.table === "jobs" && p.body?.status === "succeeded" && p.body?.result?.overall !== undefined); i++) await sleep(250);
    await sleep(800);
    const acts = mine(W, "activities", U(13));
    const scoreJob = mine(W, "jobs", U(13)).find((j: any) => j.kind === "score");
    add("a1-score-job-posts-exactly-one-score-posted-activity", acts.length === 1 && acts[0]?.type === "score_posted" && acts[0]?.call_id === callsPosted(W, U(13))[0]?.body?.id && acts[0]?.org_id === ORG_A, `acts=${acts.map((a) => a.type).join(",")} (finalize=${r.status})`);
    add("a1-activity-type-is-never-call_scored", !acts.some((a) => a.type === "call_scored"), acts.map((a) => a.type).join(","));
    add("a1-activity-payload-links-the-score-job", !!scoreJob && acts[0]?.payload?.job_id === scoreJob.id && acts[0]?.payload?.source === "score_worker", `scoreJob=${scoreJob?.id} payload=${JSON.stringify(acts[0]?.payload)} jobsPosted=${mine(W, "jobs", U(13)).map((j: any) => j.kind).join(",")}`);

    // F-38-2: failing insert is observable, call stays scored
    W = fresh(); W.activitiesError = true; W.reps[U(14)] = { id: U(14), org_id: ORG_A, company_id: CO_A, office_id: null };
    await call(A, "POST", "/v1/upload/finalize", U(14), finalizeBody(U(14)));
    for (let i = 0; i < 40 && !W.patches.some((p) => p.table === "jobs" && p.body?.result?.activity); i++) await sleep(250);
    const failPatch = W.patches.find((p) => p.table === "jobs" && p.body?.result?.activity)?.body; // only the activities-error world produces this
    add("a2-activity-insert-failure-is-observable-on-the-job", failPatch?.result?.activity?.logged === false && failPatch?.result?.activity?.error_code === "23514", JSON.stringify(failPatch?.result?.activity));
    add("a2-call-stays-scored-when-activity-logging-fails", W.patches.some((p) => p.table === "calls" && p.body?.status === "scored"), "");
  } catch (e: any) { add("e2e-suite-executed", false, String(e?.message || e).slice(0, 240)); }
  finally { app?.kill(); }
  return out;
}

// ───────────── mutation harness (COPY of src/) ─────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string, repl: string, all = false) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, all ? t.split(find).join(repl) : t.replace(find, repl));
}
const SRV = "src/server.ts", LIB = "src/lib/callerHierarchy.ts", ACT = "src/lib/scoreActivity.ts";
type Mutation = { name: string; file: string; find: string; repl: string; all?: boolean; suite: "unit" | "e2e"; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "users-only hierarchy lookup (the original F-38-1 bug)", file: LIB, find: 'client.from("reps").select("org_id, company_id, office_id")', repl: 'client.from("users").select("org_id, company_id, office_id")', suite: "e2e", expect: "h1-finalize-reps-only-caller-is-company-stamped" },
  { name: "refusal bypassed — unscoped creation allowed", file: SRV, find: "if (!hier.ok) {\n      let objectRemoved = false;", repl: "if (false as boolean) {\n      let objectRemoved = false;", suite: "e2e", expect: "h6-missing-company-refused-422-stable-error" },
  { name: "rejected finalize leaves the uploaded object (no cleanup)", file: SRV, find: "const rm = await supabase.storage.from(BUCKET).remove([path]);\n          objectRemoved = !rm.error;", repl: "objectRemoved = false;", suite: "e2e", expect: "h6-uploaded-object-removed-bounded-cleanup" },
  { name: "client-supplied company_id honoured", file: SRV, find: "      company_id: hier.hierarchy.company_id,\n      ...(hier.hierarchy.office_id", repl: "      company_id: (req.body as any)?.company_id ?? hier.hierarchy.company_id,\n      ...(hier.hierarchy.office_id", suite: "e2e", expect: "h5-client-supplied-hierarchy-and-owner-ignored" },
  { name: "global DEFAULT_ORG_ID overrides the caller's org (finalize)", file: SRV, find: "      org_id: hier.hierarchy.org_id,\n      company_id: hier.hierarchy.company_id,\n      ...(hier.hierarchy.office_id ? { office_id: hier.hierarchy.office_id } : {}),\n      filename,", repl: "      org_id: DEFAULT_ORG_ID,\n      company_id: hier.hierarchy.company_id,\n      ...(hier.hierarchy.office_id ? { office_id: hier.hierarchy.office_id } : {}),\n      filename,", suite: "e2e", expect: "h1-global-default-org-not-stamped-over-callers-org" },
  { name: "direct upload stores before resolving hierarchy / skips it", file: SRV, find: "      const hier = await resolveCallerHierarchy(supabase, userId, { defaultOrgId: DEFAULT_ORG_ID });\n      if (!hier.ok) {\n        console.warn(\"[upload] direct upload refused:\", hier.error);", repl: "      const hier = await resolveCallerHierarchy(supabase, userId, { defaultOrgId: DEFAULT_ORG_ID });\n      if (false as boolean) {\n        console.warn(\"[upload] direct upload refused:\", (hier as any).error);", suite: "e2e", expect: "h9-direct-upload-refused-before-storing-anything" },
  { name: "lookup error treated as a missing hierarchy / proceeds", file: LIB, find: 'if (rep.error) return { ok: false, error: "hierarchy_lookup_failed" };', repl: "", suite: "e2e", expect: "h8-lookup-error-is-503-creates-nothing-and-keeps-the-object-for-retry" },
  { name: "activity type reverted to call_scored (violates the CHECK constraint)", file: ACT, find: 'export const SCORE_ACTIVITY_TYPE = "score_posted" as const;', repl: 'export const SCORE_ACTIVITY_TYPE = "call_scored" as unknown as "score_posted";', suite: "e2e", expect: "a1-activity-type-is-never-call_scored" },
  { name: "ignored `.error` on the activity insert (the original F-38-2 bug)", file: ACT, find: 'if (error) return { logged: false, error_code: String((error as { code?: string }).code || "insert_failed") };', repl: "", suite: "unit", expect: "unit-activity-error-as-data-is-not-success" },
  { name: "failure no longer surfaced on the job result", file: SRV, find: 'activity: { logged: false, error_code: activity.error_code },', repl: "", suite: "e2e", expect: "a2-activity-insert-failure-is-observable-on-the-job" },
  { name: "duplicate activity posted per score job", file: SRV, find: "    if (!activity.logged) {\n      console.error(\"[activity] score_posted NOT logged:\"", repl: "    await recordScorePostedActivity(supabase, { callId, jobId, orgId: (callRow as any)?.org_id ?? DEFAULT_ORG_ID ?? null, actorUserId: (callRow as any)?.user_id ?? null, overall: result.overall, stages: { intro: 1, discovery: 1, objection: 1, close: 1 }, model: result.model });\n    if (!activity.logged) {\n      console.error(\"[activity] score_posted NOT logged:\"", suite: "e2e", expect: "a1-score-job-posts-exactly-one-score-posted-activity" },
];

function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) { console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${(!c.pass || process.env.VALIDATOR_VERBOSE) && c.detail ? " — " + c.detail : ""}`); if (!c.pass) bad++; }
  return bad;
}
async function main() {
  if (process.argv[2] === "--serve") {
    process.env.GRAVIX_API_NO_LISTEN = "1";
    const mod = await import(path.join(process.argv[3], "src/server.ts"));
    const srv = http.createServer(mod.app);
    srv.listen(0, "127.0.0.1", () => console.log(`SERVING_PORT=${(srv.address() as AddressInfo).port}`));
    return;
  }
  console.log("Go Live Day 39 — upload hierarchy + score activity (F-38-1 / F-38-2), child process + loopback only\n");
  const w = { cur: world() };
  const stub = await startStub(w);
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`;
  let failures = 0;
  const savedEnv = { ...process.env };
  Object.assign(process.env, { SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", OPENAI_API_KEY: "validator-placeholder", NODE_ENV: "test", DEFAULT_ORG_ID: DEFAULT_ORG });
  try {
    failures += report("Unit (fake clients):", await unitSuite(ROOT));
    failures += report("\nEnd-to-end (real app, loopback stub):", await e2eSuite(ROOT, stubUrl, w));
    console.log("\nMutation proof — each mutation of a COPY must fail the named check:");
    for (const m of (process.env.VALIDATOR_SKIP_MUTATIONS ? [] : MUTATIONS)) {
      let dir = "";
      try {
        dir = copyTree(ROOT); mutate(dir, m.file, m.find, m.repl, m.all);
        const checks = m.suite === "unit" ? await unitSuite(dir) : await e2eSuite(dir, stubUrl, w);
        const hit = checks.find((c) => c.id === m.expect);
        const control = checks.find((c) => c.id === (m.suite === "unit" ? "unit-activity-success" : "e2e-control-anonymous-finalize-401"));
        const caught = !!hit && !hit.pass && !!control?.pass;
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found / harness dead)"}`);
        if (!caught) failures++;
      } catch (e: any) { console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++; }
      finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally { Object.assign(process.env, savedEnv); stub.close(); fs.rmSync(EMPTY_CWD, { recursive: true, force: true }); }
  if (failures) { console.log(`\n❌ upload hierarchy / activity validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ upload hierarchy / activity validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
