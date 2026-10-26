// Go Live Day 42 — Sparring safety + tenant repair (F-41-1 … F-41-8). Network-free.
//
// Executes the REAL provider resolver, the REAL limiter/validator and the REAL Express app
// (child process) against a stateful loopback PostgREST stub and a loopback "provider canary"
// (OPENAI_BASE_URL) that records any attempted provider egress. No real DB, Auth, storage or AI
// provider is ever contacted; the child runs in an empty cwd with placeholder keys.
//
//   G1  provider contract: staging/non-prod default = stub; production unset/invalid refuses to boot;
//       paid providers need their key; /v1/version reports the resolved label (never a key);
//       the turn response carries non-secret provenance (provider + fallback)
//   G2  analyse-turn, log and the /messages alias are CLOSED (410): zero provider egress, zero writes
//   G3  verified caller only (body repId/org/company/hierarchy and x-org-id ignored); no reps upsert
//       (existing rep name preserved); missing rep/hierarchy refused with stable codes and no writes;
//       canonical (reps-first) company/org stamping at creation AND completion
//   G4  /score derives score/XP/duration/turns from stored state: client total/meta/xp/org ignored;
//       zero-turn sessions earn nothing; assignment completion is owner-scoped
//   G5  message length, per-user turn/session rate limits and per-session turn cap, keyed by the
//       verified user (not IP/headers); refusals call no provider and write nothing
//   cross-tenant and anonymous denial on every session route; managers cannot take turns
// Non-vacuity: each source mutation of a COPY of src/ MUST fail the named check.
//
// Run: npm run validate:sparring-safety   (tsx; child processes + loopback only)

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
const AS = (n: number) => `bbbbbbbb-0000-4000-8000-${String(n).padStart(12, "0")}`;
const ORG_A = "dddddddd-0000-4000-8000-00000000000a", CO_A = "dddddddd-0000-4000-8000-00000000000b", OFF_A = "dddddddd-0000-4000-8000-00000000000c";
const ORG_B = "eeeeeeee-0000-4000-8000-00000000000a", CO_B = "eeeeeeee-0000-4000-8000-00000000000b";
const DEFAULT_ORG = "99999999-0000-4000-8000-0000000000de";
const PROXY_SECRET = "planted-proxy-secret-d42";
const PLANTED_KEY = "sk-planted-never-print-d42";
const REP_A = U(1), REP_B = U(2), MGR_A = U(3), MGR_B = U(4), REP_NOREP = U(5), REP_NOCO = U(6), REP_LOOKUP_ERR = U(7);
const RL_TURNS = U(11), RL_OTHER = U(12), RL_SESS = U(13), SCORE_U = U(14), SCORE_U2 = U(15), CAP_U = U(16);

// ───────────── stateful loopback PostgREST stub ─────────────
type Row = Record<string, any>;
type Write = { method: string; table: string; body: any; query: string };
type World = { tables: Record<string, Row[]>; writes: Write[]; failReps: Set<string>; seq: number };
const newWorld = (): World => ({ tables: {}, writes: [], failReps: new Set(), seq: 0 });
const T = (W: World, t: string) => (W.tables[t] ||= []);
function startStub(w: { cur: World }): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const chunks: Buffer[] = []; req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const W = w.cur; const u = new URL(req.url || "/", "http://stub"); const raw = Buffer.concat(chunks).toString("utf8");
      const accept = String(req.headers["accept"] || ""), prefer = String(req.headers["prefer"] || "");
      const wantsObject = accept.includes("vnd.pgrst.object"), wantsRep = prefer.includes("return=representation");
      const send = (c: number, b?: unknown) => { res.writeHead(c, { "content-type": "application/json" }); res.end(b === undefined ? undefined : JSON.stringify(b)); };
      if (u.pathname.startsWith("/auth/")) return send(400, { error: "invalid_grant" });
      if (!u.pathname.startsWith("/rest/v1/")) return send(404, {});
      const table = u.pathname.replace("/rest/v1/", "");
      const body = (() => { try { return JSON.parse(raw); } catch { return null; } })();
      const match = (r: Row) => { for (const [k, v] of u.searchParams) {
        if (["select", "order", "limit", "offset", "on_conflict", "columns"].includes(k)) continue;
        const m = /^(eq|neq|in|is)\.(.*)$/.exec(v); if (!m) continue;
        const val = r[k];
        if (m[1] === "eq" && String(val) !== m[2]) return false;
        if (m[1] === "neq" && String(val) === m[2]) return false;
        if (m[1] === "in" && !m[2].replace(/^\(|\)$/g, "").split(",").includes(String(val))) return false;
        if (m[1] === "is" && !(m[2] === "null" ? val == null : String(val) === m[2])) return false;
      } return true; };
      const out = (rows: Row[], code = 200) => wantsObject
        ? (rows.length === 1 ? send(code, rows[0]) : send(406, { code: "PGRST116", message: "JSON object requested", details: `The result contains ${rows.length} rows`, hint: null }))
        : send(code, rows);
      if (req.method === "POST") {
        W.writes.push({ method: "POST", table, body, query: u.search });
        const items: Row[] = Array.isArray(body) ? body : [body];
        if (table === "sparring_sessions" && items.some((i) => !T(W, "reps").some((r) => r.id === i.rep_id))) return send(409, { code: "23503", message: "fk" });
        const outRows: Row[] = [];
        for (const it of items) {
          const conflictCol = u.searchParams.get("on_conflict"); const merge = prefer.includes("merge-duplicates");
          const ex = conflictCol ? T(W, table).find((r) => String(r[conflictCol]) === String(it[conflictCol])) : undefined;
          if (ex && merge) { Object.assign(ex, it); outRows.push(ex); continue; }
          const row = { id: crypto.randomUUID(), created_at: new Date(1_800_000_000_000 + ++W.seq * 1000).toISOString(), ...it };
          T(W, table).push(row); outRows.push(row);
        }
        if (wantsRep) return out(outRows, 201);
        res.writeHead(201); return res.end();
      }
      if (req.method === "PATCH") {
        W.writes.push({ method: "PATCH", table, body, query: u.search });
        const hit = T(W, table).filter(match); for (const r of hit) Object.assign(r, body);
        if (wantsRep) return out(hit); res.writeHead(204); return res.end();
      }
      if (req.method === "DELETE") { W.writes.push({ method: "DELETE", table, body: null, query: u.search }); res.writeHead(204); return res.end(); }
      // reads
      if (table === "reps" && W.failReps.size) { const idq = (u.searchParams.get("id") || "").replace(/^eq\./, ""); if (W.failReps.has(idq)) return send(500, { message: "planted reps failure" }); }
      let rows = T(W, table).filter(match);
      const ord = u.searchParams.get("order"); if (ord) { const [c, d] = ord.split("."); rows = [...rows].sort((a, b) => String(a[c]).localeCompare(String(b[c])) * (d === "desc" ? -1 : 1)); }
      const lim = Number(u.searchParams.get("limit")); if (lim > 0) rows = rows.slice(0, lim);
      return out(rows);
    });
  });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}
// provider canary: records any attempted provider egress and answers 400 (no SDK retry storm)
function startCanary(hits: string[]): Promise<http.Server> {
  const srv = http.createServer((req, res) => { req.resume(); req.on("end", () => { hits.push(`${req.method} ${req.url}`); res.writeHead(400, { "content-type": "application/json" }); res.end(JSON.stringify({ error: { message: "canary" } })); }); });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}

type Served = { base: string; kill: () => void; child: ChildProcess };
function childEnv(stubUrl: string, canaryUrl: string, extra: Record<string, string>): Record<string, string> {
  return {
    PATH: process.env.PATH || "", HOME: process.env.HOME || "", NODE_ENV: "test", GRAVIX_API_NO_LISTEN: "1",
    SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", OPENAI_BASE_URL: canaryUrl + "/v1",
    PROXY_SHARED_SECRET: PROXY_SECRET, DEFAULT_ORG_ID: DEFAULT_ORG, ...extra,
  };
}
function spawnChild(root: string, env: Record<string, string>): ChildProcess {
  return spawn(path.join(ROOT, "node_modules/.bin/tsx"), [__filename, "--serve", root], { env, cwd: EMPTY_CWD, stdio: ["ignore", "pipe", "pipe"] });
}
async function serve(root: string, stubUrl: string, canaryUrl: string, extra: Record<string, string>): Promise<Served> {
  const child = spawnChild(root, childEnv(stubUrl, canaryUrl, extra));
  const port: number = await new Promise((resolve, reject) => {
    let buf = ""; const t = setTimeout(() => reject(new Error("child server did not start: " + buf.slice(-240))), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); const m = buf.match(/SERVING_PORT=(\d+)/); if (m) { clearTimeout(t); resolve(Number(m[1])); } });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", (c) => { clearTimeout(t); reject(new Error(`child exited ${c}: ${buf.slice(-240)}`)); });
  });
  return { base: `http://127.0.0.1:${port}`, child, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } } };
}
// boot probe: does the app boot (SERVING_PORT) or refuse (exit != 0)? returns the message for secret scans
async function bootProbe(root: string, stubUrl: string, canaryUrl: string, extra: Record<string, string>): Promise<{ booted: boolean; output: string }> {
  const child = spawnChild(root, childEnv(stubUrl, canaryUrl, extra));
  return await new Promise((resolve) => {
    let buf = ""; const done = (booted: boolean) => { clearTimeout(t); try { child.kill("SIGKILL"); } catch { /* gone */ } resolve({ booted, output: buf }); };
    const t = setTimeout(() => done(false), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); if (/SERVING_PORT=\d+/.test(buf)) done(true); });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", () => done(false));
  });
}
async function call(base: string, method: string, p: string, uid: string | null, body?: unknown, hdr: Record<string, string> = {}) {
  const headers: Record<string, string> = { ...(uid ? { "x-user-id": uid, "x-proxy-secret": PROXY_SECRET } : {}), ...(body !== undefined ? { "content-type": "application/json" } : {}), ...hdr };
  const ctl = new AbortController(); const timer = setTimeout(() => ctl.abort(), 15000);
  try { const r = await fetch(base + p, { method, headers, body: body !== undefined ? JSON.stringify(body) : undefined, signal: ctl.signal }); let b: any = null; try { b = await r.json(); } catch { /* none */ } return { status: r.status, body: b, retryAfter: r.headers.get("retry-after") }; }
  catch { return { status: 0, body: null, retryAfter: null }; } finally { clearTimeout(timer); }
}
const writesTo = (W: World, table: string, from = 0) => W.writes.slice(from).filter((x) => x.table === table);
const noWrites = (W: World, from: number) => W.writes.length === from;

function seed(W: World) {
  const rep = (id: string, extra: Row = {}) => T(W, "reps").push({ id, name: "Alice " + id.slice(-2), tier: "Rep", org_id: ORG_A, company_id: CO_A, office_id: null, ...extra });
  rep(REP_A); rep(REP_B, { org_id: ORG_B, company_id: CO_B }); rep(MGR_A, { tier: "Manager", office_id: OFF_A }); rep(MGR_B, { tier: "Manager", org_id: ORG_B, company_id: CO_B });
  rep(REP_NOCO, { company_id: null }); rep(REP_LOOKUP_ERR);
  for (const id of [RL_TURNS, RL_OTHER, RL_SESS, SCORE_U, SCORE_U2, CAP_U]) rep(id);
  // legacy `users` rows that DISAGREE with the canonical reps rows (must never win)
  T(W, "users").push({ id: REP_A, company_id: CO_B, org_id: ORG_B, office_id: OFF_A });
  T(W, "users").push({ id: REP_NOREP, company_id: CO_A, org_id: ORG_A, office_id: null }); // users-only: no reps row
}

// ───────────── unit: the REAL resolver / limiter / validator ─────────────
async function unitSuite(root: string): Promise<Check[]> {
  const sp = await import(path.join(root, "src/lib/sparringProvider.ts"));
  const lim = await import(path.join(root, "src/lib/sparringLimits.ts"));
  const refuses = (env: Record<string, string>, code: string) => { try { sp.assertSparringProviderConfig(env); return false; } catch (e: any) { return e?.code === code && !String(e.message).includes(PLANTED_KEY) && !/zzz-secret-value/.test(String(e.message)); } };
  const label = (env: Record<string, string>) => { try { return sp.assertSparringProviderConfig(env); } catch { return "THROWS"; } };
  const prod = { NODE_ENV: "production" }, stg = { NODE_ENV: "production", APP_ENV: "staging" };
  let t = 1_000_000; const clock = () => t; const L = lim.createSparringLimiter(clock);
  const burst = (user: string, scope: string, n: number) => { let last: any = { ok: true }; for (let i = 0; i < n; i++) last = L.hit(user, scope); return last; };
  const r1 = burst("u1", "turns", lim.SPARRING_LIMITS.turnsPerMinute), r2 = L.hit("u1", "turns"), r3 = L.hit("u2", "turns");
  t += 61_000; const r4 = L.hit("u1", "turns");
  return [
    { id: "unit-staging-unset-defaults-stub", pass: label({ NODE_ENV: "test" }) === "stub" && label(stg) === "stub", detail: `${label({ NODE_ENV: "test" })}/${label(stg)}` },
    { id: "unit-staging-unset-stays-stub-even-with-paid-keys-present", pass: label({ ...stg, OPENAI_API_KEY: PLANTED_KEY, ANTHROPIC_API_KEY: PLANTED_KEY }) === "stub", detail: "" },
    { id: "unit-production-unset-refused", pass: refuses(prod, "sparring_provider_required_in_production") && refuses({ ...prod, OPENAI_API_KEY: PLANTED_KEY }, "sparring_provider_required_in_production"), detail: "" },
    { id: "unit-production-whitespace-selector-is-unset", pass: refuses({ ...prod, SPARRING_BRAIN_PROVIDER: "   " }, "sparring_provider_required_in_production"), detail: "" },
    { id: "unit-invalid-refused-in-every-environment", pass: refuses({ ...prod, SPARRING_BRAIN_PROVIDER: "gpt5" }, "sparring_provider_invalid") && refuses({ NODE_ENV: "test", SPARRING_PROVIDER: "gpt5" }, "sparring_provider_invalid"), detail: "" },
    { id: "unit-openai-requires-key", pass: refuses({ ...prod, SPARRING_BRAIN_PROVIDER: "openai" }, "sparring_provider_openai_requires_key") && label({ ...prod, SPARRING_BRAIN_PROVIDER: "openai", OPENAI_API_KEY: PLANTED_KEY }) === "openai", detail: "" },
    { id: "unit-claude-requires-key", pass: refuses({ ...prod, SPARRING_BRAIN_PROVIDER: "claude" }, "sparring_provider_claude_requires_key") && label({ ...prod, SPARRING_BRAIN_PROVIDER: "claude", ANTHROPIC_API_KEY: PLANTED_KEY }) === "claude", detail: "" },
    { id: "unit-stub-always-allowed-and-canonical-selector-wins", pass: label({ ...prod, SPARRING_BRAIN_PROVIDER: "stub" }) === "stub" && label({ ...prod, SPARRING_BRAIN_PROVIDER: "stub", SPARRING_PROVIDER: "openai" }) === "stub" && label({ ...prod, SPARRING_PROVIDER: "stub" }) === "stub", detail: "" },
    { id: "unit-describe-never-throws-and-never-reports-a-key", pass: sp.describeSparringProvider({ ...prod, SPARRING_BRAIN_PROVIDER: "bad" }) === "unresolved" && !JSON.stringify(sp.describeSparringProvider({ ...prod, SPARRING_BRAIN_PROVIDER: "openai", OPENAI_API_KEY: PLANTED_KEY })).includes(PLANTED_KEY), detail: "" },
    { id: "unit-limiter-blocks-after-max-per-user-window", pass: r1.ok === true && r2.ok === false && r2.retryAfterSec >= 1, detail: JSON.stringify(r2) },
    { id: "unit-limiter-is-per-user", pass: r3.ok === true, detail: "" },
    { id: "unit-limiter-window-resets", pass: r4.ok === true, detail: "" },
    { id: "unit-limiter-empty-user-refused", pass: L.hit("", "turns").ok === false, detail: "" },
    { id: "unit-message-validation", pass: lim.validateTurnText("hello").ok === true && lim.validateTurnText("").ok === false && lim.validateTurnText("   ").ok === false && lim.validateTurnText(5).ok === false && (lim.validateTurnText("x".repeat(lim.SPARRING_LIMITS.maxMessageChars + 1)) as any).error === "message_too_long" && lim.validateTurnText("x".repeat(lim.SPARRING_LIMITS.maxMessageChars)).ok === true, detail: "" },
  ];
}

// ───────────── boot probes (production refusal is exercised on the real app) ─────────────
async function bootSuite(root: string, stubUrl: string, canaryUrl: string): Promise<Check[]> {
  const base = { NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "stub" };
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const probes: Array<[string, Record<string, string>, boolean]> = [
    ["boot-control-production-with-explicit-stub-boots", { ...base, SPARRING_BRAIN_PROVIDER: "stub" }, true],
    ["boot-production-unset-refuses", { ...base }, false],
    ["boot-production-openai-without-key-refuses", { ...base, SPARRING_BRAIN_PROVIDER: "openai" }, false],
    ["boot-production-claude-without-key-refuses", { ...base, SPARRING_BRAIN_PROVIDER: "claude" }, false],
    ["boot-production-invalid-refuses", { ...base, SPARRING_BRAIN_PROVIDER: "nonsense" }, false],
    ["boot-staging-unset-boots-as-stub", { ...base, APP_ENV: "staging" }, true],
  ];
  for (const [id, env, shouldBoot] of probes) {
    const r = await bootProbe(root, stubUrl, canaryUrl, { ...env, OPENAI_API_KEY: env.SPARRING_BRAIN_PROVIDER === "openai" ? "" : PLANTED_KEY });
    add(id, r.booted === shouldBoot && !r.output.includes(PLANTED_KEY), `booted=${r.booted} ${shouldBoot ? "" : r.output.replace(/\s+/g, " ").slice(-140)}`);
  }
  return out;
}

// ───────────── e2e A: the real app, stub provider (G2, G3, G4, tenancy) ─────────────
async function e2eCore(root: string, stubUrl: string, canaryUrl: string, w: { cur: World }, hits: string[]): Promise<Check[]> {
  const out: Check[] = []; const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Served | null = null;
  try {
    app = await serve(root, stubUrl, canaryUrl, { SPARRING_BRAIN_PROVIDER: "stub", OPENAI_API_KEY: PLANTED_KEY });
    const A = app.base; w.cur = newWorld(); const W = w.cur; seed(W); hits.length = 0;
    const mk = async (uid: string, body: any = { personaId: "price_sensitive", difficulty: "normal" }, hdr: Record<string, string> = {}) => call(A, "POST", "/v1/sparring/sessions", uid, body, hdr);
    const sess = (id: string) => T(W, "sparring_sessions").find((s) => s.id === id);

    // control + anonymous
    const anonCreate = await call(A, "POST", "/v1/sparring/sessions", null, { personaId: "x", repId: REP_A });
    add("e2e-control-anonymous-create-401", anonCreate.status === 401, `status=${anonCreate.status}`);
    add("e2e-control-personas-public", (await call(A, "GET", "/v1/sparring/personas", null)).status === 200, "");
    const v = await call(A, "GET", "/v1/version", null);
    add("e2e-version-reports-sparring-provider-label-only", v.body?.sparring?.provider === "stub" && !JSON.stringify(v.body).includes(PLANTED_KEY), JSON.stringify(v.body?.sparring));

    // ── G2: closed routes ──
    let w0 = W.writes.length; hits.length = 0;
    const at = await call(A, "POST", "/v1/sparring/analyse-turn", REP_A, { text: "hello" });
    add("g2-analyse-turn-closed-410", at.status === 410 && at.body?.error === "route_removed", `status=${at.status}`);
    add("g2-analyse-turn-makes-no-provider-egress", hits.length === 0, `canary hits=${hits.length}`);
    const lg = await call(A, "POST", "/v1/sparring/log", REP_A, { repId: REP_A, xp: 99999, orgId: ORG_B });
    add("g2-log-closed-410", lg.status === 410, `status=${lg.status}`);
    add("g2-closed-routes-write-nothing", noWrites(W, w0), `writes=${W.writes.length - w0}`);
    add("g2-log-never-awards-client-xp", T(W, "xp_events").length === 0 && T(W, "activities").length === 0, "");
    const anonAt = await call(A, "POST", "/v1/sparring/analyse-turn", null, { text: "x" }), anonLog = await call(A, "POST", "/v1/sparring/log", null, { repId: REP_A, xp: 5 });
    add("g2-closed-routes-still-anonymous-401", anonAt.status === 401 && anonLog.status === 401, `${anonAt.status}/${anonLog.status}`);

    // ── G3: create ──
    const repBefore = JSON.stringify(T(W, "reps").find((r) => r.id === REP_A));
    w0 = W.writes.length;
    const SPOOF_OFFICE = "ffffffff-0000-4000-8000-0000000000ff";
    const c1 = await mk(REP_A, { personaId: "price_sensitive", difficulty: "normal", repId: REP_B, orgId: ORG_B, org_id: ORG_B, company_id: CO_B, office_id: SPOOF_OFFICE }, { "x-org-id": ORG_B });
    const s1 = sess(c1.body?.session?.id);
    add("g3-create-succeeds-for-canonical-rep", c1.status === 200 && !!s1, `status=${c1.status} ${JSON.stringify(c1.body).slice(0, 80)}`);
    add("g3-create-ignores-body-repId-owner-is-verified-caller", s1?.rep_id === REP_A, `rep=${s1?.rep_id}`);
    add("g3-create-stamps-canonical-reps-hierarchy-not-users", s1?.company_id === CO_A && s1?.org_id === ORG_A, `${s1?.company_id}/${s1?.org_id}`);
    // the Day-39 resolver may fill a MISSING office from the legacy users row; it must never take the client's
    add("g3-create-office-never-taken-from-the-client", s1?.office_id !== SPOOF_OFFICE && s1?.meta?.office_id !== SPOOF_OFFICE, `office=${s1?.office_id}`);
    add("g3-create-ignores-spoofed-org-header-and-body-hierarchy", s1?.org_id === ORG_A && s1?.company_id === CO_A && s1?.meta?.company_id === CO_A, `org=${s1?.org_id} meta.company=${s1?.meta?.company_id}`);
    add("g3-no-reps-write-and-existing-rep-not-overwritten", writesTo(W, "reps", w0).length === 0 && JSON.stringify(T(W, "reps").find((r) => r.id === REP_A)) === repBefore, `rep writes=${writesTo(W, "reps", w0).length}`);
    add("g3-existing-rep-name-is-not-Rep", T(W, "reps").find((r) => r.id === REP_A)?.name !== "Rep", "");

    w0 = W.writes.length;
    const cNoRep = await mk(REP_NOREP);
    add("g3-missing-canonical-rep-refused-stable-code", cNoRep.status === 422 && cNoRep.body?.error === "sparring_rep_profile_missing", `${cNoRep.status} ${JSON.stringify(cNoRep.body)}`);
    add("g3-missing-rep-fabricates-nothing", noWrites(W, w0) && !T(W, "reps").some((r) => r.id === REP_NOREP), `writes=${W.writes.length - w0}`);
    w0 = W.writes.length;
    const cNoCo = await mk(REP_NOCO);
    add("g3-missing-company-refused-stable-code", cNoCo.status === 422 && cNoCo.body?.error === "sparring_hierarchy_incomplete", `${cNoCo.status} ${JSON.stringify(cNoCo.body)}`);
    add("g3-incomplete-hierarchy-writes-nothing", noWrites(W, w0), "");
    W.failReps.add(REP_LOOKUP_ERR); w0 = W.writes.length;
    const cErr = await mk(REP_LOOKUP_ERR);
    add("g3-lookup-failure-is-503-not-unscoped", cErr.status === 503 && cErr.body?.error === "hierarchy_lookup_failed" && noWrites(W, w0), `${cErr.status}`);
    W.failReps.delete(REP_LOOKUP_ERR);
    add("g3-errors-do-not-leak-database-text", ![cNoRep, cNoCo, cErr].some((r) => /fk|violat|relation|column|planted/i.test(JSON.stringify(r.body))), "");

    // ── turns + complete (G3 completion, G5 provenance, tenancy) ──
    const sid = s1?.id as string;
    const t1 = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, REP_A, { text: "Hi, thanks for taking my call. What are you using today?", repId: REP_B });
    add("g1-turn-reports-provider-provenance", t1.status === 200 && t1.body?.ai_provider?.provider === "stub" && t1.body?.ai_provider?.fallback === false, JSON.stringify(t1.body?.ai_provider));
    const t2 = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, REP_A, { text: "Our platform cuts onboarding time by 40% and I can show you a customer case study." });
    add("g1-stub-provider-makes-zero-egress-over-turns", t1.status === 200 && t2.status === 200 && hits.length === 0, `hits=${hits.length}`);
    add("g5-turn-pair-persisted-per-turn", T(W, "sparring_turns").filter((x) => x.session_id === sid).length === 4, `rows=${T(W, "sparring_turns").filter((x) => x.session_id === sid).length}`);

    const cross: Array<[string, string, string, any]> = [
      ["turns", "POST", `/v1/sparring/sessions/${sid}/turns`, { text: "hi" }], ["complete", "POST", `/v1/sparring/sessions/${sid}/complete`, {}],
      ["score", "POST", "/v1/sparring/score", { sessionId: sid }], ["micro-score", "POST", `/v1/sparring/sessions/${sid}/micro-score`, {}],
      ["replay", "POST", `/v1/sparring/sessions/${sid}/replay`, { failed_turn: 1 }], ["get", "GET", `/v1/sparring/sessions/${sid}`, undefined],
      ["replay-get", "GET", `/v1/sparring/sessions/${sid}/replay`, undefined], ["analytics", "GET", `/v1/sparring/sessions/${sid}/analytics`, undefined],
    ];
    for (const who of [["rep-other-tenant", REP_B], ["manager-other-tenant", MGR_B]] as const) {
      const w1 = W.writes.length; const res = await Promise.all(cross.map(([, m, p, b]) => call(A, m, p, who[1], b)));
      add(`tenant-${who[0]}-denied-on-every-session-route`, res.every((r) => r.status === 403 || r.status === 404), res.map((r) => r.status).join(","));
      add(`tenant-${who[0]}-denial-writes-nothing`, noWrites(W, w1), `writes=${W.writes.length - w1}`);
    }
    const mTurn = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, MGR_A, { text: "manager speaking as the rep" });
    const mComplete = await call(A, "POST", `/v1/sparring/sessions/${sid}/complete`, MGR_A, {});
    add("tenant-same-tenant-manager-cannot-take-turns-or-complete-for-the-rep", mTurn.status === 403 && mComplete.status === 403, `${mTurn.status}/${mComplete.status}`);
    add("tenant-same-tenant-manager-can-read", (await call(A, "GET", `/v1/sparring/sessions/${sid}`, MGR_A)).status === 200, "");
    const anonRoutes = await Promise.all(cross.map(([, m, p, b]) => call(A, m, p, null, b)));
    add("tenant-anonymous-denied-on-every-session-route", anonRoutes.every((r) => r.status === 401), anonRoutes.map((r) => r.status).join(","));

    const msg = await call(A, "POST", `/v1/sparring/sessions/${sid}/messages`, REP_A, { text: "hi" });
    add("g2-messages-alias-closed-410-and-no-turn-written", msg.status === 410 && T(W, "sparring_turns").filter((x) => x.session_id === sid).length === 4, `status=${msg.status}`);

    // complete: canonical hierarchy kept (users row disagrees: CO_B), never nulled
    const comp = await call(A, "POST", `/v1/sparring/sessions/${sid}/complete`, REP_A, { repId: REP_B, org_id: ORG_B, company_id: CO_B });
    add("g3-complete-succeeds-for-owner", comp.status === 200 && comp.body?.ok === true, `status=${comp.status}`);
    const s1b = sess(sid);
    add("g3-complete-keeps-canonical-company-org-not-users-not-null", s1b?.company_id === CO_A && s1b?.org_id === ORG_A && s1b?.status === "completed", `${s1b?.company_id}/${s1b?.org_id}/${s1b?.status}`);

    // ── G4: /score is server-derived ──
    const playScore = async (uid: string, spoof: boolean) => {
      const cr = await mk(uid); const id = cr.body?.session?.id as string;
      await call(A, "POST", `/v1/sparring/sessions/${id}/turns`, uid, { text: "Thanks for your time. What does your current process look like for onboarding new reps?" });
      await call(A, "POST", `/v1/sparring/sessions/${id}/turns`, uid, { text: "Great, we can reduce ramp time by 30 percent and I would like to book a short demo next week." });
      const before = JSON.stringify(sess(id)?.meta);
      const body: any = { sessionId: id };
      if (spoof) Object.assign(body, { total: 100, durationMs: 1, turns: 999, xp: 9999, xp_awarded: 9999, orgId: ORG_B, company_id: CO_B, meta: { xp_bonus_pending: 5000, streak: 99, xp_committed: false, company_id: CO_B, ended: false, tampered: true } });
      const r = await call(A, "POST", "/v1/sparring/score", uid, body);
      return { id, r, row: sess(id), before };
    };
    const clean = await playScore(SCORE_U, false), spoof = await playScore(SCORE_U2, true);
    add("g4-score-succeeds-clean-and-spoofed", clean.r.status === 200 && spoof.r.status === 200, `${clean.r.status}/${spoof.r.status}`);
    add("g4-client-total-ignored-score-equals-clean-derivation", spoof.row?.total_score === clean.row?.total_score && spoof.row?.total_score !== 100 && typeof spoof.row?.total_score === "number", `clean=${clean.row?.total_score} spoof=${spoof.row?.total_score}`);
    add("g4-client-xp-fields-ignored-session-xp-equals-clean", spoof.row?.xp_awarded === clean.row?.xp_awarded && (spoof.row?.xp_awarded ?? 0) <= 200, `clean=${clean.row?.xp_awarded} spoof=${spoof.row?.xp_awarded}`);
    add("g4-client-meta-not-merged-into-session", spoof.row?.meta?.tampered === undefined && spoof.row?.meta?.xp_bonus_pending !== 5000 && spoof.row?.meta?.streak !== 99 && spoof.row?.meta?.company_id !== CO_B, JSON.stringify({ t: spoof.row?.meta?.tampered, b: spoof.row?.meta?.xp_bonus_pending, s: spoof.row?.meta?.streak }));
    add("g4-turns-and-duration-derived-from-stored-turns", spoof.row?.turns === 4 && spoof.row?.duration_ms !== 1, `turns=${spoof.row?.turns} dur=${spoof.row?.duration_ms}`);
    add("g4-tenant-scope-unchanged-by-request-fields", spoof.row?.company_id === CO_A && spoof.row?.org_id === ORG_A, `${spoof.row?.company_id}/${spoof.row?.org_id}`);

    // zero-turn session: nothing earned, no assignment completed
    T(W, "assignments").push({ id: AS(1), rep_id: SCORE_U, type: "sparring", status: "assigned", created_at: "2026-01-01T00:00:00Z" });
    T(W, "assignments").push({ id: AS(2), rep_id: REP_B, type: "sparring", status: "assigned", created_at: "2026-01-01T00:00:00Z" });
    const zr = await mk(SCORE_U); const zid = zr.body?.session?.id as string; w0 = W.writes.length;
    const zs = await call(A, "POST", "/v1/sparring/score", SCORE_U, { sessionId: zid, total: 100, assignmentId: AS(1) });
    add("g4-zero-turn-session-scores-zero-and-earns-no-xp", zs.status === 200 && sess(zid)?.total_score === 0 && (sess(zid)?.xp_awarded ?? 0) === 0, `total=${sess(zid)?.total_score} xp=${sess(zid)?.xp_awarded}`);
    add("g4-zero-turn-score-completes-no-assignment-and-awards-nothing", T(W, "assignments").find((a) => a.id === AS(1))?.status === "assigned" && T(W, "rep_xp_events").length === 0, `status=${T(W, "assignments").find((a) => a.id === AS(1))?.status}`);

    // assignment ownership on a scored session
    const own = await playScore(SCORE_U, false); // creates a fresh scored session for SCORE_U
    // try to complete ANOTHER rep's assignment through SCORE_U's session
    const xr = await call(A, "POST", "/v1/sparring/score", SCORE_U, { sessionId: own.id, assignmentId: AS(2) });
    add("g4-cannot-complete-another-reps-assignment", xr.status === 200 && T(W, "assignments").find((a) => a.id === AS(2))?.status === "assigned" && !T(W, "rep_xp_events").some((e) => e.rep_id === REP_B), `status=${T(W, "assignments").find((a) => a.id === AS(2))?.status}`);
    const okc = await call(A, "POST", "/v1/sparring/score", SCORE_U, { sessionId: own.id, assignmentId: AS(1) });
    const xp = T(W, "rep_xp_events").filter((e) => e.rep_id === SCORE_U);
    add("g4-own-assignment-completes-with-server-set-xp-only", okc.status === 200 && T(W, "assignments").find((a) => a.id === AS(1))?.status === "completed" && xp.length === 1 && xp[0].xp === 50, `status=${T(W, "assignments").find((a) => a.id === AS(1))?.status} xp=${JSON.stringify(xp.map((e) => e.xp))}`);
    add("g1-e2e-whole-core-flow-made-zero-provider-egress", hits.length === 0, `hits=${hits.length}`);
  } catch (e: any) { add("e2e-core-suite-executed", false, String(e?.message || e).slice(0, 240)); }
  finally { app?.kill(); }
  return out;
}

// ───────────── e2e B: limits (G5) ─────────────
async function e2eLimits(root: string, stubUrl: string, canaryUrl: string, w: { cur: World }, hits: string[]): Promise<Check[]> {
  const out: Check[] = []; const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Served | null = null;
  try {
    app = await serve(root, stubUrl, canaryUrl, { SPARRING_BRAIN_PROVIDER: "stub", OPENAI_API_KEY: PLANTED_KEY });
    const A = app.base; w.cur = newWorld(); const W = w.cur; seed(W); hits.length = 0;
    const mk = (uid: string, hdr: Record<string, string> = {}) => call(A, "POST", "/v1/sparring/sessions", uid, { personaId: "price_sensitive", difficulty: "normal" }, hdr);
    add("g5-control-anonymous-401", (await call(A, "POST", "/v1/sparring/sessions", null, {})).status === 401, "");

    // a pool of live sessions: the buyer may legitimately hang up a long session (409), which must not mask the limiter
    const pool: string[] = []; for (let i = 0; i < 5; i++) pool.push((await mk(RL_TURNS)).body?.session?.id as string);
    const sid = pool[0];
    const turnsOf = () => T(W, "sparring_turns").filter((x) => pool.includes(x.session_id)).length;
    let w0 = W.writes.length;
    const long = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, RL_TURNS, { text: "x".repeat(2001) });
    const empty = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, RL_TURNS, { text: "   " });
    add("g5-overlong-message-refused-400", long.status === 400 && long.body?.error === "message_too_long", `${long.status} ${JSON.stringify(long.body)}`);
    add("g5-empty-message-refused-400", empty.status === 400 && empty.body?.error === "text_required", `${empty.status}`);
    add("g5-validation-refusal-writes-nothing-and-calls-no-provider", noWrites(W, w0) && hits.length === 0, `writes=${W.writes.length - w0} hits=${hits.length}`);
    add("g5-max-length-message-accepted", (await call(A, "POST", `/v1/sparring/sessions/${pool[4]}/turns`, RL_TURNS, { text: "y".repeat(2000) })).status === 200, "");

    // per-user turn rate limit (20/min): vary x-forwarded-for on every request — must not evade
    const statuses: number[] = []; let lastRA: string | null = null;
    for (let i = 0; i < 24; i++) { const r = await call(A, "POST", `/v1/sparring/sessions/${pool[i % 4]}/turns`, RL_TURNS, { text: `Follow up number ${i} about pricing and timing.` }, { "x-forwarded-for": `203.0.113.${i + 1}`, "x-real-ip": `198.51.100.${i + 1}` }); statuses.push(r.status); if (r.status === 429) lastRA = r.retryAfter; }
    const first429 = statuses.indexOf(429);
    add("g5-turn-rate-limit-trips-at-the-per-user-ceiling", first429 > 0 && first429 <= 20 && statuses.slice(first429).every((s) => s === 429) && statuses.slice(0, first429).every((s) => s === 200), statuses.join(","));
    add("g5-rate-limit-not-evaded-by-forwarded-headers", first429 > 0, `first429=${first429}`);
    add("g5-429-has-retry-after-and-stable-code", !!lastRA && Number(lastRA) >= 1, `retry-after=${lastRA}`);
    const w1 = W.writes.length, before = turnsOf();
    const limited = await call(A, "POST", `/v1/sparring/sessions/${pool[0]}/turns`, RL_TURNS, { text: "one more" });
    add("g5-limited-request-writes-nothing-and-calls-no-provider", limited.status === 429 && limited.body?.error === "sparring_rate_limited" && noWrites(W, w1) && turnsOf() === before && hits.length === 0, `status=${limited.status} writes=${W.writes.length - w1}`);
    const other = await mk(RL_OTHER); const osid = other.body?.session?.id as string;
    add("g5-other-user-unaffected-by-anothers-limit", other.status === 200 && (await call(A, "POST", `/v1/sparring/sessions/${osid}/turns`, RL_OTHER, { text: "hello there" })).status === 200, "");

    // per-session turn cap (40 user turns): seed a session at the cap
    const capSess = await mk(CAP_U); const cid = capSess.body?.session?.id as string;
    for (let i = 0; i < 40; i++) { T(W, "sparring_turns").push({ id: crypto.randomUUID(), session_id: cid, role: "user", text: "u" + i, created_at: new Date(1_800_100_000_000 + i * 2000).toISOString() }); T(W, "sparring_turns").push({ id: crypto.randomUUID(), session_id: cid, role: "assistant", text: "a" + i, created_at: new Date(1_800_100_001_000 + i * 2000).toISOString() }); }
    const w2 = W.writes.length, nCap = T(W, "sparring_turns").filter((x) => x.session_id === cid).length;
    const capped = await call(A, "POST", `/v1/sparring/sessions/${cid}/turns`, CAP_U, { text: "turn forty-one" });
    add("g5-session-turn-cap-409-and-nothing-written", capped.status === 409 && capped.body?.error === "session_turn_limit" && noWrites(W, w2) && T(W, "sparring_turns").filter((x) => x.session_id === cid).length === nCap && hits.length === 0, `status=${capped.status}`);

    // per-user session creation limit (20/hour)
    const ss: number[] = []; for (let i = 0; i < 22; i++) ss.push((await mk(RL_SESS, { "x-forwarded-for": `192.0.2.${i + 1}` })).status);
    const f = ss.indexOf(429);
    add("g5-session-create-limit-trips-per-user", f === 20 && ss.slice(f).every((s) => s === 429), ss.join(","));
    const sessCount = T(W, "sparring_sessions").filter((s) => s.rep_id === RL_SESS).length;
    add("g5-limited-session-create-writes-nothing", sessCount === 20, `sessions=${sessCount}`);
    add("g5-whole-limits-flow-made-zero-provider-egress", hits.length === 0, `hits=${hits.length}`);
  } catch (e: any) { add("e2e-limits-suite-executed", false, String(e?.message || e).slice(0, 240)); }
  finally { app?.kill(); }
  return out;
}

// ───────────── e2e C: paid provider configured (canary) — worst-case egress + provenance ─────────────
async function e2eProvider(root: string, stubUrl: string, canaryUrl: string, w: { cur: World }, hits: string[]): Promise<Check[]> {
  const out: Check[] = []; const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Served | null = null;
  try {
    app = await serve(root, stubUrl, canaryUrl, { SPARRING_BRAIN_PROVIDER: "openai", OPENAI_API_KEY: PLANTED_KEY });
    const A = app.base; w.cur = newWorld(); const W = w.cur; seed(W); hits.length = 0;
    const v = await call(A, "GET", "/v1/version", null);
    add("provider-version-reports-openai-label-and-no-key", v.body?.sparring?.provider === "openai" && !JSON.stringify(v.body).includes(PLANTED_KEY), JSON.stringify(v.body?.sparring));
    // worst case: paid provider configured AND key present — closed routes must STILL make zero egress
    hits.length = 0;
    const at = await call(A, "POST", "/v1/sparring/analyse-turn", REP_A, { text: "Classify me" });
    const lg = await call(A, "POST", "/v1/sparring/log", REP_A, { repId: REP_A, xp: 10 });
    add("provider-closed-routes-zero-egress-even-with-paid-provider-and-key", at.status === 410 && lg.status === 410 && hits.length === 0, `hits=${hits.length}`);
    // control: the configured paid provider IS reachable through /turns (the canary is live — non-vacuity)
    const cr = await call(A, "POST", "/v1/sparring/sessions", REP_A, { personaId: "price_sensitive", difficulty: "normal" }); const sid = cr.body?.session?.id as string;
    const t = await call(A, "POST", `/v1/sparring/sessions/${sid}/turns`, REP_A, { text: "Hello, can I tell you about our product?" });
    add("provider-control-configured-paid-provider-reaches-canary", hits.length >= 1, `hits=${hits.length}`);
    add("provider-failure-falls-back-to-stub-with-provenance", t.status === 200 && t.body?.ai_provider?.provider === "stub" && t.body?.ai_provider?.fallback === true, JSON.stringify(t.body?.ai_provider));
  } catch (e: any) { add("e2e-provider-suite-executed", false, String(e?.message || e).slice(0, 240)); }
  finally { app?.kill(); }
  return out;
}

// ───────────── mutation harness (COPY of src/) ─────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.mkdirSync(path.join(dir, "scripts"));
  fs.copyFileSync(path.join(root, "scripts/validate-sparring-brain-claude-parity-day-259.ts"), path.join(dir, "scripts/validate-sparring-brain-claude-parity-day-259.ts"));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string, repl: string) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}

// ───────────── parity validator: no provider request without the explicit live opt-in ─────────────
// The script is run with fake keys whose base URLs point at the loopback canary, so even a regression can only reach the canary.
const PARITY = "scripts/validate-sparring-brain-claude-parity-day-259.ts", PKG = "package.json";
function runParity(root: string, canaryUrl: string, args: string[], env: Record<string, string>, cwd: string): Promise<{ code: number | null; out: string }> {
  return new Promise((resolve) => {
    const c = spawn(path.join(ROOT, "node_modules/.bin/tsx"), [path.join(root, PARITY), ...args], { env: { PATH: process.env.PATH || "", HOME: os.tmpdir(), OPENAI_BASE_URL: canaryUrl + "/v1", ANTHROPIC_BASE_URL: canaryUrl, ...env }, cwd, stdio: ["ignore", "pipe", "pipe"] });
    let out = ""; c.stdout.on("data", (d) => (out += d)); c.stderr.on("data", (d) => (out += d));
    const t = setTimeout(() => c.kill("SIGKILL"), 60000);
    c.on("close", (code) => { clearTimeout(t); resolve({ code, out }); });
  });
}
async function paritySuite(root: string, canaryUrl: string, hits: string[]): Promise<Check[]> {
  const out: Check[] = []; const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const envDir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-parity-"));
  fs.writeFileSync(path.join(envDir, ".env"), `OPENAI_API_KEY=${PLANTED_KEY}\nANTHROPIC_API_KEY=${PLANTED_KEY}\n`);
  const keys = { OPENAI_API_KEY: PLANTED_KEY, ANTHROPIC_API_KEY: PLANTED_KEY };
  try {
    hits.length = 0;
    const d1 = await runParity(root, canaryUrl, [], {}, envDir); // repo-style .env present, keys NOT in process env
    add("parity-default-with-dotenv-keys-makes-zero-provider-requests", d1.code === 0 && hits.length === 0 && /dotenv_loaded=false/.test(d1.out), `code=${d1.code} hits=${hits.length}`);
    hits.length = 0;
    const d2 = await runParity(root, canaryUrl, [], keys, EMPTY_CWD); // keys in the process environment
    add("parity-default-with-process-env-keys-makes-zero-provider-requests", d2.code === 0 && hits.length === 0 && /Network-free default/.test(d2.out), `code=${d2.code} hits=${hits.length}`);
    hits.length = 0;
    const l1 = await runParity(root, canaryUrl, ["--live"], {}, envDir); // --live without the confirmation token
    add("parity-live-flag-without-confirmation-token-refused-before-any-request", l1.code === 2 && hits.length === 0 && /REFUSED/.test(l1.out), `code=${l1.code} hits=${hits.length}`);
    hits.length = 0;
    const l2 = await runParity(root, canaryUrl, [], { GRAVIX_ALLOW_LIVE_PROVIDER_PARITY: "YES_SPEND_MONEY", ...keys }, EMPTY_CWD); // token alone (no --live) is not an opt-in
    add("parity-token-without-live-flag-is-not-an-opt-in", l2.code === 0 && hits.length === 0, `code=${l2.code} hits=${hits.length}`);
    const pkg = JSON.parse(fs.readFileSync(path.join(root, PKG), "utf8")); const sc = pkg.scripts || {};
    add("parity-default-npm-script-has-no-live-flag", !/--live/.test(sc["validate:sparring-brain-claude-parity"] || "") && /--live/.test(sc["validate:sparring-brain-claude-parity:live"] || ""), "");
    add("parity-live-command-not-wired-into-check-release", !/claude-parity/.test(sc["check:release"] || ""), "");
    // control (loopback canary only, fake keys): the full opt-in DOES reach the canary, so zero-hit results above are not vacuous
    hits.length = 0;
    await runParity(root, canaryUrl, ["--live"], { GRAVIX_ALLOW_LIVE_PROVIDER_PARITY: "YES_SPEND_MONEY", ...keys }, envDir);
    add("parity-control-full-opt-in-reaches-canary", hits.length >= 1, `hits=${hits.length}`);
  } finally { fs.rmSync(envDir, { recursive: true, force: true }); }
  return out;
}
const SP = "src/routes/sparring.ts", PROV = "src/lib/sparringProvider.ts", LIM = "src/lib/sparringLimits.ts", SRV = "src/server.ts", HIER = "src/lib/callerHierarchy.ts", ASG = "src/lib/assignmentsComplete.ts";
type Suite = "unit" | "boot" | "core" | "limits" | "provider" | "parity";
type Mutation = { name: string; file: string; find: string; repl: string; suite: Suite; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "non-production unset provider defaults to a paid provider again", file: PROV, find: '    return "stub";\n  }\n  if (raw === "stub"', repl: '    return "openai";\n  }\n  if (raw === "stub"', suite: "unit", expect: "unit-staging-unset-defaults-stub" },
  { name: "production unset provider is allowed", file: PROV, find: 'if (isProductionEnv(env)) throw new SparringProviderConfigError("sparring_provider_required_in_production");', repl: "", suite: "boot", expect: "boot-production-unset-refuses" },
  { name: "paid provider no longer requires its key", file: PROV, find: 'if (provider === "openai" && String(env.OPENAI_API_KEY ?? "").trim() === "") {', repl: 'if (false as boolean) {', suite: "unit", expect: "unit-openai-requires-key" },
  { name: "server never asserts the Sparring provider at boot", file: SRV, find: "assertSparringProviderConfig(process.env);\nif (process.env.GRAVIX_API_NO_LISTEN", repl: "if (process.env.GRAVIX_API_NO_LISTEN", suite: "boot", expect: "boot-production-invalid-refuses" },
  { name: "/v1/version stops reporting the Sparring provider", file: SRV, find: "sparring: { provider: describeSparringProvider() },", repl: "", suite: "core", expect: "e2e-version-reports-sparring-provider-label-only" },
  { name: "turn response drops provider provenance", file: SP, find: "        ai_provider: aiProvenance,\n", repl: "", suite: "core", expect: "g1-turn-reports-provider-provenance" },
  { name: "analyse-turn makes a direct provider call again", file: SP, find: "Still behind default-deny (anonymous => 401).\n  sparringDeny(res, 410, 'route_removed')\n);\n\nexport default router;", repl: "Still behind default-deny (anonymous => 401).\n  (void fetch(String(process.env.OPENAI_BASE_URL) + '/chat/completions', { method: 'POST', body: '{}' }).catch(() => undefined), sparringDeny(res, 410, 'route_removed'))\n);\n\nexport default router;", suite: "provider", expect: "provider-closed-routes-zero-egress-even-with-paid-provider-and-key" },
  { name: "analyse-turn reopened (no longer 410)", file: SP, find: `router.post("/analyse-turn", (_req, res) =>\n  // Day 42 (G2/F-41-1): CLOSED. It called OpenAI directly (ignoring SPARRING_*) for any signed-in\n  // caller and had no WEB consumer. Still behind default-deny (anonymous => 401).\n  sparringDeny(res, 410, 'route_removed')`, repl: `router.post("/analyse-turn", (_req, res) =>\n  res.json({ ok: true })`, suite: "core", expect: "g2-analyse-turn-closed-410" },
  { name: "/log reopened: client-supplied XP is awarded", file: SP, find: "router.post('/log', (_req, res) =>", repl: "router.post('/log', async (req, res) => { await supa.from('xp_events').insert({ rep_id: req.body?.repId, delta: req.body?.xp }); return res.json({ ok: true }); }, (_req, res) =>", suite: "core", expect: "g2-log-never-awards-client-xp" },
  { name: "/messages alias reopened", file: SP, find: "router.post('/sessions/:id/messages', (_req: Request, res: Response) =>\n  sparringDeny(res, 410, 'route_removed')\n);", repl: "router.post('/sessions/:id/messages', express.json(), (req: Request, res: Response, next) => { req.url = `/sessions/${req.params.id}/turns`; (router as any).handle(req, res, next); });", suite: "core", expect: "g2-messages-alias-closed-410-and-no-turn-written" },
  { name: "body repId overrides the verified caller (create)", file: SP, find: "const effectiveRepId = callerId(req);\n    if (!effectiveRepId)", repl: "const effectiveRepId = (req.body as any)?.repId || callerId(req);\n    if (!effectiveRepId)", suite: "core", expect: "g3-create-ignores-body-repId-owner-is-verified-caller" },
  { name: "existing rep overwritten with name 'Rep' on session create", file: SP, find: "    // 2) (Day 42) No reps upsert:", repl: "    await supa.from('reps').upsert([{ id: effectiveRepId, name: 'Rep' }], { onConflict: 'id' });\n    // 2) (Day 42) No reps upsert:", suite: "core", expect: "g3-no-reps-write-and-existing-rep-not-overwritten" },
  { name: "missing rep fabricated instead of refused", file: SP, find: "if (!repLookup.data) return sparringDeny(res, 422, 'sparring_rep_profile_missing');", repl: "if (!repLookup.data) await supa.from('reps').upsert([{ id: effectiveRepId, name: 'Rep' }], { onConflict: 'id' });", suite: "core", expect: "g3-missing-rep-fabricates-nothing" },
  { name: "raw x-org-id header stamped onto the session", file: SP, find: "const orgId = hier.hierarchy.org_id;", repl: "const orgId = (req.headers['x-org-id'] as string) || hier.hierarchy.org_id;", suite: "core", expect: "g3-create-ignores-spoofed-org-header-and-body-hierarchy" },
  { name: "hierarchy read from legacy users only (the F-38-1 class)", file: HIER, find: 'client.from("reps").select("org_id, company_id, office_id")', repl: 'client.from("users").select("org_id, company_id, office_id")', suite: "core", expect: "g3-create-stamps-canonical-reps-hierarchy-not-users" },
  { name: "incomplete hierarchy no longer refused", file: SP, find: "        : sparringDeny(res, 422, 'sparring_hierarchy_incomplete');", repl: "        : sparringDeny(res, 200, 'sparring_hierarchy_incomplete');", suite: "core", expect: "g3-missing-company-refused-stable-code" },
  { name: "complete nulls the canonical company", file: SP, find: "completeUpdate.company_id = ownerHier.hierarchy.company_id;", repl: "completeUpdate.company_id = null;", suite: "core", expect: "g3-complete-keeps-canonical-company-org-not-users-not-null" },
  { name: "client total honoured by /score", file: SP, find: "let numericTotal: number = hasScoredTurns ? clampScore(derived.overall) : 0;", repl: "let numericTotal: number = typeof body.total === 'number' ? clampScore(body.total) : (hasScoredTurns ? clampScore(derived.overall) : 0);", suite: "core", expect: "g4-client-total-ignored-score-equals-clean-derivation" },
  { name: "client meta merged into the session by /score", file: SP, find: "        ...existingMeta,\n        total: numericTotal,", repl: "        ...existingMeta,\n        ...(body.meta || {}),\n        total: numericTotal,", suite: "core", expect: "g4-client-meta-not-merged-into-session" },
  { name: "client turns count honoured", file: SP, find: "            finalTurns = turnRows.length;", repl: "            finalTurns = typeof body.turns === 'number' ? body.turns : turnRows.length;", suite: "core", expect: "g4-turns-and-duration-derived-from-stored-turns" },
  { name: "zero-turn session still earns XP", file: SP, find: "const xpAwarded = hasScoredTurns ? computeXpForScore(numericTotal, difficulty, mode) : 0;", repl: "const xpAwarded = computeXpForScore(numericTotal, difficulty, mode);", suite: "core", expect: "g4-zero-turn-session-scores-zero-and-earns-no-xp" },
  { name: "zero-turn session still completes assignments", file: SP, find: "if (hasScoredTurns && repIdForAssign && repIdForAssign.length > 10) {", repl: "if (repIdForAssign && repIdForAssign.length > 10) {", suite: "core", expect: "g4-zero-turn-score-completes-no-assignment-and-awards-nothing" },
  { name: "assignment completion no longer scoped to the session owner", file: ASG, find: '    .eq("id", assignmentId)\n    .eq("rep_id", repId)', repl: '    .eq("id", assignmentId)', suite: "core", expect: "g4-cannot-complete-another-reps-assignment" },
  { name: "turn ownership check removed", file: SP, find: "if (!session.rep_id || session.rep_id !== repId) {\n        return res.status(403).json({ ok: false, error: 'forbidden' });", repl: "if (false as boolean) {\n        return res.status(403).json({ ok: false, error: 'forbidden' });", suite: "core", expect: "tenant-rep-other-tenant-denied-on-every-session-route" },
  { name: "cross-tenant session access check always allows", file: SP, find: "if (!data || !repId || !(await canAccessRep((req as any).userId, repId))) {", repl: "if (!data || !repId) {", suite: "core", expect: "tenant-manager-other-tenant-denied-on-every-session-route" },
  { name: "message length cap removed", file: LIM, find: "maxMessageChars: 2000,", repl: "maxMessageChars: 2_000_000,", suite: "limits", expect: "g5-overlong-message-refused-400" },
  { name: "message validation skipped by the route", file: SP, find: "if (!checked.ok) return sparringDeny(res, 400, checked.error);", repl: "", suite: "limits", expect: "g5-overlong-message-refused-400" },
  { name: "per-user turn rate limit removed", file: LIM, find: "turnsPerMinute: 20,", repl: "turnsPerMinute: 2_000_000,", suite: "limits", expect: "g5-turn-rate-limit-trips-at-the-per-user-ceiling" },
  { name: "rate limit keyed by a browser-controlled header", file: SP, find: "if (rateLimited(res, repId, 'turns')) return;", repl: "if (rateLimited(res, String(req.headers['x-forwarded-for'] || repId), 'turns')) return;", suite: "limits", expect: "g5-rate-limit-not-evaded-by-forwarded-headers" },
  { name: "rate limit applied after the provider/write path", file: SP, find: "      if (rateLimited(res, repId, 'turns')) return;\n", repl: "", suite: "limits", expect: "g5-limited-request-writes-nothing-and-calls-no-provider" },
  { name: "per-session turn cap removed", file: SP, find: "if (userTurnsSoFar >= SPARRING_LIMITS.maxUserTurnsPerSession) {", repl: "if (false as boolean) {", suite: "limits", expect: "g5-session-turn-cap-409-and-nothing-written" },
  { name: "session creation limit removed", file: LIM, find: "sessionsPerHour: 20,", repl: "sessionsPerHour: 2_000_000,", suite: "limits", expect: "g5-session-create-limit-trips-per-user" },
  { name: "parity default path reads provider keys without opt-in", file: PARITY, find: "const hasOpenAI = wantsLive && !!process.env.OPENAI_API_KEY;\n  const hasClaude = wantsLive && !!process.env.ANTHROPIC_API_KEY;", repl: "const hasOpenAI = !!process.env.OPENAI_API_KEY;\n  const hasClaude = !!process.env.ANTHROPIC_API_KEY;", suite: "parity", expect: "parity-default-with-process-env-keys-makes-zero-provider-requests" },
  { name: "parity default path loads the repository .env", file: PARITY, find: 'if (wantsLive) { await import("dotenv/config"); dotenvLoaded = true; }', repl: 'await import("dotenv/config"); dotenvLoaded = true;', suite: "parity", expect: "parity-default-with-dotenv-keys-makes-zero-provider-requests" },
  { name: "parity --live confirmation token check removed", file: PARITY, find: "if (wantsLive && process.env[LIVE_ENV] !== LIVE_TOKEN) {", repl: "if (false as boolean) {", suite: "parity", expect: "parity-live-flag-without-confirmation-token-refused-before-any-request" },
  { name: "parity token alone treated as opt-in", file: PARITY, find: 'const wantsLive = process.argv.includes("--live");', repl: 'const wantsLive = process.argv.includes("--live") || process.env.GRAVIX_ALLOW_LIVE_PROVIDER_PARITY === "YES_SPEND_MONEY";', suite: "parity", expect: "parity-token-without-live-flag-is-not-an-opt-in" },
  { name: "default npm script gains --live", file: PKG, find: '"validate:sparring-brain-claude-parity": "tsx scripts/validate-sparring-brain-claude-parity-day-259.ts",', repl: '"validate:sparring-brain-claude-parity": "tsx scripts/validate-sparring-brain-claude-parity-day-259.ts --live",', suite: "parity", expect: "parity-default-npm-script-has-no-live-flag" },
];
const CONTROL: Record<Suite, string> = { unit: "unit-limiter-empty-user-refused", boot: "boot-control-production-with-explicit-stub-boots", core: "e2e-control-anonymous-create-401", limits: "g5-control-anonymous-401", provider: "provider-control-configured-paid-provider-reaches-canary", parity: "parity-control-full-opt-in-reaches-canary" };

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
  console.log("Go Live Day 42 — Sparring safety + tenant repair (F-41-1…8), child processes + loopback only\n");
  const w = { cur: newWorld() }; const hits: string[] = [];
  const stub = await startStub(w), canary = await startCanary(hits);
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`, canaryUrl = `http://127.0.0.1:${(canary.address() as AddressInfo).port}`;
  let failures = 0;
  const savedEnv = { ...process.env };
  Object.assign(process.env, { SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", NODE_ENV: "test", DEFAULT_ORG_ID: DEFAULT_ORG });
  const runSuite = (s: Suite, root: string): Promise<Check[]> =>
    s === "unit" ? unitSuite(root) : s === "boot" ? bootSuite(root, stubUrl, canaryUrl) : s === "core" ? e2eCore(root, stubUrl, canaryUrl, w, hits) : s === "limits" ? e2eLimits(root, stubUrl, canaryUrl, w, hits) : s === "parity" ? paritySuite(root, canaryUrl, hits) : e2eProvider(root, stubUrl, canaryUrl, w, hits);
  try {
    failures += report("Unit (real resolver / limiter / validator):", await unitSuite(ROOT));
    failures += report("\nBoot (real app, production refusal matrix):", await bootSuite(ROOT, stubUrl, canaryUrl));
    failures += report("\nEnd-to-end core (real app, stub provider, stateful loopback DB):", await e2eCore(ROOT, stubUrl, canaryUrl, w, hits));
    failures += report("\nEnd-to-end limits (real app):", await e2eLimits(ROOT, stubUrl, canaryUrl, w, hits));
    failures += report("\nEnd-to-end paid-provider worst case (canary egress recorder):", await e2eProvider(ROOT, stubUrl, canaryUrl, w, hits));
    failures += report("\nParity validator opt-in (no provider request without --live + token):", await paritySuite(ROOT, canaryUrl, hits));
    console.log("\nMutation proof — each mutation of a COPY must fail the named check:");
    for (const m of (process.env.VALIDATOR_SKIP_MUTATIONS ? [] : MUTATIONS)) {
      let dir = "";
      try {
        dir = copyTree(ROOT); mutate(dir, m.file, m.find, m.repl);
        const checks = await runSuite(m.suite, dir);
        const hit = checks.find((c) => c.id === m.expect), control = checks.find((c) => c.id === CONTROL[m.suite]);
        const caught = !!hit && !hit.pass && !!control?.pass;
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? (hit.pass ? " (check still PASSED — vacuous)" : " (control failed — harness dead?)") : " (check id not found / harness dead)"}`);
        if (!caught) failures++;
      } catch (e: any) { console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++; }
      finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally { Object.assign(process.env, savedEnv); stub.close(); canary.close(); fs.rmSync(EMPTY_CWD, { recursive: true, force: true }); }
  if (failures) { console.log(`\n❌ sparring safety validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ sparring safety validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
