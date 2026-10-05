// Go Live Day 27 — behavioural, network-free hardening checks (G5 + follow-ups).
//
// Unlike validate:route-authz (static analysis), this boots the REAL Express app
// in-process and drives it over loopback HTTP against a local PostgREST stub, so
// it proves what requests actually do:
//
//   G5      production boot refuses ALLOW_ADMIN_ENDPOINTS (child-process boots)
//   rewards the repaired /v1/rewards mount is reachable and keeps its identity +
//           tenant guards; the old doubled path is 404; bounties stay closed
//   admin   force-score / preview-slack / post-slack / score / digest / index-hints
//           need the env gate AND a SuperAdmin tier; effective handlers are the
//           server.ts ones (the adminRouter duplicates are shadowed)
//   internal /v1/internal/* stays fail-closed (403 for any authenticated caller)
//   headers raw x-user-id readers are only safe because untrusted identity
//           headers are stripped: spoofed SuperAdmin/manager headers get 401
//
// No real secret, DB or external network is touched: SUPABASE_* point at a
// 127.0.0.1 stub, tokens are synthetic. Non-vacuity: after the real tree passes,
// each source mutation (applied to a throw-away COPY of src/ in the OS temp dir)
// MUST make the named check fail. Nothing in the repo is modified.
//
// Run: npm run validate:admin-hardening   (tsx, loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import { spawnSync } from "node:child_process";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
// server.ts calls dotenv at import, which reads ./.env from the CWD. Run every
// import/child from an empty directory so no real secret file is ever loaded.
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
const PLACEHOLDER_KEYS = { SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder", OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder" };
type Check = { id: string; pass: boolean; detail?: string };

// ─────────────── synthetic identities (all fake UUIDs) ───────────────
const SUPER = "aaaaaaaa-0000-4000-8000-000000000001";
const MGR_A = "aaaaaaaa-0000-4000-8000-000000000002";
const REP_A = "aaaaaaaa-0000-4000-8000-000000000003";
const REP_A2 = "aaaaaaaa-0000-4000-8000-000000000004";
const MGR_B = "bbbbbbbb-0000-4000-8000-000000000002";
const REP_B = "bbbbbbbb-0000-4000-8000-000000000003";
const NOBODY = "cccccccc-0000-4000-8000-000000000009";
const CO_A = "dddddddd-0000-4000-8000-00000000000a";
const CO_B = "dddddddd-0000-4000-8000-00000000000b";
const REPS: Record<string, { id: string; tier: string; company_id: string | null; org_id: string | null }> = {
  [SUPER]: { id: SUPER, tier: "SuperAdmin", company_id: CO_A, org_id: CO_A },
  [MGR_A]: { id: MGR_A, tier: "Manager", company_id: CO_A, org_id: CO_A },
  [REP_A]: { id: REP_A, tier: "SalesRep", company_id: CO_A, org_id: CO_A },
  [REP_A2]: { id: REP_A2, tier: "SalesRep", company_id: CO_A, org_id: CO_A },
  [MGR_B]: { id: MGR_B, tier: "Manager", company_id: CO_B, org_id: CO_B },
  [REP_B]: { id: REP_B, tier: "SalesRep", company_id: CO_B, org_id: CO_B },
};
const PROXY_SECRET = "synthetic-proxy-secret-for-validator";

// ─────────────── loopback PostgREST stub ───────────────
function startStub(): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url || "/", "http://stub");
    const wantsObject = String(req.headers["accept"] || "").includes("vnd.pgrst.object");
    let rows: unknown[] = [];
    if (u.pathname === "/rest/v1/reps") {
      const id = u.searchParams.get("id")?.replace(/^eq\./, "");
      const co = u.searchParams.get("company_id")?.replace(/^eq\./, "");
      const org = u.searchParams.get("org_id")?.replace(/^eq\./, "");
      rows = Object.values(REPS).filter((r) => (id ? r.id === id : co ? r.company_id === co : org ? r.org_id === org : false));
    }
    if (wantsObject && rows.length !== 1) {
      res.writeHead(406, { "content-type": "application/json" });
      return res.end(JSON.stringify({ code: "PGRST116", message: "no rows", details: "", hint: null }));
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(wantsObject ? rows[0] : rows));
  });
  return new Promise((resolve) => srv.listen(0, "127.0.0.1", () => resolve(srv)));
}

// ─────────────── helpers ───────────────
type Resp = { status: number; body: any; timedOut?: boolean };
async function call(base: string, method: string, p: string, headers: Record<string, string> = {}, body?: unknown): Promise<Resp> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), 2500);
  try {
    const r = await fetch(base + p, {
      method,
      headers: { ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: ctl.signal,
    });
    const text = await r.text();
    let j: any = null;
    try { j = JSON.parse(text); } catch { j = text; }
    return { status: r.status, body: j };
  } catch (e: any) {
    return { status: 0, body: null, timedOut: e?.name === "AbortError" };
  } finally {
    clearTimeout(timer);
  }
}
const as = (uid: string) => ({ "x-user-id": uid });
const errOf = (r: Resp) => (r.body && typeof r.body === "object" ? r.body.error : undefined);

// ─────────────── runtime suite (real app, loopback) ───────────────
async function runtimeSuite(root: string, stubUrl: string): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });

  const saved = { ...process.env };
  const log = { l: console.log, e: console.error, w: console.warn, i: console.info };
  Object.assign(process.env, {
    NODE_ENV: "test",
    SUPABASE_URL: stubUrl,
    ...PLACEHOLDER_KEYS,
    GRAVIX_API_NO_LISTEN: "1",
  });
  const savedCwd = process.cwd();
  process.chdir(EMPTY_CWD);
  for (const k of ["PROXY_SHARED_SECRET", "ALLOW_ADMIN_ENDPOINTS", "DEV_TEST_UID", "CRON_SECRET", "CRON_WEBHOOK_SECRET"]) delete process.env[k];
  console.log = console.error = console.warn = console.info = () => {};

  let srv: http.Server | null = null;
  try {
    const mod = await import(path.join(root, "src/server.ts"));
    srv = http.createServer(mod.app);
    await new Promise<void>((r) => srv!.listen(0, "127.0.0.1", () => r()));
    const base = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;

    // ── rewards: repaired mount, guards preserved ──
    let r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(REP_A));
    add("rewards-reachable-self", r.status === 200 && r.body?.ok === true && Array.isArray(r.body?.badges), `status=${r.status}${r.timedOut ? " (timeout: handler never reached)" : ""}`);
    r = await call(base, "GET", `/v1/rewards/${REP_A2}`, as(REP_A));
    add("rewards-peer-rep-denied", r.status === 403 && errOf(r) === "forbidden_rep_scope", `status=${r.status}`);
    r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(MGR_A));
    add("rewards-same-tenant-manager-allowed", r.status === 200 && r.body?.ok === true, `status=${r.status}`);
    r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(MGR_B));
    add("rewards-cross-tenant-manager-denied", r.status === 403 && errOf(r) === "forbidden_rep_scope", `status=${r.status}`);
    r = await call(base, "GET", `/v1/rewards/${REP_A}`);
    add("rewards-anonymous-401", r.status === 401, `status=${r.status}`);
    r = await call(base, "POST", `/v1/rewards/${REP_A2}/select-title`, as(REP_A), { titleId: "t1" });
    add("rewards-select-title-not-self-denied", r.status === 403 && errOf(r) === "forbidden_not_self", `status=${r.status}`);
    r = await call(base, "POST", `/v1/rewards/${REP_A}/select-title`, as(REP_A), {});
    add("rewards-select-title-reaches-handler", r.status === 400 && errOf(r) === "titleId_required", `status=${r.status}`);
    r = await call(base, "GET", `/v1/rewards/rewards/${REP_A}`, as(REP_A));
    add("rewards-old-doubled-path-gone", r.status === 404, `status=${r.status}`);
    r = await call(base, "GET", "/v1/rewards/bounties/active", as(REP_A));
    add("rewards-bounties-closed", r.status === 404 && errOf(r) === "bounties_not_available", `status=${r.status}`);

    // ── admin: env gate closed by default, even for SuperAdmin ──
    const ADMIN: Array<[string, string, unknown?]> = [
      ["POST", "/v1/admin/score/00000000-0000-4000-8000-000000000001"],
      ["POST", "/v1/admin/force-score/00000000-0000-4000-8000-000000000001"],
      ["GET", "/v1/admin/preview-slack"],
      ["POST", "/v1/admin/post-slack", { callId: "x" }],
      ["POST", "/v1/admin/digest/daily"],
      ["GET", "/v1/admin/index-hints"],
    ];
    let closed = true; let closedDetail = "";
    for (const [m, p, b] of ADMIN) {
      const x = await call(base, m, p, as(SUPER), b);
      if (!(x.status === 403 && errOf(x) === "admin_required")) { closed = false; closedDetail += ` ${m} ${p}=>${x.status}`; }
    }
    add("admin-closed-without-env-flag-even-for-superadmin", closed, closedDetail.trim());

    for (const v of ["TRUE", "1", "yes", " true", "false"]) {
      process.env.ALLOW_ADMIN_ENDPOINTS = v;
      const x = await call(base, "POST", "/v1/admin/force-score/00000000-0000-4000-8000-000000000001", as(SUPER));
      add(`admin-env-gate-exact-true-only[${JSON.stringify(v)}]`, x.status === 403 && errOf(x) === "admin_required", `status=${x.status}`);
    }

    // ── admin: env open => SuperAdmin tier still required ──
    process.env.ALLOW_ADMIN_ENDPOINTS = "true";
    let tiersOk = true; let tiersDetail = "";
    for (const [m, p, b] of ADMIN) {
      for (const [who, uid] of [["SalesRep", REP_A], ["Manager", MGR_A], ["unknown-rep", NOBODY]] as const) {
        const x = await call(base, m, p, as(uid), b);
        if (!(x.status === 403 && errOf(x) === "forbidden_not_super_admin")) { tiersOk = false; tiersDetail += ` ${who} ${m} ${p}=>${x.status}`; }
      }
      const a = await call(base, m, p);
      if (a.status !== 401) { tiersOk = false; tiersDetail += ` anon ${m} ${p}=>${a.status}`; }
    }
    add("admin-env-open-requires-superadmin-tier", tiersOk, tiersDetail.trim());

    // effective handlers = server.ts versions (adminRouter duplicates are shadowed)
    r = await call(base, "POST", "/v1/admin/force-score/00000000-0000-4000-8000-000000000001", as(SUPER));
    add("force-score-effective-handler-is-server-ts", r.status === 404 && errOf(r) === "call_not_found", `status=${r.status} err=${errOf(r)}`);
    r = await call(base, "GET", "/v1/admin/preview-slack?callId=00000000-0000-4000-8000-000000000001", as(SUPER));
    add("preview-slack-effective-handler-is-server-ts", r.status === 200 && r.body?.body?.text === "Call scored ✅" && r.body?.payload === undefined, `status=${r.status}`);
    delete process.env.ALLOW_ADMIN_ENDPOINTS;

    // ── internal routes stay fail-closed ──
    r = await call(base, "GET", "/v1/internal/health", as(SUPER));
    add("internal-authenticated-superadmin-fail-closed", r.status === 403 && errOf(r) === "internal_access_required", `status=${r.status}`);
    r = await call(base, "GET", "/v1/internal/health");
    add("internal-anonymous-401", r.status === 401, `status=${r.status}`);

    // ── raw x-user-id readers only safe because of the trust boundary ──
    process.env.PROXY_SHARED_SECRET = PROXY_SECRET;
    const GUARDED: Array<[string, string]> = [
      ["GET", "/v1/admin/whoami"],            // requireSuperAdmin (reads raw header)
      ["GET", "/v1/admin/reps"],              // requireManager   (reads raw header)
      ["GET", "/v1/admin/super/health"],      // requireSuperAdmin + inline raw read
    ];
    let spoofOk = true; let spoofDetail = "";
    for (const [m, p] of GUARDED) {
      const uid = p.includes("reps") ? MGR_A : SUPER;
      const spoofed = await call(base, m, p, as(uid)); // no x-proxy-secret
      if (spoofed.status !== 401) { spoofOk = false; spoofDetail += ` spoofed ${p}=>${spoofed.status}`; }
      const wrongSecret = await call(base, m, p, { ...as(uid), "x-proxy-secret": "wrong-secret-of-different-len" });
      if (wrongSecret.status !== 401) { spoofOk = false; spoofDetail += ` wrong-secret ${p}=>${wrongSecret.status}`; }
      const good = await call(base, m, p, { ...as(uid), "x-proxy-secret": PROXY_SECRET });
      if (good.status === 401 || good.status === 403) { spoofOk = false; spoofDetail += ` trusted ${p}=>${good.status}`; }
    }
    add("raw-header-readers-protected-by-trust-boundary", spoofOk, spoofDetail.trim());
    // production + no secret: identity headers are not trusted at all
    process.env.NODE_ENV = "production";
    delete process.env.PROXY_SHARED_SECRET;
    r = await call(base, "GET", "/v1/admin/whoami", as(SUPER));
    add("production-without-secret-trusts-no-header", r.status === 401, `status=${r.status}`);
  } catch (e: any) {
    add("runtime-suite-executed", false, String(e?.stack || e).split("\n").slice(0, 3).join(" | "));
  } finally {
    if (srv) await new Promise<void>((r2) => { srv!.closeAllConnections?.(); srv!.close(() => r2()); });
    process.chdir(savedCwd);
    console.log = log.l; console.error = log.e; console.warn = log.w; console.info = log.i;
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
  }
  return out;
}

// ─────────────── production boot matrix (child processes) ───────────────
const BOOT_CASES: Array<{ id: string; env: Record<string, string | undefined>; refuse: boolean }> = [
  { id: "prod-flag-missing", env: { NODE_ENV: "production" }, refuse: false },
  { id: "prod-flag-empty", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "" }, refuse: false },
  { id: "prod-flag-whitespace", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "   " }, refuse: false },
  { id: "prod-flag-false", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "false" }, refuse: true },
  { id: "prod-flag-zero", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "0" }, refuse: true },
  { id: "prod-flag-no", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "no" }, refuse: true },
  { id: "prod-flag-padded-true", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: " true " }, refuse: true },
  { id: "prod-flag-true", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "true" }, refuse: true },
  { id: "prod-flag-TRUE", env: { NODE_ENV: "production", ALLOW_ADMIN_ENDPOINTS: "TRUE" }, refuse: true },
  { id: "dev-flag-true-still-boots", env: { NODE_ENV: "development", ALLOW_ADMIN_ENDPOINTS: "true" }, refuse: false },
];

function bootSuite(root: string, stubUrl: string): Check[] {
  const tsx = path.join(ROOT, "node_modules/.bin/tsx");
  return BOOT_CASES.map((c) => {
    const env: Record<string, string> = {
      PATH: process.env.PATH || "",
      HOME: process.env.HOME || "",
      SUPABASE_URL: stubUrl,
      ...PLACEHOLDER_KEYS,
      GRAVIX_API_NO_LISTEN: "1", // refusal must fire even though nothing would listen
    };
    for (const [k, v] of Object.entries(c.env)) if (v !== undefined) env[k] = v;
    const p = spawnSync(tsx, [__filename, "--boot", root], { env, cwd: EMPTY_CWD, encoding: "utf8", timeout: 60_000 });
    const text = `${p.stdout || ""}${p.stderr || ""}`;
    const refused = p.status !== 0 && /ALLOW_ADMIN_ENDPOINTS is set in production/.test(text);
    const booted = p.status === 0 && /BOOT_OK/.test(text);
    const pass = c.refuse ? refused : booted;
    return { id: `boot[${c.id}]`, pass, detail: `exit=${p.status} ${c.refuse ? "expected refuse" : "expected boot"}` };
  });
}

// ─────────────── pure contract table ───────────────
async function contractSuite(root: string): Promise<Check[]> {
  const { assertProductionAdminFlagUnset } = await import(path.join(root, "src/productionConfig.ts"));
  const throws = (env: Record<string, string | undefined>) => { try { assertProductionAdminFlagUnset(env); return false; } catch { return true; } };
  const P = "production";
  const msgLeaksValue = (() => { try { assertProductionAdminFlagUnset({ NODE_ENV: P, ALLOW_ADMIN_ENDPOINTS: "s3cr3t-value" }); return false; } catch (e: any) { return String(e.message).includes("s3cr3t-value"); } })();
  return [
    { id: "contract-missing-ok", pass: !throws({ NODE_ENV: P }) },
    { id: "contract-empty-ok", pass: !throws({ NODE_ENV: P, ALLOW_ADMIN_ENDPOINTS: "" }) },
    { id: "contract-whitespace-ok", pass: !throws({ NODE_ENV: P, ALLOW_ADMIN_ENDPOINTS: " \t " }) },
    { id: "contract-false-like-refused", pass: ["false", "FALSE", "0", "no", "off", "null"].every((v) => throws({ NODE_ENV: P, ALLOW_ADMIN_ENDPOINTS: v })) },
    { id: "contract-true-refused", pass: ["true", "TRUE", " true ", "1"].every((v) => throws({ NODE_ENV: P, ALLOW_ADMIN_ENDPOINTS: v })) },
    { id: "contract-non-production-untouched", pass: ["development", "test", "staging", undefined, "Production"].every((n) => !throws({ NODE_ENV: n, ALLOW_ADMIN_ENDPOINTS: "true" })) },
    { id: "contract-message-never-echoes-value", pass: !msgLeaksValue },
  ];
}

// ─────────────── raw identity-header inventory (review tripwire) ───────────────
// Every file reading a raw identity header. Day 27 audit: none was converted to
// req.userId because the two are NOT equivalent (impersonation swaps req.userId to
// the target while the header stays the actor; Bearer-only callers have no header;
// header aliases). Each is safe only behind the Day-175 trust boundary, proven
// behaviourally above. A NEW reader (or a changed count) fails here for review.
const RAW_READ_BASELINE: Record<string, number> = {
  "src/middleware/requireManager.ts": 5, "src/middleware/requirePartnerAdmin.ts": 3, "src/middleware/requireSuperAdmin.ts": 3,
  "src/routes/accounts.ts": 2, "src/routes/admin.ts": 23, "src/routes/assignments.ts": 4, "src/routes/auth.ts": 2,
  "src/routes/calls.ts": 3, "src/routes/crm.ts": 2, "src/routes/debug.ts": 1, "src/routes/intelligence.ts": 3,
  "src/routes/intelligenceObjections.ts": 3, "src/routes/intelligenceScorecards.ts": 3, "src/routes/manager.ts": 3,
  "src/routes/reps.ts": 3, "src/routes/team.ts": 3, "src/routes/users.ts": 2, "src/routes/whisperer.ts": 4,
  "src/server.ts": 1,
};
function rawReadInventory(root: string): Record<string, number> {
  const re = /header\(\s*['"]x-(?:user-id|gravix-user-id|forwarded-user-id|real-user-id)['"]\s*\)|headers\[\s*['"]x-(?:user-id|gravix-user-id|forwarded-user-id|real-user-id)['"]\s*\]/gi;
  const res: Record<string, number> = {};
  const walk = (d: string) => {
    for (const e of fs.readdirSync(path.join(root, d), { withFileTypes: true })) {
      const rel = `${d}/${e.name}`;
      if (e.isDirectory()) walk(rel);
      else if (rel.endsWith(".ts") && rel !== "src/identityTrust.ts") {
        const n = (fs.readFileSync(path.join(root, rel), "utf8").match(re) || []).length;
        if (n) res[rel] = n;
      }
    }
  };
  walk("src");
  return res;
}
function inventorySuite(root: string): Check[] {
  const got = rawReadInventory(root);
  const diffs: string[] = [];
  for (const k of new Set([...Object.keys(got), ...Object.keys(RAW_READ_BASELINE)])) {
    if ((got[k] || 0) !== (RAW_READ_BASELINE[k] || 0)) diffs.push(`${k}: ${RAW_READ_BASELINE[k] || 0} -> ${got[k] || 0}`);
  }
  return [{ id: "raw-identity-header-reads-match-reviewed-baseline", pass: diffs.length === 0, detail: diffs.join("; ") }];
}

// ─────────────── mutation harness ───────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutateCopy(dir: string, file: string, find: string, repl: string) {
  const p = path.join(dir, file);
  const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}

type Mutation = { name: string; file: string; find: string; repl: string; suite: "runtime" | "boot" | "contract"; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "requireAdmin drops SuperAdmin delegation", file: "src/server.ts", find: "return requireSuperAdmin(req, res, next);", repl: "return next();", suite: "runtime", expect: "admin-env-open-requires-superadmin-tier" },
  { name: "requireAdmin env gate fails open", file: "src/server.ts", find: 'process.env.ALLOW_ADMIN_ENDPOINTS !== "true"', repl: 'process.env.ALLOW_ADMIN_ENDPOINTS === "false"', suite: "runtime", expect: "admin-closed-without-env-flag-even-for-superadmin" },
  { name: "requireAdmin env gate loosened to truthy", file: "src/server.ts", find: 'process.env.ALLOW_ADMIN_ENDPOINTS !== "true"', repl: "!process.env.ALLOW_ADMIN_ENDPOINTS", suite: "runtime", expect: 'admin-env-gate-exact-true-only["TRUE"]' },
  { name: "rewards mounted uncalled again", file: "src/server.ts", find: 'app.use("/v1/rewards", rewardsRoutes());', repl: 'app.use("/v1/rewards", rewardsRoutes as any);', suite: "runtime", expect: "rewards-reachable-self" },
  { name: "rewards routes regain doubled /rewards prefix", file: "src/routes/rewards.ts", find: 'r.get("/:userId"', repl: 'r.get("/rewards/:userId"', suite: "runtime", expect: "rewards-reachable-self" },
  { name: "rewards GET loses tenant/self guard", file: "src/routes/rewards.ts", find: "if (!(await canAccessRep((req as any).userId, userId))) {", repl: "if (false) {", suite: "runtime", expect: "rewards-peer-rep-denied" },
  { name: "rewards select-title loses self-only guard", file: "src/routes/rewards.ts", find: 'if (userId !== String((req as any).userId || "")) {', repl: "if (false) {", suite: "runtime", expect: "rewards-select-title-not-self-denied" },
  { name: "bounties handler opened", file: "src/routes/rewards.ts", find: "const BOUNTIES_ENABLED = false;", repl: "const BOUNTIES_ENABLED = true;", suite: "runtime", expect: "rewards-bounties-closed" },
  { name: "requireInternal opened to any caller", file: "src/routes/internal.ts", find: "if (!internalUser || !canAccessInternalPortal(internalUser)) {", repl: "if (false) {", suite: "runtime", expect: "internal-authenticated-superadmin-fail-closed" },
  { name: "identity header stripping removed", file: "src/identityTrust.ts", find: "for (const h of SPOOFABLE_IDENTITY_HEADERS) delete req.headers[h];", repl: "", suite: "runtime", expect: "raw-header-readers-protected-by-trust-boundary" },
  { name: "production trusts headers without a secret", file: "src/identityTrust.ts", find: 'if (!expected) return process.env.NODE_ENV !== "production";', repl: "if (!expected) return true;", suite: "runtime", expect: "production-without-secret-trusts-no-header" },
  { name: "boot guard only refuses exact true", file: "src/productionConfig.ts", find: 'if (raw === undefined || String(raw).trim() === "") return;', repl: 'if (String(raw).trim() !== "true") return;', suite: "boot", expect: "boot[prod-flag-false]" },
  { name: "boot guard ignores whitespace-padded true", file: "src/productionConfig.ts", find: 'String(raw).trim() === ""', repl: 'String(raw) === ""', suite: "contract", expect: "contract-whitespace-ok" },
  { name: "boot guard removed from server.ts", file: "src/server.ts", find: "assertProductionAdminFlagUnset(process.env);\nif (", repl: "if (", suite: "boot", expect: "boot[prod-flag-true]" },
  { name: "boot guard also refuses outside production", file: "src/productionConfig.ts", find: 'if (env.NODE_ENV !== "production") return;', repl: "", suite: "boot", expect: "boot[dev-flag-true-still-boots]" },
  { name: "new raw x-user-id reader added", file: "src/routes/rewards.ts", find: "const { userId } = req.params;", repl: 'const { userId } = req.params; void req.header("x-user-id");', suite: "contract", expect: "raw-identity-header-reads-match-reviewed-baseline" },
];

// ─────────────── main ───────────────
function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) {
    console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${!c.pass && c.detail ? " — " + c.detail : ""}`);
    if (!c.pass) bad++;
  }
  return bad;
}

async function main() {
  if (process.argv[2] === "--boot") {
    // child mode: import the real server module; the boot refusal throws here.
    process.env.GRAVIX_API_NO_LISTEN = "1";
    await import(path.join(process.argv[3], "src/server.ts"));
    console.log("BOOT_OK");
    process.exit(0);
  }

  console.log("Go Live Day 27 — admin / rewards / boot hardening (behavioural, loopback only)\n");
  const stub = await startStub();
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`;
  let failures = 0;
  try {
    failures += report("Contract table (assertProductionAdminFlagUnset):", await contractSuite(ROOT));
    failures += report("\nProduction boot (real server module, child process):", bootSuite(ROOT, stubUrl));
    failures += report("\nRuntime behaviour (real Express app over loopback):", await runtimeSuite(ROOT, stubUrl));
    failures += report("\nRaw identity-header reader inventory:", inventorySuite(ROOT));

    console.log("\nMutation proof — each mutation of a COPY of src/ MUST fail the named check:");
    for (const m of MUTATIONS) {
      let dir = "";
      try {
        dir = copyTree(ROOT);
        mutateCopy(dir, m.file, m.find, m.repl);
        const checks =
          m.suite === "boot" ? bootSuite(dir, stubUrl)
          : m.suite === "contract" ? [...(await contractSuite(dir)), ...inventorySuite(dir)]
          : await runtimeSuite(dir, stubUrl);
        const hit = checks.find((c) => c.id === m.expect);
        if (!hit && process.env.VALIDATOR_DEBUG) console.log(JSON.stringify(checks.filter((c) => !c.pass)));
        const caught = !!hit && !hit.pass;
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found)"}`);
        if (!caught) failures++;
      } catch (e: any) {
        console.log(`  ❌  ${m.name} — harness error: ${e?.message}`);
        failures++;
      } finally {
        if (dir) fs.rmSync(dir, { recursive: true, force: true });
      }
    }
  } finally {
    stub.close();
    fs.rmSync(EMPTY_CWD, { recursive: true, force: true });
  }

  if (failures) {
    console.log(`\n❌ admin hardening validation FAILED (${failures})`);
    process.exit(1);
  }
  console.log("\n✅ admin hardening validation passed");
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
