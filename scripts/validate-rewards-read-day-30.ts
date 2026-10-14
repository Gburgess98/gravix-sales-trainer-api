// Go Live Day 30 — F-28-1 regression: GET /v1/rewards/:userId read repair.
//
// Boots the REAL Express app in-process against a 127.0.0.1 PostgREST stub (no real
// secret/DB/network/AI; run from an empty cwd with placeholder keys) and proves:
//
//   works     GET works with SUPABASE_ANON_KEY ABSENT (reads via the shared service-role client)
//   contract  empty + populated reads keep the response shape and `private, max-age=15`
//   errors    each of the three queries failing returns ONLY `rewards_fetch_failed`
//             (500) and never the planted provider message, key/table/config names
//   guard     unauthorised callers are rejected BEFORE any rewards query executes
//   matrix    self / same-tenant manager allowed; peer rep, cross-tenant manager denied;
//             anonymous 401; bounties stays 404 and touches no table; select-title unchanged
//
// Non-vacuity: each source mutation of a COPY of src/ (anon client restored, error
// ignored per query, raw message leaked, guard weakened, bounties opened) MUST fail the
// named check. Nothing in the repo is modified.
//
// Run: npm run validate:rewards-read   (tsx, loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
type Check = { id: string; pass: boolean; detail?: string };

const MGR_A = "aaaaaaaa-0000-4000-8000-000000000002";
const REP_A = "aaaaaaaa-0000-4000-8000-000000000003";
const REP_A2 = "aaaaaaaa-0000-4000-8000-000000000004";
const MGR_B = "bbbbbbbb-0000-4000-8000-000000000002";
const CO_A = "dddddddd-0000-4000-8000-00000000000a";
const CO_B = "dddddddd-0000-4000-8000-00000000000b";
const REPS: Record<string, { id: string; tier: string; company_id: string; org_id: string }> = {
  [MGR_A]: { id: MGR_A, tier: "Manager", company_id: CO_A, org_id: CO_A },
  [REP_A]: { id: REP_A, tier: "SalesRep", company_id: CO_A, org_id: CO_A },
  [REP_A2]: { id: REP_A2, tier: "SalesRep", company_id: CO_A, org_id: CO_A },
  [MGR_B]: { id: MGR_B, tier: "Manager", company_id: CO_B, org_id: CO_B },
};
const PLANTED = "PLANTED_INTERNAL_PROVIDER_MESSAGE_zq81";

// ── stub: reps lookup + controllable rewards tables, records every table hit ──
type StubState = { fail: Set<string>; populated: boolean; hits: string[] };
function startStub(st: StubState): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url || "/", "http://stub");
    const table = u.pathname.replace("/rest/v1/", "");
    st.hits.push(table);
    const wantsObject = String(req.headers["accept"] || "").includes("vnd.pgrst.object");
    const send = (code: number, body: unknown) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(body)); };
    if (table === "reps") {
      const id = u.searchParams.get("id")?.replace(/^eq\./, "");
      const co = u.searchParams.get("company_id")?.replace(/^eq\./, "");
      const rows = Object.values(REPS).filter((r) => (id ? r.id === id : co ? r.company_id === co : false));
      if (wantsObject && rows.length !== 1) return send(406, { code: "PGRST116", message: "none", details: "", hint: null });
      return send(200, wantsObject ? rows[0] : rows);
    }
    if (["user_badges", "user_titles", "user_selected_title"].includes(table)) {
      if (st.fail.has(table)) return send(500, { code: "XX000", message: `${PLANTED} (${table}) SUPABASE_ANON_KEY missing`, details: "schema public", hint: null });
      let rows: unknown[] = [];
      if (st.populated && table === "user_badges") rows = [{ badge_id: "b1", earned_at: "2026-01-02T00:00:00Z", badges: { label: "First Call", icon: "star" } }];
      if (st.populated && table === "user_titles") rows = [{ title_id: "t1", unlocked_at: "2026-01-03T00:00:00Z", titles: { label: "Closer", icon: "bolt", bg_class: "bg-amber" } }];
      if (st.populated && table === "user_selected_title") rows = [{ title_id: "t1" }];
      if (wantsObject && rows.length !== 1) return send(406, { code: "PGRST116", message: "none", details: "", hint: null });
      return send(200, wantsObject ? rows[0] : rows);
    }
    return send(200, wantsObject ? {} : []);
  });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}

type Resp = { status: number; body: any; raw: string; headers: Headers };
async function call(base: string, method: string, p: string, headers: Record<string, string> = {}, body?: unknown): Promise<Resp> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 2500);
  try {
    const r = await fetch(base + p, { method, headers: { ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers }, body: body !== undefined ? JSON.stringify(body) : undefined, signal: ctl.signal });
    const raw = await r.text();
    let j: any = null; try { j = JSON.parse(raw); } catch { /* text */ }
    return { status: r.status, body: j, raw, headers: r.headers };
  } catch {
    return { status: 0, body: null, raw: "", headers: new Headers() };
  } finally { clearTimeout(t); }
}
const as = (uid: string) => ({ "x-user-id": uid });
const rewardsHits = (st: StubState) => st.hits.filter((h) => h.startsWith("user_")).length;
const LEAK_RE = /PLANTED|supabaseKey|SUPABASE|user_badges|user_titles|user_selected_title|schema|XX000|apikey|key is required/i;

async function suite(root: string, stubUrl: string, st: StubState): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const saved = { ...process.env };
  const log = { l: console.log, e: console.error, w: console.warn, i: console.info };
  Object.assign(process.env, { NODE_ENV: "test", SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder", GRAVIX_API_NO_LISTEN: "1" });
  for (const k of ["SUPABASE_ANON_KEY", "PROXY_SHARED_SECRET", "ALLOW_ADMIN_ENDPOINTS", "DEV_TEST_UID"]) delete process.env[k]; // anon key ABSENT, like staging
  const savedCwd = process.cwd();
  process.chdir(EMPTY_CWD);
  console.log = console.error = console.warn = console.info = () => {};
  let srv: http.Server | null = null;
  try {
    const mod = await import(path.join(root, "src/server.ts"));
    srv = http.createServer(mod.app);
    await new Promise<void>((r) => srv!.listen(0, "127.0.0.1", () => r()));
    const base = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;
    const reset = (o: Partial<StubState> = {}) => { st.fail = new Set(o.fail || []); st.populated = !!o.populated; st.hits.length = 0; };

    // works without SUPABASE_ANON_KEY + empty contract
    reset();
    let r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(REP_A));
    add("get-works-without-anon-key", r.status === 200 && r.body?.ok === true, `status=${r.status} ${r.raw.slice(0, 80)}`);
    add("empty-read-contract", JSON.stringify(r.body) === JSON.stringify({ ok: true, badges: [], titles: [], selectedTitleId: null }) && r.headers.get("cache-control") === "private, max-age=15", `${r.raw.slice(0, 120)} cc=${r.headers.get("cache-control")}`);

    // populated contract
    reset({ populated: true });
    r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(REP_A));
    const want = { ok: true, badges: [{ id: "b1", label: "First Call", icon: "star", earned_at: "2026-01-02T00:00:00Z" }], titles: [{ id: "t1", label: "Closer", icon: "bolt", bg_class: "bg-amber", unlocked_at: "2026-01-03T00:00:00Z" }], selectedTitleId: "t1" };
    add("populated-read-contract", r.status === 200 && JSON.stringify(r.body) === JSON.stringify(want) && r.headers.get("cache-control") === "private, max-age=15", `status=${r.status} ${r.raw.slice(0, 160)}`);

    // each query failing => stable error, no leak
    for (const [tbl, id] of [["user_badges", "query-error-badges-stable-no-leak"], ["user_titles", "query-error-titles-stable-no-leak"], ["user_selected_title", "query-error-selected-stable-no-leak"]] as const) {
      reset({ fail: [tbl] });
      r = await call(base, "GET", `/v1/rewards/${REP_A}`, as(REP_A));
      add(id, r.status === 500 && JSON.stringify(r.body) === JSON.stringify({ ok: false, error: "rewards_fetch_failed" }) && !LEAK_RE.test(r.raw), `status=${r.status} body=${r.raw.slice(0, 140)}`);
    }

    // unauthorised rejected BEFORE any rewards query
    reset();
    const peer = await call(base, "GET", `/v1/rewards/${REP_A2}`, as(REP_A));
    const xt = await call(base, "GET", `/v1/rewards/${REP_A}`, as(MGR_B));
    const anon = await call(base, "GET", `/v1/rewards/${REP_A}`);
    add("unauthorised-rejected-before-any-rewards-query", peer.status === 403 && xt.status === 403 && anon.status === 401 && rewardsHits(st) === 0, `peer=${peer.status} xtenant=${xt.status} anon=${anon.status} rewardsQueries=${rewardsHits(st)}`);

    // access matrix
    reset();
    const self = await call(base, "GET", `/v1/rewards/${REP_A}`, as(REP_A));
    const mgr = await call(base, "GET", `/v1/rewards/${REP_A}`, as(MGR_A));
    add("access-self-and-same-tenant-manager-allowed", self.status === 200 && mgr.status === 200, `self=${self.status} manager=${mgr.status}`);
    add("access-peer-rep-denied", peer.status === 403 && peer.body?.error === "forbidden_rep_scope", `status=${peer.status}`);
    add("access-cross-tenant-manager-denied", xt.status === 403 && xt.body?.error === "forbidden_rep_scope", `status=${xt.status}`);
    add("access-anonymous-401", anon.status === 401, `status=${anon.status}`);

    // bounties closed + touches nothing; select-title unchanged
    reset();
    const b = await call(base, "GET", "/v1/rewards/bounties/active", as(REP_A));
    add("bounties-still-404-no-table-access", b.status === 404 && b.body?.error === "bounties_not_available" && rewardsHits(st) === 0 && !st.hits.includes("bounties"), `status=${b.status} hits=${st.hits.join(",")}`);
    const notSelf = await call(base, "POST", `/v1/rewards/${REP_A2}/select-title`, as(REP_A), { titleId: "t1" });
    const noTitle = await call(base, "POST", `/v1/rewards/${REP_A}/select-title`, as(REP_A), {});
    add("select-title-unchanged", notSelf.status === 403 && notSelf.body?.error === "forbidden_not_self" && noTitle.status === 400 && noTitle.body?.error === "titleId_required", `notSelf=${notSelf.status} noTitle=${noTitle.status}`);
  } catch (e: any) {
    add("suite-executed", false, String(e?.stack || e).split("\n").slice(0, 3).join(" | "));
  } finally {
    if (srv) await new Promise<void>((r2) => { srv!.closeAllConnections?.(); srv!.close(() => r2()); });
    process.chdir(savedCwd);
    console.log = log.l; console.error = log.e; console.warn = log.w; console.info = log.i;
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
  }
  return out;
}

// ── mutation harness (throw-away COPY of src/) ──
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string, repl: string) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}
const R = "src/routes/rewards.ts";
const MUTATIONS: Array<{ name: string; file: string; find: string; repl: string; expect: string }> = [
  { name: "GET builds an anon client again (needs SUPABASE_ANON_KEY)", file: R, find: "const badgesRes = await supaAdmin\n        .from(", repl: "const badgesRes = await createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_ANON_KEY!)\n        .from(", expect: "get-works-without-anon-key" },
  { name: "badges query error ignored", file: R, find: 'if (badgesRes.error) return fail("badges", badgesRes.error.code);', repl: "", expect: "query-error-badges-stable-no-leak" },
  { name: "titles query error ignored", file: R, find: 'if (titlesRes.error) return fail("titles", titlesRes.error.code);', repl: "", expect: "query-error-titles-stable-no-leak" },
  { name: "selected-title query error ignored", file: R, find: 'if (selectedRes.error) return fail("selected-title", selectedRes.error.code);', repl: "", expect: "query-error-selected-stable-no-leak" },
  { name: "raw provider message leaked to client (badges)", file: R, find: 'if (badgesRes.error) return fail("badges", badgesRes.error.code);', repl: 'if (badgesRes.error) return res.status(500).json({ ok: false, error: badgesRes.error.message });', expect: "query-error-badges-stable-no-leak" },
  { name: "raw provider message leaked to client (selected-title)", file: R, find: 'if (selectedRes.error) return fail("selected-title", selectedRes.error.code);', repl: 'if (selectedRes.error) return res.status(500).json({ ok: false, error: selectedRes.error.message });', expect: "query-error-selected-stable-no-leak" },
  { name: "access guard removed from GET", file: R, find: "if (!(await canAccessRep((req as any).userId, userId))) {\n      return res.status(403).json({ ok: false, error: \"forbidden_rep_scope\" });\n    }\n    // Day 30", repl: "// Day 30", expect: "unauthorised-rejected-before-any-rewards-query" },
  { name: "peer access allowed (guard weakened to any identified caller)", file: R, find: "if (!(await canAccessRep((req as any).userId, userId))) {\n      return res.status(403).json({ ok: false, error: \"forbidden_rep_scope\" });\n    }\n    // Day 30", repl: "if (!(req as any).userId) {\n      return res.status(403).json({ ok: false, error: \"forbidden_rep_scope\" });\n    }\n    // Day 30", expect: "access-peer-rep-denied" },
  { name: "success cache header weakened", file: R, find: 'res.set("Cache-Control", "private, max-age=15");\n      res.json({\n        ok: true,\n        badges: (badges', repl: 'res.set("Cache-Control", "public, max-age=15");\n      res.json({\n        ok: true,\n        badges: (badges', expect: "empty-read-contract" },
  { name: "bounties handler opened", file: R, find: "const BOUNTIES_ENABLED = false;", repl: "const BOUNTIES_ENABLED = true;", expect: "bounties-still-404-no-table-access" },
];

function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) { console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${!c.pass && c.detail ? " — " + c.detail : ""}`); if (!c.pass) bad++; }
  return bad;
}

async function main() {
  console.log("Go Live Day 30 — rewards read repair (F-28-1), behavioural, loopback only\n");
  const st: StubState = { fail: new Set(), populated: false, hits: [] };
  const stub = await startStub(st);
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`;
  let failures = 0;
  try {
    failures += report("Real app, SUPABASE_ANON_KEY absent:", await suite(ROOT, stubUrl, st));
    console.log("\nMutation proof — each mutation of a COPY of src/ MUST fail the named check:");
    for (const m of MUTATIONS) {
      let dir = "";
      try {
        dir = copyTree(ROOT);
        mutate(dir, m.file, m.find, m.repl);
        const checks = await suite(dir, stubUrl, st);
        const hit = checks.find((c) => c.id === m.expect);
        const control = checks.find((c) => c.id === "access-anonymous-401");
        const caught = !!hit && !hit.pass && !!control?.pass; // control keeps a dead harness from counting
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found / harness dead)"}`);
        if (!caught) failures++;
      } catch (e: any) {
        console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++;
      } finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally {
    stub.close();
    fs.rmSync(EMPTY_CWD, { recursive: true, force: true });
  }
  if (failures) { console.log(`\n❌ rewards read validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ rewards read validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
