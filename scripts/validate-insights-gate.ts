// Manager gate + Insights correctness proof (network-free: a loopback fake PostgREST stands in for Supabase).
//
//   GATE      GET /v1/assignments/manager through the REAL assignments router: Manager / Owner / PartnerAdmin / SuperAdmin pass;
//             SalesRep, an unknown "Admin" tier, an unknown user and a missing user are refused; the shared isManagerTier is the
//             only rule (no private role set remains)
//   TENANT    callers see only their own company/office; a rep id from another company yields nothing; a manager whose tenant
//             cannot be resolved (no users row, no reps.company_id) sees NOTHING instead of every company's assignments;
//             a manager with no users row is scoped through reps.company_id
//   TREND     GET /v1/crm/analytics/score-trend through the REAL crm router: unscored calls are excluded (never 0), `count` is the
//             real sample size, a genuine 0 score is kept, org + rep + date filters still apply, sparse/empty windows are empty
//   CENTRE    command-centre empty average is null (never 0), real zero counts are preserved, scoredCalls is the sample size
// Run: npm run validate:insights-gate

import * as fs from "node:fs";
import * as path from "node:path";
import http from "node:http";
import express from "express";

let failures = 0;
const check = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) failures++; };

// ───────────────────────────── fake PostgREST ─────────────────────────────
type Row = Record<string, any>;
const DB: Record<string, Row[]> = {};
const unsupported: string[] = [];
function applyFilters(rows: Row[], sp: URLSearchParams): Row[] {
  let out = rows;
  for (const [k, v] of sp.entries()) {
    if (["select", "order", "limit", "offset", "columns"].includes(k)) continue;
    let m: RegExpMatchArray | null;
    if ((m = v.match(/^eq\.(.*)$/))) out = out.filter((r) => String(r[k]) === m![1]);
    else if (v === "not.is.null") out = out.filter((r) => r[k] !== null && r[k] !== undefined);
    else if ((m = v.match(/^gte\.(.*)$/))) out = out.filter((r) => String(r[k]) >= m![1]);
    else if ((m = v.match(/^lt\.(.*)$/))) out = out.filter((r) => String(r[k]) < m![1]);
    else unsupported.push(`${k}=${v}`);
  }
  return out;
}
const fake = http.createServer((req, res) => {
  const u = new URL(req.url || "/", "http://x");
  const table = u.pathname.replace(/^\/rest\/v1\//, "");
  let rows = applyFilters(DB[table] ?? [], u.searchParams);
  const ord = u.searchParams.getAll("order");
  for (const o of ord.reverse()) { const [col, dir] = o.split("."); rows = rows.slice().sort((a, b) => (String(a[col]) < String(b[col]) ? -1 : String(a[col]) > String(b[col]) ? 1 : 0) * (dir === "desc" ? -1 : 1)); }
  const lim = Number(u.searchParams.get("limit") || 0); if (lim > 0) rows = rows.slice(0, lim);
  const wantObject = /vnd\.pgrst\.object/.test(String(req.headers.accept || ""));
  res.setHeader("content-type", "application/json");
  if (wantObject) {
    if (rows.length !== 1) { res.statusCode = 406; return res.end(JSON.stringify({ code: "PGRST116", message: "no/multiple rows", details: "", hint: null })); }
    return res.end(JSON.stringify(rows[0]));
  }
  res.end(JSON.stringify(rows));
});

// ───────────────────────────── data ─────────────────────────────
const A = "aaaaaaaa-0000-4000-8000-00000000000a", B = "bbbbbbbb-0000-4000-8000-00000000000b";
const OFF_X = "00000000-0000-4000-8000-0000000000f1", OFF_Y = "00000000-0000-4000-8000-0000000000f2";
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const U = { rep: id(1), mgr: id(2), owner: id(3), partner: id(4), superadmin: id(5), mgrB: id(6), noTenant: id(7), adminTier: id(8), mgrNoUsersRow: id(9), repB: id(10), ghost: id(99), rep2: id(11) };

function seed() {
  DB.reps = [
    { id: U.rep, tier: "SalesRep", company_id: A, org_id: A }, { id: U.mgr, tier: "Manager", company_id: A, org_id: A }, { id: U.owner, tier: "Owner", company_id: A, org_id: A },
    { id: U.partner, tier: "PartnerAdmin", company_id: A, org_id: A }, { id: U.superadmin, tier: "SuperAdmin", company_id: A, org_id: A },
    { id: U.mgrB, tier: "Manager", company_id: B, org_id: B }, { id: U.noTenant, tier: "Manager", company_id: null, org_id: null }, { id: U.adminTier, tier: "Admin", company_id: A, org_id: A },
    { id: U.mgrNoUsersRow, tier: "Manager", company_id: A, org_id: A }, { id: U.repB, tier: "SalesRep", company_id: B, org_id: B }, { id: U.rep2, tier: "SalesRep", company_id: A, org_id: A },
  ];
  // users rows drive the legacy scope (office/company). mgrNoUsersRow / noTenant / ghost intentionally have none.
  DB.users = [
    { id: U.mgr, role: "company_manager", company_id: A, office_id: null, is_admin: false },
    { id: U.owner, role: "company_manager", company_id: A, office_id: null, is_admin: false },
    { id: U.partner, role: "company_manager", company_id: A, office_id: null, is_admin: false },
    { id: U.superadmin, role: "office_manager", company_id: A, office_id: OFF_X, is_admin: true },
    { id: U.mgrB, role: "company_manager", company_id: B, office_id: null, is_admin: false },
    { id: U.rep, role: "rep", company_id: A, office_id: null, is_admin: false },
  ];
  const mk = (n: number, company: string, office: string | null, rep: string) => ({ id: id(1000 + n), rep_id: rep, manager_id: null, type: "call", target_id: null, title: `t${n}`, status: "assigned", due_at: null, created_at: `2026-07-${String(10 + n).padStart(2, "0")}T00:00:00Z`, completed_at: null, completed_by: null, source: null, meta: null, company_id: company, office_id: office });
  DB.assignments = [mk(1, A, OFF_X, U.rep), mk(2, A, OFF_Y, U.rep), mk(3, A, null, U.rep), mk(4, B, null, U.repB), mk(5, B, null, U.repB)];
  const call = (n: number, org: string, user: string, day: string, score: number | null) => ({ id: id(2000 + n), org_id: org, user_id: user, created_at: `${day}T10:00:00Z`, score_overall: score });
  const recent = (d: number) => new Date(Date.now() - d * 86400000).toISOString().slice(0, 10);
  DB.calls = [
    call(1, A, U.rep, recent(2), 80), call(2, A, U.rep, recent(2), 60), call(3, A, U.rep, recent(2), null), call(4, A, U.rep, recent(2), null), // day1: avg 70 over 2 scored (not 35)
    call(5, A, U.rep, recent(3), 0), // genuine zero kept
    call(6, A, U.rep, recent(4), null), // only-unscored day → no point
    call(7, A, U.mgr, recent(5), 90),
    call(8, B, U.repB, recent(2), 10), // other tenant
    call(9, A, U.rep, "2020-01-01", 55), // outside the window
    call(10, A, U.rep2, recent(6), 33), // another rep in the same tenant
  ];
  DB.__recent2 = [{ d: recent(2) }, { d: recent(3) }, { d: recent(4) }, { d: recent(5) }];
}

async function jget(base: string, p: string, headers: Record<string, string> = {}) {
  const r = await fetch(base + p, { headers });
  let body: any = null; try { body = await r.json(); } catch { /* none */ }
  return { status: r.status, body };
}

(async () => {
  await new Promise<void>((r) => fake.listen(0, "127.0.0.1", r));
  const fport = (fake.address() as any).port;
  process.env.SUPABASE_URL = `http://127.0.0.1:${fport}`;
  process.env.SUPABASE_SERVICE_ROLE_KEY = "validator-dummy-key";
  process.env.DEFAULT_ORG_ID = ""; // never fall back to a real org
  seed();

  const { assignmentsRoutes } = await import("../src/routes/assignments");
  const crmRouter = (await import("../src/routes/crm")).default as express.Router;
  const manager = await import("../src/routes/manager");
  const { isManagerTier } = await import("../src/lib/repAccess");

  const app = express();
  app.use(express.json());
  app.use("/v1/assignments", assignmentsRoutes());
  app.use("/v1/crm", crmRouter);
  const srv = http.createServer(app);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const base = `http://127.0.0.1:${(srv.address() as any).port}`;
  const asUser = (u: string, org?: string) => ({ "x-user-id": u, ...(org ? { "x-org-id": org } : {}) });

  // ── GATE
  const tiers: Array<[string, boolean]> = [["Manager", true], ["Owner", true], ["PartnerAdmin", true], ["SuperAdmin", true], ["SalesRep", false], ["Admin", false], ["manager", false], ["", false]];
  check("isManagerTier: Manager/Owner/PartnerAdmin/SuperAdmin pass; SalesRep, unknown 'Admin', wrong case and empty do not", tiers.every(([t, want]) => isManagerTier(t) === want) && !isManagerTier(null) && !isManagerTier(undefined));
  const ok = async (who: string) => jget(base, "/v1/assignments/manager?limit=100", asUser(who));
  for (const [name, who] of [["Manager", U.mgr], ["Owner", U.owner], ["PartnerAdmin", U.partner], ["SuperAdmin", U.superadmin]] as const) {
    const r = await ok(who);
    check(`gate: ${name} → 200`, r.status === 200 && r.body?.ok === true, `${r.status} ${JSON.stringify(r.body)?.slice(0, 80)}`);
  }
  let r = await ok(U.rep); check("gate: SalesRep → 403 forbidden_not_manager", r.status === 403 && r.body?.error === "forbidden_not_manager", `${r.status}`);
  r = await ok(U.adminTier); check("gate: unknown 'Admin' tier → 403", r.status === 403);
  r = await ok(U.ghost); check("gate: user with no reps row → 403", r.status === 403);
  r = await jget(base, "/v1/assignments/manager"); check("gate: no user id → 401", r.status === 401, `${r.status}`);
  const src = fs.readFileSync(path.join(__dirname, "../src/routes/assignments.ts"), "utf8");
  check("no private role set remains; both manager checks use isManagerTier", !/MANAGER_ROLES/.test(src) && (src.match(/isManagerTier\(/g) || []).length >= 2);

  // ── TENANT
  const idsOf = (x: any) => (x?.body?.items ?? []).map((i: any) => i.id).sort();
  const ass = (ns: number[]) => ns.map((n) => id(1000 + n)).sort();
  r = await ok(U.mgr); check("tenant: company manager A sees exactly company A's 3 assignments (none of B's)", JSON.stringify(idsOf(r)) === JSON.stringify(ass([1, 2, 3])), JSON.stringify(idsOf(r)));
  r = await ok(U.superadmin); check("tenant: SuperAdmin keeps the existing office scope (office X only)", JSON.stringify(idsOf(r)) === JSON.stringify(ass([1])), JSON.stringify(idsOf(r)));
  r = await ok(U.mgrB); check("tenant: company manager B sees only B's 2 assignments", JSON.stringify(idsOf(r)) === JSON.stringify(ass([4, 5])), JSON.stringify(idsOf(r)));
  r = await jget(base, `/v1/assignments/manager?rep_id=${U.repB}`, asUser(U.mgr)); check("tenant: manager A asking for company B's rep gets nothing", (r.body?.items ?? []).length === 0, JSON.stringify(idsOf(r)));
  r = await ok(U.mgrNoUsersRow); check("tenant: manager with no users row is scoped through reps.company_id (A only, not everyone)", JSON.stringify(idsOf(r)) === JSON.stringify(ass([1, 2, 3])), JSON.stringify(idsOf(r)));
  r = await ok(U.noTenant); check("tenant: manager whose tenant cannot be resolved sees NOTHING (fail closed)", r.status === 200 && (r.body?.items ?? []).length === 0, JSON.stringify(idsOf(r)));
  check("tenant: handler still applies applyOrgScope and fails closed", /applyOrgScope\(q, managerContext\)/.test(src) && /Fail closed/.test(src));

  // ── TREND
  const trend = async (who: string, org: string, qs = "days=30") => jget(base, `/v1/crm/analytics/score-trend?${qs}`, asUser(who, org));
  r = await trend(U.mgr, A);
  const pts: any[] = r.body?.trend ?? [];
  const recent = (d: number) => new Date(Date.now() - d * 86400000).toISOString().slice(0, 10);
  const byDate = Object.fromEntries(pts.map((p) => [p.date, p]));
  check("trend: unscored calls are excluded — day average is 70 over the 2 scored calls (not 35)", byDate[recent(2)]?.avg_score === 70 && byDate[recent(2)]?.count === 2, JSON.stringify(byDate[recent(2)]));
  check("trend: a genuine score of 0 is kept (avg 0, count 1)", byDate[recent(3)]?.avg_score === 0 && byDate[recent(3)]?.count === 1, JSON.stringify(byDate[recent(3)]));
  check("trend: a day with only unscored calls produces no point", !(recent(4) in byDate));
  check("trend: dates ascend, every point has {date, avg_score, count} (additive, compatible shape)", pts.every((p) => typeof p.date === "string" && Number.isInteger(p.avg_score) && Number.isInteger(p.count)) && pts.map((p) => p.date).join() === pts.map((p) => p.date).sort().join());
  check("trend: date window applied (2020 call excluded)", !("2020-01-01" in byDate));
  check("trend: org filter applied — org B's call (score 10) never appears for org A", pts.every((p) => p.avg_score !== 10));
  const rb = await trend(U.mgrB, B); check("trend: org B sees only its own single point", (rb.body?.trend ?? []).length === 1 && rb.body.trend[0].avg_score === 10, JSON.stringify(rb.body?.trend));
  r = await trend(U.mgr, A, `days=30&repId=${U.mgr}`); check("trend: rep filter preserved (manager's own call only → one point, 90, count 1)", (r.body?.trend ?? []).length === 1 && r.body.trend[0].avg_score === 90 && r.body.trend[0].count === 1 && r.body.scope === "rep", JSON.stringify(r.body));
  r = await trend(U.mgr, A, "days=1"); check("trend: sparse window → empty trend, ok:true (no fabricated points)", r.status === 200 && r.body?.ok === true && (r.body.trend ?? []).length === 0, JSON.stringify(r.body));
  DB.calls = DB.calls.map((c) => ({ ...c, score_overall: null }));
  r = await trend(U.mgr, A, "days=365"); check("trend: org with only unscored calls → empty trend (never a run of zeros)", (r.body?.trend ?? []).length === 0, JSON.stringify(r.body?.trend));
  const trendLib = await import("../src/lib/scoreTrend");
  check("scoreTrend helper: null / undefined / '' / NaN / true are not scores; '72' and 0 are",
    trendLib.realScore(null) === null && trendLib.realScore(undefined) === null && trendLib.realScore("") === null && trendLib.realScore(NaN) === null && trendLib.realScore(true as any) === null && trendLib.realScore("72") === 72 && trendLib.realScore(0) === 0);
  check("scoreTrend helper: empty input → []; bad dates skipped", trendLib.buildScoreTrend([]).length === 0 && trendLib.buildScoreTrend([{ score_overall: 50, created_at: null }, { score_overall: 50, created_at: "nope" }]).length === 0);
  check("trend: unsupported/unknown filter shapes never reached the fake DB (harness honesty)", unsupported.length === 0, unsupported.join(","));

  // ── TREND ACCESS (every role; self / other / no-rep; cross-tenant; fail closed)
  seed(); // fresh data (the sparse tests above blanked the scores)
  const callsOf = (x: any) => (x.body?.trend ?? []).reduce((n: number, p: any) => n + p.count, 0);
  const acc = (who: string | null, qs = "days=365", extra: Record<string, string> = {}) => jget(base, `/v1/crm/analytics/score-trend?${qs}`, { ...(who ? { "x-user-id": who } : {}), ...extra });
  for (const [name, who] of [["Manager", U.mgr], ["Owner", U.owner], ["PartnerAdmin", U.partner], ["SuperAdmin", U.superadmin]] as const) {
    r = await acc(who);
    check(`access: ${name} → org/team trend (200, scope team, only own org: 5 scored A calls in the window, none of B's)`, r.status === 200 && r.body?.scope === "team" && callsOf(r) === 5 && (r.body.trend as any[]).every((p) => p.avg_score !== 10), `${r.status} n=${callsOf(r)}`);
    r = await acc(who, `days=365&repId=${U.rep}`);
    check(`access: ${name} → a rep inside their tenant (200, scope rep)`, r.status === 200 && r.body?.scope === "rep" && r.body?.rep_id === U.rep, `${r.status}`);
  }
  const deniedBodies: string[] = [];
  const denied = async (label: string, who: string | null, qs: string, status = 403, extra: Record<string, string> = {}) => { const x = await acc(who, qs, extra); if (status === 403) deniedBodies.push(JSON.stringify(x.body)); check(label, x.status === status && !(x.body?.trend), `${x.status} ${JSON.stringify(x.body)?.slice(0, 70)}`); };
  await denied("access: manager A asking for org B's rep → 403", U.mgr, `days=365&repId=${U.repB}`);
  await denied("access: manager A asking for an unknown rep id → 403 (same as other tenant)", U.mgr, `days=365&repId=${id(555)}`);
  await denied("access: manager B asking for org A's rep → 403", U.mgrB, `days=365&repId=${U.rep}`);
  r = await acc(U.mgr, "days=365", { "x-org-id": B });
  check("access: a forged x-org-id (org B) is ignored — manager A still gets only org A's data", r.status === 200 && callsOf(r) === 5 && (r.body.trend as any[]).every((p) => p.avg_score !== 10));
  await denied("access: SalesRep, no repId (org-wide) → 403", U.rep, "days=365");
  await denied("access: SalesRep asking for another rep in the same tenant → 403", U.rep, `days=365&repId=${U.rep2}`);
  await denied("access: SalesRep asking for a manager in the same tenant → 403", U.rep, `days=365&repId=${U.mgr}`);
  await denied("access: SalesRep asking for a rep in another tenant → 403", U.rep, `days=365&repId=${U.repB}`);
  await denied("access: SalesRep asking for an unknown rep id → 403", U.rep, `days=365&repId=${id(556)}`);
  r = await acc(U.rep, `days=365&repId=${U.rep}`);
  check("access: SalesRep asking for THEMSELVES → self-only trend (200, scope rep, only their own scored calls: 3)", r.status === 200 && r.body?.scope === "rep" && r.body?.rep_id === U.rep && callsOf(r) === 3, `${r.status} n=${callsOf(r)}`);
  r = await acc(U.rep, `days=365&repId=${U.rep.toUpperCase()}`);
  check("access: self match is case-insensitive but still verified (upper-case own id → 200 self-only)", r.status === 200 && r.body?.scope === "rep" && callsOf(r) === 3);
  r = await acc(U.repB, `days=365&repId=${U.repB}`); check("access: org B's SalesRep sees only their own call (10), never org A's", r.status === 200 && callsOf(r) === 1 && r.body.trend[0].avg_score === 10);
  await denied("access: SalesRep with a forged x-org-id still cannot read the org-wide trend", U.rep, "days=365", 403, { "x-org-id": A });
  await denied("access: unknown 'Admin' tier is treated as non-manager → org-wide 403", U.adminTier, "days=365");
  r = await acc(U.adminTier, `days=365&repId=${U.adminTier}`); check("access: unknown tier may read only itself", r.status === 200 && r.body?.scope === "rep");
  await denied("access: Manager with no organisation on record → 403 (fail closed, team)", U.noTenant, "days=365");
  await denied("access: Manager with no organisation on record → 403 (fail closed, even self)", U.noTenant, `days=365&repId=${U.noTenant}`);
  await denied("access: caller with no reps row → 403", U.ghost, "days=365");
  await denied("access: no user id → 401", null, "days=365", 401);
  await denied("access: malformed user id → 401", "not-a-uuid", "days=365", 401);
  await denied("access: malformed repId → 400 invalid_rep_id for a manager (never widened to the whole team)", U.mgr, "days=365&repId=abc", 400);
  await denied("access: malformed repId → 400 for a SalesRep", U.rep, "days=365&repId=abc", 400);
  check("access: every 403 body is byte-identical (no existence/tenant oracle)", deniedBodies.length >= 10 && new Set(deniedBodies).size === 1, JSON.stringify([...new Set(deniedBodies)]));
  const trendSrc = fs.readFileSync(path.join(__dirname, "../src/routes/crm.ts"), "utf8");
  const h = trendSrc.slice(trendSrc.indexOf('router.get("/analytics/score-trend"'), trendSrc.indexOf("Day 213B — display-only rep label"));
  check("access: handler resolves scope from the verified caller (resolveScoreTrendScope + repLookups); no x-org-id / raw repId / error text", /resolveScoreTrendScope\(repLookups/.test(h) && !/getOrgIdHeader|UUID_RE\.test\(repIdRaw\)|e\?\.message|e\.message/.test(h));
  check("access: failed read is an error, not an empty trend", /if \(r\.error\) return res\.status\(500\)/.test(h));

  // ── COMMAND CENTRE
  check("command-centre: no scored calls → averageOrNull is null, not 0", manager.averageOrNull(0, 0) === null && manager.averageOrNull(Number.NaN, 3) === null);
  check("command-centre: real averages (incl. a genuine 0) are kept", manager.averageOrNull(140, 2) === 70 && manager.averageOrNull(0, 3) === 0);
  check("command-centre: team health with no average is still computed (null is handled, zero overdue stays a real 0)", manager.computeTeamHealthStatus({ averageScore: null, callsNeedingReview: 0, overdueAssignments: 0 }) === "green");
  const mgrSrc = fs.readFileSync(path.join(__dirname, "../src/routes/manager.ts"), "utf8");
  const resp = mgrSrc.slice(mgrSrc.indexOf("teamHealth: {\n        status:"), mgrSrc.indexOf("repsNeedingAttention,\n      callsNeedingReview: reviewCalls"));
  check("command-centre: response averageScore is the nullable average (no `?? 0`), scoredCalls added, real counts untouched",
    /averageScore: teamAverage,/.test(resp) && !/averageScore: teamAverage \?\? 0/.test(resp) && /scoredCalls: teamScoreCount/.test(resp) && /overdueAssignments: overdueAssignmentRows\.length/.test(resp) && /openAssignments: openAssignmentRows\.length/.test(resp));

  srv.close(); fake.close();
  console.log(failures ? `\n${failures} FAILED` : "\ninsights + gate proof PASSED");
  process.exit(failures ? 1 : 0);
})().catch((e) => { console.error("validator crashed:", e?.message || e); process.exit(2); });
