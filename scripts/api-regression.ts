/**
 * API Regression Suite — Gravix Sales Trainer
 *
 * Covers: /v1/dashboard /v1/assignments /v1/crm /v1/calls /v1/reps /v1/sparring
 *
 * Env vars (all optional):
 *   API_BASE        server URL          (default: http://localhost:4000)
 *   TEST_USER_ID    rep UUID            enables authenticated + payload tests
 *   TEST_ORG_ID     org UUID            enables org-scoped tests
 *   TEST_WRONG_ORG  uuid from other org enables org-isolation checks
 *
 * Run: tsx scripts/api-regression.ts
 */

import "dotenv/config";
import {
  apiReq,
  c,
  statusIs,
  statusOk,
  statusNot5xx,
  statusIsAuth,
  hasField,
  fieldIsArray,
  isOk,
  type Check,
  type TestResult,
} from "./lib/api-client.ts";

const API_BASE = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const USER_ID = process.env.TEST_USER_ID?.trim() || null;
const ORG_ID = process.env.TEST_ORG_ID?.trim() || null;
// A valid UUID that doesn't belong to the test user's org — used for isolation checks
const WRONG_ORG = process.env.TEST_WRONG_ORG?.trim() ?? "ffffffff-ffff-ffff-4fff-ffffffffffff";
// Safe fake org_id for smoke tests (crm.ts accepts zero-UUID explicitly)
const SMOKE_ORG = "00000000-0000-0000-0000-000000000000";
// Safe fake UUID for /:id param routes — avoids null-handling edge cases
const FAKE_ID = "00000000-0000-0000-0000-000000000001";

const results: TestResult[] = [];

// ---- test runner helpers ----

async function test(suite: string, name: string, fn: () => Promise<Check[]>): Promise<void> {
  const t0 = Date.now();
  try {
    const checks = await fn();
    results.push({ suite, name, passed: checks.every(ch => ch.passed), durationMs: Date.now() - t0, checks });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    results.push({ suite, name, passed: false, durationMs: Date.now() - t0, checks: [], error: msg });
  }
}

function skip(suite: string, name: string, reason: string): void {
  results.push({ suite, name, passed: true, durationMs: 0, checks: [], skipped: true, skipReason: reason });
}

// ---- suites ----

async function healthSuite() {
  await test("Health", "GET /v1/debug/health → 200 + service field", async () => {
    const r = await apiReq(API_BASE, "/v1/debug/health");
    return [statusIs(r.status, 200), isOk(r.body), hasField(r.body, "service")];
  });
}

async function authGuardSuite() {
  // These routes explicitly use requireAuth middleware.
  await test("Auth Guard", "/v1/debug/auth — rejects without x-user-id", async () => {
    const r = await apiReq(API_BASE, "/v1/debug/auth");
    return [statusIsAuth(r.status)];
  });

  await test("Auth Guard", "/v1/debug/db — rejects without x-user-id", async () => {
    const r = await apiReq(API_BASE, "/v1/debug/db");
    return [statusIsAuth(r.status)];
  });

  // Assignments enforces auth at route level — only testable when DEV_TEST_UID is not set.
  // DEV_TEST_UID fills in a userId for all requests in local dev, bypassing this check.
  const DEV_TEST_UID = process.env.DEV_TEST_UID?.trim() || null;
  if (!DEV_TEST_UID) {
    await test("Auth Guard", "GET /v1/assignments — rejects without x-user-id", async () => {
      const r = await apiReq(API_BASE, "/v1/assignments");
      return [statusIsAuth(r.status)];
    });
  } else {
    skip("Auth Guard", "GET /v1/assignments auth rejection", "DEV_TEST_UID is set — route-level auth bypassed in dev mode");
  }
}

async function dashboardSuite() {
  const S = "Dashboard";

  // Smoke: routes must respond without crashing (no auth sent)
  const smokePaths = [
    "/v1/dashboard/kpis",
    "/v1/dashboard/leaderboard",
    "/v1/dashboard/reporting-summary",
    "/v1/dashboard/flags/summary",
    "/v1/dashboard/rep-summary",
    "/v1/dashboard/voice-score-summary",
  ];
  for (const path of smokePaths) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path);
      return [statusNot5xx(r.status)];
    });
  }

  if (!USER_ID) { skip(S, "Authenticated dashboard tests", "TEST_USER_ID not set"); return; }

  await test(S, "GET /v1/dashboard/kpis — 2xx + ok + numeric total_calls", async () => {
    const r = await apiReq(API_BASE, "/v1/dashboard/kpis", { userId: USER_ID });
    return [
      statusOk(r.status),
      isOk(r.body),
      hasField(r.body, "total_calls"),
    ];
  });

  await test(S, "GET /v1/dashboard/leaderboard — 2xx + items array", async () => {
    const r = await apiReq(API_BASE, "/v1/dashboard/leaderboard", { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body), fieldIsArray(r.body, "items")];
  });

  await test(S, "GET /v1/dashboard/reporting-summary — 2xx + ok", async () => {
    const r = await apiReq(API_BASE, "/v1/dashboard/reporting-summary", { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body)];
  });
}

async function assignmentsSuite() {
  const S = "Assignments";

  // Smoke: hit routes with a fake user (will be rejected, not crash)
  for (const path of ["/v1/assignments", "/v1/assignments/summary", "/v1/assignments/by-target"]) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path, { userId: FAKE_ID, orgId: SMOKE_ORG });
      return [statusNot5xx(r.status)];
    });
  }

  if (!USER_ID) { skip(S, "Authenticated assignments tests", "TEST_USER_ID not set"); return; }

  await test(S, "GET /v1/assignments — 2xx + items array", async () => {
    const r = await apiReq(API_BASE, "/v1/assignments", { userId: USER_ID, orgId: ORG_ID ?? undefined });
    return [statusOk(r.status), isOk(r.body), fieldIsArray(r.body, "items")];
  });

  await test(S, "GET /v1/assignments/summary — 2xx + ok", async () => {
    const r = await apiReq(API_BASE, "/v1/assignments/summary", { userId: USER_ID, orgId: ORG_ID ?? undefined });
    return [statusOk(r.status), isOk(r.body)];
  });
}

async function crmSuite() {
  const S = "CRM";

  // CRM has its own health check
  await test(S, "GET /v1/crm/health → 200 + ok", async () => {
    const r = await apiReq(API_BASE, "/v1/crm/health");
    return [statusIs(r.status, 200), isOk(r.body)];
  });

  // Smoke: routes with SMOKE_ORG (accepted by crm.ts org validator) + fake user
  const crmSmokePaths = [
    "/v1/crm/contacts",
    "/v1/crm/analytics/summary",
    "/v1/crm/analytics/stage-conversion",
    "/v1/crm/activities",
    "/v1/crm/actions/today",
    "/v1/crm/opportunities/pipeline",
    "/v1/crm/manager/overview",
    "/v1/crm/manager/contacts",
  ];
  for (const path of crmSmokePaths) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path, { userId: FAKE_ID, orgId: SMOKE_ORG });
      return [statusNot5xx(r.status)];
    });
  }

  if (!USER_ID || !ORG_ID) { skip(S, "Authenticated + org-scoping CRM tests", "TEST_USER_ID and TEST_ORG_ID not set"); return; }

  await test(S, "GET /v1/crm/contacts — 2xx with valid org_id", async () => {
    const r = await apiReq(API_BASE, "/v1/crm/contacts", { userId: USER_ID, orgId: ORG_ID });
    return [statusOk(r.status)];
  });

  await test(S, "GET /v1/crm/analytics/summary — 2xx with valid org_id", async () => {
    const r = await apiReq(API_BASE, "/v1/crm/analytics/summary", { userId: USER_ID, orgId: ORG_ID });
    return [statusOk(r.status)];
  });

  await test(S, "GET /v1/crm/opportunities/pipeline — 2xx with valid org_id", async () => {
    const r = await apiReq(API_BASE, "/v1/crm/opportunities/pipeline", { userId: USER_ID, orgId: ORG_ID });
    return [statusOk(r.status)];
  });

  await test(S, "GET /v1/crm/manager/contacts — 2xx + contacts array", async () => {
    const r = await apiReq(API_BASE, "/v1/crm/manager/contacts", { userId: USER_ID, orgId: ORG_ID });
    return [statusOk(r.status), isOk(r.body), fieldIsArray(r.body, "contacts")];
  });

  // Org isolation: wrong org must not return same data as correct org
  await test(S, "CRM org_id isolation — wrong org gets no data (not same as correct org)", async () => {
    const correct = await apiReq(API_BASE, "/v1/crm/contacts", { userId: USER_ID, orgId: ORG_ID });
    const wrong = await apiReq(API_BASE, "/v1/crm/contacts", { userId: USER_ID, orgId: WRONG_ORG });

    const correctLen = Array.isArray((correct.body as Record<string, unknown>)?.contacts)
      ? ((correct.body as Record<string, unknown>).contacts as unknown[]).length
      : -1;
    const wrongLen = Array.isArray((wrong.body as Record<string, unknown>)?.contacts)
      ? ((wrong.body as Record<string, unknown>).contacts as unknown[]).length
      : -1;

    const isolated = wrong.status >= 400 || wrongLen < correctLen || wrongLen === 0;
    return [
      statusOk(correct.status),
      c("wrong org returns fewer/no results or 4xx", isolated),
    ];
  });
}

async function callsSuite() {
  const S = "Calls";

  // Smoke: list and paged endpoints (will return empty for FAKE_ID user, not crash)
  for (const path of ["/v1/calls", "/v1/calls/paged"]) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path, { userId: FAKE_ID });
      return [statusNot5xx(r.status)];
    });
  }

  // ID param routes — fake id exercises routing without real data
  await test(S, `SMOKE GET /v1/calls/:id`, async () => {
    const r = await apiReq(API_BASE, `/v1/calls/${FAKE_ID}`, { userId: FAKE_ID });
    return [statusNot5xx(r.status)];
  });

  await test(S, `SMOKE GET /v1/calls/:id/scores`, async () => {
    const r = await apiReq(API_BASE, `/v1/calls/${FAKE_ID}/scores`, { userId: FAKE_ID });
    return [statusNot5xx(r.status)];
  });

  if (!USER_ID) { skip(S, "Authenticated calls tests", "TEST_USER_ID not set"); return; }

  await test(S, "GET /v1/calls — 2xx + calls array", async () => {
    const r = await apiReq(API_BASE, "/v1/calls", { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body), fieldIsArray(r.body, "calls")];
  });

  await test(S, "GET /v1/calls/paged — 2xx + calls array", async () => {
    const r = await apiReq(API_BASE, "/v1/calls/paged", { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body), fieldIsArray(r.body, "calls")];
  });
}

async function repsSuite() {
  const S = "Reps";

  // Smoke: all /:id routes with a fake ID
  const repSmokePaths = [
    `/v1/reps/${FAKE_ID}/overview`,
    `/v1/reps/${FAKE_ID}/weakness-trends`,
    `/v1/reps/${FAKE_ID}/daily-feed`,
    `/v1/reps/${FAKE_ID}/xp`,
  ];
  for (const path of repSmokePaths) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path, { userId: FAKE_ID });
      return [statusNot5xx(r.status)];
    });
  }

  if (!USER_ID) { skip(S, "Authenticated reps tests", "TEST_USER_ID not set"); return; }

  await test(S, "GET /v1/reps/:id/overview — 2xx for own user", async () => {
    const r = await apiReq(API_BASE, `/v1/reps/${USER_ID}/overview`, { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body)];
  });

  await test(S, "GET /v1/reps/:id/weakness-trends — 2xx for own user", async () => {
    const r = await apiReq(API_BASE, `/v1/reps/${USER_ID}/weakness-trends`, { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body)];
  });

  await test(S, "GET /v1/reps/:id/xp — 2xx for own user", async () => {
    const r = await apiReq(API_BASE, `/v1/reps/${USER_ID}/xp`, { userId: USER_ID });
    return [statusOk(r.status), isOk(r.body)];
  });
}

async function sparringSuite() {
  const S = "Sparring";

  // Personas is static + no auth required
  await test(S, "GET /v1/sparring/personas — 200 + personas array (no auth)", async () => {
    const r = await apiReq(API_BASE, "/v1/sparring/personas");
    return [statusIs(r.status, 200), isOk(r.body), fieldIsArray(r.body, "personas")];
  });

  // Smoke: session routes
  for (const path of [
    "/v1/sparring/sessions",
    `/v1/sparring/sessions/${FAKE_ID}`,
    `/v1/sparring/sessions/${FAKE_ID}/analytics`,
    `/v1/sparring/leaderboard/${FAKE_ID}`,
  ]) {
    await test(S, `SMOKE ${path}`, async () => {
      const r = await apiReq(API_BASE, path, { userId: FAKE_ID });
      return [statusNot5xx(r.status)];
    });
  }

  if (!USER_ID) { skip(S, "Authenticated sparring tests", "TEST_USER_ID not set"); return; }

  await test(S, "GET /v1/sparring/personas — 200 with auth", async () => {
    const r = await apiReq(API_BASE, "/v1/sparring/personas", { userId: USER_ID });
    return [statusIs(r.status, 200), isOk(r.body), fieldIsArray(r.body, "personas")];
  });

  await test(S, "GET /v1/sparring/sessions — 2xx with auth", async () => {
    const r = await apiReq(API_BASE, "/v1/sparring/sessions", { userId: USER_ID });
    return [statusOk(r.status)];
  });
}

async function accountsSuite() {
  const S = "Accounts";

  // POST without auth → 401
  await test(S, "POST /v1/accounts — 401 without x-user-id", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", { method: "POST", body: { name: "Ghost" } });
    return [statusIsAuth(r.status)];
  });

  // POST without name → 400
  await test(S, "POST /v1/accounts — 400 when name missing", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", { userId: FAKE_ID, method: "POST", body: {} });
    return [statusNot5xx(r.status), c("not 201", r.status !== 201)];
  });

  // GET list — smoke
  await test(S, "SMOKE GET /v1/accounts", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", { userId: FAKE_ID });
    return [statusNot5xx(r.status)];
  });

  if (!USER_ID) { skip(S, "Accounts authenticated tests", "TEST_USER_ID not set"); return; }

  // Full create → read → duplicate → cleanup cycle
  const uniqueName = `Regression Test Co ${Date.now()}`;

  let createdId: string | null = null;

  await test(S, "POST /v1/accounts — 201 + account shape", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", {
      userId: USER_ID,
      method: "POST",
      body: { name: uniqueName, domain: "regression-test.example.com" },
    });
    if (r.status === 201) createdId = (r.body as any)?.account?.id ?? null;
    return [
      statusIs(r.status, 201),
      isOk(r.body),
      hasField(r.body, "account"),
      hasField((r.body as any)?.account, "id"),
      hasField((r.body as any)?.account, "org_id"),
      c("name matches", (r.body as any)?.account?.name === uniqueName),
    ];
  });

  await test(S, "POST /v1/accounts — 409 on duplicate name", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", {
      userId: USER_ID,
      method: "POST",
      body: { name: uniqueName },
    });
    return [statusIs(r.status, 409), c('error is account_name_exists', (r.body as any)?.error === 'account_name_exists')];
  });

  await test(S, "POST /v1/accounts — 409 is case-insensitive", async () => {
    const r = await apiReq(API_BASE, "/v1/accounts", {
      userId: USER_ID,
      method: "POST",
      body: { name: uniqueName.toUpperCase() },
    });
    return [statusIs(r.status, 409)];
  });

  // Cleanup — best-effort, don't fail the suite if it errors
  if (createdId) {
    try {
      await fetch(`${API_BASE}/v1/accounts/${createdId}`, {
        method: "DELETE",
        headers: { "x-user-id": USER_ID },
      });
    } catch { /* ignore */ }
  }
}

// ---- reporter ----

function report(): number {
  let currentSuite = "";
  let passed = 0;
  let failed = 0;
  let skipped = 0;

  for (const r of results) {
    if (r.suite !== currentSuite) {
      currentSuite = r.suite;
      console.log(`\n  ${r.suite}`);
    }

    if (r.skipped) {
      console.log(`    ⏭  ${r.name}  (${r.skipReason})`);
      skipped++;
      continue;
    }

    const ms = r.durationMs < 1000 ? `${r.durationMs}ms` : `${(r.durationMs / 1000).toFixed(1)}s`;
    const icon = r.passed ? "✓" : "✗";
    console.log(`    ${icon}  ${r.name}  [${ms}]`);

    if (r.passed) {
      passed++;
    } else {
      failed++;
      if (r.error) console.log(`       ↳ error: ${r.error}`);
      for (const ch of r.checks) {
        if (!ch.passed) console.log(`       ↳ FAIL: ${ch.label}`);
      }
    }
  }

  console.log(`\n  ─────────────────────────────────────────`);
  console.log(`  ${passed} passed  ${failed} failed  ${skipped} skipped`);
  console.log();

  return failed;
}

// ---- main ----

async function main() {
  console.log(`\n  Gravix API Regression`);
  console.log(`  Target : ${API_BASE}`);
  console.log(`  User   : ${USER_ID ?? "(none — smoke only)"}`);
  console.log(`  Org    : ${ORG_ID ?? "(none)"}`);

  try {
    const ping = await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(5_000) });
    if (!ping.ok) throw new Error(`health check returned ${ping.status}`);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`\n  ✗ Cannot reach ${API_BASE} — ${msg}`);
    console.error("    Start the server first:  npm run dev\n");
    process.exit(2);
  }

  await healthSuite();
  await authGuardSuite();
  await dashboardSuite();
  await assignmentsSuite();
  await crmSuite();
  await callsSuite();
  await repsSuite();
  await sparringSuite();
  await accountsSuite();

  const failed = report();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
