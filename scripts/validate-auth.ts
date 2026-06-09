/**
 * validate-auth.ts
 *
 * Validates the auth flow end-to-end against the live server.
 *
 * Coverage:
 *   ✓ POST /v1/auth/login — valid credentials → 200 + tokens
 *   ✓ POST /v1/auth/login — wrong password → 401
 *   ✓ POST /v1/auth/login — missing email → 400
 *   ✓ POST /v1/auth/login — missing password → 400
 *   ✓ GET  /v1/auth/me — with x-user-id → returns profile + tier
 *   ✓ GET  /v1/auth/me — with Bearer token → returns profile + tier
 *   ✓ GET  /v1/auth/me — no auth → 401
 *   ✓ POST /v1/auth/verify-role — Manager tier correct
 *   ✓ POST /v1/auth/verify-role — SalesRep tier correct
 *   ✓ POST /v1/auth/verify-role — wrong tier → ok: false
 *   ✓ POST /v1/auth/reset-password — valid email → 200
 *   ✓ POST /v1/auth/logout — 200
 *   ✓ Role mapping: Dana White → Manager
 *   ✓ Role mapping: Conor McGregor → SalesRep
 *   ✓ Role mapping: Hunter Campbell → Manager
 *   ✓ Role mapping: Anderson Silva → SalesRep
 *
 * Requirements:
 *   - Server running (npm run dev)
 *   - Demo seed applied (npm run seed:demo)
 *   - Supabase Email provider enabled (Authentication → Providers → Email)
 *
 * Usage: npm run validate:auth
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

const DEMO_PASSWORD = "DemoPass123!";

// Known demo personas (from seed-demo-org.ts)
const DEMO_MANAGERS = [
  { name: "Dana White",      email: "dana.white@ufcelite.demo",      expectedTier: "Manager"  },
  { name: "Hunter Campbell", email: "hunter.campbell@ufcelite.demo", expectedTier: "Manager"  },
];
const DEMO_REPS = [
  { name: "Conor McGregor", email: "conor.mcgregor@ufcelite.demo", expectedTier: "SalesRep" },
  { name: "Anderson Silva", email: "anderson.silva@ufcelite.demo", expectedTier: "SalesRep" },
];

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function post(path: string, body?: object, headers?: Record<string, string>) {
  const h: Record<string, string> = { "content-type": "application/json", ...headers };
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: h,
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(10_000),
  });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function get(path: string, headers?: Record<string, string>) {
  const h: Record<string, string> = { "content-type": "application/json", ...headers };
  const res = await fetch(`${API_BASE}${path}`, { headers: h, signal: AbortSignal.timeout(8_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

// Look up a demo user's ID from the DB via email (auth.users through admin API)
async function lookupUserId(email: string): Promise<string | null> {
  // Try reps table first (email column was added in migration 20260605d)
  const { data } = await supa.from("reps").select("id").eq("email", email.toLowerCase()).maybeSingle();
  if ((data as any)?.id) return (data as any).id;

  // Fallback: enumerate auth users
  try {
    let page = 1;
    while (true) {
      const { data: list, error } = await supa.auth.admin.listUsers({ page, perPage: 1000 });
      if (error || !list?.users?.length) break;
      const found = list.users.find((u: any) => u.email?.toLowerCase() === email.toLowerCase());
      if (found) return (found as any).id;
      if (list.users.length < 1000) break;
      page++;
    }
  } catch { /* ignore */ }

  return null;
}

async function main() {
  console.log("\n  Auth Validator\n");

  // Connectivity check
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  const results: Check[] = [];
  let accessToken: string | null = null;
  let loginUserId: string | null = null;

  // ── 1. Login — valid credentials ─────────────────────────────────────────────
  {
    const { status, data } = await post("/v1/auth/login", {
      email:    DEMO_MANAGERS[0].email,
      password: DEMO_PASSWORD,
    });
    results.push(c("POST /v1/auth/login (valid) → 200",      status === 200,           `actual: ${status} error=${data?.error}`));
    results.push(c("Response ok: true",                      data?.ok === true,        `ok=${data?.ok}`));
    results.push(c("accessToken present",                    !!data?.accessToken,      `token=${String(data?.accessToken).slice(0,20)}...`));
    results.push(c("userId present",                        !!data?.userId,           `userId=${data?.userId}`));
    results.push(c("user.tier is Manager",                  data?.user?.tier === "Manager", `tier=${data?.user?.tier}`));
    accessToken = data?.accessToken ?? null;
    loginUserId = data?.userId ?? null;
  }

  // ── 2. Login — wrong password ─────────────────────────────────────────────────
  {
    const { status, data } = await post("/v1/auth/login", {
      email:    DEMO_MANAGERS[0].email,
      password: "WrongPassword999!",
    });
    results.push(c("POST /v1/auth/login (wrong pw) → 401", status === 401, `actual: ${status} error=${data?.error}`));
  }

  // ── 3. Login — missing fields ─────────────────────────────────────────────────
  {
    const r1 = await post("/v1/auth/login", { password: DEMO_PASSWORD });
    results.push(c("POST /v1/auth/login (no email) → 400",    r1.status === 400, `actual: ${r1.status}`));
    const r2 = await post("/v1/auth/login", { email: DEMO_MANAGERS[0].email });
    results.push(c("POST /v1/auth/login (no password) → 400", r2.status === 400, `actual: ${r2.status}`));
  }

  // ── 4. GET /v1/auth/me — x-user-id path ──────────────────────────────────────
  if (loginUserId) {
    const { status, data } = await get("/v1/auth/me", { "x-user-id": loginUserId });
    results.push(c("GET /v1/auth/me (x-user-id) → 200", status === 200,     `actual: ${status}`));
    results.push(c("me.tier is Manager",                 data?.user?.tier === "Manager", `tier=${data?.user?.tier}`));
    results.push(c("me.id matches loginUserId",          data?.user?.id === loginUserId, `id=${data?.user?.id}`));
  } else {
    results.push(c("GET /v1/auth/me (x-user-id) → skipped (no loginUserId)", false, "login failed above"));
    results.push(c("me.tier/id checks skipped", false, "login failed above"));
    results.push(c("me.id check skipped", false));
  }

  // ── 5. GET /v1/auth/me — Bearer token path ────────────────────────────────────
  if (accessToken) {
    const { status, data } = await get("/v1/auth/me", { "Authorization": `Bearer ${accessToken}` });
    results.push(c("GET /v1/auth/me (Bearer) → 200", status === 200,     `actual: ${status}`));
    results.push(c("me.tier is Manager (Bearer)",     data?.user?.tier === "Manager", `tier=${data?.user?.tier}`));
  } else {
    results.push(c("GET /v1/auth/me (Bearer) → skipped", false, "no access token from login"));
    results.push(c("me.tier check skipped (Bearer)", false));
  }

  // ── 6. GET /v1/auth/me — no auth ─────────────────────────────────────────────
  {
    const { status } = await get("/v1/auth/me");
    results.push(c("GET /v1/auth/me (no auth) → 401", status === 401, `actual: ${status}`));
  }

  // ── 7. Role mapping — all 4 lighthouse users ──────────────────────────────────
  const allPersonas = [...DEMO_MANAGERS, ...DEMO_REPS];
  for (const persona of allPersonas) {
    const userId = await lookupUserId(persona.email);

    if (!userId) {
      results.push(c(`${persona.name} has reps row`, false, `user not found for ${persona.email} — run npm run seed:demo`));
      results.push(c(`${persona.name} tier = ${persona.expectedTier}`, false, "no reps row"));
      continue;
    }

    const { status, data } = await post("/v1/auth/verify-role", { userId, expectedTier: persona.expectedTier });
    results.push(c(`${persona.name} has reps row`,            !!data?.userId,          `userId=${userId}`));
    results.push(c(`${persona.name} tier = ${persona.expectedTier}`, data?.tier_match === true, `actual tier=${data?.tier}`));
  }

  // ── 8. verify-role — wrong tier → tier_match: false ──────────────────────────
  if (loginUserId) {
    const { data } = await post("/v1/auth/verify-role", { userId: loginUserId, expectedTier: "SalesRep" });
    results.push(c("verify-role wrong expectedTier → tier_match: false", data?.tier_match === false, `tier_match=${data?.tier_match}`));
  } else {
    results.push(c("verify-role wrong tier check skipped", false, "no loginUserId"));
  }

  // ── 9. Password reset ─────────────────────────────────────────────────────────
  {
    const { status, data } = await post("/v1/auth/reset-password", { email: DEMO_MANAGERS[0].email });
    results.push(c("POST /v1/auth/reset-password → 200", status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c("reset-password body.ok",              data?.ok === true, `ok=${data?.ok}`));
  }

  // ── 10. Logout ────────────────────────────────────────────────────────────────
  if (loginUserId) {
    const { status, data } = await post("/v1/auth/logout", {}, {
      "x-user-id":    loginUserId,
      "Authorization": accessToken ? `Bearer ${accessToken}` : "",
    });
    results.push(c("POST /v1/auth/logout → 200", status === 200, `actual: ${status}`));
    results.push(c("logout body.ok",             data?.ok === true, `ok=${data?.ok}`));
  } else {
    results.push(c("POST /v1/auth/logout skipped", false, "no session"));
    results.push(c("logout body.ok skipped", false));
  }

  // ── Report ─────────────────────────────────────────────────────────────────
  console.log("\n  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);

  if (failed > 0) {
    const seedNeeded = results.some(r => !r.passed && r.detail?.includes("seed:demo"));
    if (seedNeeded) {
      console.log("  ⚠  Some failures are because demo users do not exist yet.");
      console.log("     Run:  npm run seed:demo   then retry.\n");
    }
    process.exit(1);
  }
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
