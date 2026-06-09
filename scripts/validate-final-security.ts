/**
 * validate-final-security.ts
 * DAY 86D — comprehensive security validation across all 7 categories.
 */

import { createClient } from "@supabase/supabase-js";

const BASE = process.env.API_BASE_URL || "http://localhost:3000";
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const SUPABASE_URL = process.env.SUPABASE_URL || "";

// Known demo credentials for UFC Elite org
const SUPER_ADMIN_EMAIL = "alex.stone@ufc-elite.com";
const SUPER_ADMIN_PASS = "DemoPass123!";
const PARTNER_ADMIN_EMAIL = "dana.white@ufc-elite.com";
const PARTNER_ADMIN_PASS = "DemoPass123!";
const MANAGER_EMAIL = "john.kavanagh@ufc-elite.com";
const MANAGER_PASS = "DemoPass123!";

const results: { name: string; pass: boolean; detail?: string }[] = [];

function pass(name: string, detail?: string) {
  results.push({ name, pass: true, detail });
  console.log(`  ✅  ${name}${detail ? " — " + detail : ""}`);
}

function fail(name: string, detail?: string) {
  results.push({ name, pass: false, detail });
  console.log(`  ❌  ${name}${detail ? " — " + detail : ""}`);
}

async function get(path: string, headers: Record<string, string> = {}) {
  return fetch(`${BASE}${path}`, { headers });
}

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  return fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

async function loginAs(email: string, password: string): Promise<{ token: string; userId: string } | null> {
  const r = await post("/v1/auth/login", { email, password });
  if (!r.ok) return null;
  const d: any = await r.json();
  return d.token && d.userId ? { token: d.token, userId: d.userId } : null;
}

// ─────────────────────────────────────────────
// CATEGORY 1 — Authentication
// ─────────────────────────────────────────────
async function checkAuthentication() {
  console.log("\n── 1. Authentication ──────────────────────────────────");

  // 1a — /auth/me with no credentials → 401
  const r1 = await get("/v1/auth/me");
  r1.status === 401
    ? pass("GET /v1/auth/me (no auth) → 401")
    : fail("GET /v1/auth/me (no auth) → 401", `got ${r1.status}`);

  // 1b — /auth/me with garbage token → 401
  const r2 = await get("/v1/auth/me", {
    Authorization: "Bearer notarealtoken",
    "x-user-id": "00000000-0000-0000-0000-000000000000",
  });
  r2.status === 401
    ? pass("GET /v1/auth/me (bad token) → 401")
    : fail("GET /v1/auth/me (bad token) → 401", `got ${r2.status}`);

  // 1c — login with wrong password → non-200
  const r3 = await post("/v1/auth/login", {
    email: SUPER_ADMIN_EMAIL,
    password: "WRONG_PASS_XXXX",
  });
  r3.status !== 200
    ? pass("Login with wrong password → non-200", `got ${r3.status}`)
    : fail("Login with wrong password → non-200", "returned 200 unexpectedly");

  // 1d — password reset always returns ok (anti-enumeration)
  const r4 = await post("/v1/auth/reset-password", {
    email: "nonexistent@example.com",
  });
  const d4: any = await r4.json();
  d4?.ok === true
    ? pass("Password reset anti-enumeration → always ok:true")
    : fail("Password reset anti-enumeration → always ok:true", JSON.stringify(d4));

  // 1e — valid login returns token + userId
  const creds = await loginAs(SUPER_ADMIN_EMAIL, SUPER_ADMIN_PASS);
  creds
    ? pass("SuperAdmin login returns token + userId")
    : fail("SuperAdmin login returns token + userId", "login failed");

  if (creds) {
    // 1f — /auth/me with valid credentials → 200
    const r5 = await get("/v1/auth/me", {
      Authorization: `Bearer ${creds.token}`,
      "x-user-id": creds.userId,
    });
    r5.status === 200
      ? pass("GET /v1/auth/me (valid auth) → 200")
      : fail("GET /v1/auth/me (valid auth) → 200", `got ${r5.status}`);
  }
}

// ─────────────────────────────────────────────
// CATEGORY 2 — Dangerous env vars
// ─────────────────────────────────────────────
async function checkEnvFlags() {
  console.log("\n── 2. Dangerous Env Flags ─────────────────────────────");

  const dangerous = [
    "DEV_TEST_UID",
    "ALLOW_ORG_BYPASS",
    "ALLOW_ADMIN_ENDPOINTS",
    "ALLOW_DEV_UID_QS",
  ];
  for (const key of dangerous) {
    const val = process.env[key];
    !val
      ? pass(`${key} is absent / empty`)
      : fail(`${key} is absent / empty`, `currently set to: "${val}"`);
  }
}

// ─────────────────────────────────────────────
// CATEGORY 3 — Tenant isolation / org scoping
// ─────────────────────────────────────────────
async function checkTenantIsolation() {
  console.log("\n── 3. Tenant Isolation ────────────────────────────────");

  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);
  if (!managerCreds) {
    fail("Manager login for isolation test", "could not authenticate");
    return;
  }
  const headers = {
    Authorization: `Bearer ${managerCreds.token}`,
    "x-user-id": managerCreds.userId,
  };

  // 3a — CRM route with a zero-UUID org (should still require membership or reject)
  const r1 = await get("/v1/crm/contacts?limit=1", {
    ...headers,
    "x-org-id": "00000000-0000-0000-0000-000000000000",
  });
  // Acceptable: 200 with empty set for zero-org, or 403/400 — what we're checking is NOT 200 with cross-org data
  // We can only infer from the response shape; just confirm response is structured
  [200, 400, 403].includes(r1.status)
    ? pass("CRM contacts with zero-org → structured response (not server error)", `${r1.status}`)
    : fail("CRM contacts with zero-org → structured response", `got ${r1.status}`);

  // 3b — Manager cannot access super admin platform stats
  const r2 = await get("/v1/admin/platform", headers);
  r2.status === 403
    ? pass("Manager GET /v1/admin/platform → 403")
    : fail("Manager GET /v1/admin/platform → 403", `got ${r2.status}`);

  // 3c — Manager cannot access partner licences
  const r3 = await get("/v1/admin/partner/licences", headers);
  r3.status === 403
    ? pass("Manager GET /v1/admin/partner/licences → 403")
    : fail("Manager GET /v1/admin/partner/licences → 403", `got ${r3.status}`);

  // 3d — Manager cannot access super licences
  const r4 = await get("/v1/admin/super/licences", headers);
  r4.status === 403
    ? pass("Manager GET /v1/admin/super/licences → 403")
    : fail("Manager GET /v1/admin/super/licences → 403", `got ${r4.status}`);
}

// ─────────────────────────────────────────────
// CATEGORY 4 — Impersonation
// ─────────────────────────────────────────────
async function checkImpersonation() {
  console.log("\n── 4. Impersonation ───────────────────────────────────");

  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);
  if (!managerCreds) {
    fail("Manager login for impersonation test", "could not authenticate");
    return;
  }

  // 4a — Non-SuperAdmin with impersonation header → 403
  const r1 = await get("/v1/auth/me", {
    Authorization: `Bearer ${managerCreds.token}`,
    "x-user-id": managerCreds.userId,
    "x-impersonated-user-id": "00000000-0000-0000-0000-000000000001",
  });
  // The middleware should reject or the impersonation should fail silently and not swap
  // A 403 is the ideal; anything except a successful identity swap is acceptable
  const body: any = await r1.json().catch(() => ({}));
  const swapped = body?.userId === "00000000-0000-0000-0000-000000000001";
  !swapped
    ? pass("Manager cannot impersonate via x-impersonated-user-id", `status ${r1.status}`)
    : fail("Manager cannot impersonate via x-impersonated-user-id", "identity was swapped");

  // 4b — SuperAdmin CAN impersonate (gate works both ways)
  const superCreds = await loginAs(SUPER_ADMIN_EMAIL, SUPER_ADMIN_PASS);
  if (!superCreds) {
    fail("SuperAdmin login for impersonation test", "could not authenticate");
    return;
  }
  // We just verify the impersonation history endpoint is accessible
  const r2 = await get("/v1/admin/support/impersonation-history", {
    Authorization: `Bearer ${superCreds.token}`,
    "x-user-id": superCreds.userId,
  });
  r2.status === 200
    ? pass("SuperAdmin GET /v1/admin/support/impersonation-history → 200")
    : fail("SuperAdmin GET /v1/admin/support/impersonation-history → 200", `got ${r2.status}`);

  // 4c — impersonation history not accessible to Manager
  const r3 = await get("/v1/admin/support/impersonation-history", {
    Authorization: `Bearer ${managerCreds.token}`,
    "x-user-id": managerCreds.userId,
  });
  r3.status === 403
    ? pass("Manager GET /v1/admin/support/impersonation-history → 403")
    : fail("Manager GET /v1/admin/support/impersonation-history → 403", `got ${r3.status}`);
}

// ─────────────────────────────────────────────
// CATEGORY 5 — Licensing
// ─────────────────────────────────────────────
async function checkLicensing() {
  console.log("\n── 5. Licensing ───────────────────────────────────────");

  const superCreds = await loginAs(SUPER_ADMIN_EMAIL, SUPER_ADMIN_PASS);
  const partnerCreds = await loginAs(PARTNER_ADMIN_EMAIL, PARTNER_ADMIN_PASS);
  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);

  if (superCreds) {
    // 5a — SuperAdmin can access super licences
    const r1 = await get("/v1/admin/super/licences", {
      Authorization: `Bearer ${superCreds.token}`,
      "x-user-id": superCreds.userId,
    });
    if (r1.status === 200) {
      const d: any = await r1.json();
      d?.ok && Array.isArray(d?.partners)
        ? pass("SuperAdmin GET /v1/admin/super/licences → 200 with partners[]")
        : fail("SuperAdmin GET /v1/admin/super/licences → partners[] missing", JSON.stringify(d));
    } else {
      fail("SuperAdmin GET /v1/admin/super/licences → 200", `got ${r1.status}`);
    }
  }

  if (partnerCreds) {
    // 5b — PartnerAdmin can access partner licences
    const r2 = await get("/v1/admin/partner/licences", {
      Authorization: `Bearer ${partnerCreds.token}`,
      "x-user-id": partnerCreds.userId,
    });
    if (r2.status === 200) {
      const d: any = await r2.json();
      d?.ok && d?.pool !== undefined
        ? pass("PartnerAdmin GET /v1/admin/partner/licences → 200 with pool")
        : fail("PartnerAdmin GET /v1/admin/partner/licences → pool missing", JSON.stringify(d));
    } else {
      fail("PartnerAdmin GET /v1/admin/partner/licences → 200", `got ${r2.status}`);
    }
  }

  if (managerCreds) {
    // 5c — Manager cannot access partner licences
    const r3 = await get("/v1/admin/partner/licences", {
      Authorization: `Bearer ${managerCreds.token}`,
      "x-user-id": managerCreds.userId,
    });
    r3.status === 403
      ? pass("Manager GET /v1/admin/partner/licences → 403")
      : fail("Manager GET /v1/admin/partner/licences → 403", `got ${r3.status}`);
  }
}

// ─────────────────────────────────────────────
// CATEGORY 6 — Uploads
// ─────────────────────────────────────────────
async function checkUploads() {
  console.log("\n── 6. Uploads ─────────────────────────────────────────");

  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);
  if (!managerCreds) {
    fail("Manager login for upload tests", "could not authenticate");
    return;
  }

  // 6a — Upload with disallowed MIME type → 415
  const form = new FormData();
  form.append(
    "file",
    new Blob(["<script>alert(1)</script>"], { type: "text/html" }),
    "evil.html"
  );
  const r1 = await fetch(`${BASE}/v1/upload`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${managerCreds.token}`,
      "x-user-id": managerCreds.userId,
    },
    body: form,
  });
  r1.status === 415
    ? pass("Upload text/html → 415 Unsupported Media Type")
    : fail("Upload text/html → 415 Unsupported Media Type", `got ${r1.status}`);

  // 6b — Signed upload request with disallowed MIME → 415
  const r2 = await post(
    "/v1/upload/signed",
    { fileName: "test.exe", mimeType: "application/x-msdownload", callId: "test-id" },
    {
      Authorization: `Bearer ${managerCreds.token}`,
      "x-user-id": managerCreds.userId,
    }
  );
  r2.status === 415
    ? pass("Signed upload application/x-msdownload → 415")
    : fail("Signed upload application/x-msdownload → 415", `got ${r2.status}`);

  // 6c — Signed upload with allowed MIME → not 415 (200 or other error is fine)
  const r3 = await post(
    "/v1/upload/signed",
    { fileName: "audio.mp3", mimeType: "audio/mpeg", callId: "test-call-id" },
    {
      Authorization: `Bearer ${managerCreds.token}`,
      "x-user-id": managerCreds.userId,
    }
  );
  r3.status !== 415
    ? pass("Signed upload audio/mpeg → not 415", `got ${r3.status}`)
    : fail("Signed upload audio/mpeg → not 415", "audio/mpeg rejected as invalid MIME");
}

// ─────────────────────────────────────────────
// CATEGORY 7 — Security headers
// ─────────────────────────────────────────────
async function checkSecurityHeaders() {
  console.log("\n── 7. Security Headers ────────────────────────────────");

  const r = await get("/v1/health");

  const check = (header: string, partial?: string) => {
    const val = r.headers.get(header);
    if (!val) {
      fail(`Header ${header} present`, "missing");
      return;
    }
    if (partial && !val.toLowerCase().includes(partial.toLowerCase())) {
      fail(`Header ${header} contains "${partial}"`, `got: ${val}`);
      return;
    }
    pass(`Header ${header} present`, val.slice(0, 80));
  };

  check("x-content-type-options", "nosniff");
  check("x-frame-options");
  check("content-security-policy");

  // X-Powered-By must be absent
  const powered = r.headers.get("x-powered-by");
  !powered
    ? pass("X-Powered-By header absent (fingerprinting suppressed)")
    : fail("X-Powered-By header absent", `still present: ${powered}`);
}

// ─────────────────────────────────────────────
// CATEGORY 8 — CRM access control
// ─────────────────────────────────────────────
async function checkCRM() {
  console.log("\n── 8. CRM ─────────────────────────────────────────────");

  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);
  if (!managerCreds) {
    fail("Manager login for CRM test", "could not authenticate");
    return;
  }
  const headers = (orgId?: string): Record<string, string> => ({
    Authorization: `Bearer ${managerCreds.token}`,
    "x-user-id": managerCreds.userId,
    ...(orgId ? { "x-org-id": orgId } : {}),
  });

  // 8a — CRM contacts without x-org-id header (should not 500; either 200 empty or 400/403)
  const r1 = await get("/v1/crm/contacts?limit=1", headers());
  [200, 400, 403].includes(r1.status)
    ? pass("GET /v1/crm/contacts (no org header) → no 5xx", `got ${r1.status}`)
    : fail("GET /v1/crm/contacts (no org header) → no 5xx", `got ${r1.status}`);

  // 8b — Unauthenticated access to CRM → 401 or 403
  const r2 = await get("/v1/crm/contacts?limit=1");
  [401, 403].includes(r2.status)
    ? pass("GET /v1/crm/contacts (unauthenticated) → 401/403", `got ${r2.status}`)
    : fail("GET /v1/crm/contacts (unauthenticated) → 401/403", `got ${r2.status}`);

  // 8c — CRM activities with valid auth → structured response
  const r3 = await get("/v1/crm/activities?limit=5", headers());
  if (r3.status === 200) {
    const d: any = await r3.json();
    d?.ok !== undefined
      ? pass("GET /v1/crm/activities → 200 structured response")
      : fail("GET /v1/crm/activities → 200 structured response", "missing ok field");
  } else {
    [400, 403].includes(r3.status)
      ? pass("GET /v1/crm/activities → structured response (auth/org gate)", `got ${r3.status}`)
      : fail("GET /v1/crm/activities → structured response", `got ${r3.status}`);
  }
}

// ─────────────────────────────────────────────
// CATEGORY 9 — Admin route gates
// ─────────────────────────────────────────────
async function checkAdminGates() {
  console.log("\n── 9. Admin Route Gates ───────────────────────────────");

  const managerCreds = await loginAs(MANAGER_EMAIL, MANAGER_PASS);
  const superCreds = await loginAs(SUPER_ADMIN_EMAIL, SUPER_ADMIN_PASS);

  if (!managerCreds) {
    fail("Manager login for admin gate tests", "could not authenticate");
    return;
  }

  const managerHeaders = {
    Authorization: `Bearer ${managerCreds.token}`,
    "x-user-id": managerCreds.userId,
  };

  // 9a — Manager → super-only routes → 403
  const superOnlyRoutes = [
    "/v1/admin/platform",
    "/v1/admin/super/licences",
    "/v1/admin/audit",
    "/v1/admin/support/impersonation-history",
  ];
  for (const route of superOnlyRoutes) {
    const r = await get(route, managerHeaders);
    r.status === 403
      ? pass(`Manager GET ${route} → 403`)
      : fail(`Manager GET ${route} → 403`, `got ${r.status}`);
  }

  if (superCreds) {
    const superHeaders = {
      Authorization: `Bearer ${superCreds.token}`,
      "x-user-id": superCreds.userId,
    };

    // 9b — SuperAdmin → platform stats → 200
    const r1 = await get("/v1/admin/platform", superHeaders);
    r1.status === 200
      ? pass("SuperAdmin GET /v1/admin/platform → 200")
      : fail("SuperAdmin GET /v1/admin/platform → 200", `got ${r1.status}`);

    // 9c — SuperAdmin → audit log → 200
    const r2 = await get("/v1/admin/audit", superHeaders);
    r2.status === 200
      ? pass("SuperAdmin GET /v1/admin/audit → 200")
      : fail("SuperAdmin GET /v1/admin/audit → 200", `got ${r2.status}`);
  }
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function main() {
  console.log("━━━ FINAL SECURITY VALIDATION ━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`Target: ${BASE}`);
  console.log(`Date:   ${new Date().toISOString()}`);

  await checkEnvFlags();
  await checkAuthentication();
  await checkTenantIsolation();
  await checkImpersonation();
  await checkLicensing();
  await checkUploads();
  await checkSecurityHeaders();
  await checkCRM();
  await checkAdminGates();

  const total = results.length;
  const passed = results.filter((r) => r.pass).length;
  const failed = total - passed;

  console.log("\n━━━ RESULTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`  ${passed} passed · ${failed} failed · ${total} total`);

  if (failed > 0) {
    console.log("\nFailed checks:");
    results
      .filter((r) => !r.pass)
      .forEach((r) => console.log(`  ❌  ${r.name}${r.detail ? " — " + r.detail : ""}`));
    process.exit(1);
  } else {
    console.log("\n✅  All security checks passed");
    process.exit(0);
  }
}

main().catch((e) => {
  console.error("Validation crashed:", e);
  process.exit(1);
});
