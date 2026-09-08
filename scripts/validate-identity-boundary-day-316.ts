// Day 316 — API identity-boundary regression guard (network-free, behavioural).
//
// Exercises the REAL trust primitives in src/identityTrust.ts (the same code the
// server middleware runs) to lock the Day-175 contract:
//   1. With PROXY_SHARED_SECRET set, a spoofed x-user-id WITHOUT the matching
//      x-proxy-secret is STRIPPED and never honoured.
//   2. With the correct x-proxy-secret, proxy identity IS honoured.
//   3. The header-vs-JWT mismatch guard still fires (only on trusted headers).
//   4. Dev env fallback is never honoured in production.
//
// Non-vacuity: cases 1 vs 2 differ ONLY by the proxy secret (null vs trusted);
// a mutation that skips the strip is shown to break case 1 (see VULNERABLE ctrl).
//
// Run: npm run validate:identity-boundary   (tsx, no DB, no network)

import {
  identityHeadersTrusted,
  resolveIdentity,
  SPOOFABLE_IDENTITY_HEADERS,
  type IdentityRequestLike,
} from "../src/identityTrust";

const A = "11111111-1111-4111-8111-111111111111";
const B = "22222222-2222-4222-8222-222222222222";
const SPOOF = "33333333-3333-4333-8333-333333333333";
const SECRET = "proxy-shared-secret-abcdef-1234567890";

let failures = 0;
function ok(name: string, pass: boolean, detail = "") {
  console.log(`  ${pass ? "✅" : "❌"}  ${name}${detail ? " — " + detail : ""}`);
  if (!pass) failures++;
}

function makeReq(headers: Record<string, string>): IdentityRequestLike {
  const h: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(headers)) h[k.toLowerCase()] = v;
  return {
    headers: h,
    header(name: string) {
      const v = h[name.toLowerCase()];
      return typeof v === "string" ? v : undefined;
    },
  };
}

function jwtWithSub(sub: string): string {
  const b64url = (o: unknown) =>
    Buffer.from(JSON.stringify(o)).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `${b64url({ alg: "none", typ: "JWT" })}.${b64url({ sub })}.sig`;
}

function withEnv(env: Record<string, string | undefined>, fn: () => void) {
  const keys = ["PROXY_SHARED_SECRET", "NODE_ENV", "DEV_TEST_UID"];
  const saved: Record<string, string | undefined> = {};
  for (const k of keys) saved[k] = process.env[k];
  try {
    for (const k of keys) {
      if (env[k] === undefined) delete process.env[k];
      else process.env[k] = env[k];
    }
    fn();
  } finally {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  }
}

console.log("Day 316 — API identity boundary regression guard\n");

// ── identityHeadersTrusted ─────────────────────────────────────────────
withEnv({ PROXY_SHARED_SECRET: undefined }, () => {
  ok("secret UNSET ⇒ trusted (legacy opt-in)", identityHeadersTrusted(makeReq({})) === true);
});
withEnv({ PROXY_SHARED_SECRET: SECRET }, () => {
  ok("secret set + correct x-proxy-secret ⇒ trusted",
    identityHeadersTrusted(makeReq({ "x-proxy-secret": SECRET })) === true);
  ok("secret set + WRONG same-length secret ⇒ untrusted",
    identityHeadersTrusted(makeReq({ "x-proxy-secret": SECRET.slice(0, -1) + "X" })) === false);
  ok("secret set + ABSENT secret ⇒ untrusted",
    identityHeadersTrusted(makeReq({})) === false);
  ok("secret set + different-length secret ⇒ untrusted",
    identityHeadersTrusted(makeReq({ "x-proxy-secret": "short" })) === false);
});

// ── STRIP when untrusted (the core impersonation defence) ──────────────
withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
  const req = makeReq({
    "x-user-id": SPOOF,
    "x-gravix-user-id": SPOOF,
    "x-forwarded-user-id": SPOOF,
    "x-real-user-id": SPOOF,
  });
  const r = resolveIdentity(req);
  ok("secret set, spoofed x-user-id, NO proxy secret ⇒ userId null (stripped)",
    r.userId === null && r.via === null, `userId=${r.userId}`);
  ok("all spoofable identity headers physically removed from req.headers",
    SPOOFABLE_IDENTITY_HEADERS.every((h) => !(h in req.headers)));

  // Same inputs but WITH the correct secret ⇒ honoured. (Non-vacuity vs the above.)
  const req2 = makeReq({ "x-user-id": SPOOF, "x-proxy-secret": SECRET });
  const r2 = resolveIdentity(req2);
  ok("secret set, x-user-id + CORRECT proxy secret ⇒ honoured",
    r2.userId === SPOOF && r2.via === "header", `userId=${r2.userId}`);
});

// ── mismatch guard preserved (only on trusted headers) ─────────────────
withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
  const trusted = resolveIdentity(makeReq({
    "x-user-id": A, "x-proxy-secret": SECRET, authorization: `Bearer ${jwtWithSub(B)}`,
  }));
  ok("trusted header uid ≠ jwt sub ⇒ mismatch=true", trusted.mismatch === true);

  const stripped = resolveIdentity(makeReq({
    "x-user-id": A, authorization: `Bearer ${jwtWithSub(B)}`, // no proxy secret ⇒ header stripped
  }));
  ok("untrusted spoofed uid + real bearer ⇒ stripped, resolves to jwt, no mismatch",
    stripped.userId === B && stripped.via === "jwt" && stripped.mismatch === false,
    `userId=${stripped.userId} mismatch=${stripped.mismatch}`);
});

// ── dev env fallback never in production ───────────────────────────────
withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "production", DEV_TEST_UID: A }, () => {
  const r = resolveIdentity(makeReq({}));
  ok("DEV_TEST_UID ignored in production ⇒ userId null", r.userId === null, `userId=${r.userId}`);
});
withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "development", DEV_TEST_UID: A }, () => {
  const r = resolveIdentity(makeReq({}));
  ok("DEV_TEST_UID honoured only in non-production ⇒ via=env", r.userId === A && r.via === "env");
});

// ── legacy (secret unset) documents the live-risk state ────────────────
withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "production" }, () => {
  const r = resolveIdentity(makeReq({ "x-user-id": SPOOF }));
  ok("secret UNSET ⇒ raw x-user-id trusted (the deployed-secret P1)", r.userId === SPOOF && r.via === "header");
});

// ── explicit non-vacuity control: a strip-less resolver WOULD trust the spoof ──
withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
  // Simulate the pre-hardening / regressed behaviour: never strip.
  const req = makeReq({ "x-user-id": SPOOF });
  const vulnerableUserId = req.header("x-user-id") || null; // no trust check at all
  ok("mutation control: a strip-less resolver returns the spoof (so the real guard is non-vacuous)",
    vulnerableUserId === SPOOF && resolveIdentity(makeReq({ "x-user-id": SPOOF })).userId === null);
});

console.log(`\n${failures === 0 ? "✅ PASS" : `❌ FAIL (${failures})`} — identity boundary guard\n`);
process.exit(failures === 0 ? 0 : 1);
