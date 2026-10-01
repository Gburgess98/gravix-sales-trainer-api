// Day 316 / Go Live Day 26 — API identity-boundary regression guard
// (network-free, behavioural).
//
// Exercises the REAL trust primitives the server middleware runs:
//   A. identityHeadersTrusted (src/identityTrust.ts) — x-user-id is only
//      honoured with the matching x-proxy-secret; Day 26: an unset/empty
//      secret trusts NO headers in production (dev/local unchanged).
//   B. resolveIdentity — strips untrusted headers; takes ONLY an already
//      verified Bearer `sub`; header-vs-verified-sub mismatch still fires;
//      dev env fallback never in production.
//   C. createClaimsVerifier (src/tokenVerification.ts) — a Bearer token only
//      becomes an identity with a valid signature, unexpired exp, expected
//      iss/aud and an accepted alg. A real ES256 key pair is generated
//      locally; the signature check is injected, so no network is used.
//
// Non-vacuity: explicit mutation controls show a strip-less resolver would
// trust the spoof and a decode-only verifier would accept a forged token.
//
// Run: npm run validate:identity-boundary   (tsx, no DB, no network)

import crypto from "crypto";
import {
  identityHeadersTrusted,
  resolveIdentity,
  SPOOFABLE_IDENTITY_HEADERS,
  type IdentityRequestLike,
} from "../src/identityTrust";
import {
  createClaimsVerifier,
  decodeUnverified,
  EXPECTED_AUDIENCE,
  type SignatureCheck,
} from "../src/tokenVerification";

const A = "11111111-1111-4111-8111-111111111111";
const B = "22222222-2222-4222-8222-222222222222";
const SPOOF = "33333333-3333-4333-8333-333333333333";
const SECRET = "proxy-shared-secret-abcdef-1234567890";
const ISS = "https://validator-test.supabase.co/auth/v1";

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

async function withEnv(env: Record<string, string | undefined>, fn: () => void | Promise<void>) {
  const keys = ["PROXY_SHARED_SECRET", "NODE_ENV", "DEV_TEST_UID"];
  const saved: Record<string, string | undefined> = {};
  for (const k of keys) saved[k] = process.env[k];
  try {
    for (const k of keys) {
      if (env[k] === undefined) delete process.env[k];
      else process.env[k] = env[k];
    }
    await fn();
  } finally {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  }
}

// ── token helpers (local ES256 key pair; nothing leaves the process) ────
const b64url = (buf: Buffer) =>
  buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const b64json = (o: unknown) => b64url(Buffer.from(JSON.stringify(o)));

const trustedKeys = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
const otherKeys = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
const nowSec = () => Math.floor(Date.now() / 1000);

function claims(over: Record<string, unknown> = {}) {
  return { sub: A, iss: ISS, aud: EXPECTED_AUDIENCE, exp: nowSec() + 600, ...over };
}

function signES256(payload: Record<string, unknown>, key = trustedKeys.privateKey, header: Record<string, unknown> = { alg: "ES256", typ: "JWT", kid: "k1" }) {
  const input = `${b64json(header)}.${b64json(payload)}`;
  const sig = crypto.sign("sha256", Buffer.from(input), { key, dsaEncoding: "ieee-p1363" });
  return `${input}.${b64url(sig)}`;
}

let signatureCalls = 0;
// Stands in for Supabase getClaims(): verifies ES256 against the trusted key only.
const es256Check: SignatureCheck = async (token) => {
  signatureCalls++;
  const parts = token.split(".");
  const decoded = decodeUnverified(token);
  if (!decoded || decoded.header.alg !== "ES256") return null;
  const valid = crypto.verify(
    "sha256",
    Buffer.from(`${parts[0]}.${parts[1]}`),
    { key: trustedKeys.publicKey, dsaEncoding: "ieee-p1363" },
    Buffer.from(parts[2].replace(/-/g, "+").replace(/_/g, "/"), "base64")
  );
  return valid ? { claims: decoded.payload, header: decoded.header } : null;
};

const verify = createClaimsVerifier({ signatureCheck: es256Check, issuer: () => ISS });

async function main() {
  console.log("Day 316 / Day 26 — API identity boundary regression guard\n");

  // ── A. identityHeadersTrusted ─────────────────────────────────────────
  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "production" }, () => {
    ok("production + secret MISSING ⇒ untrusted (fail closed)", identityHeadersTrusted(makeReq({ "x-proxy-secret": "anything" })) === false);
  });
  await withEnv({ PROXY_SHARED_SECRET: "   ", NODE_ENV: "production" }, () => {
    ok("production + secret EMPTY/whitespace ⇒ untrusted (fail closed)", identityHeadersTrusted(makeReq({})) === false);
  });
  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "development" }, () => {
    ok("non-production + secret missing ⇒ trusted (local dev unchanged)", identityHeadersTrusted(makeReq({})) === true);
  });
  await withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
    ok("secret set + MATCHING x-proxy-secret ⇒ trusted",
      identityHeadersTrusted(makeReq({ "x-proxy-secret": SECRET })) === true);
    ok("secret set + WRONG same-length secret ⇒ untrusted",
      identityHeadersTrusted(makeReq({ "x-proxy-secret": SECRET.slice(0, -1) + "X" })) === false);
    ok("secret set + ABSENT secret ⇒ untrusted",
      identityHeadersTrusted(makeReq({})) === false);
    ok("secret set + different-length secret ⇒ untrusted",
      identityHeadersTrusted(makeReq({ "x-proxy-secret": "short" })) === false);
  });

  // ── B. resolveIdentity (verified sub only) ───────────────────────────
  await withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
    const req = makeReq({
      "x-user-id": SPOOF,
      "x-gravix-user-id": SPOOF,
      "x-forwarded-user-id": SPOOF,
      "x-real-user-id": SPOOF,
    });
    const r = resolveIdentity(req, null);
    ok("spoofed x-user-id, NO proxy secret ⇒ userId null (stripped)",
      r.userId === null && r.via === null, `userId=${r.userId}`);
    ok("all spoofable identity headers physically removed from req.headers",
      SPOOFABLE_IDENTITY_HEADERS.every((h) => !(h in req.headers)));

    const r2 = resolveIdentity(makeReq({ "x-user-id": SPOOF, "x-proxy-secret": SECRET }), null);
    ok("x-user-id + MATCHING proxy secret ⇒ honoured",
      r2.userId === SPOOF && r2.via === "header", `userId=${r2.userId}`);

    const mism = resolveIdentity(makeReq({ "x-user-id": A, "x-proxy-secret": SECRET }), B);
    ok("trusted header uid ≠ verified sub ⇒ mismatch=true", mism.mismatch === true);

    const viaJwt = resolveIdentity(makeReq({ "x-user-id": A }), B);
    ok("untrusted spoofed uid + verified sub ⇒ stripped, resolves to verified sub, no mismatch",
      viaJwt.userId === B && viaJwt.via === "jwt" && viaJwt.mismatch === false,
      `userId=${viaJwt.userId} mismatch=${viaJwt.mismatch}`);

    const invalidNoProxy = resolveIdentity(makeReq({ "x-user-id": SPOOF }), null);
    ok("invalid Bearer (unverified ⇒ null) + no trusted proxy identity ⇒ no identity",
      invalidNoProxy.userId === null);

    const invalidWithProxy = resolveIdentity(makeReq({ "x-user-id": A, "x-proxy-secret": SECRET }), null);
    ok("invalid Bearer ignored when a trusted proxy identity is present ⇒ proxy identity",
      invalidWithProxy.userId === A && invalidWithProxy.via === "header" && !invalidWithProxy.mismatch);
  });

  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "production" }, () => {
    const r = resolveIdentity(makeReq({ "x-user-id": SPOOF }), null);
    ok("production + secret MISSING + spoofed x-user-id ⇒ userId null", r.userId === null, `userId=${r.userId}`);
  });
  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "development" }, () => {
    const r = resolveIdentity(makeReq({ "x-user-id": SPOOF }), null);
    ok("development + secret missing ⇒ raw x-user-id honoured (local only)", r.userId === SPOOF && r.via === "header");
  });
  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "production", DEV_TEST_UID: A }, () => {
    ok("DEV_TEST_UID ignored in production ⇒ userId null", resolveIdentity(makeReq({}), null).userId === null);
  });
  await withEnv({ PROXY_SHARED_SECRET: undefined, NODE_ENV: "development", DEV_TEST_UID: A }, () => {
    const r = resolveIdentity(makeReq({}), null);
    ok("DEV_TEST_UID honoured only in non-production ⇒ via=env", r.userId === A && r.via === "env");
  });

  // ── C. createClaimsVerifier ───────────────────────────────────────────
  const valid = signES256(claims());
  const v1 = await verify(valid);
  ok("valid ES256 token (trusted key, iss/aud/exp ok) ⇒ verified sub", v1?.sub === A, `sub=${v1?.sub}`);

  const callsBefore = signatureCalls;
  const v1again = await verify(valid);
  ok("verified token is cached (no second signature check)", v1again?.sub === A && signatureCalls === callsBefore);

  const forged = signES256(claims({ sub: SPOOF }), otherKeys.privateKey);
  ok("forged token (signed with an untrusted key) ⇒ rejected", (await verify(forged)) === null);

  const [h, , s] = valid.split(".");
  const tampered = `${h}.${b64json(claims({ sub: SPOOF }))}.${s}`;
  ok("tampered payload (sub swapped after signing) ⇒ rejected", (await verify(tampered)) === null);

  let before = signatureCalls;
  const unsigned = `${b64json({ alg: "none", typ: "JWT" })}.${b64json(claims({ sub: SPOOF }))}.x`;
  ok("alg=none token ⇒ rejected before any signature/network check",
    (await verify(unsigned)) === null && signatureCalls === before);

  before = signatureCalls;
  const edDsa = `${b64json({ alg: "EdDSA", typ: "JWT" })}.${b64json(claims())}.x`;
  ok("EdDSA (not verifiable by getClaims) ⇒ rejected", (await verify(edDsa)) === null && signatureCalls === before);

  ok("expired token (validly signed) ⇒ rejected", (await verify(signES256(claims({ exp: nowSec() - 5 })))) === null);
  ok("missing exp ⇒ rejected", (await verify(signES256(claims({ exp: undefined })))) === null);
  ok("wrong issuer (validly signed) ⇒ rejected", (await verify(signES256(claims({ iss: "https://evil.example/auth/v1" })))) === null);
  ok("wrong audience (validly signed) ⇒ rejected", (await verify(signES256(claims({ aud: "anon" })))) === null);
  ok("non-UUID sub ⇒ rejected", (await verify(signES256(claims({ sub: "admin" })))) === null);

  for (const [label, tok] of [
    ["two segments", `${b64json({ alg: "ES256" })}.${b64json(claims())}`],
    ["empty signature", `${b64json({ alg: "ES256" })}.${b64json(claims())}.`],
    ["garbage", "not-a-jwt"],
    ["non-JSON payload", `${b64json({ alg: "ES256" })}.${b64url(Buffer.from("{{{"))}.sig`],
    ["empty string", ""],
  ] as const) {
    ok(`malformed token (${label}) ⇒ rejected`, (await verify(tok)) === null);
  }

  const throwingVerify = createClaimsVerifier({
    signatureCheck: async () => { throw new Error("auth server unreachable"); },
    issuer: () => ISS,
  });
  ok("signature check throws (e.g. auth server down) ⇒ rejected, never throws",
    (await throwingVerify(signES256(claims()))) === null);

  const subSwapVerify = createClaimsVerifier({
    signatureCheck: async () => ({ claims: claims({ sub: B }), header: { alg: "ES256" } }),
    issuer: () => ISS,
  });
  ok("verified claims must match the pre-checked sub ⇒ rejected on mismatch",
    (await subSwapVerify(signES256(claims()))) === null);

  const noIssuerVerify = createClaimsVerifier({ signatureCheck: es256Check, issuer: () => null });
  ok("no configured issuer (SUPABASE_URL unset) ⇒ every token rejected",
    (await noIssuerVerify(signES256(claims()))) === null);

  // ── mutation controls (non-vacuity) ──────────────────────────────────
  await withEnv({ PROXY_SHARED_SECRET: SECRET, NODE_ENV: "production" }, () => {
    const vulnerableUserId = makeReq({ "x-user-id": SPOOF }).header("x-user-id") || null; // no trust check
    ok("mutation control: a strip-less resolver returns the spoof (header guard is load-bearing)",
      vulnerableUserId === SPOOF && resolveIdentity(makeReq({ "x-user-id": SPOOF }), null).userId === null);
  });
  const decodeOnly = createClaimsVerifier({
    signatureCheck: async (t) => {
      const d = decodeUnverified(t);
      return d ? { claims: d.payload, header: d.header } : null;
    },
    issuer: () => ISS,
  });
  ok("mutation control: a decode-only verifier WOULD accept the forged token (signature step is load-bearing)",
    (await decodeOnly(forged))?.sub === SPOOF && (await verify(forged)) === null);

  console.log(`\n${failures === 0 ? "✅ PASS" : `❌ FAIL (${failures})`} — identity boundary guard\n`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
