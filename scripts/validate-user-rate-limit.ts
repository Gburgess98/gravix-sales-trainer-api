// Per-user rate limiting behind the trusted Vercel proxy (network-free: loopback HTTP only).
//
// Builds an app with the SAME middleware order as src/server.ts, using the REAL limiter factories, the REAL resolveIdentity and
// getBearerToken (only the JWT signature check is replaced by a fake verifier: "valid.<uuid>" = verified token for <uuid>):
//   trust proxy → IP limiter (skips our proxy) → identity middleware (+ auth_mismatch 401) → per-user limiter → default-deny → route
// Proves:
//   1 two authenticated proxied users have separate buckets      5 direct traffic stays IP-limited (and per address)
//   2 one user reaches 429 without affecting the other           6 rotating forged forwarded/proxy headers cannot evade limits
//   3 forged identity without the proxy secret cannot create     7 missing / invalid authentication: 401, limited by IP
//     or switch buckets                                          8 key hygiene: namespaced hash, no raw id/email/token/secret;
//   4 a valid bearer cannot be replaced by a conflicting header      no logging of keys
//   9 wiring in server.ts has the audited order; NON-VACUITY: the old single IP limiter fails scenarios 1–2
// Run: npm run validate:user-rate-limit

import * as fs from "node:fs";
import * as path from "node:path";
import http from "node:http";
import express from "express";

process.env.NODE_ENV = "production";
process.env.PROXY_SHARED_SECRET = "validator-proxy-secret-0123456789abcdef";
const SECRET = process.env.PROXY_SHARED_SECRET;

let failures = 0;
const check = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) failures++; };

const U1 = "11111111-1111-4111-8111-111111111111";
const U2 = "22222222-2222-4222-8222-222222222222";
const U3 = "33333333-3333-4333-8333-333333333333";
const MAX = 5;

async function main() {
  const { createIpRateLimit, createUserRateLimit, rateLimitKey, isTrustedProxyRequest } = await import("../src/middleware/rateLimits");
  const { resolveIdentity, getBearerToken, isUuid } = await import("../src/identityTrust");
  const { resolveTrustProxyHops } = await import("../src/trustProxy");

  async function serve(opts: { legacy?: boolean } = {}) {
    const app = express();
    app.set("trust proxy", resolveTrustProxyHops({ NODE_ENV: "production" }));
    if (opts.legacy) {
      // Previous design: one plain limiter keyed on req.ip for EVERY request, including our own proxy's.
      const { default: rateLimit } = await import("express-rate-limit");
      app.use(rateLimit({ windowMs: 60_000, max: MAX, standardHeaders: "draft-7", legacyHeaders: false, handler: (_q: any, r: any) => r.status(429).json({ ok: false }) }));
    } else {
      app.use(createIpRateLimit(MAX));
    }
    app.use((req, res, next) => {
      const token = getBearerToken(req as any);
      const m = token ? /^valid\.([0-9a-f-]{36})$/.exec(token) : null; // fake verifier: only "valid.<uuid>" verifies
      const resolved = resolveIdentity(req as any, m ? m[1] : null);
      (req as any).auth = { userId: resolved.userId, via: resolved.via };
      (req as any).userId = resolved.userId;
      if (resolved.mismatch) return res.status(401).json({ ok: false, error: "auth_mismatch" });
      next();
    });
    if (!opts.legacy) app.use(createUserRateLimit(MAX));
    app.use((req, res, next) => ((req as any).userId ? next() : res.status(401).json({ ok: false, error: "missing_user_identity" })));
    app.get("/who", (req, res) => res.json({ userId: (req as any).userId, ip: req.ip, key: rateLimitKey(req) }));
    const server = http.createServer(app);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const port = (server.address() as any).port;
    let n = 0;
    // `egress`/`client` = what Railway's edge reports: "<client>, <edge node>"; the edge overwrites any client-supplied value.
    const call = async (h: { client?: string; uid?: string; bearer?: string; secret?: string | null; extra?: Record<string, string> }) => {
      const headers: Record<string, string> = { "x-forwarded-for": `${h.client ?? "198.51.100.1"}, 152.233.29.${(n++ % 5) + 1}`, ...(h.extra || {}) };
      if (h.uid) headers["x-user-id"] = h.uid;
      if (h.bearer) headers["authorization"] = `Bearer ${h.bearer}`;
      if (h.secret !== null) headers["x-proxy-secret"] = h.secret ?? SECRET;
      const r = await fetch(`http://127.0.0.1:${port}/who`, { headers });
      let body: any = null; try { body = await r.json(); } catch { /* */ }
      return { status: r.status, body, rl: r.headers.get("ratelimit") };
    };
    return { call, close: () => new Promise<void>((r) => server.close(() => r())) };
  }
  const VERCEL = "76.76.21.21"; // every web user shares this egress address
  const viaProxy = (uid: string, extra: Record<string, string> = {}) => ({ client: VERCEL, uid, extra });
  const statuses = async (s: any, n: number, h: any) => { const out: number[] = []; for (let i = 0; i < n; i++) out.push((await s.call(h)).status); return out; };

  // 1 + 2
  {
    const s = await serve();
    const a = await statuses(s, MAX + 1, viaProxy(U1));
    check(`1/2 user A: ${MAX} requests pass, request ${MAX + 1} is 429 (per-user bucket)`, a.slice(0, MAX).every((x) => x === 200) && a[MAX] === 429, a.join());
    const b = await statuses(s, MAX, viaProxy(U2));
    check("1/2 user B, behind the SAME Vercel egress address, is unaffected by A's 429 and gets its own full allowance", b.every((x) => x === 200), b.join());
    const bAgain = await s.call(viaProxy(U2));
    check("1/2 B hits its own limit independently afterwards", bAgain.status === 429);
    const c = await s.call(viaProxy(U3));
    check("1/2 a third user is still fresh", c.status === 200);
    check("1/2 the user limiter reports per-user RateLimit headers on proxied responses", /remaining=/.test(c.rl || ""), String(c.rl));
    await s.close();
  }
  // 3 — forged identity without trust
  {
    const s = await serve();
    const out: number[] = [];
    for (let i = 0; i < MAX + 3; i++) out.push((await s.call({ client: "203.0.113.50", uid: `aaaaaaaa-aaaa-4aaa-8aaa-${String(i).padStart(12, "0")}`, secret: null, extra: { "x-org-id": `org-${i}` } })).status);
    check("3 forged, rotating x-user-id / x-org-id with NO proxy secret: never authenticated (401) and stays in ONE address bucket (429 after the budget)", out.slice(0, MAX).every((x) => x === 401) && out.slice(MAX).every((x) => x === 429), out.join());
    const out2: number[] = [];
    for (let i = 0; i < MAX + 2; i++) out2.push((await s.call({ client: "203.0.113.51", uid: U1, secret: "wrong-secret-of-different-length", extra: {} })).status);
    check("3 a WRONG proxy secret is never trusted: still IP-limited and unauthenticated", out2.slice(0, MAX).every((x) => x === 401) && out2[MAX] === 429, out2.join());
    const same = Buffer.from(SECRET).toString().replace(/.$/, "X"); // same length, one char off
    const out3 = await s.call({ client: "203.0.113.52", uid: U1, secret: same });
    check("3 a same-length near-miss secret is not trusted", out3.status === 401 && !isTrustedProxyRequest({ headers: { "x-proxy-secret": same } }));
    await s.close();
  }
  // 4 — bearer vs header
  {
    const s = await serve();
    const mismatch = await s.call({ client: VERCEL, uid: U2, bearer: `valid.${U1}` });
    check("4 trusted proxy: a valid bearer for A with a conflicting x-user-id for B → 401 auth_mismatch (no identity switch)", mismatch.status === 401 && mismatch.body?.error === "auth_mismatch");
    const before = await s.call(viaProxy(U1));
    const spam = await statuses(s, MAX, { client: VERCEL, uid: U2, bearer: `valid.${U1}` });
    const after = await s.call(viaProxy(U1));
    check("4 repeated mismatches do not consume or switch either user's bucket", spam.every((x) => x === 401) && before.status === 200 && after.status === 200);
    const direct = await s.call({ client: "203.0.113.60", bearer: `valid.${U1}`, uid: U2, secret: null });
    check("4 direct caller: valid bearer wins, the forged x-user-id is stripped (identity = bearer's user)", direct.status === 200 && direct.body?.userId === U1);
    check("4 …and its bucket is the ADDRESS, not the (untrusted) identity", direct.body?.key.startsWith("ip:") === true);
    await s.close();
  }
  // 5 — direct stays IP limited
  {
    const s = await serve();
    const a = await statuses(s, MAX + 1, { client: "203.0.113.70", bearer: `valid.${U1}`, secret: null });
    const other = await s.call({ client: "203.0.113.71", bearer: `valid.${U1}`, secret: null });
    check("5 direct traffic: limited per address even with a valid bearer", a[MAX] === 429);
    check("5 a different direct address (same user) is not collapsed into that bucket", other.status === 200);
    await s.close();
  }
  // 6 — rotating forged forwarded / proxy headers
  {
    const s = await serve();
    const out: number[] = [];
    for (let i = 0; i < MAX + 2; i++) out.push((await s.call({ client: "203.0.113.80", bearer: `valid.${U1}`, secret: null, extra: { "x-real-ip": `10.9.${i}.1`, "x-forwarded-host": `h${i}`, forwarded: `for=10.8.${i}.1`, "x-envoy-external-address": `10.7.${i}.1` } })).status);
    check("6 direct: rotating x-real-ip / forwarded / envoy headers cannot evade the address limit", out[MAX] === 429, out.join());
    const p: number[] = [];
    for (let i = 0; i < MAX + 1; i++) p.push((await s.call({ client: `10.6.${i}.1`, uid: U2, extra: { "x-real-ip": `10.5.${i}.1`, forwarded: `for=10.4.${i}.1` } })).status);
    check("6 proxied: the user bucket does not depend on any forwarded address (rotating them still hits 429)", p[MAX] === 429, p.join());
    await s.close();
  }
  // 7 — missing / invalid auth
  {
    const s = await serve();
    const none = await statuses(s, MAX + 1, { client: "203.0.113.90", secret: null });
    check("7 anonymous: 401 until the address budget is spent, then 429", none.slice(0, MAX).every((x) => x === 401) && none[MAX] === 429, none.join());
    const bogus = await statuses(s, MAX + 1, { client: "203.0.113.91", bearer: "not.a.verified.token", secret: null });
    check("7 invalid bearer: ignored (401), IP-limited", bogus.slice(0, MAX).every((x) => x === 401) && bogus[MAX] === 429, bogus.join());
    const proxiedNoId = await s.call({ client: VERCEL, secret: undefined });
    check("7 trusted proxy WITHOUT an identity is still refused (401)", proxiedNoId.status === 401);
    await s.close();
  }
  // 8 — hygiene
  {
    const key = rateLimitKey({ headers: { "x-proxy-secret": SECRET }, auth: { userId: U1, via: "header" }, ip: "76.76.21.21" });
    check("8 user key is a namespaced hash (u:<32 hex>), not the raw id", /^u:[0-9a-f]{32}$/.test(key) && !key.includes(U1) && !key.includes(U1.replace(/-/g, "")), key);
    check("8 the key never contains the proxy secret, a token or an email", !key.includes(SECRET) && !/@|valid\./.test(key));
    check("8 same user → same key; different users → different keys", key === rateLimitKey({ headers: { "x-proxy-secret": SECRET }, auth: { userId: U1, via: "header" } }) && key !== rateLimitKey({ headers: { "x-proxy-secret": SECRET }, auth: { userId: U2, via: "header" } }));
    check("8 an identity that did not come from header/jwt (env/none) is never a user key", rateLimitKey({ headers: { "x-proxy-secret": SECRET }, auth: { userId: U1, via: "env" }, ip: "1.2.3.4" }).startsWith("ip:"));
    check("8 without the proxy secret the key is the address, even if an identity is attached", rateLimitKey({ headers: {}, auth: { userId: U1, via: "jwt" }, ip: "1.2.3.4" }) === "ip:1.2.3.4");
    const src = fs.readFileSync(path.resolve(__dirname, "../src/middleware/rateLimits.ts"), "utf8");
    check("8 rateLimits.ts never logs (no console/key output)", !/console\./.test(src));
    check("8 isUuid sanity (validator wiring)", isUuid(U1));
  }
  // 9 — wiring + non-vacuity
  {
    const server = fs.readFileSync(path.resolve(__dirname, "../src/server.ts"), "utf8");
    const at = (needle: string) => server.indexOf(needle);
    const order = [at('app.set("trust proxy"'), at("app.use(globalRateLimit)"), at("resolveIdentity(req, verified"), at("app.use(userRateLimit)"), at('app.use("/v1/upload", uploadRateLimit)'), at("app.use(requireIdentityByDefault)")];
    check("9 server.ts order: trust proxy → IP limiter → identity middleware → user limiter → upload limiter → default-deny", order.every((x) => x > 0) && order.every((x, i) => i === 0 || x > order[i - 1]), order.join(","));
    check("9 the identity middleware still carries the auth_mismatch guard before the user limiter", at('"auth_mismatch"') > at("resolveIdentity(req, verified") && at('"auth_mismatch"') < at("app.use(userRateLimit)"));
    check("9 the auth/login throttles remain IP-keyed and before authentication", at('app.use("/v1/auth/login"') > 0 && at('app.use("/v1/auth/login"') < at("resolveIdentity(req, verified"));
    const legacy = await serve({ legacy: true });
    const a = await statuses(legacy, MAX + 1, viaProxy(U1)); // valid proxy secret, but the old limiter only sees the shared egress address
    const b = await legacy.call(viaProxy(U2));
    check("NON-VACUITY the previous design (single address limiter) lets user A exhaust user B's allowance behind one egress address", a[MAX] === 429 && b.status === 429, `${a.join()} / ${b.status}`);
    await legacy.close();
  }

  console.log(failures ? `\n${failures} check(s) FAILED` : "\nPer-user rate-limit checks PASSED");
  process.exit(failures ? 1 : 0);
}
main().catch((e) => { console.error(e); process.exit(1); });
