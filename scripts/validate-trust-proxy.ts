// Trust-proxy / rate-limit regression guard (network-free: loopback HTTP only).
//
// Simulates the MEASURED Railway topology (staging, 2026-11-06): the app receives X-Forwarded-For "<real client>, <edge node>"
// where the edge node address varies per request and the edge overwrites any client-supplied header. To be safe even if a hop
// ever appended instead of overwrote, forged entries are also placed to the LEFT ("F, C, E"). Proves, with the REAL globalRateLimit and the REAL
// resolveTrustProxyHops():
//   1 production resolves exactly one trusted hop; dev none; overrides are bounded integers; bad values refuse to boot
//   2 effective client IP (req.ip) is the edge-appended entry, not a forged one
//   3 a client cannot bypass the limit by rotating a forged X-Forwarded-For
//   4 two legitimate client addresses get separate buckets (not collapsed)
//   5 the old behaviour (off) collapses everyone into one bucket; one hop splits ONE client across edge-node buckets; `true` lets a
//     forged header bypass — the scenarios above fail against all three (non-vacuity)
//   6 server.ts wires the resolver and no other trust-proxy setting
// Run: npm run validate:trust-proxy

import * as fs from "node:fs";
import * as path from "node:path";
import http from "node:http";
import express from "express";
import { resolveTrustProxyHops } from "../src/trustProxy";
import { globalRateLimit } from "../src/middleware/rateLimits";

let failures = 0;
const check = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) failures++; };

async function serve(trust: unknown) {
  const app = express();
  app.set("trust proxy", trust as any);
  app.use(globalRateLimit);
  app.get("/ip", (req, res) => res.json({ ip: req.ip }));
  const server = http.createServer(app);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as any).port;
  // `real` is the client address the edge saw; `forged` is anything the client put in the header itself.
  let edgeN = 0;
  const hit = async (real: string, forged?: string) => {
    const edge = `152.233.29.${(edgeN++ % 5) + 1}`; // the edge node varies per request
    const xff = `${forged ? `${forged}, ` : ""}${real}, ${edge}`;
    const r = await fetch(`http://127.0.0.1:${port}/ip`, { headers: { "x-forwarded-for": xff } });
    return { status: r.status, body: r.status === 200 ? ((await r.json()) as any) : null };
  };
  return { hit, close: () => new Promise<void>((r) => server.close(() => r())) };
}
const MAX = 300; // globalRateLimit: 300 / minute / key

async function burst(hit: (real: string, forged?: string) => Promise<{ status: number }>, n: number, real: string, forge: boolean) {
  let last = 0;
  for (let i = 0; i < n; i++) last = (await hit(real, forge ? `10.${(i >> 8) & 255}.${i & 255}.9` : undefined)).status;
  return last;
}

async function main() {
  // 1 — resolver contract
  check("production resolves exactly the two measured Railway hops", resolveTrustProxyHops({ NODE_ENV: "production" }) === 2);
  check("production on the staging service (APP_ENV=staging) resolves the same hop count", resolveTrustProxyHops({ NODE_ENV: "production", APP_ENV: "staging" }) === 2);
  check("development trusts no proxy", resolveTrustProxyHops({ NODE_ENV: "development" }) === 0 && resolveTrustProxyHops({}) === 0);
  check("TRUST_PROXY_HOPS overrides within 0–2", resolveTrustProxyHops({ NODE_ENV: "production", TRUST_PROXY_HOPS: "2" }) === 2 && resolveTrustProxyHops({ NODE_ENV: "production", TRUST_PROXY_HOPS: "0" }) === 0);
  for (const bad of ["true", "*", "loopback", "3", "-1", "1.5", "10.0.0.0/8"]) {
    let refused = false;
    try { resolveTrustProxyHops({ NODE_ENV: "production", TRUST_PROXY_HOPS: bad }); } catch { refused = true; }
    check(`TRUST_PROXY_HOPS=${JSON.stringify(bad)} refuses to boot`, refused);
  }
  check('TRUST_PROXY_HOPS=" 1 " (padded) is accepted as 1', resolveTrustProxyHops({ NODE_ENV: "production", TRUST_PROXY_HOPS: " 1 " }) === 1);

  // 2 — effective client IP
  {
    const s = await serve(resolveTrustProxyHops({ NODE_ENV: "production" }));
    const a = await s.hit("203.0.113.10", "6.6.6.6");
    check("req.ip is the real client address, not a forged entry and not the edge node", a.body?.ip === "203.0.113.10", JSON.stringify(a.body));
    const b = await s.hit("203.0.113.11");
    check("req.ip with no forged entry is the real address", b.body?.ip === "203.0.113.11");
    await s.close();
  }

  // 3 — forged rotation cannot bypass
  {
    const s = await serve(2);
    const last = await burst(s.hit, MAX + 5, "198.51.100.7", true);
    check(`rotating a forged X-Forwarded-For does not escape the limit (${MAX + 5} requests → 429)`, last === 429, String(last));
    await s.close();
  }

  // 4 — two legitimate clients stay separate (and one client is ONE bucket although the edge node varies)
  {
    const s = await serve(2);
    const a = await burst(s.hit, MAX + 5, "198.51.100.20", false);
    const other = await s.hit("198.51.100.21");
    check("one client is a single bucket even though the edge node changes per request (limited after the budget)", a === 429);
    check("a different legitimate client address is NOT limited (separate bucket)", other.status === 200 && other.body?.ip === "198.51.100.21", String(other.status));
    await s.close();
  }

  // 5 — non-vacuity: the previous and a tempting-but-wrong configuration fail the same scenarios
  {
    const off = await serve(false);
    const a = await burst(off.hit, MAX + 5, "198.51.100.30", false);
    const other = await off.hit("198.51.100.31");
    check("NON-VACUITY trust proxy OFF collapses distinct clients into one bucket (the unfixed behaviour)", a === 429 && other.status === 429, `${a}/${other.status}`);
    await off.close();
    const one = await serve(1);
    const oneLast = await burst(one.hit, MAX + 5, "198.51.100.50", false);
    check("NON-VACUITY one hop keys on the varying edge node, so ONE client escapes the limit (observed live on staging)", oneLast === 200, String(oneLast));
    await one.close();
    const all = await serve(true);
    const last = await burst(all.hit, MAX + 5, "198.51.100.40", true);
    check("NON-VACUITY trust proxy `true` lets a forged header bypass the limit", last === 200, String(last));
    await all.close();
  }

  // 6 — wiring
  const server = fs.readFileSync(path.resolve(__dirname, "../src/server.ts"), "utf8");
  const sets = server.match(/app\.set\(\s*["']trust proxy["'][^)]*\)/g) || [];
  check("server.ts sets trust proxy exactly once, from resolveTrustProxyHops()", sets.length === 1 && /resolveTrustProxyHops\(\)/.test(sets[0]), sets.join(" | "));
  check("trust proxy is set before the global rate limit is mounted", server.indexOf('app.set("trust proxy"') < server.indexOf("app.use(globalRateLimit)") && server.indexOf('app.set("trust proxy"') > 0);

  console.log(failures ? `\n${failures} check(s) FAILED` : "\nTrust-proxy / rate-limit checks PASSED");
  process.exit(failures ? 1 : 0);
}
main().catch((e) => { console.error(e); process.exit(1); });
