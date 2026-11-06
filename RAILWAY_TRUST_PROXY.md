# Railway proxy trust and rate-limit keying

**Problem.** Behind Railway the app received `X-Forwarded-For` while Express `trust proxy` was off, so `req.ip` was the edge's own address: every caller shared one rate-limit bucket (and express-rate-limit logged `ERR_ERL_UNEXPECTED_X_FORWARDED_FOR`).

**Measured topology (staging Railway service, 2026-11-06, temporary diagnostic route, since removed).** The app sees socket peer `100.64.0.x` (Railway internal) and `X-Forwarded-For: "<real client>, <Railway edge node 152.233.29.x>"`. The edge **overwrites** any client-supplied `X-Forwarded-For` (a forged `6.6.6.6` never arrived) and the edge-node address varies per request (observed 152.233.29.1–5). `x-real-ip` carries the client too.

**First attempt and why it was wrong.** One trusted hop makes `req.ip` the *edge node*: one client was spread over several buckets (observed live: `RateLimit remaining` jumping 297, 296, 297, 295…), i.e. its effective limit was multiplied and distinct clients still shared buckets.

**Fix.** `app.set("trust proxy", resolveTrustProxyHops())` (`src/trustProxy.ts`): exactly **two** trusted hops in production and on the staging service, none in local dev. `TRUST_PROXY_HOPS` may override with an integer 0–2; any other value (`true`, `*`, a CIDR, `3`…) refuses to boot. `true` is unsafe: `req.ip` becomes the leftmost entry.

**Effective client IP.** With two hops Express resolves `req.ip` to the real client address regardless of which edge node served the request.

**Residual assumption.** A caller that reaches the container directly over Railway's private network (not via the edge) is not covered by this model; the service has no such callers.

**Proof.** `npm run validate:trust-proxy` (in `check:release`) runs the real limiter and resolver: forged rotation cannot escape the limit; two real addresses get separate buckets; the old configuration (off) collapses them and `true` is bypassable (non-vacuity). Staging verification: see the report for the live measurements.

**Known consequence, not changed here.** Traffic from the web app reaches the API through the Vercel proxy, so the "client" the API sees is the proxy's egress address; the per-IP limits (300/min global) therefore apply to the web app's egress IPs, not individual end users. Consider keying authenticated traffic by verified user id, or raising the limit, after measuring real load.
