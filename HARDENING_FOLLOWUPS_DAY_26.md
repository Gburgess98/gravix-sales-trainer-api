# Hardening follow-ups after Go Live Day 26 (G1–G4)

Found by `validate:route-authz`; deliberately NOT changed in the G1–G4 commit.

## Next hardening day
1. **`ALLOW_ADMIN_ENDPOINTS` production boot refusal.** `requireAdmin` (server.ts `/v1/admin/*`: score, force-score, preview-slack, post-slack, digest/daily, index-hints) is closed unless `ALLOW_ADMIN_ENDPOINTS==='true'`, but when set ANY authenticated identity passes (no role check). Make boot refuse (or require SuperAdmin) when `NODE_ENV=production` and the var is set. The guard already fails if committed config enables it.
2. **Unreachable `rewardsRoutes` mount.** `app.use("/v1/rewards", rewardsRoutes)` passes the factory uncalled (same at HEAD), so Express runs it as middleware and `/v1/rewards/*` never reaches its routes. Fixing means `rewardsRoutes()` and deciding the path (currently would be `/v1/rewards/rewards/:userId`). Then delete the `DECLARED_UNCALLED` exception in `scripts/validate-route-authz-day-26.ts` (it fails as stale once fixed).

## Remaining policy decisions
- Leaderboard scope; PartnerAdmin/SuperAdmin cross-tenant views; XP; rate limits; `trust proxy`.
- `src/index.ts`: legacy UNAUTHENTICATED dev app (`/v1/env-check`, `/v1/db/now`), `start:dev` only — delete?
- adminRouter `force-score/:id` and `preview-slack` (SuperAdmin) are shadowed by earlier `requireAdmin` versions in server.ts.
- `requireInternal` reads `req.authUserId`, which no global middleware sets → `/v1/internal/*` always 403 (fails closed, portal unreachable).
- `requireSuperAdmin`/`requirePartnerAdmin`/admin.ts `requireManager`/inline admin scope read raw `x-user-id`, safe only because `resolveIdentity` strips spoofable headers when untrusted (guard-enforced). Consider reading `req.userId`.
- assignments.ts local `requireManager` allows Manager/Admin/Owner (excludes PartnerAdmin/SuperAdmin; "Admin" is not a tier).
