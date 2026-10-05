# Hardening follow-ups after Go Live Day 27

Supersedes the "Next hardening day" section of HARDENING_FOLLOWUPS_DAY_26.md. Local commit only; not pushed or deployed.

## Done in Day 27 (proved by `npm run validate:admin-hardening`, wired into `check:release`)
- **G5** `src/productionConfig.ts`: with `NODE_ENV==="production"`, boot throws before listening if `ALLOW_ADMIN_ENDPOINTS` is non-empty after trim (`true`, `TRUE`, `false`, `0`, `no`, ` true ` all refuse; absent/empty/whitespace boot). Message never echoes the value. Non-production unchanged.
- **Admin guard** `requireAdmin` (server.ts): env gate (exact `"true"`) THEN `requireSuperAdmin`. Previously any authenticated identity passed when the flag was set. Effective `force-score`/`preview-slack` handlers remain the server.ts ones (adminRouter duplicates stay shadowed and were NOT exposed or reordered).
- **Rewards** mount repaired: `rewardsRoutes()` called, doubled prefix removed. URLs: `GET /v1/rewards/:userId`, `POST /v1/rewards/:userId/select-title`. No WEB consumer exists or ever called these (orphaned `/rewards` page redirects to /dashboard), so the path follows the explicit mount. Self/same-tenant-manager guards preserved. `DECLARED_UNCALLED` exception removed from `validate-route-authz-day-26.ts`.
- **`GET /v1/rewards/bounties/active` is deliberately CLOSED (404)** until policy is decided (see below): newly reachable, reads `bounties` with the anon client and no tenant filter.
- `/v1/internal/*` unchanged and still fail-closed (403 for every authenticated caller, incl. SuperAdmin) — no service-auth contract invented.
- Raw `x-user-id` audit: 19 files / ~75 reads pinned as a reviewed baseline. None converted: header and `req.userId` are NOT equivalent (impersonation sets `req.userId` to the target while the header stays the actor; Bearer-only callers have no header; alias headers; non-UUID header). Safety rests on the Day-175 strip, now proved behaviourally (spoofed/wrong-secret headers get 401 on SuperAdmin and manager guards; production without a secret trusts none).

## Still open (policy / separate scope)
1. **Bounties tenancy**: `bounties.org_id` is nullable; is null = global? Then add a tenant filter and enable (`BOUNTIES_ENABLED` in `src/routes/rewards.ts`). Also confirm RLS for the anon-key reads (`user_badges`, `user_titles`, `user_selected_title`).
2. Leaderboard scope; PartnerAdmin/SuperAdmin cross-tenant views; XP; rate limits; `trust proxy`.
3. Assignments `requireManager` role policy (Manager/Admin/Owner; excludes PartnerAdmin/SuperAdmin).
4. `requireInternal` reads never-set `req.authUserId` → internal portal unreachable. Decision needed on the intended service-auth/identity contract before changing.
5. Bearer-only (no header) callers cannot use SuperAdmin-guarded routes (fail closed). Convert guards to verified `req.userId` only after deciding impersonation semantics.
6. Legacy dev entry point `src/index.ts` (`start:dev`) still present — deliberately not deleted.
7. adminRouter `force-score` (needs unwired `req.services`) and `preview-slack` stub remain shadowed dead code; delete or wire in a reviewed change.
8. Pre-existing: the validator suite imports `server.ts`, whose dotenv call reads `./.env` from the CWD; Day-27 validator runs from an empty directory with placeholder keys so no real secret is loaded.
