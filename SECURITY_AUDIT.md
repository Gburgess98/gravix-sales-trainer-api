# Security Audit — Gravix Sales Trainer API

**Date:** 2026-06-09  
**Branch:** claude/sprint-3-api  
**Auditor:** Automated analysis + manual review

Legend: ✅ PASS · ⚠️ WARNING · ❌ FAIL

---

## 1. Manager-only Routes

### Gate implementation

| Middleware | File | Tiers allowed | Reads from |
|---|---|---|---|
| `requireManager` (inline) | `src/routes/admin.ts:65` | Manager, Owner, PartnerAdmin, SuperAdmin | `(req as any).userId` |
| `requireManager` (exported) | `src/middleware/requireManager.ts:50` | Same | `req.userId` or header |
| `requirePartnerAdmin` | `src/middleware/requirePartnerAdmin.ts:32` | PartnerAdmin, SuperAdmin | Header only (explicit) |
| `requireSuperAdmin` | `src/middleware/requireSuperAdmin.ts:29` | SuperAdmin | Header only (explicit) |

### Findings

⚠️ **WARNING — requireManager reads `req.userId` which DEV_TEST_UID can populate**  
`requireManager.ts:35` reads `(req as any)?.userId` as its first priority. Server.ts middleware (line 207) sets `req.userId = DEV_TEST_UID` when the env var is present and no real auth header is provided. If `DEV_TEST_UID` is set in a production environment and that UID belongs to a Manager-tier rep, all requireManager gates would pass silently.

✅ **PASS — `requirePartnerAdmin` and `requireSuperAdmin` are hardened**  
Both read only from explicit request headers (`x-user-id`, `x-forwarded-user-id`), never from `req.userId`. A missing header returns 401, not a fallthrough.

✅ **PASS — Manager-gated routes check tier from DB, not from client claims**  
Tier is re-fetched on every request; a client cannot self-elevate by passing a fake tier header.

⚠️ **WARNING — Two separate `requireManager` implementations exist**  
`admin.ts:65` has an inline copy; `src/middleware/requireManager.ts` is the exported version. They are functionally equivalent but diverge on the `DEV_TEST_UID` exposure. Consolidate to the exported version and guard it the same way as `requirePartnerAdmin`.

---

## 2. Org / Company Tenant Isolation

### CRM route scoping

CRM routes resolve org context via `getOrgIdHeader` (`crm.ts:30`):
1. Read `x-org-id` header (UUID)
2. Fall back to `DEFAULT_ORG_ID` env var

`requireManagerOrg` (`crm.ts:211`) then validates that the requesting user is a member of that org — **unless bypassed**.

### Bypass conditions (crm.ts:222)

```
if (ALLOW_ORG_BYPASS=1 || orgId == all-zero UUID || x-org-id header not provided) {
  return { bypassed: true }   // org membership check SKIPPED
}
```

❌ **FAIL — Missing `x-org-id` header silently bypasses org membership check**  
If a client omits the `x-org-id` header entirely, `requireManagerOrg` sets `bypassed: true` and skips the membership check. The org still defaults to `DEFAULT_ORG_ID` (env), but any authenticated user can query manager-level CRM data for that org without proving membership.

❌ **FAIL — `ALLOW_ORG_BYPASS=1` completely disables org membership validation**  
If this env var is set in production, any authenticated Manager-tier user can access any org's CRM data. Verify this is not set in the production Railway/Vercel environment.

⚠️ **WARNING — Zero UUID (`00000000-...`) is a magic bypass value**  
Any client that discovers this can omit the real org ID and still get data. This should be treated the same as a missing header (reject, not bypass).

✅ **PASS — `assertRequesterInOrg` validates membership when not bypassed**  
`crm.ts:168` correctly cross-checks the requesting user's `org_id` against the requested org when the bypass is not active.

✅ **PASS — Admin routes scope by company_id via `getVisibleCompanies`**  
`partnerAccess.ts:36` correctly limits SuperAdmin to all, PartnerAdmin to partner's companies, Manager to own company.

---

## 3. CRM Ownership Permissions

### Contact reads
- `crm.ts:519` — `.eq("org_id", orgId)` filters contacts to the request org ✅
- `crm.ts:260,273` — `.eq("user_id", requester)` on assignment-linked queries ✅

### Write operations
- `crm.ts:660` — `org_id: orgId` stamped on contact creation ✅
- `crm.ts:1039,1049` — `org_id: orgId` stamped on opportunity creation ✅
- `crm.ts:4815` — `user_id: requester` stamped on email unsubscribe ✅

⚠️ **WARNING — Some CRM writes do not re-verify ownership before update**  
`crm.ts:2813` (stage update) and `crm.ts:3106` (opportunity patch) update by `id` alone without re-confirming `org_id` ownership. If an attacker guesses a valid UUID from another org, they could patch it. Mitigated by the UUIDs being non-guessable, but a belt-and-suspenders `.eq("org_id", orgId)` on each update is the correct fix.

✅ **PASS — Activity reads scoped to `user_id = requester`** (`crm.ts:3915`)

---

## 4. Service-Role Key Usage

All database operations across the entire API use `SUPABASE_SERVICE_ROLE_KEY`. This key bypasses Row Level Security (see Section 9).

| File | Service-role client count |
|---|---|
| `src/server.ts` | 1 (module-level) |
| `src/routes/crm.ts` | 1 (module-level) |
| `src/routes/admin.ts` | Many (inline createClient calls) |
| `src/lib/scoring.ts` | 5 |
| `src/lib/audit.ts` | 1 |
| `src/middleware/requireManager.ts` | 1 |
| `src/middleware/requirePartnerAdmin.ts` | 1 |
| `src/middleware/requireSuperAdmin.ts` | 1 |
| Total | ~20+ distinct clients |

⚠️ **WARNING — Proliferation of inline `createClient` calls**  
`admin.ts` re-creates the Supabase client inside each route handler instead of using a shared singleton. This is wasteful but not a direct security issue. Centralize to `src/lib/supa.ts`.

❌ **FAIL — Service role key used in all queries; RLS is inert**  
Because every query uses the service role, database-level Row Level Security provides zero protection for the current API. Application-level checks are the only defence. See Section 9.

✅ **PASS — Anon key used for user-facing auth operations**  
`src/routes/auth.ts:getAnonSupa` uses `SUPABASE_ANON_KEY` for `signInWithPassword` and `resetPasswordForEmail`, which is correct.

---

## 5. Exposed Environment Variables

### Variables logged at startup

| Location | Variable logged | Risk |
|---|---|---|
| `server.ts:47` | `SUPABASE_URL` | Low — URL is in the JWT anyway |

✅ **PASS — `.env` is in `.gitignore`** (`/.env`, `/\.env.*`)

✅ **PASS — No secrets logged at startup** (only SUPABASE_URL, which is not secret)

⚠️ **WARNING — `DEV_TEST_UID` in production is an auth bypass**  
If this env var is set in Railway/Vercel production, any request without an auth header gets the specified user's identity. Must be absent from production environments.

⚠️ **WARNING — `ALLOW_ADMIN_ENDPOINTS=true` bypasses `requireAdmin` gate**  
`server.ts:596` — if set, `/v1/admin/score/:id`, `/v1/admin/force-score/:id`, etc. are accessible to anyone.

⚠️ **WARNING — `ALLOW_DEV_UID_QS=1` allows user ID via query string**  
`calls.ts:47` — enables `?uid=<uuid>` to bypass header-based identity. Must not be set in production.

⚠️ **WARNING — `ALLOW_ORG_BYPASS=1` disables org membership validation**  
See Section 2. Must not be set in production.

### Recommended production env audit checklist

```
DEV_TEST_UID          — must be absent or empty
ALLOW_ADMIN_ENDPOINTS — must be absent or "false"
ALLOW_DEV_UID_QS      — must be absent or empty
ALLOW_ORG_BYPASS      — must be absent or empty
```

---

## 6. Database Schema Permissions

✅ **PASS — Migrations use `public` schema; no tables exposed via API schema**

⚠️ **WARNING — No RLS policies on any table**  
All tables (`reps`, `companies`, `calls`, `crm_contacts`, etc.) rely entirely on application-level scoping. A compromised service-role key means full database access. See Section 9.

⚠️ **WARNING — `reps` table contains tier information**  
A direct Supabase client with the service role key can read or write any user's tier, bypassing the API tier-enforcement logic. This risk is only mitigated by keeping the service role key secret.

✅ **PASS — Tier CHECK constraint in database** (`20260605b_reps_tier_constraint.sql`)  
`reps.tier` is constrained to known values; an arbitrary string tier cannot be injected via a direct DB write.

---

## 7. Session / Authentication Security

### JWT handling

⚠️ **WARNING — JWTs are decoded without signature verification in most routes**  
`server.ts:tryDecodeJwtSub` (line 172) and `requireUserId.ts:tryDecodeJwtSub` (line 10) base64-decode the JWT payload and extract `sub` without calling `supabase.auth.getUser(token)`. This means a crafted JWT with a valid-format UUID in `sub` would pass the identity check, as long as the user ID exists in the `reps` table.

Only `GET /v1/auth/me` performs real signature verification via `supabase.auth.getUser(token)`.

**Risk:** Medium. An attacker who knows a valid user UUID could forge a JWT sub. Supabase's JWTs are RS256-signed; without the private key forging is not feasible, but the code does not verify.

**Fix:** Replace `tryDecodeJwtSub` with `supabase.auth.getUser(token)` in server.ts identity middleware, or at minimum add a signature check on the key routes.

✅ **PASS — Impersonation requires SuperAdmin tier (verified from DB)**  
`server.ts:252` — impersonation middleware re-checks tier from `reps` table on every request.

⚠️ **WARNING — Impersonation has no time-bound token or expiry**  
The `x-impersonated-user-id` header alone grants impersonation for the duration of the session. A stolen or leaked header can be replayed indefinitely. Recommend a signed, time-limited impersonation token (see `AUTH_AUDIT.md`).

✅ **PASS — Auth rate limiting in place** (10 req / 15 min on login + reset-password)

✅ **PASS — Password reset anti-enumeration** (always returns `ok: true`)

✅ **PASS — Timing-safe CRON secret comparison** (`server.ts:571`)

✅ **PASS — CORS configured with explicit allowlist** (not `*`)

---

## 8. Sensitive Data Storage

✅ **PASS — Passwords never stored in application DB** (Supabase Auth / GoTrue handles password hashing)

✅ **PASS — Access/refresh tokens never persisted to application DB**

✅ **PASS — File SHA-256 hashes stored, not file contents** in calls table

✅ **PASS — Supabase Storage handles file storage** (not server filesystem)

⚠️ **WARNING — `reps.email` column stores email in application DB**  
Added in sprint-3. Supabase Auth is the authoritative email store. The application DB copy could diverge and be used in ways auth.users cannot. Acceptable for search/display; should not be used for authentication decisions.

⚠️ **WARNING — PII in call transcripts stored without field-level encryption**  
`calls.transcript` (and related scoring tables) stores conversation transcripts which may contain sensitive sales data, customer names, pricing. No encryption at rest beyond what Supabase/Postgres provides. Acceptable at MVP scale; note for compliance review if handling regulated industries.

---

## Summary Table

| Area | Status | Priority |
|---|---|---|
| Manager-only routes | ⚠️ | Medium |
| Org/company isolation | ❌ | **High** |
| CRM ownership | ⚠️ | Medium |
| Service-role usage | ❌ | **High** |
| Env variable exposure | ⚠️ | **High** |
| DB schema permissions / RLS | ⚠️ | Medium |
| Session / auth security | ⚠️ | **High** |
| Sensitive data storage | ⚠️ | Low |

## Top 5 Actions

1. **Verify production env vars** — confirm `DEV_TEST_UID`, `ALLOW_ORG_BYPASS`, `ALLOW_ADMIN_ENDPOINTS`, `ALLOW_DEV_UID_QS` are all absent in Railway/Vercel production
2. **Fix missing org header** — `requireManagerOrg` should return 400 when `x-org-id` is absent rather than bypassing
3. **Verify JWT signatures** — replace `tryDecodeJwtSub` with `supabase.auth.getUser(token)` in server.ts
4. **Add `org_id` to CRM update queries** — belt-and-suspenders ownership check on every write
5. **Plan RLS migration** — see `RLS_ROADMAP.md`
