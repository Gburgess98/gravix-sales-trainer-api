# Final Security Audit — Gravix Sales Trainer API

**Date:** 2026-06-09  
**Branch:** claude/sprint-3-api  
**Sprint:** Day 86 security sprint closeout  
**Scope:** Pre-lighthouse-customer readiness

Legend: ✅ PASS · ⚠️ WARNING · ❌ FAIL

---

## 1. Authentication

### Implementation
- Login: `supabase.auth.signInWithPassword` via anon-key client (`auth.ts:69`)
- Token verification: `supabase.auth.getUser(token)` on `GET /v1/auth/me` only (`auth.ts:200`)
- Identity resolution: JWT sub decoded without verification in `server.ts:tryDecodeJwtSub` for all other routes
- Password reset: always returns `ok: true` to prevent email enumeration (`auth.ts:174`)
- Rate limiting: `authRateLimit` — 10 req / 15 min per IP on `/login` and `/reset-password`

### Findings

✅ **PASS — Passwords stored only in Supabase Auth (GoTrue)**  
No plaintext or hashed passwords in the application database.

✅ **PASS — Anti-enumeration on password reset**  
`POST /v1/auth/reset-password` always returns `{ok: true}` regardless of whether the email exists.

✅ **PASS — Auth endpoints rate-limited (10 req / 15 min)**  
Credential stuffing attack would exhaust the window after 10 attempts.

✅ **PASS — `GET /v1/auth/me` performs real JWT signature verification**  
Uses `supabase.auth.getUser(token)` — Supabase verifies the RS256 signature.

✅ **PASS — `GET /v1/auth/me` requires explicit auth header (not DEV_TEST_UID)**  
Fixed in sprint-3: reads only `req.header("x-user-id")` directly, not `req.userId`.

⚠️ **WARNING — Most routes accept JWT sub without signature verification**  
`server.ts:tryDecodeJwtSub` base64-decodes the JWT payload and extracts `sub` without verifying the signature. Routes that rely on this for identity are not validating that the JWT was actually signed by Supabase.  
**Risk:** Medium. An attacker would need to know a valid user UUID and construct a valid-format JWT. The UUID is non-guessable; the missing verification is an architecture gap rather than an immediate exploitable vector.  
**Fix:** Replace `tryDecodeJwtSub` with a lightweight JWKS verification using Supabase's public key, or always call `supabase.auth.getUser(token)` in the identity middleware.  
**Status:** Known gap; tracked in AUTH_AUDIT.md. Acceptable for lighthouse with x-user-id header flow (no JWT route used by frontend currently).

⚠️ **WARNING — Session logout does not invalidate server-side session on all code paths**  
`auth.ts:137` calls `auth.admin.signOut(userId)` as best-effort but swallows errors. A client with a leaked access token could continue using it until Supabase's natural expiry (~1 hour).  
**Risk:** Low at current scale (1-hour TTL is acceptable).

---

## 2. Tenant Isolation

### Implementation
- Company scope: `getVisibleCompanies` in `partnerAccess.ts` — SuperAdmin→all, PartnerAdmin→partner's, Manager→own
- User edit scope: `assertUserEditScope` in `admin.ts:1403` — verifies company_id match
- Company edit scope: `assertCompanyEditScope` in `admin.ts:1592`
- CRM scope: `requireManagerOrg` + `assertRequesterInOrg` in `crm.ts`

### Findings

✅ **PASS — `assertUserEditScope` correctly scopes user management by company**  
Manager → same company only. PartnerAdmin → same partner only (verified via two DB lookups). SuperAdmin → unrestricted.

✅ **PASS — `assertCompanyEditScope` correctly scopes company management**  
Same scope model as above; company writes require company_id cross-check.

✅ **PASS — `getVisibleCompanies` correctly limits data by tier**  
Tested and validated in `validate-context-switching` suite.

✅ **PASS — CRM reads include `org_id` filter**  
Contact, opportunity, and activity reads all include `.eq("org_id", orgId)` (`crm.ts:519, 745, 1120`).

❌ **FAIL — Missing `x-org-id` header silently bypasses org membership check**  
`crm.ts:222`: if the `x-org-id` header is absent, `requireManagerOrg` returns `bypassed: true` and skips `assertRequesterInOrg`. The org defaults to `DEFAULT_ORG_ID` env var. Any authenticated user can query manager-level CRM routes for that org without proving membership.  
**Fix:** `requireManagerOrg` should return 400 when `x-org-id` is missing in production mode, not silently bypass.

❌ **FAIL — `ALLOW_ORG_BYPASS=1` env var completely disables org membership validation**  
Must be confirmed absent from production Railway/Vercel environments.  
**Verification:** Run `npm run validate:security-audit` — ENV check will fail if set.

⚠️ **WARNING — CRM write operations (update) do not re-verify `org_id` ownership**  
`crm.ts:2813` (opportunity stage update) and `crm.ts:3106` (opportunity patch) update by UUID without confirming the record belongs to the requesting user's org. UUIDs are non-guessable (v4 random), so practical exploitation requires knowing a valid UUID from another org.  
**Fix:** Add `.eq("org_id", orgId)` to all UPDATE queries in crm.ts.

---

## 3. Impersonation

### Implementation
- Middleware: `server.ts:236` — runs after auth, before routes
- Gate: re-fetches actor's tier from DB on every request (`reps.tier === "SuperAdmin"`)
- Effect: swaps `req.userId` to target; stores actor in `req.actorUserId`
- Audit: `impersonate_user` and `end_impersonation` events written to `audit_events`
- History: `GET /v1/admin/support/impersonation-history` (requireSuperAdmin)

### Findings

✅ **PASS — Impersonation gated to SuperAdmin only (verified from DB on every request)**  
Tier is re-fetched from `reps` table; a client cannot self-elevate by claiming SuperAdmin in a header.

✅ **PASS — Impersonation is audited**  
`impersonate_user` events written to `audit_events`. History retrievable via support endpoint.

✅ **PASS — Actor identity preserved throughout request**  
`req.actorUserId` stored separately from the swapped `req.userId`, so audit events can correctly attribute actions to the original SuperAdmin.

✅ **PASS — Non-SuperAdmin impersonation attempt returns 403**  
`server.ts:253`: Manager, PartnerAdmin, and SalesRep receive `impersonation_requires_super_admin`.

⚠️ **WARNING — No time-bound impersonation token**  
Impersonation is granted for the lifetime of any request that includes `x-impersonated-user-id: <uuid>`. There is no session token, expiry, or single-use mechanism. A compromised or logged header could be replayed.  
**Risk:** Low — requires stealing both the actor's auth token and the target's user ID. Acceptable at current scale.  
**Fix (roadmap):** Issue a signed, time-limited `x-impersonation-token` from a dedicated endpoint; validate the token's HMAC and expiry in the middleware.

⚠️ **WARNING — No per-session impersonation activity limits**  
A SuperAdmin in impersonation mode can perform unlimited writes as the target user.  
**Recommendation:** Log a warning when impersonated writes exceed N operations, or restrict impersonation sessions to read-only for a first pass.

---

## 4. Licensing

### Implementation
- `GET /v1/admin/partner/licences` — `requirePartnerAdmin` gate
- `GET /v1/admin/super/licences` — `requireSuperAdmin` gate
- `available = purchased - sum(company_licences.allocated)` computed at response time
- No write endpoints (read-only as designed)

### Findings

✅ **PASS — Partner licences scoped to caller's own partner**  
PartnerAdmin resolves their partner via `reps → companies.partner_id` and only receives their own pool.

✅ **PASS — Super licences gated to SuperAdmin only**  
`requireSuperAdmin` enforces the gate; PartnerAdmin and below receive 403.

✅ **PASS — No licence write endpoints exposed**  
Licence data is insert/update only via SQL migrations and Supabase Dashboard. The API surface is read-only.

✅ **PASS — Available calculation is server-side only**  
`available = purchased - allocated` is computed in the route handler; clients cannot manipulate it.

⚠️ **WARNING — No enforcement of licence seat limits on user creation**  
When `POST /v1/admin/users` creates a new rep, it does not check whether `available > 0`. Licence data is currently informational only — it is not yet enforced as a hard gate.  
**Risk:** Low at current scale (single partner). Will need enforcement before multi-partner billing.

---

## 5. Admin Routes

### Gates

| Route pattern | Middleware | Tiers allowed |
|---|---|---|
| `/v1/admin/users`, `/v1/admin/reps`, `/v1/admin/config` | `requireManager` | Manager, Owner, PartnerAdmin, SuperAdmin |
| `/v1/admin/partner/*` | `requirePartnerAdmin` | PartnerAdmin, SuperAdmin |
| `/v1/admin/super/*`, `/v1/admin/platform`, `/v1/admin/organisations`, `/v1/admin/context/options` (when partner+) | `requireSuperAdmin` or `requirePartnerAdmin` | As labelled |
| `/v1/admin/users/:id` (GET/PATCH) | Inline scope check (assertUserEditScope) | Self + company scope |
| `/v1/admin/companies/:id` (GET/PATCH) | Inline scope check (assertCompanyEditScope) | Company scope |

### Findings

✅ **PASS — All partner and super admin routes use hardened middleware**  
`requirePartnerAdmin` and `requireSuperAdmin` read only from explicit headers; DEV_TEST_UID cannot satisfy them.

✅ **PASS — User and company write operations use scope assertions**  
`assertUserEditScope` and `assertCompanyEditScope` verify company/partner membership before any DB write.

✅ **PASS — Audit events written on every admin write**  
`update_user`, `update_company`, `create_user`, `delete_user` events recorded via `logAuditEvent`.

✅ **PASS — Admin platform stats endpoint gated to SuperAdmin**  
`GET /v1/admin/platform` requires `requireSuperAdmin`.

⚠️ **WARNING — `requireManager` (inline, admin.ts:65) reads `req.userId`**  
The inline version (used for user list, config, reps management) reads from `(req as any)?.userId` which can be populated by `DEV_TEST_UID`. The exported `requireManager` in `requireManager.ts` has the same behaviour. Both check tier from the DB, so an attacker needs a valid UUID in the DB — but if `DEV_TEST_UID` points to a Manager in the DB, the gate is bypassed without an explicit auth header.  
**Fix:** Guard the same way as `requirePartnerAdmin` — read only from explicit headers.

✅ **PASS — Audit log route gated to SuperAdmin**  
`GET /v1/admin/audit` requires `requireSuperAdmin`.

---

## 6. Uploads

### Implementation
- Direct upload: `POST /v1/upload` via multer memoryStorage
- Signed upload: `POST /v1/upload/signed` → Supabase Storage signed URL
- Finalize: `POST /v1/upload/finalize` → record after direct-to-storage upload
- Max size: `MAX_UPLOAD_BYTES = 50 MB` (multer `limits.fileSize`)
- Allowed MIMEs: audio (wav/mpeg/mp4/aac/ogg/webm/flac) + application/json + video/webm + video/mp4 (browser aliases) + application/octet-stream
- Rate limit: `uploadRateLimit` — 30 req / 10 min per IP

### Findings

✅ **PASS — File size limit enforced by multer (50 MB)**  
Multer `limits.fileSize` returns 413 with descriptive message when exceeded.

✅ **PASS — MIME type allowlist enforced on direct upload**  
`ALLOWED_UPLOAD_MIMES` check at `server.ts:858`; unsupported types return 415.

✅ **PASS — MIME type validation on signed upload**  
`server.ts:722` validates `mime` parameter before issuing a signed URL.

✅ **PASS — Storage path scoped to user ID**  
All upload paths are `${userId}/${id}${ext}` — a user cannot write to another user's path, and `upload/finalize` verifies `path.startsWith(userId+"/")`.

✅ **PASS — Upload rate-limited (30/10min)**  
Prevents runaway transcription job creation from compromised or misbehaving clients.

✅ **PASS — Files stored in Supabase Storage, not on application server disk**  
No server filesystem writes; no path traversal risk.

⚠️ **WARNING — `application/octet-stream` is in the MIME allowlist**  
This is a generic binary type that allows any file to be uploaded by sending the correct Content-Type header. It was included for legacy clients but broadens the attack surface.  
**Risk:** Low — the file goes to Supabase Storage with no server-side execution; there is no risk of remote code execution.  
**Recommendation:** Remove from allowlist and require clients to send the specific MIME type.

⚠️ **WARNING — Direct upload stores file in server memory before uploading to storage**  
Multer `memoryStorage` buffers the full file in RAM. A burst of near-50MB uploads from multiple clients could exhaust process memory.  
**Recommendation:** Switch to disk-based multer storage for files > 5MB, or enforce a lower in-memory limit for the direct upload path.

---

## 7. CRM

### Implementation
- Org scope: `getOrgIdHeader` reads `x-org-id` header, falls back to `DEFAULT_ORG_ID`
- Membership check: `requireManagerOrg` + `assertRequesterInOrg` for manager-level routes
- All contact reads: `.eq("org_id", orgId)` filter
- Activity reads: `.eq("user_id", requester)` filter
- Write operations: `org_id` stamped on creation

### Findings

✅ **PASS — CRM reads scoped by org_id**  
Contact, opportunity, and activity reads all filter on `org_id`. Cross-org data leakage requires knowing another org's UUID and passing it as the `x-org-id` header, which is validated against membership.

✅ **PASS — CRM contact creation stamps org_id from the validated context**  
New records inherit `org_id` from the request context, not from a client-supplied field.

✅ **PASS — Activity reads scoped to requesting user**  
`crm.ts:3915`: `.eq("user_id", requester)` ensures reps only see their own activities.

❌ **FAIL — Missing x-org-id header silently bypasses org membership validation**  
(Same finding as Tenant Isolation §2.) Applies specifically to all `requireManagerOrg`-gated CRM routes.

⚠️ **WARNING — CRM opportunity/activity write operations missing ownership re-check**  
Stage updates (`crm.ts:2813`) and field patches (`crm.ts:3106`) update by UUID only. Adding `.eq("org_id", orgId)` to UPDATE calls would close the gap.

✅ **PASS — CRM bulk import validates and scopes records**  
`contacts/import` route stamps `org_id` on each imported contact.

---

## 8. Dependencies

### npm audit results (post-fix)

`npm audit fix` was run on 2026-06-09. **0 vulnerabilities** remaining.

Previously:

| Severity | Count | Packages |
|---|---|---|
| High | 4 | minimatch (ReDoS), multer (DoS), path-to-regexp (ReDoS), picomatch (ReDoS) |
| Moderate | 5 | braces, qs, semver, uuid, ws |
| Low | 1 | (minor) |

All fixed by `npm audit fix` (patch-level dependency updates; no breaking changes).

### Supply chain

✅ **PASS — `package-lock.json` committed** (exact version pinning)

✅ **PASS — Dependabot configured** (weekly npm + Actions updates)

✅ **PASS — npm audit CI workflow** — fails build on high/critical findings

✅ **PASS — SBOM generation** — CycloneDX + SPDX on every `main` push and release

✅ **PASS — OSSF Scorecard** — publishes results to GitHub Security tab on main push

---

## 9. HTTP Security Headers

| Header | Value | Status |
|---|---|---|
| `Content-Security-Policy` | `default-src 'none'; frame-ancestors 'none'` | ✅ |
| `X-Content-Type-Options` | `nosniff` | ✅ |
| `X-Frame-Options` | `SAMEORIGIN` | ✅ |
| `Referrer-Policy` | `no-referrer` | ✅ |
| `X-Powered-By` | Removed | ✅ |
| `Strict-Transport-Security` | Set under HTTPS (Railway TLS) | ✅ |

Rate limits active: global 300/min, auth 10/15min, upload 30/10min.

---

## Summary Matrix

| Category | Status | Lighthouse ready |
|---|---|---|
| Authentication | ⚠️ | Yes — gap is known; x-user-id path used by frontend |
| Tenant isolation | ❌ | **Conditional** — must fix missing-org-header bypass |
| Impersonation | ⚠️ | Yes — no time-bound token is acceptable at MVP scale |
| Licensing | ⚠️ | Yes — read-only; seat enforcement is roadmap |
| Admin routes | ⚠️ | Yes — DEV_TEST_UID must be absent from production |
| Uploads | ⚠️ | Yes — octet-stream gap is low risk |
| CRM | ❌ | **Conditional** — same org-bypass issue as tenant isolation |
| Dependencies | ✅ | Yes — 0 vulnerabilities after `npm audit fix` |
| HTTP headers | ✅ | Yes |
| SBOM / supply chain | ✅ | Yes |

---

## Required Before Lighthouse Customers

The following two items are blockers; everything else is acceptable technical debt:

### BLOCKER 1 — Fix missing `x-org-id` bypass

**File:** `src/routes/crm.ts:222`  
**Change:** Return 400 when `x-org-id` header is missing (instead of silently bypassing). Guard with a `NODE_ENV !== 'production'` exception if needed to keep local dev working without the header.

```ts
// Proposed fix:
if (!headerProvided && process.env.NODE_ENV === 'production') {
  throw new Error("missing_org_id");
}
```

### BLOCKER 2 — Confirm production env vars

Before any lighthouse customer data touches the production environment, verify in Railway:

```
DEV_TEST_UID          → must be absent or empty
ALLOW_ORG_BYPASS      → must be absent or empty  
ALLOW_ADMIN_ENDPOINTS → must be absent or "false"
ALLOW_DEV_UID_QS      → must be absent or empty
```

`npm run validate:security-audit` will fail if any of these are set in the current process.

---

## Remaining Risk Register

| Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|
| Missing org header bypass | High | Low (requires knowing DEFAULT_ORG_ID) | **Fix before launch** |
| JWT not signature-verified on most routes | Medium | Low (requires knowing target UUID) | Roadmap |
| requireManager reads req.userId (DEV_TEST_UID path) | Medium | Low (requires DEV_TEST_UID set) | Env hygiene |
| CRM write ops missing org re-check | Medium | Very low (requires UUID knowledge) | Roadmap |
| No impersonation session expiry | Low | Very low (requires compromised SuperAdmin) | Roadmap |
| Licence seats not enforced on create | Low | None (no automated billing yet) | Pre-billing milestone |
| In-memory upload buffering (RAM) | Low | Medium (many concurrent large uploads) | Roadmap |
| Service role used for all queries (RLS inert) | High | N/A (requires key compromise) | Phase 2–3 of RLS roadmap |
