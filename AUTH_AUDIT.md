# AUTH_AUDIT.md — Gravix Sales Trainer API

**Audited:** 2026-06-09  
**Branch:** claude/sprint-3-api

---

## 1. Current Auth Flow

### Identity Resolution

Identity is resolved in `src/server.ts` via a global middleware that runs before all routes. Priority order:

```
1. x-user-id header (UUID validated)
2. Authorization: Bearer <JWT> → sub claim extracted
3. DEV_TEST_UID env var (local dev only)
```

The resolved identity is attached to `req.userId` and `res.locals.userId`. If a valid `x-user-id` header and a valid JWT sub are both present and differ, the request is rejected with `401 auth_mismatch`.

### Impersonation

A second global middleware handles impersonation. When `x-impersonated-user-id` is present and valid:
1. The real actor must be `SuperAdmin` (checked against `reps.tier`)
2. `req.userId` is swapped to the target user for the remainder of the request
3. `req.actorUserId` retains the original actor for audit purposes

### Role / Tier Model

Roles live in `reps.tier`. The tier check constraint (post-migration) is:

```
SalesRep | TeamLead | Manager | Owner | PartnerAdmin | SuperAdmin
```

Narrowest → broadest: `SalesRep < TeamLead < Manager < Owner < PartnerAdmin < SuperAdmin`

Helper predicates are in `src/lib/permissions.ts`:
- `isManagerOrAbove(tier)` → Manager, Owner, PartnerAdmin, SuperAdmin
- `isPartnerAdminOrAbove(tier)` → PartnerAdmin, SuperAdmin
- `isSuperAdmin(tier)` → SuperAdmin only

### Middleware Gates

| Middleware | File | Tiers Allowed |
|---|---|---|
| `requireManager` (inline, admin.ts) | `src/routes/admin.ts` | Manager, Owner, PartnerAdmin, SuperAdmin |
| `requirePartnerAdmin` | `src/middleware/requirePartnerAdmin.ts` | PartnerAdmin, SuperAdmin |
| `requireSuperAdmin` | `src/middleware/requireSuperAdmin.ts` | SuperAdmin only |

**Note:** `requireManager` is defined inline in `admin.ts` and creates its own Supabase client on every call. The middleware files use module-level singletons.

---

## 2. OAuth / Email-Password

### Current State

No dedicated OAuth or email/password handler exists in this API. Authentication is handled externally (Supabase Auth or the Next.js frontend) and the API receives an already-resolved user identity via:
- `x-user-id` header (set by the frontend after login)
- `Authorization: Bearer <JWT>` (Supabase JWT, sub = user UUID)

### Google OAuth

- **Status:** Assumed handled by Supabase Auth on the frontend
- **Gap:** The API does not verify the JWT signature. It only decodes the `sub` claim from the Bearer token without cryptographic verification. This is safe only because Supabase's service-role key is never exposed to clients — all DB calls use service role.
- **Risk:** If `x-user-id` can be forged by a client, any user can impersonate any other. The frontend must set this header only from the authenticated session.

### Email / Password

- **Status:** Not implemented in the API layer. Supabase Auth handles this.
- **Gap:** Same as OAuth — the API trusts the `x-user-id` header without independent verification.

---

## 3. Role Mapping After Login

There is no automatic role/tier mapping on login. The tier in `reps.tier` must be set manually (admin PATCH, seeder, or direct DB edit). There is no trigger that sets tier from auth metadata.

**Gap:** New users created via Supabase Auth have no `reps` row. A post-signup trigger or onboarding flow is needed to:
1. Create the `reps` row
2. Assign the correct tier
3. Link `company_id` and `org_id`

The SQL migration `20260603_fix_auth_user_trigger.sql` attempts to address part of this but the trigger scope is unknown without inspecting the live DB.

---

## 4. Tier-Specific Login Verification

| Role | Can log in | API access | Known gaps |
|---|---|---|---|
| SalesRep | Yes (via Supabase) | Own calls, profile, CRM | No reps row → 404 on /v1/users/me |
| Manager | Yes | Admin routes (manager-gated) | Same as above |
| PartnerAdmin | Yes | Partner-scoped admin routes | Requires `company_id` set + company has `partner_id` |
| SuperAdmin | Yes | All routes | Must have `reps` row with tier=SuperAdmin |

---

## 5. Missing Pieces

1. **JWT signature verification** — The API decodes but does not verify the Supabase JWT signature. Consider using `@supabase/supabase-js` `auth.getUser(token)` to verify before accepting the claim.

2. **Post-signup onboarding** — No mechanism to automatically create a `reps` row and assign tier when a new Supabase Auth user is created.

3. **Session management** — No concept of active sessions in the DB. The `active_sessions` stat on `/v1/admin/platform` returns `null`. If session tracking is needed, a `sessions` table with TTL or Supabase's built-in session management should be used.

4. **Rate limiting** — No rate limiting on auth-sensitive endpoints (login, impersonation). Recommend adding express-rate-limit for production.

5. **Impersonation token** — Current impersonation token IS the target user ID (MVP). A proper signed token with expiry should replace this before external access is enabled.

6. **Refresh tokens** — The API uses `autoRefreshToken: false, persistSession: false` on all Supabase clients. This is correct for a server-side service role client but means the API never refreshes user-facing tokens.

---

## 6. Recommended Lighthouse Setup

### Priority 1 — Fix Before External Traffic

1. Verify JWT with `supabase.auth.getUser(token)` instead of raw decode:
   ```ts
   const { data: { user }, error } = await supabase.auth.getUser(bearerToken);
   if (error || !user) return sendJsonError(res, 401, 'invalid_token');
   ```

2. Add post-signup DB trigger to create `reps` row:
   ```sql
   CREATE OR REPLACE FUNCTION public.handle_new_user()
   RETURNS TRIGGER AS $$
   BEGIN
     INSERT INTO public.reps (id, name, tier, org_id)
     VALUES (NEW.id, COALESCE(NEW.raw_user_meta_data->>'name', NEW.email), 'SalesRep', <default_org_id>)
     ON CONFLICT (id) DO NOTHING;
     RETURN NEW;
   END;
   $$ LANGUAGE plpgsql SECURITY DEFINER;
   
   CREATE TRIGGER on_auth_user_created
   AFTER INSERT ON auth.users
   FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();
   ```

### Priority 2 — Production Hardening

3. Add `express-rate-limit` to impersonation and any future login endpoints.
4. Replace the impersonation token with a signed JWT with 1-hour expiry.
5. Add `Content-Security-Policy` and `X-Frame-Options` headers via helmet.
6. Restrict `requireAdmin` guard (`ALLOW_ADMIN_ENDPOINTS`) to internal network only.

### Priority 3 — Observability

7. Log failed auth attempts to `audit_events` with action `auth_failed`.
8. Add `last_login_at` column to `reps` and update it on successful token verification.
9. Surface active session count via a `sessions` table or Supabase Auth admin API.

---

## 7. What Is Working

- Identity resolution from header + JWT with mismatch detection ✓
- Tier-based middleware gates (Manager, PartnerAdmin, SuperAdmin) ✓
- Impersonation with actor audit trail ✓
- Audit event logging on all write operations ✓
- Scope enforcement (Manager → own company, PartnerAdmin → partner companies, SuperAdmin → all) ✓
- CORS correctly configured for Vercel prod/staging + local dev ✓ (PATCH now included)
