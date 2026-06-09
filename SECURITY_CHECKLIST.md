# Security Checklist — Gravix Sales Trainer API

Run this before every production deployment.

---

## Pre-Deployment Checklist

### Environment Variables (CRITICAL)

- [ ] `DEV_TEST_UID` is **absent or empty** in Railway/Vercel production
- [ ] `ALLOW_ORG_BYPASS` is **absent or empty** in production
- [ ] `ALLOW_ADMIN_ENDPOINTS` is **absent or "false"** in production
- [ ] `ALLOW_DEV_UID_QS` is **absent or empty** in production
- [ ] `SUPABASE_SERVICE_ROLE_KEY` is different from staging key
- [ ] `.env` file is not committed (`git status` shows no `.env*` files)

### Code Review

- [ ] No `console.log` with tokens, passwords, or full request bodies
- [ ] No hardcoded secrets, API keys, or credentials in new code
- [ ] New routes requiring auth have a middleware gate (`requireManager`, `requirePartnerAdmin`, or `requireSuperAdmin`)
- [ ] New DB write operations include `org_id` or `company_id` filter
- [ ] New CRM write operations include `.eq("org_id", orgId)` on updates

### Dependencies

- [ ] `npm audit --audit-level=high` returns no high/critical findings
- [ ] New packages reviewed for known CVEs before adding
- [ ] `package-lock.json` committed alongside `package.json`

### Upload Endpoints

- [ ] New upload endpoints use `ALLOWED_UPLOAD_MIMES` allowlist
- [ ] Multer `limits.fileSize` is set to `MAX_UPLOAD_BYTES`
- [ ] `uploadRateLimit` applied to new upload routes

### Auth

- [ ] New auth-sensitive routes have `authRateLimit` applied
- [ ] Password reset always returns `ok: true` (no enumeration)
- [ ] No new JWT-decoding code without `supabase.auth.getUser(token)` verification

---

## Post-Deployment Verification

```bash
npm run validate:security          # Headers, rate limits, upload gates
npm run validate:auth              # Auth flow end-to-end
npm run validate:permissions       # Tier-based access control
npm run validate:security-audit    # Env var flags, bypass checks
```

---

## Monthly Security Review

- [ ] Review Railway/Vercel environment variable list — remove any `DEV_*` or `ALLOW_*` flags
- [ ] Review Supabase Auth logs for unusual login patterns
- [ ] Review audit_events for unexpected admin actions
- [ ] Check `npm audit` output
- [ ] Confirm Supabase auto-backup is running (Settings → Backups)
- [ ] Rotate CRON_SECRET if it has been in use > 90 days
- [ ] Review impersonation history: `GET /v1/admin/support/impersonation-history`

---

## New Developer Onboarding Security Notes

1. **Never commit `.env`** — use `.env.example` as reference, fill values from the team password manager
2. **Local dev only flags** — `DEV_TEST_UID`, `ALLOW_ORG_BYPASS` are local convenience only; never ask for them in staging or prod
3. **Service role key** — treat it like a root password. Do not log it, share it in Slack, or put it in client-side code
4. **JWT claims are not trusted** — the API decodes JWT sub for convenience but routes must verify tier from the `reps` DB table on every request
5. **Rate limits are in-memory** — they reset on every redeploy. A restart during an attack resets the counter. This is acceptable at current scale; Redis-backed limits are on the roadmap

---

## Security Contacts

| Type | Contact |
|---|---|
| Security bug report | george.burgessx@gmail.com |
| Supabase incident | support@supabase.io |
| Railway incident | support@railway.app |

---

*See also: `SECURITY_AUDIT.md`, `RLS_ROADMAP.md`, `INCIDENT_RESPONSE.md`, `BACKUP_RECOVERY.md`*
