# Incident Response — Gravix Sales Trainer API

**Owner:** Engineering  
**Last updated:** 2026-06-09

---

## Severity Levels

| Level | Definition | Response time |
|---|---|---|
| P0 — Critical | Data breach, auth bypass in production, service-role key exposed | Immediate (< 30 min) |
| P1 — High | Auth degraded, data leak risk, rate limit bypass | < 2 hours |
| P2 — Medium | Single-tenant isolation failure, abnormal auth patterns | < 24 hours |
| P3 — Low | Security misconfiguration, informational finding | Next sprint |

---

## P0 Playbooks

### Playbook: Service-Role Key Compromised

**Indicators:** Unexpected reads/writes in Supabase logs; alerts from Supabase or Railway; key found in a public repo or log.

1. **Rotate key immediately** — Supabase Dashboard → Settings → API → Regenerate service_role key
2. **Redeploy** — update `SUPABASE_SERVICE_ROLE_KEY` in Railway/Vercel and trigger redeploy
3. **Audit** — Supabase Logs → search for requests made with the old key in the last 24 h
4. **Check for data exfiltration** — look for large SELECT * queries or unexpected table access
5. **Rotate `SUPABASE_ANON_KEY`** too if the same key was used
6. **Notify** — inform relevant stakeholders; if customer data was accessed, prepare breach notification per legal requirements
7. **Post-mortem** — identify how the key leaked (git history, logs, env file committed)

### Playbook: Authentication Bypass Discovered

**Indicators:** Unauthenticated requests succeeding, `DEV_TEST_UID` found in prod env, `ALLOW_ORG_BYPASS=1` in prod.

1. **Check production env vars immediately**
   - Railway: Dashboard → Service → Variables
   - Remove: `DEV_TEST_UID`, `ALLOW_ORG_BYPASS`, `ALLOW_ADMIN_ENDPOINTS`, `ALLOW_DEV_UID_QS`
2. **Redeploy** to pick up the removed vars
3. **Audit audit_events** for actions taken during the bypass window:
   ```sql
   SELECT * FROM audit_events
   WHERE created_at > '<incident_start>'
   ORDER BY created_at DESC;
   ```
4. **Check admin operations** — look for unexpected user tier changes, company modifications
5. **Assess scope** — determine which users and orgs were potentially visible
6. **Invalidate all active sessions** — Supabase Dashboard → Authentication → Users → bulk sign out if needed

### Playbook: Data Breach / Tenant Isolation Failure

**Indicators:** User reports seeing another company's data; org_id filter confirmed bypassed.

1. **Identify scope** — which orgs/users were affected, what data was visible
2. **Patch the route** immediately — add `.eq("org_id", orgId)` or `.eq("company_id", companyId)` to the leaking query
3. **Deploy hotfix** — push to production branch, trigger Railway redeploy
4. **Notify affected users** — per privacy obligations
5. **Review all write operations** in the affected window for cross-tenant writes

---

## P1 Playbooks

### Playbook: Abnormal Authentication Volume

**Indicators:** `auth_rate_limit_exceeded` errors spiking in logs; many 401s from the same IP.

1. **Check logs** — Railway Logs → filter for `[vv1/auth/login] 401`
2. **Identify IP** — look for `x-forwarded-for` or source IP in Railway logs
3. **Block at infrastructure level** if attack is ongoing — Railway IP allowlisting or Cloudflare WAF rule
4. **Check if any accounts were compromised** — look for successful logins from the same IP range in Supabase Auth logs
5. **Reset passwords** for any accounts showing login from unusual IPs

### Playbook: Unusual API Traffic

**Indicators:** `rate_limit_exceeded` in logs; bandwidth spike; abnormal pattern in Railway metrics.

1. **Identify the endpoint and origin** from Railway logs
2. **Check if legitimate** — is this a frontend deploy, a new cron, a mobile release?
3. **Temporary rate limit tighten** — update `MAX` in `src/middleware/rateLimits.ts` and redeploy
4. **Long-term** — consider Redis-backed rate limiting for persistence across deploys

---

## Communication Template

```
Subject: [GRAVIX SECURITY] P{level} Incident — {brief description}

Severity: P{level}
Detected: {timestamp}
Status: Investigating / Contained / Resolved

Summary:
{What was found}

Impact:
{What data/users/systems were affected}

Actions taken:
- {action 1}
- {action 2}

Next steps:
- {step 1}
```

---

## Key Contacts

| Role | Contact |
|---|---|
| Engineering lead | george.burgessx@gmail.com |
| Supabase support | support@supabase.io |
| Railway support | support@railway.app |

---

## Post-Incident Checklist

- [ ] Timeline documented
- [ ] Root cause identified
- [ ] Fix deployed and verified
- [ ] Affected users/orgs notified (if applicable)
- [ ] `SECURITY_AUDIT.md` updated with new finding
- [ ] `SECURITY_CHECKLIST.md` updated with new check
- [ ] Regression test added to `scripts/validate-security.ts`
- [ ] Post-mortem written and added to engineering docs
