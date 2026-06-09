# Security Policy

## Supported Versions

| Version | Status |
|---|---|
| `main` branch | ✅ Actively maintained |
| Feature branches | ⚠️ In development — not for production use |

## Reporting a Vulnerability

**Do not report security vulnerabilities in public GitHub issues.**

Email: **george.burgessx@gmail.com**

Include in your report:
- Description of the vulnerability
- Steps to reproduce
- Affected components (routes, middleware, etc.)
- Your assessment of severity and impact

### Response SLA

| Severity | Acknowledgement | Patch target |
|---|---|---|
| Critical (P0) | 24 hours | 48 hours |
| High (P1) | 48 hours | 7 days |
| Medium (P2) | 5 days | 30 days |
| Low (P3) | 10 days | Next scheduled release |

### What to expect

1. You'll receive acknowledgement within the above SLA
2. We'll investigate and keep you updated on progress
3. When a fix is deployed, we'll notify you and credit your report (if you wish)
4. We do not currently offer a bug bounty program

## Out of scope

- Social engineering attacks
- Physical access attacks
- Volumetric DoS (use the rate limiting contact path instead)
- Issues in third-party services (Supabase, Railway, Vercel) — report those to the vendor
