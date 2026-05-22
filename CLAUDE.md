# Gravix Sales Trainer — API Context

Stack:
- Express
- TypeScript
- Supabase Postgres
- Org-scoped multi-tenant architecture

Rules:
- Do not invent DB fields
- Preserve existing route structure
- Small reversible patches only
- Avoid unnecessary abstractions
- Maintain org_id scoping
- Keep APIs backward compatible
- Reuse existing patterns

Important:
- Frontend communicates via /api/proxy
- Avoid large unrelated refactors
- Do not break existing routes
- Prefer additive changes

Current Priorities:
- Global app shell support
- CRM intelligence
- Realtime systems
- Prompt versioning
- Testing infrastructure
- Reliability hardening
- External integrations