# Architecture Decision Record — Identity Architecture

**ADR-001**  
**Date:** 2026-06-09  
**Status:** Accepted — implementation in progress  
**Deciders:** George Burgess

---

## Context

The Gravix Sales Trainer API was built initially with a `users` + `profiles` dual-table pattern common in early Supabase projects. As the product grew to support multi-tenant org structures (companies, partners, licence pools), a richer identity model was needed: one that carries tier/role, company membership, partner membership, and profile data in a single queryable table.

Three concurrent pressures forced the decision:
1. **Middleware fragmentation** — role checks read `reps`; user creation wrote `users`; name hydration read `profiles`. Three tables for one user.
2. **Hierarchy growth** — adding PartnerAdmin and SuperAdmin tiers to `users.role` would require schema changes to a legacy table shared with the old org-scoped model.
3. **Security** — the `org_id` bypass pattern in CRM routes exists partly because `org_id` is not directly tied to `reps.company_id`. Unifying on `company_id` removes a class of scoping errors.

---

## Decision

**Use `reps` as the single application identity table.** `reps.id = auth.users.id` (shared UUID). `users` and `profiles` are transitioned to archive status via a phased migration. No new code writes to or reads from `users` or `profiles`.

The hierarchy is:

```
auth.users (Supabase GoTrue)
    └── reps (application identity; id = auth.users.id)
             └── companies (via reps.company_id)
                      └── partners (via companies.partner_id)
```

---

## Rationale

### Why `reps` and not a new `users` v2?

The `reps` table was already the de-facto identity table for all middleware by the time this decision was formalised. Every `requireManager`, `requirePartnerAdmin`, `requireSuperAdmin` call reads `reps`. Creating a parallel table would have introduced a fourth identity surface rather than consolidating to one.

The name `reps` does not perfectly describe all tiers — a PartnerAdmin is not a "rep" in the sales sense — but renaming the table at this stage would require touching every file in the codebase. The table name is internal; the `tier` field carries the semantic distinction.

### Why not use `auth.users` directly?

Supabase's `auth` schema is managed by GoTrue. Adding application columns to `auth.users` is unsupported and would break on Supabase upgrades. The `auth.users.id` UUID is shared with `reps.id` — this is the standard Supabase pattern.

### Why keep `org_id` during the transition?

The `org_id` field is present in ~5 CRM tables and is the current CRM scoping mechanism. Cutting it over in a single migration would require: (a) verifying UUID equivalence between `org_id` values and `company_id` values, (b) updating all crm.ts filters, (c) updating the frontend to send `company_id` as the `x-org-id` header. Breaking this into Phase 2 decouples the identity migration from the CRM scoping migration.

### Why not enforce `reps.id → auth.users.id` with a FK?

Supabase does not support cross-schema foreign keys from `public.reps` to `auth.users` via standard SQL. The relationship is enforced by convention (the auth trigger creates the `reps` row using `auth.users.id`) and by the middleware (which validates `reps.id` exists before proceeding).

---

## Tradeoffs

### Accepted tradeoffs

| Tradeoff | Accepted because |
|---|---|
| `reps` table name does not describe all tiers | Rename cost exceeds benefit at current scale |
| `reps.email` is denormalised from `auth.users.email` | Avoids a join on every auth check; write-through sync is a Phase 2 task |
| `users` and `profiles` tables persist during transition | Archiving before migration is complete risks data loss; rename-to-archive is reversible |
| `x-org-id` header semantics change in Phase 2 | Frontend must be updated simultaneously; coordinated deploy required |
| No FK `reps → auth.users` | Supabase cross-schema FK limitation; mitigated by signup trigger |

### Rejected alternatives

**Alternative 1: Extend `users` table with tier system**
Rejected — `users` uses an old role system (`rep`/`manager`) incompatible with the six-tier model. Extending it would require a schema change to a table with production data and no migration safety net.

**Alternative 2: New `identities` table, keep `users` + `reps` as joins**
Rejected — adds a third join surface. The existing `reps` table was already carrying 90% of the needed fields after the Sprint 3 profile field additions.

**Alternative 3: Single-phase hard cut-over**
Rejected — requires simultaneous migrations across 12+ files and 3 tables. The risk of a missed reference causing a production 500 is too high. Phased migration with the shadow-write pattern is safer.

---

## Consequences

### Positive
- All identity queries go to one table (`reps`) — no fallback chains
- Tier-based access control is uniform across all middleware
- Company + partner hierarchy is navigable from a single `reps` row: `reps → company → partner`
- Fewer DB round-trips per request (no `profiles` lookup for display name)
- RLS policies (Phase 2 of security roadmap) only need to cover one table

### Negative / risks
- Migration window: during Phase 1–2 transition, two writes exist for user creation (shadow-write). If the `reps` insert fails silently, the user exists in `users` but not in `reps` and cannot pass middleware checks.
- `x-org-id` header value semantics change in Phase 2 requires coordinated frontend + backend deploy.
- Denormalised `reps.email` can drift from `auth.users.email` if the email is changed via Supabase Auth but the trigger/webhook is not set up to sync the change.

---

## Implementation status

| Component | Status |
|---|---|
| `reps` table profile fields | ✅ Done (`20260605d`) |
| `reps.company_id` populated | ✅ Done (`20260604`) |
| `companies.partner_id` | ✅ Done (`20260605`) |
| All middleware reads `reps` | ✅ Done |
| `GET/PATCH /v1/users/me` reads `reps` | ✅ Done |
| `POST /v1/admin/users` shadow-write to `reps` | ⬜ Phase 1 |
| Name hydration off `profiles` | ⬜ Phase 2 |
| Admin user list off `users` | ⬜ Phase 2 |
| CRM org_id → company_id | 🟡 Migration written (`20260723`), API company-aware; SQL apply + backfill pending |
| `users` / `profiles` archived | ⬜ Phase 3 |

---

## Related documents

- [REPS_FIRST_ARCHITECTURE.md](REPS_FIRST_ARCHITECTURE.md) — target model detail
- [USERS_COMPATIBILITY_LAYER.md](USERS_COMPATIBILITY_LAYER.md) — compatibility strategy + deprecation
- [FK_MIGRATION_PLAN.md](FK_MIGRATION_PLAN.md) — FK changes, order, rollback
- [OWNERSHIP_MIGRATION_PLAN.md](OWNERSHIP_MIGRATION_PLAN.md) — org_id / company_id / partner_id
- [IDENTITY_ROADMAP.md](IDENTITY_ROADMAP.md) — Phase 1 / 2 / 3 task lists

---

## ADR — CRM account tenant scoping (Day 248)

**Decision:** CRM accounts (`crm_accounts`) are scoped by **company_id**, the tenant
boundary. Managers see only their company's accounts. `org_id` is kept for legacy /
backwards compatibility and is still written, but is no longer the isolation key.

**Why:** the table originally had a non-existent `user_id` ownership model (fixed
Day 247 by falling back to `org_id`). But both demo companies share one org, so
`org_id` cannot isolate companies within an org. `company_id` is the real boundary
(the same one `reps` already carries).

**Not user-owned:** accounts have no owner column. Reps reach accounts through
contacts, calls, activities and opportunities — not ownership.

**Migration:** `sql/20260723_crm_accounts_company_scope.sql` adds `company_id`
(nullable), an index, and partial unique constraints on `(company_id, lower(name))`
and `(company_id, lower(domain))`. Nullable and **no SQL backfill**: there is no
deterministic path from an existing account to exactly one company (only `org_id`,
which spans two companies), so guessing would invent ownership. Existing rows stay
`company_id = NULL` and are excluded from company-scoped reads until repaired. The
demo rows are repaired by re-running `seed:demo` (now stamps `company_id =
DEMO_COMPANY_ID`). New rows always carry `company_id`.

**API:** `src/routes/crm.ts` resolves the requester's company from rep context and
feature-detects the column, so the code is correct **before** the migration
(org-scoped fallback — creation never breaks) and **after** (company-scoped) without
a redeploy. `company_id` is never derived from a client header — only from
server-side identity.

**Verify:** `npm run validate:crm-account-ownership` — proves org isolation in
migration-pending mode, and company isolation (two companies in one org) once the
SQL is applied.
