# Database schema — baseline + migrations

## Authoritative baseline

`00000000_base_schema.sql` is the **authoritative, consolidated schema baseline**.
It is a schema-only definition (DDL: tables, functions/RPCs, RLS, extensions) with
**no exported data rows** — the only `INSERT` statements it contains live inside
`plpgsql` function bodies (trigger logic), never as top-level seed data, and it
contains no `COPY ... FROM stdin` blocks.

Applied once against an empty database it produces the full 71-table schema
(verified against staging: 71/71 tables, `match_knowledge_embeddings`/pgvector
present, `npm run validate:schema-selects` passes).

## Historical migrations are ABSORBED — do not replay after the baseline

The 28 historical timestamped migrations in this directory
(`20251106_*` … `20260723_crm_accounts_company_scope.sql`) are **already absorbed
into `00000000_base_schema.sql`**. They are retained only for history/provenance.

> **Do NOT replay the historical timestamped migrations after applying the
> baseline.** The baseline is the single source of truth. Re-running the old
> migrations on top of it would attempt duplicate DDL (`already exists` errors)
> and re-run their inline reference `INSERT`s. Apply **only** the baseline to
> provision a new environment; add any *future* schema change as a **new**
> timestamped migration applied *after* the baseline.

## Synthetic staging seed

`seed/staging-day272-fixtures.sql` — **staging-only**, synthetic Day-272 fixtures
for the Scoring v2 stub proof. Transactional (`begin`/`commit`), fully idempotent
(`on conflict do nothing`), deterministic `00000000-2711/2722-…` UUIDs, and
`*.invalid` QA emails only. **No customer data, no secrets, no production rows.**
Never run against production.

The synthetic `auth.users` rows it inserts are schema fixtures only; they carry no
credentials and are **not** expected to authenticate interactively — provision a
login-capable QA user via the Supabase Admin Auth API against the staging project.
