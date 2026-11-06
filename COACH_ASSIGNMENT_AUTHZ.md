# Coach-assignment object-level authorisation

## The flaw
`DELETE /v1/coach/assignments/:id` and `PATCH /v1/coach/assignments/:id` (complete/reopen) in `src/server.ts` were guarded only by
`requireIdentity` (any verified identity). They then acted on the row by bare id with the service-role client, so any signed-in user
(including a rep, in any tenant) could delete, complete or reopen any assignment whose id they knew. Responses also echoed raw
database error text. `POST /v1/coach/assign` (`src/routes/coach.ts`) required the caller to own the call but trusted the
client-supplied `assigneeUserId`, so an owner could assign into any tenant.

## Data model (audited)
`coach_assignments(id, call_id, assignee_user_id, drill_id, notes, created_at, status, completed_at, org_id)`.
- Target rep = `assignee_user_id` (nullable). There is **no** creator column, no company_id and no office_id on the row.
- Tenant = the assignee's `reps` row (`company_id`, falling back to `org_id`); `org_id` on the row is a second, weaker signal.
- Manager tiers come from `reps.tier` (`Manager`, `Owner`, `PartnerAdmin`, `SuperAdmin`) via the existing `src/lib/repAccess.ts`
  (`canAccessRep`, `isManagerTier`, `tenantKey`) used by dashboard/sparring (Day 26 G2). The verified caller id is `req.userId`
  (identity middleware); headers such as `x-org-id` are never read.

## Policy (`src/lib/coachAssignmentAccess.ts`)
| Operation | Allowed | Otherwise |
|---|---|---|
| DELETE | manager tier **and** assignee in the caller's tenant (org_id must also agree when both present; assignee-less rows only inside the manager's own org) | rep/unknown caller → 403 `forbidden_not_manager` *before any lookup*; unknown **and** out-of-scope ids → identical 404 `not_found`; malformed id → 400 `invalid_id` |
| PATCH status | the assignee, or an in-scope manager | 404 `not_found` (same as unknown) |
| POST /coach/assign | call owner **and** assignee is self, or (manager) a rep in the owner's tenant | 403 `forbidden` |
Success shapes are unchanged (`{ok:true}` for delete, `{ok:true, assignment}` for patch). Database errors return a calm 500 with no
database text.

PartnerAdmin/SuperAdmin keep the existing semantics: they are manager tiers constrained by the same verified tenant key, not a
cross-tenant bypass.

## Reads and notes (same commit)
| Route | Policy |
|---|---|
| `GET /v1/coach/assignments` | A row is returned only when its **assignee** is in the caller's scope (`accessibleRepIds`: reps = themselves; manager tiers = their verified tenant). `orgId`, `x-org-id`, role and rep-id parameters never widen it (`orgId` is ignored). An out-of-scope assignee, a foreign call/account/contact and an unknown id all return `{ok:true, items:[]}`; malformed ids → 400. A call's filename is only used as the label when the call belongs to an in-scope rep. Shape unchanged. |
| `GET /v1/coach/assignments/by-entity` | Same rule; entity → calls resolved server-side; the slim original item shape is kept. |
| `GET /v1/coach/notes`, `POST /v1/coach/notes` | A note belongs to its **call** (`coach_notes`: one free-text note per call, no author column, so there is no author to expose). Read/write requires acting for the call's owner: the owner, or a manager tier in the owner's verified tenant. Another rep (even same tenant), another tenant and unknown calls all get the identical 404; a denied POST writes nothing; non-text `notes` → 400. |
| `GET /v1/coach/drills` | Static public catalogue (allow-listed); no object. |
| `POST /v1/coach/assign` | Owner + assignee scope (earlier in this document). |

Every `/v1/coach/*` route in `server.ts` is now delegated to `src/lib/coachAssignmentAccess.ts`; the validator fails if an unreviewed `/v1/coach` route appears or if a delegated route reads headers/`orgId`/tables directly.

## Search result: other `requireIdentity`-only coach routes
`grep` of every `/v1/coach/*` registration (server.ts + routes/coach.ts): 6 `requireIdentity` routes (list, by-entity, delete, patch, notes GET/POST) — all now object-scoped; `POST /v1/coach/assign` (own check); `GET /v1/coach/drills` (public static). No other coach route exists. Adjacent, **not** a `/v1/coach` route and not changed: `POST /v1/crm/coaching/nudges` creates a task for the caller but trusts client-supplied `account_id`/`contact_id`/`opportunity_id` — belongs in the CRM authorisation lane.

## Limits / not changed
- The office-level hierarchy (office_manager within an office) is **not modelled on `reps`**; this fix uses the same tenant-wide manager scope as the rest of the API. Narrowing to offices needs a model decision.
- A rep who is not the call owner can no longer read/write that call's coach note (previously any signed-in user could), and a rep viewing a teammate's call sees no assignments/notes for it. This is stricter than call visibility by design.
- `POST /v1/coach/assign` refuses a rep assigning to another rep (previously any assignee was accepted).

## Proof
`npm run validate:coach-assignment-authz` (wired into `check:release`): in-memory store + HTTP-level spoofing of `x-org-id`/role headers;
the DB row is asserted unchanged after every denied attempt; the pre-fix identity-only handler is run through the same scenarios and must
fail them (non-vacuity). Not exercised against a live database.

## Deploy base
Runtime `src/` on staging equals `b183363`; the branches `claude/staging-tenant-uuid-repair` and `claude/rls-lockdown` add no `src/` changes. This commit is a single commit directly on `b183363`. Deploy API first; then the web change that hides Delete from reps.
