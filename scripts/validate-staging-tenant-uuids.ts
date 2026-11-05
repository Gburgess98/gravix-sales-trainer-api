/**
 * Offline validator (no network, no secrets) for the staging tenant-UUID repair.
 *
 * It judges every id with THE APPLICATION'S OWN predicate — extracted from the source the API actually runs
 * (UUID_RE in src/routes/crm.ts, which is what raises "Missing or invalid x-org-id") — not a copy written here.
 * Validation is never relaxed; this proves the repair data satisfies it and that the old pattern still fails it.
 *
 *   npx tsx scripts/validate-staging-tenant-uuids.ts
 */
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { STAGING_TENANT_IDS as NEW, LEGACY_STAGING_TENANT_IDS as OLD, STAGING_FIXTURE_CALL_IDS } from "./staging-tenant-ids";
import { STAGING_TENANT } from "./provision-staging-qa";
import { extractAppUuidPattern } from "./app-uuid-predicate";
import { buildRepairSql, buildRetireLegacyAuthSql, buildVerifySql, REPAIR_FILES, STRICT_PG } from "./generate-staging-tenant-uuid-repair";

const root = join(__dirname, "..");
const read = (rel: string) => readFileSync(join(root, rel), "utf8");
let failures = 0;
const gate = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "  ✅" : "  ❌"} ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) failures++; };
const section = (t: string) => console.log(`\n── ${t}`);

// ── the application's actual predicate ────────────────────────────────────────────────────────────────────────────
const extract = (rel: string) => extractAppUuidPattern(rel, root);
const crm = extract("src/routes/crm.ts");
const predicate = crm ? new RegExp(crm, "i") : null;

section("the predicate under test is the app's own");
gate("extracted UUID_RE from src/routes/crm.ts (the x-org-id check)", !!predicate);
const sources = ["src/routes/crm.ts", "src/routes/reps.ts", "src/routes/calls.ts", "src/routes/accounts.ts", "src/routes/users.ts", "src/routes/pins.ts", "src/tokenVerification.ts"];
gate("every API file that validates ids uses the identical pattern (no divergent copy)", sources.every((f) => extract(f) === crm), sources.filter((f) => extract(f) !== crm).join(", "));
gate("the strict predicate was not relaxed (still version [1-5], variant [89ab])", !!crm && /\[1-5\]\[0-9a-f\]\{3\}-\[89ab\]\[0-9a-f\]\{3\}/.test(crm));
gate("the generator's SQL pattern equals the app predicate", !!crm && STRICT_PG === crm, `${STRICT_PG}`);
const valid = (id: string) => !!predicate && predicate.test(id);

section("valid replacement ids");
for (const [k, v] of Object.entries(NEW)) gate(`${k} ${v} passes`, valid(v));
for (const [k, v] of Object.entries(STAGING_FIXTURE_CALL_IDS)) gate(`${k} ${v} passes`, valid(v));
gate("the QA provisioning script's tenant is the valid set", Object.values(STAGING_TENANT).every(valid));
gate("version nibble is 4 and variant nibble is 8 on every tenant id", Object.values(NEW).every((v) => v[14] === "4" && v[19] === "8"));

section("NON-VACUITY — the legacy ids are rejected by the same predicate");
const legacy = [OLD.tmcId, OLD.orgId, OLD.companyId, OLD.ownerRepId, OLD.callOwnerId, ...OLD.legacyAuthUserIds, ...OLD.staleCallIds];
for (const v of legacy) gate(`legacy ${v} FAILS`, !valid(v));
gate("the zero-nibble pattern is rejected (version 0 / variant 0)", !valid("00000000-2711-0000-0000-000000000099"));
gate("a v4/variant-8 id with the same prefix is accepted", valid("00000000-2711-4000-8000-000000000099"));

section("seed, QA tooling and fixtures no longer carry a malformed id");
const FULL_LEGACY = /00000000-27(?:11|22)-0000-0000-[0-9a-f]{12}/gi;
const files = ["sql/seed/staging-day272-fixtures.sql", "scripts/provision-staging-qa.ts", "scripts/prove-account-ai-actor-fk-day-283.mjs", "STAGING_QA_IDENTITY_RUNBOOK.md", "sql/README.md", "scripts/staging-tenant-ids.ts"];
const allowedLegacy = new Set<string>([...OLD.staleCallIds]); // the seed's Day-276 cleanup deletes the stale call twins
for (const f of files) {
  const hits = (read(f).match(FULL_LEGACY) || []).filter((h) => !(f.endsWith("staging-day272-fixtures.sql") && allowedLegacy.has(h)) && !(f === "scripts/staging-tenant-ids.ts"));
  gate(`${f}: no malformed tenant id`, hits.length === 0, hits.join(", "));
}
gate("detector self-test: it flags a planted legacy id", (`insert into orgs values ('${OLD.orgId}')`.match(FULL_LEGACY) || []).length === 1);
const seed = read("sql/seed/staging-day272-fixtures.sql");
const seeded = [NEW.tmcId, NEW.orgId, NEW.companyId, NEW.ownerRepId];
gate("seed inserts the valid tenant (tmc, org, company, owner rep)", seeded.every((id) => seed.includes(id)));
const ownerCalls = seed.match(/user_id[\s\S]{0,400}?'(00000000-2711-[0-9a-f-]{23})'/g);
gate("seed call owner is the single synthetic rep (no phantom owner id)", !seed.includes(OLD.callOwnerId));
void ownerCalls;

section("repair SQL is generated from the same ids and committed unchanged");
gate("forward SQL equals the generator output", read(REPAIR_FILES.forward) === buildRepairSql("forward"));
gate("rollback SQL equals the generator output", read(REPAIR_FILES.rollback) === buildRepairSql("rollback"));
gate("legacy-auth retirement SQL equals the generator output", read(REPAIR_FILES.retire) === buildRetireLegacyAuthSql());
gate("verification SQL equals the generator output", read(REPAIR_FILES.verify) === buildVerifySql());
const fwd = buildRepairSql("forward");
gate("forward: one transaction, fail-closed guards, residual-reference proof, then delete", /\bbegin;/.test(fwd) && /commit;\s*$/.test(fwd) && /refusing: org/.test(fwd) && /repair aborted — references remain/.test(fwd) && fwd.indexOf("references remain") < fwd.indexOf("delete from public.orgs"));
gate("forward: copies parents before children (tmcs → orgs → companies → reps)", ["public.tmcs", "public.orgs", "public.companies", "public.reps"].map((t) => fwd.indexOf(`insert into ${t}`)).every((v, i, a) => v > 0 && (i === 0 || v > a[i - 1])));
gate("forward: deletes children before parents", ["org_settings", "reps", "companies", "orgs", "tmcs"].map((t) => fwd.indexOf(`delete from public.${t} `)).every((v, i, a) => v > 0 && (i === 0 || v > a[i - 1])));
gate("forward: every legacy tenant id is a sweep source", [OLD.tmcId, OLD.orgId, OLD.companyId, OLD.ownerRepId, OLD.callOwnerId, ...OLD.staleCallIds].every((id) => new RegExp(`\\('${id}'::uuid, '`).test(fwd)));
gate("forward: every value it WRITES (jsonb_build_object / sweep target) is a valid id", [...fwd.matchAll(/'(?:id|org_id|company_id|tmc_id|org_id)',\s*'([0-9a-f-]{36})'/g)].every((m) => valid(m[1])) && [...fwd.matchAll(/::uuid, '([0-9a-f-]{36})'::uuid\)/g)].every((m) => valid(m[1])));
gate("forward: never touches profiles.user_id / auth (FK to auth.users) and never writes outside the staging fixture", /\('profiles','user_id'\)/.test(fwd) && !/auth\.users/.test(fwd.replace(/^--.*$/gm, "")));
const rb = buildRepairSql("rollback");
gate("rollback: labelled EMERGENCY and is the exact reverse mapping", /EMERGENCY ROLLBACK/.test(rb) && rb.includes(`('${NEW.orgId}'::uuid, '${OLD.orgId}'::uuid)`));
gate("verification SQL uses the app predicate", buildVerifySql().includes(STRICT_PG));

section("the repair is never aimed at production");
gate("repair files live under sql/staging/; the writing ones say STAGING ONLY / never production", Object.values(REPAIR_FILES).every((f) => f.startsWith("sql/staging/")) && [REPAIR_FILES.forward, REPAIR_FILES.rollback, REPAIR_FILES.retire].every((f) => /STAGING ONLY/.test(read(f)) && /production/i.test(read(f))));
gate("no application code or script executes the repair", !readdirSync(join(root, "scripts")).some((f) => f.endsWith(".ts") && !/generate-staging|validate-staging-tenant/.test(f) && /20261105_repair_staging_tenant_uuids/.test(read(`scripts/${f}`))));

console.log(`\n${failures === 0 ? "✅ PASS" : `❌ FAIL (${failures})`} — staging tenant UUID repair validator\n`);
process.exit(failures === 0 ? 0 : 1);
