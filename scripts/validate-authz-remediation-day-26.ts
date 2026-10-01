// Go Live Day 26 — focused, network-free authorization checks for the G2/G3
// remediation (NOT the G4 route-wide release guard).
//
//   A. POST /v1/calls ownership (src/lib/callStoragePath.ts): the storage path
//      must sit in the caller's own folder, and an existing row for that path
//      can never be re-assigned to another user or tenant.
//   B. Rep/tenant rules (src/lib/repAccess.ts) with injected lookups: self
//      always; manager tiers within their own tenant only; everything else
//      denied; aggregate scope = self, or the tenant for manager tiers.
//
// Non-vacuity: a control replays the pre-fix behaviour (unconditional upsert
// by storage_path) and shows it WOULD have re-assigned the victim's row.
//
// Run: npm run validate:authz-remediation   (tsx, no DB, no network)

// repAccess imports the shared supabase client module; give it inert values
// so the import cannot fail. No request is ever made (lookups are injected).
process.env.SUPABASE_URL ||= "http://127.0.0.1:9";
process.env.SUPABASE_SERVICE_ROLE_KEY ||= "validator-placeholder";

let failures = 0;
function ok(name: string, pass: boolean, detail = "") {
  console.log(`  ${pass ? "✅" : "❌"}  ${name}${detail ? " — " + detail : ""}`);
  if (!pass) failures++;
}

const CALLER = "11111111-1111-4111-8111-111111111111";
const VICTIM = "22222222-2222-4222-8222-222222222222";
const TEAMMATE = "33333333-3333-4333-8333-333333333333";
const MANAGER = "44444444-4444-4444-8444-444444444444";
const OTHER_MGR = "55555555-5555-4555-8555-555555555555";
const SUPER = "66666666-6666-4666-8666-666666666666";
const CO_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const CO_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

async function main() {
  const { storagePathOwnedBy, decideCallWrite } = await import("../src/lib/callStoragePath");
  const { canAccessRep, accessibleRepIds } = await import("../src/lib/repAccess");

  console.log("Go Live Day 26 — authorization remediation checks\n");

  // ── A. storage path ownership ────────────────────────────────────────
  ok("own folder path ⇒ owned", storagePathOwnedBy(CALLER, `${CALLER}/2026/call.webm`));
  ok("another user's folder ⇒ not owned", !storagePathOwnedBy(CALLER, `${VICTIM}/call.webm`));
  ok("prefix-collision folder (uid + suffix) ⇒ not owned", !storagePathOwnedBy(CALLER, `${CALLER}x/call.webm`));
  ok("folder root only ⇒ not owned", !storagePathOwnedBy(CALLER, `${CALLER}/`));
  ok("'..' traversal out of own folder ⇒ not owned", !storagePathOwnedBy(CALLER, `${CALLER}/../${VICTIM}/call.webm`));
  ok("'.' / empty segment ⇒ not owned", !storagePathOwnedBy(CALLER, `${CALLER}/./call.webm`) && !storagePathOwnedBy(CALLER, `${CALLER}//call.webm`));
  ok("backslash path ⇒ not owned", !storagePathOwnedBy(CALLER, `${CALLER}\\..\\x`));
  ok("leading slash ⇒ not owned", !storagePathOwnedBy(CALLER, `/${CALLER}/call.webm`));
  ok("empty owner or path ⇒ not owned", !storagePathOwnedBy("", "a/b") && !storagePathOwnedBy(CALLER, ""));

  // ── A. write decision ────────────────────────────────────────────────
  const victimRow = { id: "row-victim", user_id: VICTIM, company_id: CO_B };
  const attack = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${VICTIM}/call.webm`, existing: victimRow,
  });
  ok("NEGATIVE: victim's storage path ⇒ 403 storage_path_not_owned (no write)",
    !attack.ok && attack.status === 403 && attack.error === "storage_path_not_owned");

  const sneaky = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: victimRow,
  });
  ok("NEGATIVE: own-folder path already owned by another user ⇒ 409 conflict (no overwrite)",
    !sneaky.ok && sneaky.status === 409);

  const teammateRow = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: { id: "row-teammate", user_id: TEAMMATE, company_id: CO_A },
  });
  ok("NEGATIVE: own-folder path already owned by a SAME-tenant teammate ⇒ 409 conflict (no overwrite)",
    !teammateRow.ok && teammateRow.status === 409);

  const crossTenant = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: { id: "row-x", user_id: CALLER, company_id: CO_B },
  });
  ok("NEGATIVE: same user but different tenant on the existing row ⇒ 409 conflict",
    !crossTenant.ok && crossTenant.status === 409);  const onBehalf = decideCallWrite({
    callerId: MANAGER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${MANAGER}/call.webm`, existing: null,
  });
  ok("POLICY: caller-owned contract — manager creating a row for a rep ⇒ 403 on_behalf_not_supported",
    !onBehalf.ok && onBehalf.status === 403 && onBehalf.error === "on_behalf_not_supported");

  const onBehalfRepPath = decideCallWrite({
    callerId: MANAGER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: null,
  });
  ok("POLICY: manager using the rep's storage folder ⇒ 403 storage_path_not_owned",
    !onBehalfRepPath.ok && onBehalfRepPath.status === 403 && onBehalfRepPath.error === "storage_path_not_owned");



  const fresh = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: null,
  });
  ok("POSITIVE: own path, no existing row ⇒ insert", fresh.ok && fresh.mode === "insert");

  const same = decideCallWrite({
    callerId: CALLER, targetUserId: CALLER, targetCompanyId: CO_A,
    storagePath: `${CALLER}/call.webm`, existing: { id: "row-own", user_id: CALLER, company_id: CO_A },
  });
  ok("POSITIVE: own path, existing row owned by caller ⇒ update that row only",
    same.ok && same.mode === "update" && same.id === "row-own");

  // Non-vacuity control: the pre-fix handler upserted unconditionally on
  // storage_path, i.e. it would have rewritten the victim's row.
  const preFixUpsert = (existing: typeof victimRow | null, insert: { user_id: string; company_id: string }) =>
    existing ? { ...existing, ...insert } : { id: "new", ...insert };
  const hijacked = preFixUpsert(victimRow, { user_id: CALLER, company_id: CO_A });
  ok("mutation control: pre-fix upsert WOULD re-assign the victim's row (fix is load-bearing)",
    hijacked.id === "row-victim" && hijacked.user_id === CALLER && !attack.ok);

  // ── B. rep / tenant rules ────────────────────────────────────────────
  const reps: Record<string, { id: string; tier: string | null; company_id: string | null; org_id: string | null }> = {
    [CALLER]: { id: CALLER, tier: "SalesRep", company_id: CO_A, org_id: null },
    [TEAMMATE]: { id: TEAMMATE, tier: "SalesRep", company_id: CO_A, org_id: null },
    [MANAGER]: { id: MANAGER, tier: "Manager", company_id: CO_A, org_id: null },
    [VICTIM]: { id: VICTIM, tier: "SalesRep", company_id: CO_B, org_id: null },
    [OTHER_MGR]: { id: OTHER_MGR, tier: "Manager", company_id: CO_B, org_id: null },
    [SUPER]: { id: SUPER, tier: "SuperAdmin", company_id: CO_A, org_id: null },
  };
  const lookups = {
    getRep: async (id: string) => reps[id] ?? null,
    listTenantRepIds: async (s: { company_id: string | null; org_id: string | null }) =>
      Object.values(reps).filter((r) => (r.company_id || r.org_id) === (s.company_id || s.org_id)).map((r) => r.id),
  };

  ok("self ⇒ allowed", await canAccessRep(CALLER, CALLER, lookups));
  ok("rep → teammate (same tenant, rep tier) ⇒ denied", !(await canAccessRep(CALLER, TEAMMATE, lookups)));
  ok("manager → own-tenant rep ⇒ allowed", await canAccessRep(MANAGER, CALLER, lookups));
  ok("manager → other-tenant rep ⇒ denied", !(await canAccessRep(MANAGER, VICTIM, lookups)));
  ok("other-tenant manager → our rep ⇒ denied", !(await canAccessRep(OTHER_MGR, CALLER, lookups)));
  ok("SuperAdmin → other-tenant rep ⇒ denied (tenant-scoped; policy follow-up)", !(await canAccessRep(SUPER, VICTIM, lookups)));
  ok("unknown caller (forged/random id) → any rep ⇒ denied", !(await canAccessRep("99999999-9999-4999-8999-999999999999", CALLER, lookups)));
  ok("unknown target ⇒ denied", !(await canAccessRep(MANAGER, "99999999-9999-4999-8999-999999999999", lookups)));
  ok("empty ids ⇒ denied", !(await canAccessRep("", CALLER, lookups)) && !(await canAccessRep(CALLER, "", lookups)));

  const repScope = await accessibleRepIds(CALLER, lookups);
  ok("aggregate scope for a rep ⇒ only self", repScope.length === 1 && repScope[0] === CALLER, repScope.join(","));
  const mgrScope = (await accessibleRepIds(MANAGER, lookups)).sort();
  ok("aggregate scope for a manager ⇒ own tenant only",
    mgrScope.includes(CALLER) && mgrScope.includes(TEAMMATE) && !mgrScope.includes(VICTIM) && !mgrScope.includes(OTHER_MGR));
  const unknownScope = await accessibleRepIds("99999999-9999-4999-8999-999999999999", lookups);
  ok("aggregate scope for an unknown caller ⇒ only that id (sees nothing real)",
    unknownScope.length === 1 && !unknownScope.includes(CALLER));
  ok("aggregate scope with no identity ⇒ empty", (await accessibleRepIds("", lookups)).length === 0);

  console.log(`\n${failures === 0 ? "✅ PASS" : `❌ FAIL (${failures})`} — authorization remediation checks\n`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
