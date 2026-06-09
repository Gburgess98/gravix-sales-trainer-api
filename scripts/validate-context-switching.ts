/**
 * validate-context-switching.ts
 *
 * Validates GET /v1/admin/context/options — the organisation
 * context switcher options endpoint.
 *
 * Coverage:
 *   ✓ SuperAdmin sees all partners
 *   ✓ SuperAdmin sees all companies (across all partners)
 *   ✓ PartnerAdmin sees only their own partner (not others)
 *   ✓ PartnerAdmin sees only companies under their partner
 *   ✓ PartnerAdmin cannot see companies outside their partner
 *   ✓ currentPartnerId and currentCompanyId are present in response
 *   ✓ Manager → 403
 *   ✓ SalesRep → 403
 *   ✓ No auth → 401
 *   ✓ Response shape: ok, partners[], companies[], currentPartnerId, currentCompanyId
 *   ✓ Company isolation: PartnerAdmin companies all belong to their partner
 *
 * Requirements:
 *   - Server running (npm run dev)
 *   - At least one SuperAdmin and one PartnerAdmin in reps table
 *
 * Usage: npm run validate:context-switching
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const c = (label: string, passed: boolean, detail?: string): Check => ({ label, passed, detail });

async function hit(path: string, userId?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const res = await fetch(`${API_BASE}${path}`, { headers, signal: AbortSignal.timeout(10_000) });
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function findUserByTier(tier: string): Promise<{ id: string; company_id: string | null } | null> {
  const { data } = await supa
    .from("reps")
    .select("id, company_id")
    .eq("tier", tier)
    .limit(1)
    .maybeSingle();
  if (!(data as any)?.id) return null;
  return { id: (data as any).id, company_id: (data as any).company_id ?? null };
}

async function getPartnerIdForCompany(companyId: string): Promise<string | null> {
  const { data } = await supa
    .from("companies")
    .select("partner_id")
    .eq("id", companyId)
    .maybeSingle();
  return (data as any)?.partner_id ?? null;
}

async function countAllPartners(): Promise<number> {
  const { count } = await supa
    .from("partners")
    .select("id", { count: "exact", head: true });
  return count ?? 0;
}

async function countAllCompanies(): Promise<number> {
  const { count } = await supa
    .from("companies")
    .select("id", { count: "exact", head: true });
  return count ?? 0;
}

async function countCompaniesForPartner(partnerId: string): Promise<number> {
  const { count } = await supa
    .from("companies")
    .select("id", { count: "exact", head: true })
    .eq("partner_id", partnerId);
  return count ?? 0;
}

async function main() {
  console.log("\n  Context Switching Validator\n");

  // Connectivity
  try {
    await fetch(`${API_BASE}/v1/debug/health`, { signal: AbortSignal.timeout(4_000) });
  } catch {
    console.error(`  ✗  Server not reachable at ${API_BASE}\n`); process.exit(2);
  }

  // Resolve test users
  process.stdout.write("  Resolving test users... ");
  const [superAdmin, partnerAdmin, manager, salesRep] = await Promise.all([
    findUserByTier("SuperAdmin"),
    findUserByTier("PartnerAdmin"),
    findUserByTier("Manager"),
    findUserByTier("SalesRep"),
  ]);
  console.log("done");
  console.log(`  SuperAdmin:   ${superAdmin?.id   ?? "(none)"}`);
  console.log(`  PartnerAdmin: ${partnerAdmin?.id ?? "(none)"}`);
  console.log(`  Manager:      ${manager?.id      ?? "(none)"}`);
  console.log(`  SalesRep:     ${salesRep?.id     ?? "(none)"}\n`);

  const results: Check[] = [];

  // ── Auth gates ────────────────────────────────────────────────────────────

  {
    const { status } = await hit("/v1/admin/context/options");
    results.push(c("No auth → 401", status === 401, `actual: ${status}`));
  }

  if (manager?.id) {
    const { status } = await hit("/v1/admin/context/options", manager.id);
    results.push(c("Manager → 403", status === 403, `actual: ${status}`));
  } else {
    results.push(c("Manager gate (skipped — no Manager found)", true));
  }

  if (salesRep?.id) {
    const { status } = await hit("/v1/admin/context/options", salesRep.id);
    results.push(c("SalesRep → 403", status === 403, `actual: ${status}`));
  } else {
    results.push(c("SalesRep gate (skipped — no SalesRep found)", true));
  }

  // ── SuperAdmin checks ─────────────────────────────────────────────────────

  if (superAdmin?.id) {
    const { status, data } = await hit("/v1/admin/context/options", superAdmin.id);
    results.push(c("SuperAdmin: 200",                    status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c("SuperAdmin: ok = true",              data?.ok === true, `ok=${data?.ok}`));
    results.push(c("SuperAdmin: partners is array",      Array.isArray(data?.partners), `type=${typeof data?.partners}`));
    results.push(c("SuperAdmin: companies is array",     Array.isArray(data?.companies), `type=${typeof data?.companies}`));
    results.push(c("SuperAdmin: currentPartnerId field", "currentPartnerId" in (data ?? {}), `missing key`));
    results.push(c("SuperAdmin: currentCompanyId field", "currentCompanyId" in (data ?? {}), `missing key`));

    if (status === 200) {
      // SuperAdmin should see ALL partners
      const dbPartnerCount = await countAllPartners();
      results.push(c(
        `SuperAdmin: sees all ${dbPartnerCount} partners`,
        (data?.partners?.length ?? 0) === dbPartnerCount,
        `got ${data?.partners?.length} expected ${dbPartnerCount}`
      ));

      // SuperAdmin should see ALL companies
      const dbCompanyCount = await countAllCompanies();
      results.push(c(
        `SuperAdmin: sees all ${dbCompanyCount} companies`,
        (data?.companies?.length ?? 0) === dbCompanyCount,
        `got ${data?.companies?.length} expected ${dbCompanyCount}`
      ));

      // Company shape check
      if (Array.isArray(data?.companies) && data.companies.length > 0) {
        const first = data.companies[0];
        results.push(c("SuperAdmin: company has id",         !!first.id,    `id=${first.id}`));
        results.push(c("SuperAdmin: company has name",       !!first.name,  `name=${first.name}`));
        results.push(c("SuperAdmin: company has partner_id field", "partner_id" in first, `keys=${Object.keys(first).join(",")}`));
      }
    }
  } else {
    results.push(c("SuperAdmin checks (skipped — no SuperAdmin found)", true));
  }

  // ── PartnerAdmin checks ───────────────────────────────────────────────────

  if (partnerAdmin?.id) {
    const { status, data } = await hit("/v1/admin/context/options", partnerAdmin.id);
    results.push(c("PartnerAdmin: 200",               status === 200, `actual: ${status} error=${data?.error}`));
    results.push(c("PartnerAdmin: ok = true",         data?.ok === true, `ok=${data?.ok}`));
    results.push(c("PartnerAdmin: partners is array", Array.isArray(data?.partners), `type=${typeof data?.partners}`));
    results.push(c("PartnerAdmin: companies is array",Array.isArray(data?.companies), `type=${typeof data?.companies}`));

    if (status === 200) {
      // PartnerAdmin should see exactly ONE partner (their own)
      results.push(c(
        "PartnerAdmin: sees exactly 1 partner",
        data?.partners?.length === 1,
        `got ${data?.partners?.length}`
      ));

      // Resolve their partner_id from DB for cross-checking
      const partnerAdminPartnerId = partnerAdmin.company_id
        ? await getPartnerIdForCompany(partnerAdmin.company_id)
        : null;

      if (partnerAdminPartnerId && data?.partners?.length === 1) {
        results.push(c(
          "PartnerAdmin: partner in response matches their own partner",
          data.partners[0].id === partnerAdminPartnerId,
          `got ${data.partners[0].id} expected ${partnerAdminPartnerId}`
        ));
      }

      // Company count should match DB count for their partner
      if (partnerAdminPartnerId) {
        const dbCount = await countCompaniesForPartner(partnerAdminPartnerId);
        results.push(c(
          `PartnerAdmin: sees correct ${dbCount} companies for their partner`,
          (data?.companies?.length ?? 0) === dbCount,
          `got ${data?.companies?.length} expected ${dbCount}`
        ));

        // Company isolation: all returned companies must belong to their partner
        const allBelongToPartner = (data?.companies ?? []).every(
          (co: any) => co.partner_id === partnerAdminPartnerId
        );
        results.push(c(
          "PartnerAdmin: all returned companies belong to their partner",
          allBelongToPartner,
          `some companies have wrong partner_id`
        ));
      }

      // currentPartnerId should match their partner
      if (partnerAdminPartnerId) {
        results.push(c(
          "PartnerAdmin: currentPartnerId matches their partner",
          data?.currentPartnerId === partnerAdminPartnerId,
          `got ${data?.currentPartnerId} expected ${partnerAdminPartnerId}`
        ));
      }

      // currentCompanyId should match their company
      if (partnerAdmin.company_id) {
        results.push(c(
          "PartnerAdmin: currentCompanyId matches their company",
          data?.currentCompanyId === partnerAdmin.company_id,
          `got ${data?.currentCompanyId} expected ${partnerAdmin.company_id}`
        ));
      }
    }
  } else {
    results.push(c("PartnerAdmin checks (skipped — no PartnerAdmin found)", true));
  }

  // ── Cross-role isolation: PartnerAdmin cannot see SuperAdmin's extra companies ─
  if (superAdmin?.id && partnerAdmin?.id) {
    const { data: saData } = await hit("/v1/admin/context/options", superAdmin.id);
    const { data: paData } = await hit("/v1/admin/context/options", partnerAdmin.id);

    if (saData?.ok && paData?.ok) {
      const saCompanyCount = saData.companies?.length ?? 0;
      const paCompanyCount = paData.companies?.length ?? 0;

      // If there are multiple partners, SuperAdmin sees more companies than PartnerAdmin
      const saPartnerCount = saData.partners?.length ?? 0;
      if (saPartnerCount > 1) {
        results.push(c(
          "PartnerAdmin sees fewer companies than SuperAdmin (multi-partner isolation)",
          paCompanyCount < saCompanyCount,
          `PA=${paCompanyCount} SA=${saCompanyCount}`
        ));
      } else {
        // Single partner — counts should match
        results.push(c(
          "Single partner: PartnerAdmin company count matches SuperAdmin",
          paCompanyCount === saCompanyCount,
          `PA=${paCompanyCount} SA=${saCompanyCount}`
        ));
      }
    }
  }

  // ── Report ────────────────────────────────────────────────────────────────
  console.log("\n  ── Results ──────────────────────────────────────────");
  let passed = 0, failed = 0;
  for (const r of results) {
    const icon = r.passed ? "✓" : "✗";
    console.log(`  ${icon}  ${r.label}`);
    if (!r.passed && r.detail) console.log(`       → ${r.detail}`);
    if (r.passed) passed++; else failed++;
  }
  console.log(`\n  ${passed} passed  ${failed} failed\n`);

  if (failed > 0) {
    const noPrivUsers = !superAdmin && !partnerAdmin;
    if (noPrivUsers) {
      console.log("  ⚠  No SuperAdmin or PartnerAdmin found in reps table.");
      console.log("     Create at least one of each tier to run full validation.\n");
    }
    process.exit(1);
  }
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
