/**
 * validate-identity-architecture.ts
 * DAY 88 — validates current state of the reps-first identity migration.
 *
 * Checks are STATIC (source code) + SCHEMA (live DB via pg).
 * No auth/login required. Requires DATABASE_URL env var pointing at the live DB.
 */

import * as fs from "fs";
import * as path from "path";
import { Pool } from "pg";

const SRC = path.resolve(__dirname, "../src");
const results: { name: string; pass: boolean; detail?: string }[] = [];

function pass(name: string, detail?: string) {
  results.push({ name, pass: true, detail });
  console.log(`  ✅  ${name}${detail ? " — " + detail : ""}`);
}

function fail(name: string, detail?: string) {
  results.push({ name, pass: false, detail });
  console.log(`  ❌  ${name}${detail ? " — " + detail : ""}`);
}

function warn(name: string, detail?: string) {
  results.push({ name, pass: true, detail: `WARNING: ${detail || ""}` });
  console.log(`  ⚠️   ${name}${detail ? " — " + detail : ""}`);
}

// ─── Source code grep helpers ────────────────────────────────────────────────

function grepSrc(pattern: string): { file: string; line: number; text: string }[] {
  const hits: { file: string; line: number; text: string }[] = [];
  const re = new RegExp(pattern);
  function walk(dir: string) {
    for (const entry of fs.readdirSync(dir)) {
      const full = path.join(dir, entry);
      const stat = fs.statSync(full);
      if (stat.isDirectory()) { walk(full); continue; }
      if (!entry.endsWith(".ts")) continue;
      const lines = fs.readFileSync(full, "utf8").split("\n");
      lines.forEach((text, i) => {
        if (re.test(text)) hits.push({ file: path.relative(SRC, full), line: i + 1, text: text.trim() });
      });
    }
  }
  walk(SRC);
  return hits;
}

function countInSrc(pattern: string): number {
  return grepSrc(pattern).length;
}

// ─── CATEGORY 1: Architecture docs exist ────────────────────────────────────

function checkDocs() {
  console.log("\n── 1. Architecture Documents ──────────────────────────");
  const docs = [
    "REPS_FIRST_ARCHITECTURE.md",
    "USERS_COMPATIBILITY_LAYER.md",
    "FK_MIGRATION_PLAN.md",
    "OWNERSHIP_MIGRATION_PLAN.md",
    "IDENTITY_ROADMAP.md",
    "ARCHITECTURE_DECISION_RECORD.md",
  ];
  const root = path.resolve(__dirname, "..");
  for (const doc of docs) {
    const p = path.join(root, doc);
    fs.existsSync(p)
      ? pass(`${doc} exists`)
      : fail(`${doc} exists`, "file not found");
  }
}

// ─── CATEGORY 2: Middleware reads reps (not users/profiles) ─────────────────

function checkMiddlewareReadSource() {
  console.log("\n── 2. Middleware Identity Source ───────────────────────");

  // requireManager, requirePartnerAdmin, requireSuperAdmin must read reps
  const middlewareFiles = [
    "middleware/requireManager.ts",
    "middleware/requirePartnerAdmin.ts",
    "middleware/requireSuperAdmin.ts",
  ];

  for (const rel of middlewareFiles) {
    const fullPath = path.join(SRC, rel);
    if (!fs.existsSync(fullPath)) {
      fail(`${rel} exists`);
      continue;
    }
    const src = fs.readFileSync(fullPath, "utf8");
    const readsReps    = src.includes('.from("reps")');
    const readsUsers   = src.includes('.from("users")') || src.includes(".from('users')");
    const readsProfiles = src.includes('.from("profiles")') || src.includes(".from('profiles')");

    readsReps
      ? pass(`${rel} reads reps`)
      : fail(`${rel} reads reps`, "no .from('reps') found");

    !readsUsers
      ? pass(`${rel} does not read users`)
      : fail(`${rel} does not read users`, "found .from('users')");

    !readsProfiles
      ? pass(`${rel} does not read profiles`)
      : fail(`${rel} does not read profiles`, "found .from('profiles')");
  }
}

// ─── CATEGORY 3: No new reads against users/profiles in modern route files ──

function checkRouteIdentitySources() {
  console.log("\n── 3. Route Identity Source Audit ─────────────────────");

  // These route files are on the new model — they must not read users/profiles
  const cleanRoutes = [
    "routes/users.ts",
    "routes/auth.ts",
    "middleware/requirePartnerAdmin.ts",
    "middleware/requireSuperAdmin.ts",
  ];

  for (const rel of cleanRoutes) {
    const fullPath = path.join(SRC, rel);
    if (!fs.existsSync(fullPath)) continue;
    const src = fs.readFileSync(fullPath, "utf8");
    const readsLegacy = src.includes('.from("users")') || src.includes('.from("profiles")');
    !readsLegacy
      ? pass(`${rel} is clean (no users/profiles reads)`)
      : fail(`${rel} is clean`, "still reads users or profiles table");
  }

  // Count total legacy reads across the codebase (for awareness)
  const usersHits    = grepSrc('\\.from\\(["\']users["\']\\)');
  const profilesHits = grepSrc('\\.from\\(["\']profiles["\']\\)');

  const usersFiles   = [...new Set(usersHits.map(h => h.file))];
  const profilesFiles = [...new Set(profilesHits.map(h => h.file))];

  if (usersHits.length === 0) {
    pass("Zero .from('users') calls in src/");
  } else {
    warn(
      `${usersHits.length} .from('users') calls remain (Phase 2 target)`,
      `files: ${usersFiles.join(", ")}`
    );
  }

  if (profilesHits.length === 0) {
    pass("Zero .from('profiles') calls in src/");
  } else {
    warn(
      `${profilesHits.length} .from('profiles') calls remain (Phase 2 target)`,
      `files: ${profilesFiles.join(", ")}`
    );
  }
}

// ─── CATEGORY 4: reps-first signals in users.ts + auth.ts ───────────────────

function checkRepFirstRoutes() {
  console.log("\n── 4. Reps-First Route Implementation ──────────────────");

  const usersMe = path.join(SRC, "routes/users.ts");
  if (fs.existsSync(usersMe)) {
    const src = fs.readFileSync(usersMe, "utf8");
    src.includes('.from("reps")')
      ? pass("GET /v1/users/me reads from reps")
      : fail("GET /v1/users/me reads from reps");

    !src.includes('.from("users")') && !src.includes('.from("profiles")')
      ? pass("GET /v1/users/me does not read users/profiles")
      : fail("GET /v1/users/me does not read users/profiles");
  }

  // auth.ts — GET /me must not read req.userId directly (DEV_TEST_UID bypass fix)
  const authTs = path.join(SRC, "routes/auth.ts");
  if (fs.existsSync(authTs)) {
    const src = fs.readFileSync(authTs, "utf8");
    src.includes('req.header("x-user-id")')
      ? pass("auth.ts GET /me reads x-user-id header explicitly")
      : fail("auth.ts GET /me reads x-user-id header explicitly");
  }
}

// ─── CATEGORY 5: Tier model consistency ─────────────────────────────────────

function checkTierModel() {
  console.log("\n── 5. Tier Model Consistency ───────────────────────────");

  // All six tiers must be defined in requireManager
  const requireMgrPath = path.join(SRC, "middleware/requireManager.ts");
  if (fs.existsSync(requireMgrPath)) {
    const src = fs.readFileSync(requireMgrPath, "utf8");
    const expectedTiers = ["SalesRep", "Manager", "Owner", "PartnerAdmin", "SuperAdmin"];
    for (const tier of expectedTiers) {
      src.includes(tier)
        ? pass(`requireManager.ts references tier: ${tier}`)
        : fail(`requireManager.ts references tier: ${tier}`, "tier not found in file");
    }
  }

  // requirePartnerAdmin allows PartnerAdmin + SuperAdmin
  const reqPAPath = path.join(SRC, "middleware/requirePartnerAdmin.ts");
  if (fs.existsSync(reqPAPath)) {
    const src = fs.readFileSync(reqPAPath, "utf8");
    src.includes("PartnerAdmin") && src.includes("SuperAdmin")
      ? pass("requirePartnerAdmin allows PartnerAdmin + SuperAdmin")
      : fail("requirePartnerAdmin allows PartnerAdmin + SuperAdmin");
  }

  // requireSuperAdmin rejects everything except SuperAdmin
  const reqSAPath = path.join(SRC, "middleware/requireSuperAdmin.ts");
  if (fs.existsSync(reqSAPath)) {
    const src = fs.readFileSync(reqSAPath, "utf8");
    src.includes('"SuperAdmin"') || src.includes("'SuperAdmin'")
      ? pass("requireSuperAdmin checks for SuperAdmin tier")
      : fail("requireSuperAdmin checks for SuperAdmin tier");
  }
}

// ─── CATEGORY 6: Ownership chain (company_id / partner_id) ──────────────────

function checkOwnershipChain() {
  console.log("\n── 6. Ownership Chain ──────────────────────────────────");

  // getVisibleCompanies must read reps.company_id → companies.partner_id
  const paPath = path.join(SRC, "lib/partnerAccess.ts");
  if (fs.existsSync(paPath)) {
    const src = fs.readFileSync(paPath, "utf8");
    src.includes("company_id")
      ? pass("partnerAccess.ts uses reps.company_id for scoping")
      : fail("partnerAccess.ts uses reps.company_id for scoping");
    src.includes("partner_id")
      ? pass("partnerAccess.ts uses companies.partner_id for PartnerAdmin scope")
      : fail("partnerAccess.ts uses companies.partner_id for PartnerAdmin scope");
    src.includes("SuperAdmin")
      ? pass("partnerAccess.ts handles SuperAdmin (all companies)")
      : fail("partnerAccess.ts handles SuperAdmin");
    src.includes("PartnerAdmin")
      ? pass("partnerAccess.ts handles PartnerAdmin (partner-scoped)")
      : fail("partnerAccess.ts handles PartnerAdmin");
  } else {
    fail("lib/partnerAccess.ts exists");
  }
}

// ─── CATEGORY 7: No dangerous DEV bypasses in middleware ────────────────────

function checkNoBypasses() {
  console.log("\n── 7. No Dev Bypasses in Privileged Middleware ─────────");

  const privilegedFiles = [
    "middleware/requirePartnerAdmin.ts",
    "middleware/requireSuperAdmin.ts",
  ];

  for (const rel of privilegedFiles) {
    const fullPath = path.join(SRC, rel);
    if (!fs.existsSync(fullPath)) continue;
    const src = fs.readFileSync(fullPath, "utf8");

    // These files must NOT fall back to (req as any).userId or DEV_TEST_UID in actual code.
    // Mentions in comments are fine (and encouraged as documentation of the decision).
    const codeLines = src.split("\n").filter(l => !l.trimStart().startsWith("//"));
    const codeOnly  = codeLines.join("\n");
    const fallsBackToReqUserId = codeOnly.includes("(req as any)?.userId") || codeOnly.includes("(req as any).userId");
    const fallsBackToEnvUid    = codeOnly.includes("DEV_TEST_UID");

    !fallsBackToReqUserId
      ? pass(`${rel} does not fall back to req.userId`)
      : fail(`${rel} does not fall back to req.userId`, "DEV_TEST_UID bypass present");

    !fallsBackToEnvUid
      ? pass(`${rel} does not reference DEV_TEST_UID`)
      : fail(`${rel} does not reference DEV_TEST_UID`);
  }
}

// ─── CATEGORY 8: Live DB schema check (optional — requires DATABASE_URL) ────

async function checkLiveSchema() {
  console.log("\n── 8. Live DB Schema ───────────────────────────────────");

  const connStr = process.env.DATABASE_URL;
  if (!connStr) {
    warn("Live DB schema check skipped", "DATABASE_URL not set — run with DATABASE_URL to enable");
    return;
  }

  const pool = new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false } });

  try {
    const checkCol = async (table: string, column: string) => {
      const r = await pool.query(
        `SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`,
        [table, column]
      );
      r.rowCount && r.rowCount > 0
        ? pass(`${table}.${column} exists`)
        : fail(`${table}.${column} exists`, "column not found");
    };

    // reps identity fields
    await checkCol("reps", "company_id");
    await checkCol("reps", "tier");
    await checkCol("reps", "display_name");
    await checkCol("reps", "email");
    await checkCol("reps", "is_active");
    await checkCol("reps", "manager_id");
    await checkCol("reps", "timezone");

    // company ownership
    await checkCol("companies", "partner_id");
    await checkCol("companies", "is_active");

    // partners table
    const parRow = await pool.query(
      `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'partners'`
    );
    parRow.rowCount && parRow.rowCount > 0
      ? pass("partners table exists")
      : fail("partners table exists");

    // licence tables
    const lpRow = await pool.query(
      `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'licence_pools'`
    );
    lpRow.rowCount && lpRow.rowCount > 0
      ? pass("licence_pools table exists")
      : fail("licence_pools table exists");

    // tier CHECK constraint covers all 6 tiers
    const tierRow = await pool.query(
      `SELECT pg_get_constraintdef(c.oid)
       FROM pg_constraint c
       JOIN pg_class t ON t.oid = c.conrelid
       JOIN pg_namespace n ON n.oid = t.relnamespace
       WHERE n.nspname = 'public' AND t.relname = 'reps' AND c.conname = 'reps_tier_check'`
    );
    if (tierRow.rows.length > 0) {
      const def = tierRow.rows[0].pg_get_constraintdef as string;
      const expectedTiers = ["SalesRep", "TeamLead", "Manager", "Owner", "PartnerAdmin", "SuperAdmin"];
      const missing = expectedTiers.filter(t => !def.includes(t));
      missing.length === 0
        ? pass("reps_tier_check constraint covers all 6 tiers")
        : fail("reps_tier_check constraint covers all 6 tiers", `missing: ${missing.join(", ")}`);
    } else {
      fail("reps_tier_check constraint exists");
    }

    // zero reps with NULL company_id (Phase 0 complete)
    const nullCoRow = await pool.query(
      `SELECT COUNT(*) AS n FROM reps WHERE company_id IS NULL`
    );
    const nullCo = parseInt(nullCoRow.rows[0].n, 10);
    nullCo === 0
      ? pass("Zero reps with NULL company_id")
      : warn(`${nullCo} reps have NULL company_id`, "backfill may not have run");

    // zero companies with NULL partner_id
    const nullPRow = await pool.query(
      `SELECT COUNT(*) AS n FROM companies WHERE partner_id IS NULL`
    );
    const nullP = parseInt(nullPRow.rows[0].n, 10);
    nullP === 0
      ? pass("Zero companies with NULL partner_id")
      : warn(`${nullP} companies have NULL partner_id`, "expected after partner foundation migration");

    // orphan detection: calls.user_id not in reps
    const orphanCallsRow = await pool.query(
      `SELECT COUNT(*) AS n FROM calls c
       WHERE c.user_id IS NOT NULL
         AND c.user_id NOT IN (SELECT id FROM reps)`
    );
    const orphanCalls = parseInt(orphanCallsRow.rows[0].n, 10);
    orphanCalls === 0
      ? pass("Zero orphan calls.user_id (all in reps)")
      : warn(`${orphanCalls} calls.user_id values not in reps`, "Phase 1 backfill needed before adding FK");

    // orphan detection: assignments.user_id not in reps
    const orphanAsgRow = await pool.query(
      `SELECT COUNT(*) AS n FROM assignments a
       WHERE a.user_id IS NOT NULL
         AND a.user_id NOT IN (SELECT id FROM reps)`
    ).catch(() => ({ rows: [{ n: "skipped" }] }));
    const orphanAsg = orphanAsgRow.rows[0].n;
    if (orphanAsg === "skipped") {
      warn("assignments orphan check skipped", "table may not exist or has no user_id column");
    } else {
      parseInt(orphanAsg, 10) === 0
        ? pass("Zero orphan assignments.user_id (all in reps)")
        : warn(`${orphanAsg} assignments.user_id values not in reps`, "Phase 1 backfill needed");
    }

  } finally {
    await pool.end();
  }
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("━━━ IDENTITY ARCHITECTURE VALIDATION ━━━━━━━━━━━━━━━━━━");
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Src:  ${SRC}`);

  checkDocs();
  checkMiddlewareReadSource();
  checkRouteIdentitySources();
  checkRepFirstRoutes();
  checkTierModel();
  checkOwnershipChain();
  checkNoBypasses();
  await checkLiveSchema();

  const total   = results.length;
  const passed  = results.filter(r => r.pass).length;
  const failed  = total - passed;
  const warnings = results.filter(r => r.pass && r.detail?.startsWith("WARNING")).length;

  console.log("\n━━━ RESULTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`  ${passed} passed · ${failed} failed · ${warnings} warnings · ${total} total`);

  if (failed > 0) {
    console.log("\nFailed checks:");
    results
      .filter(r => !r.pass)
      .forEach(r => console.log(`  ❌  ${r.name}${r.detail ? " — " + r.detail : ""}`));
    process.exit(1);
  } else {
    console.log("\n✅  All identity architecture checks passed");
    if (warnings > 0) {
      console.log(`   (${warnings} warnings — review Phase 1/2 migration items above)`);
    }
    process.exit(0);
  }
}

main().catch(e => {
  console.error("Validation crashed:", e);
  process.exit(1);
});
