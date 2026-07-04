// Day 174 — Security lockdown static validation.
// Verifies the critical access-control guarantees patched or confirmed in the
// Day 174 audit still hold. Static checks only — no network, no DB.
//
// Run: npx tsx scripts/validate-security-lockdown.ts

import * as fs from "fs";
import * as path from "path";

const ROOT = path.resolve(__dirname, "..");

function read(rel: string): string {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

type Check = { name: string; pass: boolean; detail?: string };
const checks: Check[] = [];

function check(name: string, pass: boolean, detail?: string) {
  checks.push({ name, pass, detail });
}

// ---------------------------------------------------------------------------
const serverTs = read("src/server.ts");
const dashboardTs = read("src/routes/dashboard.ts");
const crmTs = read("src/routes/crm.ts");
const managerTs = read("src/routes/manager.ts");
const assignmentsTs = read("src/routes/assignments.ts");
const teamTs = read("src/routes/team.ts");

// 1. Manager routers are guarded ---------------------------------------------
check(
  "manager router mounts requireManager for all routes",
  /router\.use\(requireManager\)/.test(managerTs)
);

check(
  "assignments manager endpoints use requireManager",
  /r\.get\("\/manager",\s*requireManager/.test(assignmentsTs) &&
    /r\.post\("\/",\s*requireManager/.test(assignmentsTs) &&
    /r\.delete\("\/:id",\s*requireManager/.test(assignmentsTs)
);

// 2. Dashboard aggregates require an identity --------------------------------
check(
  "dashboard router requires identity (Day 174 guard)",
  /missing_user_identity/.test(dashboardTs) && /requesterIdOf\(req\)/.test(dashboardTs)
);

check(
  "dashboard no longer reads the never-set req.authUserId alone",
  !/String\(\(req as any\)\.authUserId \|\| ''\)\.trim\(\)/.test(dashboardTs)
);

// 3. team/users is tenant scoped ---------------------------------------------
check(
  "team/users resolves requester company and filters by company_id",
  /resolveCompanyId/.test(teamTs) &&
    /\.eq\("company_id",\s*companyId\)/.test(teamTs) &&
    /if \(!companyId\) return res\.json\(\{ ok: true, items: \[\] \}\)/.test(teamTs)
);

// 4. Upload finalize stamps user/org safely ----------------------------------
check(
  "upload finalize rejects paths outside the uploader's prefix",
  /path\.startsWith\(`\$\{userId\}\/`\)/.test(serverTs)
);

check(
  "upload finalize stamps uploader hierarchy (office_id/company_id)",
  /callInsert\.office_id = urow\.office_id/.test(serverTs) &&
    /callInsert\.company_id = urow\.company_id/.test(serverTs)
);

// 5. DEV_TEST_UID never honoured in production --------------------------------
{
  const lines = serverTs.split("\n");
  const offenders: number[] = [];
  lines.forEach((line, i) => {
    if (!line.includes("DEV_TEST_UID")) return;
    // A use is safe if it sits inside a NODE_ENV production gate within
    // the surrounding three lines (both Day 174 gates match this shape).
    const context = lines.slice(Math.max(0, i - 2), i + 2).join("\n");
    if (!/NODE_ENV\s*[=!]==?\s*"production"/.test(context)) offenders.push(i + 1);
  });
  check(
    "server.ts DEV_TEST_UID fallbacks are gated on NODE_ENV !== production",
    offenders.length === 0,
    offenders.length ? `ungated at line(s): ${offenders.join(", ")}` : undefined
  );
}

// 6. CRM manager org scope fails closed in production -------------------------
check(
  "crm requireManagerOrg fails closed in production (no header/zero-org bypass)",
  /forbidden_org_scope/.test(crmTs) &&
    /isProduction && \(isZeroOrg \|\| !headerProvided\)/.test(crmTs)
);

// 7. Legacy app-level endpoints carry the Day 174 identity guard ---------------
{
  const guarded = [
    'app.get("/v1/reps/:id/overview", requireIdentity',
    'app.get("/v1/jobs/:id", requireIdentity',
    'app.get("/v1/coach/assignments", requireIdentity',
    'app.delete("/v1/coach/assignments/:id", requireIdentity',
    'app.patch("/v1/coach/assignments/:id", requireIdentity',
    'app.get("/v1/coach/notes", requireIdentity',
    'app.post("/v1/coach/notes", requireIdentity',
    "app.get('/v1/crm/accounts/:id/overview', requireIdentity",
    "app.get('/v1/crm/contacts/:id/overview', requireIdentity",
    "app.get('/v1/coach/assignments/by-entity', requireIdentity",
  ];
  const missing = guarded.filter((g) => !serverTs.includes(g));
  check(
    "legacy server.ts endpoints all carry requireIdentity",
    missing.length === 0,
    missing.length ? `missing guard: ${missing.join(" | ")}` : undefined
  );
}

check(
  "job status endpoint enforces owner-only reads",
  /data\.user_id && String\(data\.user_id\) !== requester/.test(serverTs)
);

// 8. Scope guards: no voice/TTS or new LLM hot path added ----------------------
{
  const srcFiles: string[] = [];
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name === "dist") continue;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (/\.ts$/.test(entry.name) && !/\.ts\.ts$/.test(entry.name)) srcFiles.push(full);
    }
  };
  walk(path.join(ROOT, "src"));

  const voiceHits = srcFiles.filter((f) =>
    /elevenlabs|text[-_]?to[-_]?speech/i.test(fs.readFileSync(f, "utf8"))
  );
  check(
    "no ElevenLabs/TTS/voice work present",
    voiceHits.length === 0,
    voiceHits.length ? voiceHits.map((f) => path.relative(ROOT, f)).join(", ") : undefined
  );

  // The security middleware and guards added today must not call LLMs.
  const guardFiles = ["src/middleware", "src/lib/callAccess.ts"];
  const llmInGuards = guardFiles.some((rel) => {
    const full = path.join(ROOT, rel);
    const files = fs.statSync(full).isDirectory()
      ? fs.readdirSync(full).filter((f) => f.endsWith(".ts")).map((f) => path.join(full, f))
      : [full];
    return files.some((f) => /scoreWithLLM|openai|anthropic/i.test(fs.readFileSync(f, "utf8")));
  });
  check("no LLM calls in auth/guard hot path", !llmInGuards);
}

// ---------------------------------------------------------------------------
let failed = 0;
for (const c of checks) {
  const mark = c.pass ? "✅" : "❌";
  console.log(`${mark} ${c.name}${c.detail ? ` — ${c.detail}` : ""}`);
  if (!c.pass) failed++;
}

console.log("");
if (failed) {
  console.error(`❌ security lockdown validation FAILED (${failed}/${checks.length} checks)`);
  process.exit(1);
}
console.log(`✅ security lockdown validation PASSED (${checks.length} checks)`);
