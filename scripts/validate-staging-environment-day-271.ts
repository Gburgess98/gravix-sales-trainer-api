/**
 * validate-staging-environment-day-271.ts
 *
 * Refuses dangerous staging↔production configuration combinations, so a staging
 * deployment can never silently point at, write to, or notify production.
 *
 * TWO LANES:
 *   • SELF-TEST (default, NO network, NO secrets): a clean synthetic staging
 *     config passes; a battery of planted violations each fail a named check.
 *     This is what CI/local runs — `npm run validate:staging-environment`.
 *   • LIVE (opt-in, only when STAGING_API_BASE + STAGING_CONFIRMED=1): GETs the
 *     staging health/version endpoint, refuses production hosts, and checks the
 *     reported APP_ENV/commit. Never prints tokens, refs or transcripts.
 *
 * Safety: compares SAFE identifiers only (equality/inequality), redacts values,
 * refuses ambiguous targets, exits non-zero on unsafe configuration. Supabase
 * project refs are treated as sensitive — they are compared, never printed.
 *
 * Usage:
 *   npx tsx scripts/validate-staging-environment-day-271.ts
 *   STAGING_API_BASE=https://staging... STAGING_CONFIRMED=1 EXPECTED_API_SHA=6b996ea \
 *     npx tsx scripts/validate-staging-environment-day-271.ts
 */

import { readFileSync } from "fs";
import { join } from "path";

let failures = 0;
function gate(label: string, ok: boolean, detail?: string) {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${!ok && detail ? `  — ${detail}` : ""}`);
  if (!ok) failures += 1;
}
function section(t: string) {
  console.log(`\n── ${t} ──`);
}

// Public production hosts (NOT secrets) — extend via PROD_HOST_DENYLIST.
const DEFAULT_PROD_DENYLIST = ["gravixbots.com", "api.gravixbots.com", "vercel.app"];
function prodDenylist(): string[] {
  const extra = (process.env.PROD_HOST_DENYLIST || "").split(",").map((s) => s.trim()).filter(Boolean);
  return [...DEFAULT_PROD_DENYLIST, ...extra];
}
function hostOf(url: string): string {
  try {
    return new URL(/^https?:\/\//.test(url) ? url : `https://${url}`).host.toLowerCase();
  } catch {
    return "";
  }
}
export function isProductionHost(url: string): boolean {
  const host = hostOf(url);
  if (!host) return true; // unparseable → unsafe
  return prodDenylist().some((d) => host === d || host.endsWith("." + d) || host.includes(d));
}

export interface StagingConfig {
  appEnv?: string; // must be "staging" (NODE_ENV alone is NOT proof)
  supabaseRef?: string; // staging Supabase project ref (sensitive — compared, never printed)
  prodSupabaseRef?: string; // production ref (to prove staging differs)
  apiHost?: string; // staging API host
  webHost?: string; // staging WEB host
  webApiBase?: string; // URL the staging WEB calls (must be the staging API, not prod)
  skipSideEffects?: string; // must be "1"
  scoringContract?: string; // must be "v1" on Day 271 (v2 not enabled yet)
  scoringProvider?: string; // "stub"
  slackWebhook?: string | null; // must be empty/disabled in staging
  postmarkToken?: string | null; // must be empty/disabled in staging
  seedTargetRef?: string; // must equal the staging ref (never prod)
  apiCommit?: string;
  expectedCommit?: string;
}

/**
 * Validate a staging config. Returns FIELD-LEVEL issues only — never prints a
 * secret, ref or URL value.
 */
export function checkStagingConfig(cfg: StagingConfig): string[] {
  const issues: string[] = [];
  const disabled = (v: string | null | undefined) => v == null || String(v).trim() === "";

  // 1. Explicit staging marker (not NODE_ENV).
  if (cfg.appEnv !== "staging") issues.push("APP_ENV is not 'staging'");

  // 2. Staging DB must differ from production.
  if (!cfg.supabaseRef) issues.push("staging Supabase ref missing");
  if (!cfg.prodSupabaseRef) issues.push("production Supabase ref (for comparison) missing");
  if (cfg.supabaseRef && cfg.prodSupabaseRef && cfg.supabaseRef === cfg.prodSupabaseRef) {
    issues.push("staging Supabase project EQUALS production (must be separate)");
  }

  // 3. Hosts must not be production.
  if (cfg.apiHost && isProductionHost(cfg.apiHost)) issues.push("staging API host is a production host");
  if (cfg.webHost && isProductionHost(cfg.webHost)) issues.push("staging WEB host is a production host");

  // 4. Staging WEB must call the staging API, not production.
  if (cfg.webApiBase && isProductionHost(cfg.webApiBase)) issues.push("staging WEB points at the production API");

  // 5. Side effects must be skipped during the staging proof window.
  if (cfg.skipSideEffects !== "1") issues.push("SKIP_SCORING_SIDE_EFFECTS is not '1'");

  // 6. Scoring stays on v1/default with the stub provider (v2 not enabled Day 271).
  if (cfg.scoringContract !== "v1") issues.push("SCORING_CONTRACT is not 'v1' (v2 must not be enabled on Day 271)");
  if (cfg.scoringProvider && cfg.scoringProvider !== "stub") issues.push("SCORING_PROVIDER is not 'stub'");

  // 7. Production notification integrations must be disabled in staging.
  if (!disabled(cfg.slackWebhook)) issues.push("Slack webhook is set in staging (must be disabled)");
  if (!disabled(cfg.postmarkToken)) issues.push("Postmark token is set in staging (must be disabled)");

  // 8. Seed must target staging, never production.
  if (cfg.seedTargetRef && cfg.prodSupabaseRef && cfg.seedTargetRef === cfg.prodSupabaseRef) {
    issues.push("seed target is the PRODUCTION Supabase project");
  }
  if (cfg.seedTargetRef && cfg.supabaseRef && cfg.seedTargetRef !== cfg.supabaseRef) {
    issues.push("seed target is not the staging Supabase project");
  }

  // 9. Deployment commit match (when an expectation is supplied).
  if (cfg.expectedCommit && cfg.apiCommit && !cfg.apiCommit.startsWith(cfg.expectedCommit)) {
    issues.push("deployed API commit does not match the expected commit");
  }
  return issues;
}

/** A production config that accidentally uses the staging Supabase is also unsafe. */
export function checkProductionNotUsingStaging(prodAppEnv: string, prodSupabaseRef: string, stagingSupabaseRef: string): string[] {
  const issues: string[] = [];
  if (prodAppEnv !== "staging" && stagingSupabaseRef && prodSupabaseRef === stagingSupabaseRef) {
    issues.push("production is using the STAGING Supabase project");
  }
  return issues;
}

const CLEAN: StagingConfig = {
  appEnv: "staging",
  supabaseRef: "stagingref00staging",
  prodSupabaseRef: "prodref00production",
  apiHost: "gravix-sales-trainer-api-staging.example.dev",
  webHost: "gravix-sales-trainer-web-staging.example.dev",
  webApiBase: "https://gravix-sales-trainer-api-staging.example.dev",
  skipSideEffects: "1",
  scoringContract: "v1",
  scoringProvider: "stub",
  slackWebhook: "",
  postmarkToken: "",
  seedTargetRef: "stagingref00staging",
  apiCommit: "6b996ea221cfd",
  expectedCommit: "6b996ea",
};

function clone<T>(x: T): T {
  return JSON.parse(JSON.stringify(x));
}

async function liveLane(): Promise<void> {
  const base = process.env.STAGING_API_BASE;
  if (!base) return;
  section("LIVE — staging health/version (network)");
  if (process.env.STAGING_CONFIRMED !== "1") {
    gate("LIVE requires STAGING_CONFIRMED=1 (refused)", false);
    return;
  }
  if (isProductionHost(base)) {
    gate("LIVE target is NOT a production host", false, `refused ${hostOf(base)}`);
    return;
  }
  gate("LIVE target host is not on the production denylist", true, hostOf(base));
  try {
    const url = `${base.replace(/\/$/, "")}/health`;
    const resp = await fetch(url);
    gate("staging health endpoint reachable", resp.ok, `HTTP ${resp.status}`);
    if (!resp.ok) return;
    const body: any = await resp.json().catch(() => ({}));
    if (body?.app_env || body?.env) gate("staging reports APP_ENV=staging", (body.app_env ?? body.env) === "staging");
    if (process.env.EXPECTED_API_SHA && body?.commit) {
      gate("deployed commit matches EXPECTED_API_SHA", String(body.commit).startsWith(process.env.EXPECTED_API_SHA));
    }
  } catch (e: any) {
    gate("staging health fetch did not throw", false, e?.message || String(e));
  }
}

async function main() {
  console.log("Staging environment safety-guard (Day 271) — self-test makes NO network call, prints NO secrets/refs\n");

  section("SELF-TEST — a clean synthetic staging config passes");
  gate("clean staging config passes", checkStagingConfig(CLEAN).length === 0, checkStagingConfig(CLEAN).join("; "));

  section("SAFETY — production-host refusal");
  gate("refuses api.gravixbots.com", isProductionHost("https://api.gravixbots.com"));
  gate("refuses a *.vercel.app prod origin", isProductionHost("https://gravix-web.vercel.app"));
  gate("refuses an unparseable/empty target", isProductionHost("http://") && isProductionHost(""));
  gate("allows a clearly-staging host", !isProductionHost("https://staging-api.example.dev"));

  section("NON-VACUITY — planted unsafe configs must be caught");
  const caught = (label: string, mutate: (c: StagingConfig) => void) => {
    const c = clone(CLEAN);
    mutate(c);
    gate(`caught: ${label}`, checkStagingConfig(c).length > 0);
  };
  caught("missing APP_ENV", (c) => { delete c.appEnv; });
  caught("APP_ENV=staging with a production API host", (c) => { c.apiHost = "api.gravixbots.com"; });
  caught("staging Supabase equals production", (c) => { c.supabaseRef = c.prodSupabaseRef; c.seedTargetRef = c.prodSupabaseRef; });
  caught("staging WEB points at the production API", (c) => { c.webApiBase = "https://api.gravixbots.com"; });
  caught("side effects enabled (SKIP != 1)", (c) => { c.skipSideEffects = "0"; });
  caught("SCORING_CONTRACT flipped to v2", (c) => { c.scoringContract = "v2"; });
  caught("provider not stub", (c) => { c.scoringProvider = "openai"; });
  caught("production Slack webhook present", (c) => { c.slackWebhook = "https://hooks.slack.com/services/xxx"; });
  caught("production Postmark token present", (c) => { c.postmarkToken = "pm-token"; });
  caught("seed target is production", (c) => { c.seedTargetRef = c.prodSupabaseRef; });
  caught("missing staging marker (blank APP_ENV)", (c) => { c.appEnv = ""; });
  caught("deployed commit mismatch", (c) => { c.apiCommit = "deadbeef000"; });
  caught("staging WEB host is production", (c) => { c.webHost = "gravix-web.vercel.app"; });
  // production accidentally using the staging DB
  gate("caught: production using the staging Supabase", checkProductionNotUsingStaging("production", "sharedref", "sharedref").length > 0);
  gate("clean prod/staging DB separation passes", checkProductionNotUsingStaging("production", "prodref", "stagingref").length === 0);

  section("SAFETY — no secret/ref leakage in validator output");
  const src = readFileSync(join(__dirname, "validate-staging-environment-day-271.ts"), "utf8");
  gate("validator never console.logs a supabase ref/url", !/console\.log\([^)]*(supabaseRef|SUPABASE_URL|prodSupabaseRef)/.test(src));
  gate("validator never console.logs a token", !/console\.log\([^)]*(TOKEN|token|webhook)/.test(src));

  await liveLane();

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} gate failure(s).`);
  console.log(process.env.STAGING_API_BASE ? "Ran the LIVE lane against the confirmed staging target." : "Self-test only (no STAGING_API_BASE) — no network, no secrets, no refs printed.");
  process.exit(failures === 0 ? 0 : 1);
}

main();
