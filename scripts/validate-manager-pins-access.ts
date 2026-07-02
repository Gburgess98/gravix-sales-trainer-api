/**
 * Day 171 — manager pins read access own-checks (Day 135 rhythm, no live DB).
 *
 * Asserts at source level that pin reads follow the shared org-scoped call
 * visibility rule (canAccessCall in lib/callAccess), that strict ownership
 * is no longer the read gate, that pin writes stay owner-only, and that no
 * cross-tenant broadening slipped in.
 */
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const pins = readFileSync(join(root, "src", "routes", "pins.ts"), "utf8");
const access = readFileSync(join(root, "src", "lib", "callAccess.ts"), "utf8");
const calls = readFileSync(join(root, "src", "routes", "calls.ts"), "utf8");

let fail = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) fail = 1;
}

// ── read gate: shared visibility, not strict ownership ──
check("pins imports shared canAccessCall from lib/callAccess", /import \{ canAccessCall \} from "\.\.\/lib\/callAccess"/.test(pins));

const getHandler = pins.slice(pins.indexOf('router.get("/"'), pins.indexOf('router.post("/"'));
check("GET pins uses canAccessCall", /canAccessCall\(requester, call\.user_id, call\.org_id \?\? null\)/.test(getHandler));
check("GET pins no longer uses strict user_id-only ownership", !/call\.user_id !== requester/.test(getHandler));
check("GET pins still 403s when access is denied", /status\(403\)/.test(getHandler));
check("GET pins still 404s missing calls", /status\(404\)/.test(getHandler));

// ── writes stay owner-only ──
const postHandler = pins.slice(pins.indexOf('router.post("/"'), pins.indexOf('router.delete("/:id"'));
const deleteHandler = pins.slice(pins.indexOf('router.delete("/:id"'));
check("POST pins keeps strict ownership", /call\.user_id !== requester/.test(postHandler));
check("DELETE pins keeps strict ownership", /call\.user_id !== requester/.test(deleteHandler));
check("writes do not use canAccessCall", !/canAccessCall/.test(postHandler) && !/canAccessCall/.test(deleteHandler));

// ── shared helper preserves tenant isolation ──
check("canAccessCall denies when call has no org", /if \(!callOrgId\) return false/.test(access));
check("canAccessCall requires requester org to match call org", /requesterOrgId !== callOrgId\) return false/.test(access));
check("canAccessCall honours org visibility 'disabled'", /visibility === "disabled"\) return false/.test(access));
check("callAccess keeps the Day 167 reps-membership fallback", /from\("reps"\)/.test(access));

// ── calls.ts now shares the same helper (single rule, no drift) ──
check("calls.ts imports canAccessCall from lib/callAccess", /import \{ canAccessCall, getRequesterOrgId \} from "\.\.\/lib\/callAccess"/.test(calls));
check("calls.ts no longer defines a local canAccessCall", !/async function canAccessCall/.test(calls));

// ── no cross-tenant broadening / forbidden lanes ──
check("pins has no admin/service bypass", !/is_admin|service_role_bypass/i.test(pins));
check("no ElevenLabs/TTS/voice agent in pins or callAccess", !/elevenlabs|text-to-speech|voice[ _-]?agent/i.test(pins + access));
check("no LLM call in pins or callAccess", !/openai|anthropic|chat\.completions|responses\.create/i.test(pins + access));

if (fail) {
  console.log("\nManager pins access validation FAILED");
  process.exit(1);
}
console.log("\nManager pins access validation PASSED");
