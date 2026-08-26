/**
 * proof-crm-actions-create-day-302.ts
 *
 * Guarded staging proof for the Day 302 POST /crm/actions retry-ladder fix. Faithfully
 * replicates the route's attempts ladder and runs it against staging crm_actions with
 * (a) the OLD missing-column detection — proving it BREAKS on the first attempt and
 * creates nothing — and (b) the FIXED detection — proving it continues to the minimal
 * fallback and creates a valid action. Reads back the created row, then fail-safe
 * cleans up.
 *
 * NO route/scoring execution, NO paid AI, NO outbound. crm_actions has no FKs, so a
 * synthetic DAY302 contact_id/user_id suffices.
 *
 * Usage: npx tsx scripts/proof-crm-actions-create-day-302.ts
 */

import { readFileSync } from "fs";
import { join } from "path";
import { createClient } from "@supabase/supabase-js";
import { randomUUID } from "crypto";

function parseEnv(p: string): Record<string, string> {
  const o: Record<string, string> = {};
  for (const l of readFileSync(p, "utf8").split("\n")) {
    const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m) o[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
  return o;
}

let failures = 0;
const ok = (label: string, cond: boolean, detail?: string) => {
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${label}${!cond && detail ? `  — ${detail}` : ""}`);
  if (!cond) failures += 1;
};

// Detection variants (verbatim semantics from the route).
const oldIsMissingColumn = (msg: string) => msg.includes("column") && msg.includes("does not exist");
const fixedIsMissingColumn = (msg: string) =>
  (msg.includes("column") && msg.includes("does not exist")) ||
  (msg.includes("could not find the") && msg.includes("column")) ||
  msg.includes("schema cache");

async function main() {
  const env = parseEnv(join(__dirname, "..", ".env.staging.local"));
  const url = env.SUPABASE_URL, key = env.SUPABASE_SERVICE_ROLE_KEY;
  const ref = (url.match(/https:\/\/([a-z0-9]+)\.supabase/) || [])[1];
  console.log("Day 302 — POST /crm/actions retry-ladder guarded staging proof\n");
  if (ref !== "dnumqzxthfthsmfnzvdz") { console.error(`REFUSING: not staging (${ref?.slice(0, 6)}…)`); process.exit(1); }
  console.log(`  target: dedicated staging (ref ${ref.slice(0, 6)}…)\n`);

  const svc = createClient(url, key);
  const requester = randomUUID();
  const contactIdRaw = "DAY302-" + randomUUID().slice(0, 8);
  const repId = requester;
  const type = "follow_up", title = "Follow up", importance = "normal", dueAt = null;

  // The route's attempts ladder (verbatim shape).
  const basePayload: any = { user_id: requester, rep_id: repId, contact_id: contactIdRaw, type, title, due_at: dueAt, importance, meta: { source: "manager_inline_create_v1" }, status: "open", source: "manager_inline_create_v1" };
  const attempts: any[] = [
    { ...basePayload },
    (() => { const p = { ...basePayload }; delete p.status; return p; })(),
    (() => { const p = { ...basePayload }; delete p.importance; return p; })(),
    (() => { const p = { ...basePayload }; delete p.meta; return p; })(),
    (() => { const p = { ...basePayload }; delete p.source; return p; })(),
    (() => { const p = { ...basePayload }; delete p.due_at; return p; })(),
    { user_id: requester, rep_id: repId, contact_id: contactIdRaw, type, title },
    { user_id: requester, contact_id: contactIdRaw, type, title },
  ];

  // Runs the ladder using a given detection fn; returns {created, attemptsTried}.
  async function runLadder(isMissingColumn: (m: string) => boolean) {
    let attemptsTried = 0;
    for (const payload of attempts) {
      attemptsTried++;
      const r = await svc.from("crm_actions").insert(payload).select("*").single();
      if (!r.error) return { created: r.data, attemptsTried };
      const msg = String((r.error as any)?.message ?? "").toLowerCase();
      if (msg.includes("relation") && msg.includes("does not exist")) return { created: null, attemptsTried, tableMissing: true };
      if (isMissingColumn(msg)) continue;
      break; // matches the route: stop on a non-missing-column error
    }
    return { created: null, attemptsTried };
  }

  try {
    // ── Negative control: OLD detection breaks immediately, creates nothing ──────
    const oldRun = await runLadder(oldIsMissingColumn);
    ok("OLD detection breaks on the first attempt (PGRST204 unrecognised)", oldRun.attemptsTried === 1 && !oldRun.created);
    const afterOld = await svc.from("crm_actions").select("id", { count: "exact", head: true }).eq("contact_id", contactIdRaw);
    ok("OLD detection created NO action (reproduces the 500)", (afterOld.count ?? 0) === 0, `${afterOld.count}`);

    // ── Fixed detection: continues to the minimal fallback and creates the action ─
    const fixRun = await runLadder(fixedIsMissingColumn);
    ok("FIXED detection reaches a working payload and creates the action", !!fixRun.created);
    ok("FIXED detection fell through to the minimal fallback (last attempt)", fixRun.attemptsTried === attempts.length, `tried ${fixRun.attemptsTried}`);

    // ── Read back + verify canonical crm_actions contract ───────────────────────
    const back = await svc.from("crm_actions").select("id,user_id,contact_id,type,title,status,importance").eq("contact_id", contactIdRaw);
    ok("exactly one action persisted for the synthetic contact", back.data?.length === 1, `${back.data?.length}`);
    const row: any = (back.data || [])[0] || {};
    ok("user_id = requester (owner)", row.user_id === requester);
    ok("contact_id = synthetic DAY302 contact", row.contact_id === contactIdRaw);
    ok("type + title persisted", row.type === type && row.title === title);
    ok("status defaulted to 'open'", row.status === "open");
    ok("importance defaulted to 'normal'", row.importance === "normal");
  } finally {
    await svc.from("crm_actions").delete().eq("contact_id", contactIdRaw);
    const residue = await svc.from("crm_actions").select("id", { count: "exact", head: true }).eq("contact_id", contactIdRaw);
    ok("cleanup: zero DAY302 residue", (residue.count ?? 0) === 0, `${residue.count}`);
  }

  console.log(`\n${failures === 0 ? "PASS" : "FAIL"} — ${failures} failure(s). No route/AI/outbound; staging only.`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error("proof crashed:", e?.message || e); process.exit(1); });
