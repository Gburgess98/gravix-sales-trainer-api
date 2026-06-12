/**
 * backfill-sparring-data-model.ts — Tier 2A Day 106
 *
 * Lifts sparring session/turn data out of meta into the first-class columns
 * added by sql/20260612_sparring_data_model_hardening.sql:
 *   sessions: assignment_id, org_id, company_id, office_id, status,
 *             completed_at, state
 *   turns:    turn_score (mapped from meta.turn_scores by turnId)
 *
 * Idempotent: only fills columns that are currently null; never overwrites,
 * never deletes meta fields. Tenant fields prefer assignment linkage, then
 * the rep's users-row hierarchy; left null (and counted) when unknown.
 *
 * Prerequisite: run sql/20260612_sparring_data_model_hardening.sql in the
 * Supabase SQL editor first (the script checks and exits if missing).
 *
 * Usage:
 *   tsx scripts/backfill-sparring-data-model.ts          # dry run (default)
 *   tsx scripts/backfill-sparring-data-model.ts --apply  # write changes
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;
const APPLY = process.argv.includes("--apply");

const supa = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const isUuid = (v: any) => UUID_RE.test(String(v || "").trim());

async function schemaReady(): Promise<boolean> {
  const { error: sErr } = await supa.from("sparring_sessions").select("status").limit(1);
  const { error: tErr } = await supa.from("sparring_turns").select("turn_score").limit(1);
  if (sErr || tErr) {
    console.error("✗  Hardening columns missing.");
    console.error("   Run sql/20260612_sparring_data_model_hardening.sql in the Supabase SQL editor first.");
    return false;
  }
  return true;
}

async function main() {
  console.log(`\n  Backfill — sparring data model ${APPLY ? "(APPLY)" : "(dry run)"}\n`);

  if (!(await schemaReady())) process.exit(1);

  // ── Sessions ──
  const { data: sessions, error: sessErr } = await supa
    .from("sparring_sessions")
    .select("id, rep_id, meta, summary, total_score, created_at, assignment_id, org_id, company_id, office_id, status, completed_at, state")
    .limit(10000);
  if (sessErr) {
    console.error("✗  Failed to read sessions:", sessErr.message);
    process.exit(1);
  }

  const rows = sessions ?? [];

  // Validate assignment linkage against live assignments (no FK on the column)
  const candidateAssignmentIds = Array.from(
    new Set(
      rows
        .map((s: any) => {
          const m = s?.meta || {};
          return String(s.assignment_id || m.assignment_id || m.source_assignment_id || "").trim();
        })
        .filter(isUuid)
    )
  );
  const liveAssignments = new Map<string, { rep_id: string | null; company_id: string | null; office_id: string | null }>();
  if (candidateAssignmentIds.length > 0) {
    const { data: assignRows } = await supa
      .from("assignments")
      .select("id, rep_id, company_id, office_id")
      .in("id", candidateAssignmentIds);
    for (const a of assignRows ?? []) {
      liveAssignments.set(String((a as any).id), {
        rep_id: (a as any).rep_id || null,
        company_id: (a as any).company_id || null,
        office_id: (a as any).office_id || null,
      });
    }
  }

  // Rep hierarchy fallback
  const repIds = Array.from(new Set(rows.map((s: any) => String(s.rep_id || "")).filter(Boolean)));
  const repHierarchy = new Map<string, { company_id: string | null; office_id: string | null }>();
  if (repIds.length > 0) {
    const { data: userRows } = await supa.from("users").select("id, company_id, office_id").in("id", repIds);
    for (const u of userRows ?? []) {
      repHierarchy.set(String((u as any).id), {
        company_id: (u as any).company_id || null,
        office_id: (u as any).office_id || null,
      });
    }
  }

  let sessionsScanned = 0;
  let sessionsUpdated = 0;
  let sessionsSkipped = 0;
  let missingAssignmentLinks = 0;
  let missingTenantLinks = 0;

  for (const s of rows as any[]) {
    sessionsScanned += 1;
    const m = s?.meta && typeof s.meta === "object" ? s.meta : {};
    const patch: Record<string, any> = {};

    // assignment_id (validated against live assignments; dead links counted)
    if (!s.assignment_id) {
      const candidate = String(m.assignment_id || m.source_assignment_id || "").trim();
      if (isUuid(candidate)) {
        if (liveAssignments.has(candidate)) patch.assignment_id = candidate;
        else missingAssignmentLinks += 1;
      }
    }

    // state
    if (!s.state && m.state && typeof m.state === "object") patch.state = m.state;

    // completed_at
    const isCompleted = Boolean(
      m.ended || m.completed_at || s.summary || typeof s.total_score === "number"
    );
    if (!s.completed_at) {
      const fromMeta = m.completed_at || null;
      if (fromMeta) patch.completed_at = fromMeta;
      else if (isCompleted && s.created_at) patch.completed_at = s.created_at; // best available fallback for legacy ended rows
    }

    // status
    if (!s.status) patch.status = isCompleted ? "completed" : "active";

    // tenant fields — prefer assignment linkage, fall back to rep hierarchy
    if (!s.company_id || !s.office_id) {
      const viaAssignment = patch.assignment_id || s.assignment_id
        ? liveAssignments.get(String(patch.assignment_id || s.assignment_id))
        : null;
      const viaRep = repHierarchy.get(String(s.rep_id || ""));
      const company = viaAssignment?.company_id || viaRep?.company_id || null;
      const office = viaAssignment?.office_id || viaRep?.office_id || null;
      if (!s.company_id && company) patch.company_id = company;
      if (!s.office_id && office) patch.office_id = office;
      if (!company && !office) missingTenantLinks += 1;
    }
    // org_id: sessions meta does not store org reliably; left null (documented).

    if (Object.keys(patch).length === 0) {
      sessionsSkipped += 1;
      continue;
    }

    if (APPLY) {
      const { error: updErr } = await supa.from("sparring_sessions").update(patch).eq("id", s.id);
      if (updErr) {
        console.error(`✗  session ${s.id}: ${updErr.message}`);
        continue;
      }
    }
    sessionsUpdated += 1;
  }

  // ── Turns (turn_score from meta.turn_scores, mapped by turnId) ──
  let turnsScanned = 0;
  let turnsUpdated = 0;

  const sessionsWithScores = (rows as any[]).filter(
    (s) => Array.isArray(s?.meta?.turn_scores) && s.meta.turn_scores.length > 0
  );

  for (const s of sessionsWithScores) {
    const scoreByTurnId = new Map<string, any>();
    for (const entry of s.meta.turn_scores) {
      if (entry?.turnId && entry?.score) scoreByTurnId.set(String(entry.turnId), entry.score);
    }
    if (scoreByTurnId.size === 0) continue;

    const { data: turns } = await supa
      .from("sparring_turns")
      .select("id, turn_score")
      .eq("session_id", s.id)
      .eq("role", "user");

    for (const t of (turns ?? []) as any[]) {
      turnsScanned += 1;
      if (t.turn_score) continue; // idempotent
      const score = scoreByTurnId.get(String(t.id));
      if (!score) continue;
      if (APPLY) {
        const { error: tErr } = await supa
          .from("sparring_turns")
          .update({ turn_score: score })
          .eq("id", t.id);
        if (tErr) {
          console.error(`✗  turn ${t.id}: ${tErr.message}`);
          continue;
        }
      }
      turnsUpdated += 1;
    }
  }
  // state_snapshot is not backfilled: per-turn historical state was never
  // stored before Day 106, so there is nothing faithful to lift.

  console.log(`Sessions scanned:        ${sessionsScanned}`);
  console.log(`Sessions ${APPLY ? "updated" : "would update"}:  ${sessionsUpdated}`);
  console.log(`Sessions skipped:        ${sessionsSkipped}`);
  console.log(`Turns scanned:           ${turnsScanned}`);
  console.log(`Turns ${APPLY ? "updated" : "would update"}:     ${turnsUpdated}`);
  console.log(`Dead assignment links:   ${missingAssignmentLinks}`);
  console.log(`Missing tenant links:    ${missingTenantLinks}`);
  if (!APPLY) console.log("\nRe-run with --apply to write changes.");
}

main().then(() => process.exit(0));
