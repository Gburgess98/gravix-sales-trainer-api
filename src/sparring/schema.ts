// src/sparring/schema.ts
// TIER 2A — Day 106: fail-soft probe for the sparring hardening columns.
//
// The repo's migration workflow is manual (Supabase SQL editor), so any
// environment may or may not have run sql/20260612_sparring_data_model_hardening.sql
// yet. Write paths probe once per process and include the new columns only
// when they exist; everything still lands in meta either way.

type ProbeResult = { sessions: boolean; turns: boolean };

let _probe: ProbeResult | null = null;
let _probeAt = 0;
const REPROBE_MS = 5 * 60 * 1000; // re-check every 5 minutes (covers mid-process migration)

function isMissingColumnError(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache");
}

export async function sparringHardeningColumns(supa: any): Promise<ProbeResult> {
  const now = Date.now();
  if (_probe && now - _probeAt < REPROBE_MS) return _probe;

  const [sessionsProbe, turnsProbe] = await Promise.all([
    supa.from("sparring_sessions").select("status").limit(1),
    supa.from("sparring_turns").select("turn_score").limit(1),
  ]);

  _probe = {
    sessions: !sessionsProbe.error || !isMissingColumnError(sessionsProbe.error),
    turns: !turnsProbe.error || !isMissingColumnError(turnsProbe.error),
  };
  _probeAt = now;

  if (!_probe.sessions || !_probe.turns) {
    console.warn(
      "[sparring/schema] hardening columns missing — run sql/20260612_sparring_data_model_hardening.sql in the Supabase SQL editor",
      _probe
    );
  }
  return _probe;
}

// Test hook: clear the cached probe.
export function resetSparringSchemaProbe() {
  _probe = null;
  _probeAt = 0;
}
