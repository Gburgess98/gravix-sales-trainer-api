// src/whisperer/schema.ts
// TIER 2B — Day 111: fail-soft probe for the whisperer tables.
// Mirrors src/sparring/schema.ts: migrations are manual (Supabase SQL editor),
// so until sql/20260612_whisperer_stub_loop.sql runs in an environment the
// routes fall back to an in-memory session store (flagged persistence:false).

let _probe: boolean | null = null;
let _probeAt = 0;
const REPROBE_MS = 5 * 60 * 1000;

function isMissingTableError(error: any): boolean {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("does not exist") || msg.includes("could not find") || msg.includes("schema cache");
}

export async function whispererTablesAvailable(supa: any): Promise<boolean> {
  const now = Date.now();
  if (_probe !== null && now - _probeAt < REPROBE_MS) return _probe;

  const { error } = await supa.from("whisperer_sessions").select("id").limit(1);
  _probe = !error || !isMissingTableError(error);
  _probeAt = now;

  if (!_probe) {
    console.warn(
      "[whisperer/schema] tables missing — run sql/20260612_whisperer_stub_loop.sql in the Supabase SQL editor (in-memory fallback active)"
    );
  }
  return _probe;
}

export function resetWhispererSchemaProbe() {
  _probe = null;
  _probeAt = 0;
}
