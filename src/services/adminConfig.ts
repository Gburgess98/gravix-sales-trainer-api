import { supabaseAdmin } from "../lib/supabase";

export type AdminConfig = {
  streak_threshold: number;
  xp_multiplier: number;
  comeback_bonus: number;
  // Day 297 — persisted score-review thresholds (truthful contract, no longer a
  // silent hard-coded fallback). Drive review flags / needs_manager_review /
  // critical-assignment decisions.
  low_score_threshold: number;
  critical_score_threshold: number;
  updated_at: string;
};

const SELECT_COLS =
  "streak_threshold,xp_multiplier,comeback_bonus,low_score_threshold,critical_score_threshold,updated_at";

// Day 294 — idempotently ensure the canonical singleton (id=true) exists using
// database defaults. Safe to call repeatedly (ON CONFLICT DO NOTHING); it never
// overwrites existing values. A genuine transport/schema error is surfaced, never
// swallowed — we do not fabricate success.
async function ensureAdminConfigSingleton(): Promise<void> {
  const { error } = await supabaseAdmin
    .from("admin_config")
    .upsert({ id: true }, { onConflict: "id", ignoreDuplicates: true });

  if (error) {
    throw new Error(`Failed to ensure admin config singleton: ${error.message}`);
  }
}

export async function getAdminConfig(): Promise<AdminConfig> {
  // Resolve exactly the singleton (id=true), tolerating a genuinely absent row.
  const first = await supabaseAdmin
    .from("admin_config")
    .select(SELECT_COLS)
    .eq("id", true)
    .maybeSingle();

  if (first.error) {
    // A real transport/schema error — surface it (do not self-heal over it).
    throw new Error(`Failed to load admin config: ${first.error.message}`);
  }
  if (first.data) return first.data as AdminConfig;

  // Singleton genuinely absent (partially-migrated environment). Restore exactly
  // one canonical row with database defaults, then re-read. If it still fails,
  // surface the error rather than returning fabricated values.
  await ensureAdminConfigSingleton();

  const second = await supabaseAdmin
    .from("admin_config")
    .select(SELECT_COLS)
    .eq("id", true)
    .single();

  if (second.error || !second.data) {
    throw new Error(`Failed to load admin config: ${second.error?.message ?? "No data"}`);
  }

  return second.data as AdminConfig;
}

export async function patchAdminConfig(patch: Partial<Pick<AdminConfig,
  "streak_threshold" | "xp_multiplier" | "comeback_bonus" |
  "low_score_threshold" | "critical_score_threshold"
>>): Promise<AdminConfig> {
  const clean: Record<string, number> = {};

  if (patch.streak_threshold !== undefined) clean.streak_threshold = patch.streak_threshold;
  if (patch.xp_multiplier !== undefined) clean.xp_multiplier = patch.xp_multiplier;
  if (patch.comeback_bonus !== undefined) clean.comeback_bonus = patch.comeback_bonus;
  // Day 297 — thresholds now persist (were silently dropped). Only fields actually
  // supplied are written, so a caller's value is never dropped while claiming success.
  if (patch.low_score_threshold !== undefined) clean.low_score_threshold = patch.low_score_threshold;
  if (patch.critical_score_threshold !== undefined) clean.critical_score_threshold = patch.critical_score_threshold;

  // Day 294 — upsert the canonical singleton (id=true). If the row is genuinely
  // absent it is created (these values over DB defaults); if it exists, ONLY the
  // provided columns are updated (others preserved, updated_at trigger fires as
  // before). This keeps exactly one row and can no longer 500 on a missing
  // singleton — while a genuine DB error is still surfaced, never faked.
  const { data, error } = await supabaseAdmin
    .from("admin_config")
    .upsert({ id: true, ...clean }, { onConflict: "id" })
    .select(SELECT_COLS)
    .single();

  if (error || !data) {
    throw new Error(`Failed to update admin config: ${error?.message ?? "No data"}`);
  }

  return data as AdminConfig;
}
