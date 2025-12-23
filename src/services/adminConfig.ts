import { supabaseAdmin } from "../lib/supabase";

export type AdminConfig = {
  streak_threshold: number;
  xp_multiplier: number;
  comeback_bonus: number;
  updated_at: string;
};

export async function getAdminConfig(): Promise<AdminConfig> {
  const { data, error } = await supabaseAdmin
    .from("admin_config")
    .select("streak_threshold,xp_multiplier,comeback_bonus,updated_at")
    .eq("id", true)
    .single();

  if (error || !data) {
    throw new Error(`Failed to load admin config: ${error?.message ?? "No data"}`);
  }

  return data as AdminConfig;
}

export async function patchAdminConfig(patch: Partial<Pick<AdminConfig,
  "streak_threshold" | "xp_multiplier" | "comeback_bonus"
>>): Promise<AdminConfig> {
  const clean: any = {};

  if (patch.streak_threshold !== undefined) clean.streak_threshold = patch.streak_threshold;
  if (patch.xp_multiplier !== undefined) clean.xp_multiplier = patch.xp_multiplier;
  if (patch.comeback_bonus !== undefined) clean.comeback_bonus = patch.comeback_bonus;

  const { data, error } = await supabaseAdmin
    .from("admin_config")
    .update(clean)
    .eq("id", true)
    .select("streak_threshold,xp_multiplier,comeback_bonus,updated_at")
    .single();

  if (error || !data) {
    throw new Error(`Failed to update admin config: ${error?.message ?? "No data"}`);
  }

  return data as AdminConfig;
}