import { supabaseAdmin } from "../lib/supabase";
export async function getAdminConfig() {
    const { data, error } = await supabaseAdmin
        .from("admin_config")
        .select("streak_threshold,xp_multiplier,comeback_bonus,updated_at")
        .eq("id", true)
        .single();
    if (error || !data) {
        throw new Error(`Failed to load admin config: ${error?.message ?? "No data"}`);
    }
    return data;
}
export async function patchAdminConfig(patch) {
    const clean = {};
    if (patch.streak_threshold !== undefined)
        clean.streak_threshold = patch.streak_threshold;
    if (patch.xp_multiplier !== undefined)
        clean.xp_multiplier = patch.xp_multiplier;
    if (patch.comeback_bonus !== undefined)
        clean.comeback_bonus = patch.comeback_bonus;
    const { data, error } = await supabaseAdmin
        .from("admin_config")
        .update(clean)
        .eq("id", true)
        .select("streak_threshold,xp_multiplier,comeback_bonus,updated_at")
        .single();
    if (error || !data) {
        throw new Error(`Failed to update admin config: ${error?.message ?? "No data"}`);
    }
    return data;
}
