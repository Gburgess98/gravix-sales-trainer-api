// 🔥 ORG CONFIG (Day 66)
import { createClient } from "@supabase/supabase-js";
const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
export async function getOrgCallVisibility(orgId) {
    try {
        const { data, error } = await supa
            .from("org_settings")
            .select("call_visibility")
            .eq("org_id", orgId)
            .maybeSingle();
        if (error)
            throw error;
        return data?.call_visibility || "everyone"; // default
    }
    catch (e) {
        console.warn("[org.config] fallback", e);
        return "everyone";
    }
}
