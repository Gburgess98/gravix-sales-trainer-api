import { createClient } from "@supabase/supabase-js";
const rawUrl = (process.env.SUPABASE_URL || "").trim();
const rawKey = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
if (!rawUrl.startsWith("http")) {
    throw new Error(`Bad SUPABASE_URL: "${rawUrl}"`);
}
if (!rawKey) {
    throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
}
export const supabaseAdmin = createClient(rawUrl, rawKey, {
    auth: { persistSession: false },
});
