import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const supabase = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false },
  });

  const since = new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString();

  const { data: calls } = await supabase
    .from("calls")
    .select("user_id, score_overall")
    .gte("created_at", since);

  const xp: Record<string, number> = {};
  for (const c of calls || []) {
    const uid = (c as any).user_id;
    const sc = Number((c as any).score_overall);
    if (uid && Number.isFinite(sc) && sc >= 70) xp[uid] = (xp[uid] || 0) + 10;
  }

  const { data: assigns } = await supabase
    .from("coach_assignments")
    .select("assignee_user_id, status")
    .gte("created_at", since);

  for (const a of assigns || []) {
    const uid = (a as any).assignee_user_id;
    if (uid && (a as any).status === "completed") xp[uid] = (xp[uid] || 0) + 5;
  }

  console.log("XP (30d):", xp);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});