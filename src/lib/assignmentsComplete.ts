import { createClient } from "@supabase/supabase-js";

type CompletedBy = "rep" | "system";

export async function completeAssignmentsForTarget(opts: {
  repId: string;
  type: "sparring" | "call_review" | "custom";
  targetId: string | null;
  completedBy?: CompletedBy;
}) {
  const { repId, type, targetId, completedBy = "system" } = opts;

  if (!repId) return;

  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY!;
  const supa = createClient(url, key, { auth: { persistSession: false } });

  // Mark any matching assignments completed (idempotent)
  const { error } = await supa
    .from("assignments")
    .update({
      status: "completed",
      completed_at: new Date().toISOString(),
      completed_by: completedBy,
    })
    .eq("rep_id", repId)
    .eq("type", type)
    .eq("target_id", targetId)
    .neq("status", "completed");

  if (error) throw error;
}