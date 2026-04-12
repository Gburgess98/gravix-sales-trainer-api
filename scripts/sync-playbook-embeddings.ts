import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { syncCompanyPlaybookEmbedding } from "../src/lib/embeddings";

async function main() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing");
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  });

  const { data: playbooks, error } = await supabase
    .from("company_playbook")
    .select("*")
    .order("priority", { ascending: true });

  if (error) throw error;

  if (!playbooks || playbooks.length === 0) {
    console.log("[sync-playbook-embeddings] No company_playbook rows found.");
    return;
  }

  console.log(`[sync-playbook-embeddings] Found ${playbooks.length} playbook rows.`);

  let synced = 0;
  let failed = 0;

  for (const row of playbooks) {
    try {
      await syncCompanyPlaybookEmbedding(supabase, row as any);
      synced += 1;
      console.log(
        `[sync-playbook-embeddings] Synced ${row.id} (${row.title ?? "untitled"})`
      );
    } catch (e: any) {
      failed += 1;
      console.error(
        `[sync-playbook-embeddings] Failed ${row.id} (${row.title ?? "untitled"}):`,
        e?.message || e
      );
    }
  }

  console.log(
    `[sync-playbook-embeddings] Done. synced=${synced} failed=${failed}`
  );
}

main().catch((err) => {
  console.error("[sync-playbook-embeddings] Fatal:", err?.message || err);
  process.exit(1);
});