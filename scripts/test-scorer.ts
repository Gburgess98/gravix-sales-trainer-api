import * as path from "node:path";
import * as dotenv from "dotenv";
dotenv.config({ path: path.resolve(process.cwd(), ".env") });
import { createClient } from "@supabase/supabase-js";
import { scoreWithLLM } from "../src/lib/scoring";

async function main() {
  const callId = process.argv[2];
  if (!callId) {
    console.error("Usage: pnpm tsx api/scripts/test-scorer.ts <CALL_ID>");
    process.exit(1);
  }

  const supabase = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false },
  });

  const res = await scoreWithLLM({ supabase, callId });
  console.log("Scored:", res);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});