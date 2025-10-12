import "dotenv/config";
import OpenAI from "openai";

async function main() {
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const model = process.env.AI_MODEL || "gpt-4o-mini";

  const res = await client.chat.completions.create({
    model,
    messages: [{ role: "user", content: "Respond ONLY with JSON: {\"ping\":\"pong\"}" }],
    response_format: { type: "json_object" },
    temperature: 0,
    max_tokens: 40,
  });

  console.log("SMOKE OK:", res.choices?.[0]?.message?.content);
}

main().catch((e:any) => {
  console.error("SMOKE FAIL:", e?.status, e?.code, e?.message || e);
  process.exit(1);
});