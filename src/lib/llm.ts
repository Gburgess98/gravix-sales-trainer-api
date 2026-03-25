

import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

export async function callLLM(prompt: string): Promise<string> {
  const res = await client.responses.create({
    model: "gpt-4o-mini",
    input: prompt,
  });

  return res.output_text || "";
}