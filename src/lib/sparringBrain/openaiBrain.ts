// src/lib/sparringBrain/openaiBrain.ts
// OpenAI Prospect Brain provider. This is a FAITHFUL move of the Day-101 inline
// call — model, temperature, token cap and fallback line match exactly. It
// stays the default provider until Claude parity is proven.

import OpenAI from "openai";
import type { BrainProvider, ProviderReply } from "./types";

const DEFAULT_OPENAI_MODEL = "gpt-4o-mini";
const FALLBACK_LINE =
  "I'm not convinced yet. Can you explain why this is worth the price?";

let _openai: OpenAI | null = null;
function getOpenAI(): OpenAI {
  if (_openai) return _openai;
  _openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  return _openai;
}

export const openaiBrain: BrainProvider = {
  name: "openai",
  async generateProspectReply(input): Promise<ProviderReply> {
    const model = process.env.OPENAI_SPARRING_MODEL || DEFAULT_OPENAI_MODEL;

    const completion = await getOpenAI().chat.completions.create({
      model,
      messages: [
        { role: "system", content: input.systemPrompt },
        ...input.history,
      ],
      temperature: 0.7,
      max_tokens: 220,
    });

    const text =
      completion.choices[0]?.message?.content?.trim() || FALLBACK_LINE;

    return {
      text,
      model,
      usage: {
        inputTokens: completion.usage?.prompt_tokens,
        outputTokens: completion.usage?.completion_tokens,
      },
    };
  },
};
