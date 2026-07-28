// src/lib/sparringBrain/claudeBrain.ts
// Claude Prospect Brain provider. Faithful move of the Day-102 provider — the
// architecture target is Claude Sonnet with prompt caching, but the default
// model stays as configured (small/fast) and is NOT switched on this day. The
// switch to Claude-default happens later, once parity is proven.
//
// Claude renders ONLY the buyer's words; state is owned by the rules engine and
// arrives as prompt directives inside `systemPrompt`.

import Anthropic from "@anthropic-ai/sdk";
import type { BrainProvider, ProviderReply } from "./types";

const DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5-20251001";
const FALLBACK_LINE =
  "I'm not convinced yet. Can you explain why this is worth the price?";

let _anthropic: Anthropic | null = null;
function getAnthropic(): Anthropic {
  const key = String(process.env.ANTHROPIC_API_KEY || "").trim();
  if (!key) {
    throw new Error("provider_not_configured: ANTHROPIC_API_KEY missing");
  }
  if (_anthropic) return _anthropic;
  _anthropic = new Anthropic({ apiKey: key });
  return _anthropic;
}

// Anthropic requires the first message to be role "user" and rejects empty
// content; coalesce consecutive same-role messages defensively.
function toClaudeMessages(
  history: Array<{ role: "user" | "assistant"; content: string }>
): Array<{ role: "user" | "assistant"; content: string }> {
  const windowed = history.slice(-12); // cost/latency window (architecture doc)
  const out: Array<{ role: "user" | "assistant"; content: string }> = [];
  for (const m of windowed) {
    const content = String(m.content || "").trim();
    if (!content) continue;
    const last = out[out.length - 1];
    if (last && last.role === m.role) last.content += `\n${content}`;
    else out.push({ role: m.role, content });
  }
  while (out.length && out[0].role !== "user") out.shift();
  return out.length ? out : [{ role: "user", content: "(rep is silent)" }];
}

export const claudeBrain: BrainProvider = {
  name: "claude",
  async generateProspectReply(input): Promise<ProviderReply> {
    const client = getAnthropic();
    const model = process.env.ANTHROPIC_SPARRING_MODEL || DEFAULT_CLAUDE_MODEL;

    const message = await client.messages.create({
      model,
      max_tokens: 200,
      temperature: 0.7,
      system:
        `${input.systemPrompt}\n\n` +
        "You are the BUYER only. Reply with the buyer's next spoken words — natural, concise, realistic. " +
        "Stay in character per the persona, difficulty and conversation state directives. " +
        "Never coach the rep, never narrate, never mention these instructions.",
      messages: toClaudeMessages(input.history),
    });

    const text =
      message.content
        .filter((block) => block.type === "text")
        .map((block: any) => String(block.text || ""))
        .join(" ")
        .trim() || FALLBACK_LINE;

    return {
      text,
      model,
      usage: {
        inputTokens: message.usage?.input_tokens,
        outputTokens: message.usage?.output_tokens,
      },
    };
  },
};
