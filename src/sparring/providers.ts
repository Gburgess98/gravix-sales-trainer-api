// src/sparring/providers.ts
// TIER 2A — Day 101: provider router skeleton for sparring buyer replies.
//
// Day 101 scope: stub (deterministic) + openai (behaviour-preserving move of
// the existing inline call) + claude skeleton (not configured until Day 102).
// Selection via SPARRING_PROVIDER env (default "openai" = current behaviour).
// A provider failure falls back down the chain and never crashes a turn.

import OpenAI from "openai";
import {
  type SparringState,
  type NextBuyerMove,
  summariseStateForPrompt,
} from "./state";

export type SparringProviderName = "openai" | "claude" | "stub";

export type BuyerReplyInput = {
  // Fully-built system prompt from the existing route (persona, company
  // profile, emotional state, scenario, rep context...)
  systemPrompt: string;
  history: Array<{ role: "user" | "assistant"; content: string }>;
  state?: SparringState | null;
  personaId?: string | null;
  difficulty?: string | null;
};

export type BuyerReplyResult = {
  ok: boolean;
  text: string;
  provider: SparringProviderName;
  fallback: boolean; // true when the configured provider failed and another answered
  latencyMs: number;
};

export interface SparringProvider {
  name: SparringProviderName;
  generateBuyerReply(input: BuyerReplyInput): Promise<{ text: string }>;
}

// ── OpenAI provider (existing behaviour, moved behind the interface) ─────────
// Model, temperature and token cap match the previous inline call exactly.

let _openai: OpenAI | null = null;
function getOpenAI(): OpenAI {
  if (_openai) return _openai;
  _openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  return _openai;
}

const openaiProvider: SparringProvider = {
  name: "openai",
  async generateBuyerReply(input) {
    const completion = await getOpenAI().chat.completions.create({
      model: process.env.OPENAI_SPARRING_MODEL || "gpt-4o-mini",
      messages: [
        { role: "system", content: input.systemPrompt },
        ...input.history,
      ],
      temperature: 0.7,
      max_tokens: 220,
    });

    const text =
      completion.choices[0]?.message?.content?.trim() ||
      "I'm not convinced yet. Can you explain why this is worth the price?";
    return { text };
  },
};

// ── Claude provider (skeleton — live call lands Day 102) ─────────────────────

const claudeProvider: SparringProvider = {
  name: "claude",
  async generateBuyerReply() {
    throw new Error("provider_not_configured: claude lands on Day 102 (needs ANTHROPIC_API_KEY)");
  },
};

// ── Stub provider (deterministic, no API key — dev/CI/fallback) ─────────────

const STUB_LINES: Record<NextBuyerMove, string[]> = {
  ask_question: [
    "Before we go further — how is this different from what we already do today?",
    "Alright. Who else is using this, and what changed for them?",
  ],
  raise_objection: [
    "Honestly, this sounds expensive for what it is. Why shouldn't I just stay with what we have?",
    "I'm struggling to see the value here. Convince me this isn't another cost we don't need.",
  ],
  request_info: [
    "Send me something concrete — numbers, not promises. What exactly would we get?",
    "Okay, walk me through exactly how that would work for a team like ours.",
  ],
  soften: [
    "That's a fair point, actually. Go on — I'm listening.",
    "Hmm, alright. That does address part of my concern.",
  ],
  push_back: [
    "You haven't really answered my concern. I need a proper answer before we move on.",
    "That's a bit vague. Give me a straight answer on the point I raised.",
  ],
  close_window: [
    "I've got another call in a minute — wrap it up. What's the one thing I need to know?",
    "I need to drop off shortly. If there's a next step, name it now.",
  ],
};

const stubProvider: SparringProvider = {
  name: "stub",
  async generateBuyerReply(input) {
    const move: NextBuyerMove = input.state?.nextBuyerMove || "ask_question";
    const lines = STUB_LINES[move] || STUB_LINES.ask_question;
    // Deterministic pick: alternate by history length
    const text = lines[input.history.length % lines.length];
    return { text };
  },
};

const PROVIDERS: Record<SparringProviderName, SparringProvider> = {
  openai: openaiProvider,
  claude: claudeProvider,
  stub: stubProvider,
};

export function resolveProviderName(): SparringProviderName {
  const raw = String(process.env.SPARRING_PROVIDER || "openai").toLowerCase();
  if (raw === "claude" || raw === "stub" || raw === "openai") return raw;
  return "openai";
}

// Build a state-augmented system prompt (state block appended, never replaces
// the existing persona prompt).
export function withStateDirectives(
  systemPrompt: string,
  state?: SparringState | null
): string {
  if (!state) return systemPrompt;
  return `${systemPrompt}\n\n${summariseStateForPrompt(state)}`;
}

/**
 * Generate a buyer reply via the configured provider, falling back down the
 * chain (configured → stub). Never throws — the worst case is a deterministic
 * stub line, so a sparring turn cannot crash on provider availability.
 */
export async function generateBuyerReply(
  input: BuyerReplyInput
): Promise<BuyerReplyResult> {
  const configured = resolveProviderName();
  const chain: SparringProviderName[] =
    configured === "stub" ? ["stub"] : [configured, "stub"];

  const started = Date.now();
  let fallback = false;

  for (const name of chain) {
    try {
      const { text } = await PROVIDERS[name].generateBuyerReply(input);
      return {
        ok: true,
        text,
        provider: name,
        fallback: fallback || name !== configured,
        latencyMs: Date.now() - started,
      };
    } catch (e: any) {
      console.error(`[sparring/providers] ${name} failed:`, e?.message || e);
      fallback = true;
    }
  }

  // Unreachable (stub never throws), but keep the route safe regardless.
  return {
    ok: false,
    text: "I'm still not sure about this. The price feels high compared to what I'm getting.",
    provider: "stub",
    fallback: true,
    latencyMs: Date.now() - started,
  };
}
