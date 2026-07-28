// src/lib/sparringBrain/stubBrain.ts
// Deterministic Prospect Brain — no LLM call, no API key. Used by validators/CI
// and as the last link in the fallback chain so a provider outage degrades to a
// sensible buyer line rather than breaking a turn (architecture §5.5).
//
// Given a fixed input it returns a fixed buyer response: the reply is chosen by
// the rules-engine's next buyer move and the conversation length, both of which
// are deterministic for a given input.

import type { NextBuyerMove } from "../../sparring/state";
import type { BrainProvider, ProviderReply } from "./types";

export const STUB_MODEL = "gravix-stub-buyer";

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

export const stubBrain: BrainProvider = {
  name: "stub",
  async generateProspectReply(input): Promise<ProviderReply> {
    const move: NextBuyerMove = input.state?.nextBuyerMove || "ask_question";
    const lines = STUB_LINES[move] || STUB_LINES.ask_question;
    // Deterministic pick: alternate by history length.
    const text = lines[input.history.length % lines.length];
    return { text, model: STUB_MODEL };
  },
};
