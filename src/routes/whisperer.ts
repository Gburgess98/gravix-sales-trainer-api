import express from "express";
import OpenAI from "openai";

const router = express.Router();

type TranscriptTurn = { role: string; text: string };

function normalisePersonaId(raw: string): string {
  const id = (raw || "").toLowerCase().trim();
  switch (id) {
    case "angry":
    case "angry_buyer":
      return "angry";
    case "silent":
    case "ultra_silent":
      return "silent";
    case "cfo":
      return "cfo";
    case "procurement":
    case "procurement_wall":
      return "procurement";
    default:
      return "price_sensitive";
  }
}

function buildSuggestions(opts: {
  personaId: string;
  lastBuyerText: string;
  transcript: TranscriptTurn[];
}): string[] {
  const { personaId, lastBuyerText } = opts;
  const text = lastBuyerText.toLowerCase();
  const suggestions: string[] = [];

  const mentionsPrice = /price|cost|expensive|budget|roi|return on investment/.test(
    text,
  );
  const thinking =
    /think about it|need to think|send me info|email me/.test(text);
  const pushback =
    /not interested|happy with|already using/i.test(lastBuyerText);

  if (personaId === "price_sensitive") {
    if (mentionsPrice) {
      suggestions.push(
        "Acknowledge their price concern, then reframe around ROI and time-to-value. Finish with a binary question like: “If it paid for itself in 60 days, would it be worth exploring?”",
      );
    }
    if (pushback && !mentionsPrice) {
      suggestions.push(
        "Surface the real cost of staying the same. Ask a question that quantifies their current spend or lost revenue, then link it to your solution.",
      );
    }
  }

  if (personaId === "angry") {
    suggestions.push(
      "Lower your tone, acknowledge their frustration, and take responsibility for clarity. Then ask one calm, specific question to regain control.",
    );
  }

  if (personaId === "silent") {
    suggestions.push(
      "Use a light pattern interrupt and ask a short, direct question that forces a choice, like: “Is this more about timing or fit right now?”",
    );
  }

  if (thinking) {
    suggestions.push(
      "Respect the need to think, but lock in a next step: “Totally — why don’t we pencil in 10 minutes on Thursday to see if this is actually worth moving forward?”",
    );
  }

  if (!suggestions.length) {
    suggestions.push(
      "Summarise what they said in one sentence, then ask a sharp follow-up that uncovers the real constraint — budget, timing, or trust.",
    );
  }

  return suggestions;
}

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

async function rewriteSuggestionWithLLM(
  base: string,
  personaId: string,
  opts: { repId: string | null },
): Promise<string> {
  try {
    if (!process.env.OPENAI_API_KEY) return base;

    const personaLabel = personaId.replace(/_/g, " ");
    const repTag = opts.repId ? `The rep id is ${opts.repId}.` : "";

    const resp = await openai.responses.create({
      model: "gpt-4.1-mini",
      input: [
        {
          role: "system",
          content:
            "You rewrite coaching tips for sales reps. Keep them punchy, 1–2 sentences max, concrete and tactical. Do not add disclaimers or emojis.",
        },
        {
          role: "user",
          content: `Persona: ${personaLabel}. ${repTag} Rewrite this coaching tip in a sharper, clearer way:\n\n${base}`,
        },
      ],
      max_output_tokens: 120,
    });

    const out =
      (resp.output[0].content[0] as any)?.text?.trim?.() ??
      (resp.output[0].content as any)?.[0]?.text?.trim?.();

    if (out && typeof out === "string") return out;
    return base;
  } catch (e: any) {
    console.warn(
      "[whisperer] rewriteSuggestionWithLLM failed:",
      e?.message || e,
    );
    return base;
  }
}

// --- Route: POST /v1/whisperer/preview -------------------
router.post("/preview", express.json(), async (req, res) => {
  res.setHeader("Cache-Control", "no-store");

  try {
    const body = (req.body || {}) as {
      personaId?: string;
      transcript?: string | TranscriptTurn[];
    };

    const personaId = normalisePersonaId(body.personaId || "price_sensitive");

    const repIdHeader = req.header("x-user-id");
    const repId =
      repIdHeader && String(repIdHeader).trim().length > 0
        ? String(repIdHeader).trim()
        : null;

    const transcriptRaw = body.transcript;
    let transcript: TranscriptTurn[] = [];

    if (Array.isArray(transcriptRaw)) {
      transcript = (transcriptRaw as any[])
        .filter((t) => t && typeof (t as any).text === "string")
        .map((t) => ({
          role: String((t as any).role || "").toLowerCase(),
          text: String((t as any).text),
        }));
    } else if (typeof transcriptRaw === "string") {
      transcript = [
        {
          role: "buyer",
          text: transcriptRaw,
        },
      ];
    }

    if (!transcript.length) {
      return res.json({
        ok: true,
        personaId,
        suggestions: [],
      });
    }

    const lastBuyerTurn =
      [...transcript].reverse().find((t) => t.role.includes("buyer")) ||
      transcript[transcript.length - 1];

    if (!lastBuyerTurn || !lastBuyerTurn.text?.trim()) {
      return res.json({
        ok: true,
        personaId,
        suggestions: [],
      });
    }

    const baseSuggestions = buildSuggestions({
      personaId,
      lastBuyerText: lastBuyerTurn.text,
      transcript,
    });

    const enhanced: string[] = [];
    for (const s of baseSuggestions) {
      const rewritten = await rewriteSuggestionWithLLM(s, personaId, { repId });
      enhanced.push(rewritten);
    }

    return res.json({
      ok: true,
      personaId,
      suggestions: enhanced,
    });
  } catch (err: any) {
    console.error("[whisperer] preview failed", err);
    return res.status(500).json({
      ok: false,
      error: "whisperer_preview_failed",
      message: err?.message || "Failed to build Whisperer suggestions.",
    });
  }
});

export default router;