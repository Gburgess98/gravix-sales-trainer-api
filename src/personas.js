// src/personas.ts
export const PERSONAS = [
    {
        id: "price_sensitive",
        label: "Price Sensitive",
        traits: ["ROI-focused", "Budget restricted"],
        description: "Pushes back on price early and often. Fixated on ROI, alternatives and discounts.",
        difficultyDefault: "normal",
        modes: ["standard", "time_trial", "close_in_2m"],
        behaviour: {
            tone: "cautious and ROI-focused, slightly sceptical",
            replyLength: "medium",
            pace: "normal",
            objectionFrequency: "high",
            interruptionLevel: "medium",
            pricePressure: "high",
            hangupChanceBase: 0.15,
            patienceTurns: 10,
        },
        difficulty: {
            easy: {
                objectionFrequency: "medium",
                pricePressure: "medium",
                hangupMultiplier: 0.5,
                patienceOffset: +4,
            },
            normal: {
                // baseline
                hangupMultiplier: 1,
                patienceOffset: 0,
            },
            hard: {
                objectionFrequency: "high",
                pricePressure: "high",
                hangupMultiplier: 1.4,
                patienceOffset: -2,
            },
            nightmare: {
                objectionFrequency: "high",
                pricePressure: "high",
                interruptionLevel: "high",
                hangupMultiplier: 2,
                patienceOffset: -4,
            },
        },
    },
    {
        id: "angry",
        label: "Angry Buyer",
        traits: ["Short fuse", "Interrupts"],
        description: "Easily irritated, talks over you and demands strong justification for every claim.",
        difficultyDefault: "hard",
        modes: ["standard", "time_trial", "close_in_2m"],
        behaviour: {
            tone: "frustrated, impatient and sharp",
            replyLength: "short",
            pace: "fast",
            objectionFrequency: "high",
            interruptionLevel: "high",
            pricePressure: "medium",
            hangupChanceBase: 0.25,
            patienceTurns: 8,
        },
        difficulty: {
            easy: {
                objectionFrequency: "medium",
                interruptionLevel: "medium",
                hangupMultiplier: 0.6,
                patienceOffset: +3,
            },
            normal: {
                hangupMultiplier: 1,
                patienceOffset: 0,
            },
            hard: {
                objectionFrequency: "high",
                interruptionLevel: "high",
                hangupMultiplier: 1.5,
                patienceOffset: -2,
            },
            nightmare: {
                objectionFrequency: "high",
                interruptionLevel: "high",
                hangupMultiplier: 2,
                patienceOffset: -4,
            },
        },
    },
    {
        id: "silent",
        label: "Ultra Silent Mode",
        traits: ["1–3 word answers", "Low engagement"],
        description: "Barely gives anything. You must carry the conversation, earn trust and create momentum.",
        difficultyDefault: "nightmare",
        modes: ["standard", "time_trial", "close_in_2m"],
        behaviour: {
            tone: "flat, low energy, guarded",
            replyLength: "short",
            pace: "slow",
            objectionFrequency: "medium",
            interruptionLevel: "low",
            pricePressure: "low",
            hangupChanceBase: 0.2,
            patienceTurns: 7,
        },
        difficulty: {
            easy: {
                replyLength: "medium",
                objectionFrequency: "low",
                hangupMultiplier: 0.5,
                patienceOffset: +4,
            },
            normal: {
                hangupMultiplier: 1,
                patienceOffset: 0,
            },
            hard: {
                objectionFrequency: "medium",
                hangupMultiplier: 1.3,
                patienceOffset: -2,
            },
            nightmare: {
                objectionFrequency: "high",
                hangupMultiplier: 2,
                patienceOffset: -3,
            },
        },
    },
    {
        id: "cfo",
        label: "The CFO",
        traits: ["Analytical", "Sceptical", "Risk averse"],
        description: "Wants numbers, risk minimisation and clear upside justification. Hates fluff.",
        difficultyDefault: "hard",
        modes: ["standard", "time_trial", "close_in_2m"],
        behaviour: {
            tone: "calm, analytical, slightly cold",
            replyLength: "medium",
            pace: "normal",
            objectionFrequency: "high",
            interruptionLevel: "medium",
            pricePressure: "medium",
            hangupChanceBase: 0.18,
            patienceTurns: 9,
        },
        difficulty: {
            easy: {
                objectionFrequency: "medium",
                hangupMultiplier: 0.7,
                patienceOffset: +3,
            },
            normal: {
                hangupMultiplier: 1,
                patienceOffset: 0,
            },
            hard: {
                objectionFrequency: "high",
                hangupMultiplier: 1.4,
                patienceOffset: -2,
            },
            nightmare: {
                objectionFrequency: "high",
                interruptionLevel: "high",
                hangupMultiplier: 2,
                patienceOffset: -4,
            },
        },
    },
    {
        id: "procurement",
        label: "Procurement Wall",
        traits: ["Policy-driven", "Process-focused"],
        description: "Everything must go through process or an existing vendor. Loves policy and red tape.",
        difficultyDefault: "normal",
        modes: ["standard", "time_trial", "close_in_2m"],
        behaviour: {
            tone: "formal, guarded, process-obsessed",
            replyLength: "medium",
            pace: "normal",
            objectionFrequency: "medium",
            interruptionLevel: "medium",
            pricePressure: "medium",
            hangupChanceBase: 0.16,
            patienceTurns: 10,
        },
        difficulty: {
            easy: {
                objectionFrequency: "low",
                hangupMultiplier: 0.6,
                patienceOffset: +3,
            },
            normal: {
                hangupMultiplier: 1,
                patienceOffset: 0,
            },
            hard: {
                objectionFrequency: "high",
                hangupMultiplier: 1.4,
                patienceOffset: -2,
            },
            nightmare: {
                objectionFrequency: "high",
                interruptionLevel: "high",
                hangupMultiplier: 2,
                patienceOffset: -4,
            },
        },
    },
];
export function getPersonaConfig(id) {
    const fallback = PERSONAS[0];
    if (!id)
        return fallback;
    return PERSONAS.find((p) => p.id === id) || fallback;
}
export function buildPersonaBehaviourSummary(persona, difficulty) {
    const mod = persona.difficulty[difficulty] || persona.difficulty.normal;
    const beh = persona.behaviour;
    const parts = [];
    parts.push(`Tone: ${beh.tone}.`);
    parts.push(`Reply length: ${beh.replyLength} sentences, pace ${beh.pace}.`);
    parts.push(`Objection frequency: ${mod.objectionFrequency || beh.objectionFrequency}, interruption level: ${mod.interruptionLevel || beh.interruptionLevel}, price pressure: ${mod.pricePressure || beh.pricePressure}.`);
    parts.push(`Patience: around ${beh.patienceTurns + (mod.patienceOffset || 0)} turns before they mentally check out. Hangup chance scales with mistakes (base ${(beh.hangupChanceBase * (mod.hangupMultiplier || 1)).toFixed(2)}).`);
    return parts.join(" ");
}
