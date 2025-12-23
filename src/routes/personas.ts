// src/personas.ts

export type PersonaDifficulty = "easy" | "normal" | "hard" | "nightmare";
export type PersonaMode = "standard" | "time_trial" | "close_in_2m";

export type PersonaConfig = {
  id: string;
  label: string;
  description: string;
  traits: string[];
  difficultyDefault?: PersonaDifficulty;
  modes?: PersonaMode[];
  tags?: string[];
};

export const PERSONAS: PersonaConfig[] = [
  {
    id: "price_sensitive",
    label: "Price Sensitive",
    traits: ["ROI-focused", "Budget restricted"],
    description:
      "Pushes back on price early and often. Fixated on ROI, alternatives, and total cost over time.",
    difficultyDefault: "normal",
    modes: ["standard", "time_trial", "close_in_2m"],
    tags: ["budget", "price", "roi"],
  },
  {
    id: "angry",
    label: "Angry Buyer",
    traits: ["Short fuse", "Interrupts"],
    description:
      "Easily irritated, talks over you, demands strong justification and quick answers.",
    difficultyDefault: "hard",
    modes: ["standard", "time_trial", "close_in_2m"],
    tags: ["pressure", "tone", "control"],
  },
  {
    id: "silent",
    label: "Ultra Silent Mode",
    traits: ["1–3 word answers", "Low engagement"],
    description:
      "Barely gives anything. You must carry the conversation, dig for context, and create momentum.",
    difficultyDefault: "nightmare",
    modes: ["standard", "time_trial", "close_in_2m"],
    tags: ["probing", "discovery", "control"],
  },
  {
    id: "cfo",
    label: "The CFO",
    traits: ["Analytical", "Sceptical", "Risk averse"],
    description:
      "Wants numbers, clear upside, and risk minimisation. Cares about payback period and TCO.",
    difficultyDefault: "hard",
    modes: ["standard", "time_trial", "close_in_2m"],
    tags: ["finance", "risk", "roi"],
  },
  {
    id: "procurement",
    label: "Procurement Wall",
    traits: ["Policy-driven", "Process-focused"],
    description:
      "Everything must go through process, committees, or existing vendors. Loves policy and procedure.",
    difficultyDefault: "normal",
    modes: ["standard", "time_trial", "close_in_2m"],
    tags: ["gatekeeper", "process", "stalling"],
  },
];