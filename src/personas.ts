// api/src/personas.ts
export type Persona = {
  id: string;
  name: string;
  style: string;
  difficulty: "Easy" | "Medium" | "Hard";
  intro: string;
  exampleOpeners: string[];
  coachingFocus: string[];
};

export const personas: Persona[] = [
  {
    id: "price_sensitive",
    name: "Price-Sensitive Buyer",
    style: "Direct and cost-focused",
    difficulty: "Medium",
    intro:
      "You're speaking with a buyer who fixates on price and discounts. They often say things like 'That’s too expensive.'",
    exampleOpeners: [
      "Yeah, but your competitor is cheaper.",
      "I like it, but I can’t justify the price.",
    ],
    coachingFocus: [
      "Handle price objections confidently",
      "Reframe value vs cost",
      "Maintain control and tone under pressure",
    ],
  },
  {
    id: "indecisive",
    name: "Indecisive Prospect",
    style: "Friendly but uncertain",
    difficulty: "Easy",
    intro:
      "A hesitant buyer who keeps saying they need to 'think about it' or talk to someone else.",
    exampleOpeners: [
      "I’ll have to check with my partner.",
      "I’m not sure if now’s the right time.",
    ],
    coachingFocus: [
      "Guide toward a decision",
      "Use assumptive closing",
      "Show empathy while steering urgency",
    ],
  },
  {
    id: "dominant_buyer",
    name: "Dominant Buyer",
    style: "Assertive, fast-paced, skeptical",
    difficulty: "Hard",
    intro:
      "A confident decision-maker who interrupts and challenges authority. You must match energy and authority.",
    exampleOpeners: [
      "Cut to the chase—what’s your best offer?",
      "I’ve heard it all before, convince me why this matters.",
    ],
    coachingFocus: [
      "Match pace and confidence",
      "Avoid filler language",
      "Use firm, concise closes",
    ],
  },
];