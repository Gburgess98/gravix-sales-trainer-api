// Intelligence Layer — Day 218: Context Engine compile + validation helpers.
//
// compileContextBlock turns a company_context.context jsonb payload into the
// bounded plain-text block the scoring prompt will (later) receive. Contract
// per CONTEXT_ENGINE_FIELD_SPEC.md §4:
//   1. Deterministic — pure function of the input, no AI, no network, no
//      randomness. The same context always compiles to the same block.
//   2. Fixed section order: profile → products & services → pricing &
//      positioning → objections → competitors → compliance → tone.
//   3. Empty sections omitted entirely; empty context compiles to "".
//   4. List caps (first N in saved order): offerings ≤ 10 · objections ≤ 12 ·
//      competitors ≤ 6 · no-go phrases ≤ 20 · disclosures ≤ 20.
//   5. Whole-block budget ≈ 1,500 tokens — hard cap MAX_BLOCK_CHARS.
//
// Nothing in the scoring runtime imports this yet — Day 218 is data layer
// only. When integration lands, an empty block must mean "today's exact
// prompt" (zero regression path).

export const CONTEXT_SECTION_KEYS = [
  "profile",
  "offering",
  "objections",
  "competitors",
  "compliance",
  "tone",
] as const;
export type ContextSectionKey = (typeof CONTEXT_SECTION_KEYS)[number];

const MAX_FIELD_CHARS = 1_000; // longtext fields soft-cap at ~800 in the editor
const MAX_BLOCK_CHARS = 6_000; // ≈ 1,500 tokens
const CAPS = { offerings: 10, objections: 12, competitors: 6, no_go: 20, disclosures: 20 };

/** Top-level keys in the payload that are not known sections. */
export function unknownContextKeys(context: Record<string, unknown>): string[] {
  const known = new Set<string>(CONTEXT_SECTION_KEYS);
  return Object.keys(context).filter((k) => !known.has(k));
}

function str(v: unknown): string {
  return typeof v === "string" ? v.trim() : "";
}

function clip(v: unknown): string {
  const s = str(v);
  return s.length > MAX_FIELD_CHARS ? `${s.slice(0, MAX_FIELD_CHARS)}…` : s;
}

function line(label: string, v: unknown): string | null {
  const s = clip(v);
  return s ? `${label}: ${s}` : null;
}

function list(v: unknown, cap: number): Record<string, unknown>[] {
  if (!Array.isArray(v)) return [];
  return v
    .filter((e): e is Record<string, unknown> => !!e && typeof e === "object" && !Array.isArray(e))
    .slice(0, cap);
}

function tags(v: unknown, cap: number): string[] {
  if (!Array.isArray(v)) return [];
  return v.map(str).filter(Boolean).slice(0, cap);
}

function section(header: string, lines: (string | null)[]): string | null {
  const body = lines.filter((l): l is string => !!l);
  return body.length ? `## ${header}\n${body.join("\n")}` : null;
}

/**
 * Compile a context payload into the bounded plain-text block. Pure and
 * deterministic; unknown or malformed fields are skipped, never guessed at.
 */
export function compileContextBlock(context: unknown): string {
  if (!context || typeof context !== "object" || Array.isArray(context)) return "";
  const ctx = context as Record<string, any>;

  const profile = ctx.profile ?? {};
  const offering = ctx.offering ?? {};
  const pricing = offering.pricing_positioning ?? {};
  const compliance = ctx.compliance ?? {};
  const tone = ctx.tone ?? {};

  const sections: (string | null)[] = [
    section("Company profile", [
      line("About", profile.about),
      line("Sales motion", profile.sales_motion),
      line("Sales motion notes", profile.sales_motion_notes),
      line("Ideal customer", profile.icp),
    ]),
    section(
      "Products & services",
      list(offering.products_services, CAPS.offerings).map((p) => {
        const name = clip(p.name);
        const description = clip(p.description);
        if (!name && !description) return null;
        return `- ${[name, description].filter(Boolean).join(": ")}`;
      })
    ),
    section("Pricing & positioning", [
      line("Pricing notes", pricing.pricing_notes),
      line("Positioning", pricing.positioning_notes),
    ]),
    section(
      "Objections & approved responses",
      list(ctx.objections, CAPS.objections).map((o) => {
        const objection = clip(o.objection);
        if (!objection) return null;
        const parts = [`- Objection: ${objection}`];
        const approved = clip(o.approved_response);
        const weak = clip(o.weak_response);
        const notes = clip(o.notes);
        if (approved) parts.push(`  Approved response: ${approved}`);
        if (weak) parts.push(`  Weak response to coach away: ${weak}`);
        if (notes) parts.push(`  Notes: ${notes}`);
        return parts.join("\n");
      })
    ),
    section(
      "Competitors",
      list(ctx.competitors, CAPS.competitors).map((c) => {
        const name = clip(c.name);
        if (!name) return null;
        const parts = [`- ${name}`];
        const notes = clip(c.notes);
        const positioning = clip(c.positioning);
        if (notes) parts.push(`  What they pitch: ${notes}`);
        if (positioning) parts.push(`  How we win: ${positioning}`);
        return parts.join("\n");
      })
    ),
    section("Compliance & no-go (advisory)", [
      tags(compliance.no_go_language, CAPS.no_go).length
        ? `Never say: ${tags(compliance.no_go_language, CAPS.no_go).join(" · ")}`
        : null,
      ...tags(compliance.required_disclosures, CAPS.disclosures).map(
        (d) => `Required disclosure: ${d}`
      ),
    ]),
    section("Tone & coaching style", [
      line("Playbook guidance", tone.playbook_guidance),
      line("Tone notes", tone.tone_notes),
    ]),
  ];

  const block = sections.filter(Boolean).join("\n\n");
  if (block.length <= MAX_BLOCK_CHARS) return block;
  return `${block.slice(0, MAX_BLOCK_CHARS)}\n[Context truncated]`;
}
