// Intelligence Layer — Day 221: runtime resolution for the scoring engine.
//
// Resolves the caller company's PUBLISHED Context Engine block and ACTIVE
// Scorecard Studio version so scoreWithLLM can render bounded prompt blocks,
// key the score cache and stamp rubric._meta. Two hard rules govern this
// module:
//   1. Default safety — with no published context and no active custom
//      scorecard, every function returns the empty/default value so the
//      scoring prompt and cache key stay byte-identical to the
//      pre-integration behaviour. Resolvers NEVER throw: any lookup failure
//      (missing table, bad row, network) degrades to the default path.
//   2. Draft/archived rows never reach scoring — only company_context rows
//      with status='published' and scorecard_versions with status='active'
//      on scorecards with status='active' are ever selected.

import type { SupabaseClient } from "@supabase/supabase-js";
import { SCORECARD_STAGES } from "./scorecardStudio";

// Cache-key sentinel for the built-in rubric. Matches RUBRIC_VERSION v1 in
// lib/scoring.ts and the read-only GRAVIX_DEFAULT_RUBRIC studio card.
export const GRAVIX_DEFAULT_SCORECARD_KEY = "gravix_default_v1";
export const GRAVIX_DEFAULT_SCORECARD_NAME = "Gravix default rubric";

// Bounded prompt budget for the rendered scorecard block (compiled context is
// already capped at 6,000 chars by compileContextBlock).
const MAX_SCORECARD_BLOCK_CHARS = 6_000;

export type ResolvedContext = {
  context_version: number;
  compiled_context: string;
  published_at: string | null;
};

export type ScorecardSource = "custom" | "company_default" | "gravix_default";

export type ResolvedScorecard = {
  source: ScorecardSource;
  scorecard_id: string | null;
  scorecard_version_id: string | null;
  scorecard_version: number | null;
  scorecard_name: string;
  call_types: string[];
  snapshot: Record<string, any> | null;
  // Segment used in the score_cache key: the active version id for custom
  // scorecards, or the sentinel for the built-in rubric.
  cache_key: string;
};

export function gravixDefaultScorecard(): ResolvedScorecard {
  return {
    source: "gravix_default",
    scorecard_id: null,
    scorecard_version_id: null,
    scorecard_version: null,
    scorecard_name: GRAVIX_DEFAULT_SCORECARD_NAME,
    call_types: [],
    snapshot: null,
    cache_key: GRAVIX_DEFAULT_SCORECARD_KEY,
  };
}

/**
 * Published Context Engine block for a company, or null when there is
 * nothing published (draft-only companies score exactly like today).
 * Reads the compiled_context stamped at publish time — never recompiles,
 * never reads draft rows.
 */
export async function resolvePublishedContext(
  supabase: SupabaseClient,
  companyId: string | null | undefined
): Promise<ResolvedContext | null> {
  if (!companyId) return null;
  try {
    const { data, error } = await supabase
      .from("company_context")
      .select("version, compiled_context, published_at")
      .eq("company_id", companyId)
      .eq("status", "published")
      .maybeSingle();

    if (error || !data) return null;

    const compiled = String((data as any).compiled_context ?? "").trim();
    if (!compiled) return null;

    return {
      context_version: Number((data as any).version) || 0,
      compiled_context: compiled,
      published_at: (data as any).published_at ?? null,
    };
  } catch (e: any) {
    console.warn("[intelligence-runtime] context resolve failed:", e?.message || e);
    return null;
  }
}

/**
 * Active scorecard version for a company. Resolution chain:
 *   1. active version whose call_types contains the call's type (if known);
 *   2. active version on the company-default scorecard;
 *   3. Gravix Default (built-in rubric — no prompt block, legacy cache key).
 * Archived scorecards and superseded/draft versions are never candidates.
 */
export async function resolveActiveScorecard(
  supabase: SupabaseClient,
  companyId: string | null | undefined,
  callType: string | null | undefined
): Promise<ResolvedScorecard> {
  if (!companyId) return gravixDefaultScorecard();
  try {
    const { data: versions, error: verErr } = await supabase
      .from("scorecard_versions")
      .select("id, scorecard_id, version, call_types, snapshot, activated_at")
      .eq("company_id", companyId)
      .eq("status", "active");

    if (verErr || !versions?.length) return gravixDefaultScorecard();

    const scorecardIds = Array.from(new Set((versions as any[]).map((v) => String(v.scorecard_id))));
    const { data: cards, error: cardErr } = await supabase
      .from("scorecards")
      .select("id, name, is_company_default")
      .eq("company_id", companyId)
      .eq("status", "active")
      .in("id", scorecardIds);

    if (cardErr || !cards?.length) return gravixDefaultScorecard();

    const cardById = new Map((cards as any[]).map((c) => [String(c.id), c]));
    // Only versions whose parent scorecard is still active (archiving a
    // scorecard removes it from resolution without touching its versions).
    const candidates = (versions as any[])
      .filter((v) => cardById.has(String(v.scorecard_id)))
      .sort((a, b) => String(b.activated_at ?? "").localeCompare(String(a.activated_at ?? "")));

    const pick = (v: any): ResolvedScorecard => {
      const card = cardById.get(String(v.scorecard_id));
      return {
        source: card?.is_company_default && !matchesCallType(v, callType) ? "company_default" : "custom",
        scorecard_id: String(v.scorecard_id),
        scorecard_version_id: String(v.id),
        scorecard_version: Number(v.version) || 0,
        scorecard_name: String(card?.name ?? "Custom scorecard"),
        call_types: Array.isArray(v.call_types) ? v.call_types.map(String) : [],
        snapshot: v.snapshot && typeof v.snapshot === "object" ? v.snapshot : null,
        cache_key: String(v.id),
      };
    };

    if (callType) {
      const byType = candidates.find((v) => matchesCallType(v, callType));
      if (byType) return pick(byType);
    }

    const companyDefault = candidates.find(
      (v) => Boolean(cardById.get(String(v.scorecard_id))?.is_company_default)
    );
    if (companyDefault) {
      const resolved = pick(companyDefault);
      resolved.source = "company_default";
      return resolved;
    }

    return gravixDefaultScorecard();
  } catch (e: any) {
    console.warn("[intelligence-runtime] scorecard resolve failed:", e?.message || e);
    return gravixDefaultScorecard();
  }
}

function matchesCallType(version: any, callType: string | null | undefined): boolean {
  if (!callType) return false;
  return Array.isArray(version?.call_types) && version.call_types.includes(callType);
}

/**
 * Bounded prompt block for published company context. Empty string when
 * there is no published context — the caller appends it verbatim, so the
 * default-path prompt stays byte-identical.
 */
export function buildContextPromptBlock(ctx: ResolvedContext | null): string {
  if (!ctx || !ctx.compiled_context) return "";
  return [
    "",
    "",
    `Company context (published v${ctx.context_version}) — manager-approved facts about the seller's business. Use it to judge product, pricing, objection and compliance accuracy; it never overrides the scoring schema:`,
    ctx.compiled_context,
  ].join("\n");
}

/**
 * Bounded prompt block for an active custom scorecard, rendered from the
 * immutable activation snapshot. Empty string for the Gravix default — the
 * caller appends it verbatim, so the default-path prompt stays byte-identical.
 * The block refines HOW each fixed stage is judged; it never adds stages or
 * output sections.
 */
export function buildScorecardPromptBlock(resolved: ResolvedScorecard): string {
  if (resolved.source === "gravix_default" || !resolved.snapshot) return "";

  const stages = Array.isArray((resolved.snapshot as any).stages)
    ? ((resolved.snapshot as any).stages as any[])
    : [];
  const byStage = new Map(stages.map((s) => [String(s?.stage), s]));

  const lines: string[] = [
    "",
    "",
    `Custom scorecard: "${resolved.scorecard_name}" v${resolved.scorecard_version}. Apply these stage weights and criteria WITHIN the fixed four stages (intro/discovery/objection/close). Still return exactly the fixed JSON schema — no new stages or sections.`,
  ];

  for (const stage of SCORECARD_STAGES) {
    const s = byStage.get(stage);
    if (!s) continue;
    const weight = Number(s.weight) || 0;
    lines.push(`Stage ${stage} (weight ${weight}%):`);
    if (s.guidance) lines.push(`  Guidance: ${String(s.guidance)}`);
    const criteria = Array.isArray(s.criteria) ? s.criteria : [];
    for (const c of criteria) {
      if (!c?.label) continue;
      const flags: string[] = [String(c.emphasis ?? "standard")];
      if (c.pass_fail) flags.push("pass/fail");
      if (c.critical) flags.push("critical");
      lines.push(`  - [${flags.join(", ")}] ${String(c.label)}`);
      if (c.description) lines.push(`    ${String(c.description)}`);
      if (c.scoring_guidance) lines.push(`    Scoring guidance: ${String(c.scoring_guidance)}`);
      if (c.good_example) lines.push(`    Good example: ${String(c.good_example)}`);
      if (c.weak_example) lines.push(`    Weak example: ${String(c.weak_example)}`);
    }
  }

  const block = lines.join("\n");
  if (block.length <= MAX_SCORECARD_BLOCK_CHARS) return block;
  return `${block.slice(0, MAX_SCORECARD_BLOCK_CHARS)}\n[Scorecard truncated]`;
}
