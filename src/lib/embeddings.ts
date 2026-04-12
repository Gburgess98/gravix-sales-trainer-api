

import OpenAI from "openai";
import type { SupabaseClient } from "@supabase/supabase-js";

const EMBEDDING_MODEL = process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";

type KnowledgeSourceType = "company_playbook" | "rep_memory" | "call" | "manual_note";
type KnowledgeStage = "intro" | "discovery" | "objection" | "close" | "general";

type UpsertKnowledgeEmbeddingInput = {
  companyId?: string | null;
  userId?: string | null;
  sourceType: KnowledgeSourceType;
  sourceId?: string | null;
  stage?: KnowledgeStage | null;
  title?: string | null;
  content: string;
  metadata?: Record<string, any> | null;
};

type SearchKnowledgeEmbeddingsInput = {
  companyId?: string | null;
  userId?: string | null;
  matchCount?: number;
  query: string;
  sourceTypes?: KnowledgeSourceType[];
  stage?: KnowledgeStage | null;
};

function getOpenAIClient(): OpenAI {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is missing");
  }
  return new OpenAI({ apiKey });
}

function cleanEmbeddingText(text: string): string {
  return String(text || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 8000);
}

async function createEmbedding(text: string): Promise<number[]> {
  const input = cleanEmbeddingText(text);
  if (!input) {
    throw new Error("Cannot create embedding for empty text");
  }

  const openai = getOpenAIClient();
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input,
  });

  const vector = response.data?.[0]?.embedding;
  if (!vector || !Array.isArray(vector)) {
    throw new Error("Embedding response did not contain a valid vector");
  }

  return vector;
}

export async function upsertKnowledgeEmbedding(
  supabase: SupabaseClient,
  input: UpsertKnowledgeEmbeddingInput
) {
  const content = cleanEmbeddingText(input.content);
  if (!content) {
    throw new Error("content is required for upsertKnowledgeEmbedding");
  }

  const embedding = await createEmbedding(content);

  const payload = {
    company_id: input.companyId ?? null,
    user_id: input.userId ?? null,
    source_type: input.sourceType,
    source_id: input.sourceId ?? null,
    stage: input.stage ?? "general",
    title: input.title ?? null,
    content,
    embedding,
    metadata: input.metadata ?? {},
  };

  if (input.sourceId) {
    const { data: existing, error: existingErr } = await supabase
      .from("knowledge_embeddings")
      .select("id")
      .eq("source_type", input.sourceType)
      .eq("source_id", input.sourceId)
      .maybeSingle();

    if (existingErr) throw existingErr;

    if ((existing as any)?.id) {
      const { data, error } = await supabase
        .from("knowledge_embeddings")
        .update(payload)
        .eq("id", (existing as any).id)
        .select()
        .single();

      if (error) throw error;
      return data;
    }
  }

  const { data, error } = await supabase
    .from("knowledge_embeddings")
    .insert(payload)
    .select()
    .single();

  if (error) throw error;
  return data;
}

export async function deleteKnowledgeEmbeddingBySource(
  supabase: SupabaseClient,
  args: { sourceType: KnowledgeSourceType; sourceId: string }
) {
  const { error } = await supabase
    .from("knowledge_embeddings")
    .delete()
    .eq("source_type", args.sourceType)
    .eq("source_id", args.sourceId);

  if (error) throw error;
}

export async function searchKnowledgeEmbeddings(
  supabase: SupabaseClient,
  input: SearchKnowledgeEmbeddingsInput
) {
  const matchCount = Math.max(1, Math.min(input.matchCount ?? 8, 20));
  const queryEmbedding = await createEmbedding(input.query);

  const { data, error } = await supabase.rpc("match_knowledge_embeddings", {
    query_embedding: queryEmbedding,
    match_count: matchCount,
    filter_company_id: input.companyId ?? null,
    filter_user_id: input.userId ?? null,
    filter_source_types: input.sourceTypes ?? null,
    filter_stage: input.stage ?? null,
  });

  if (error) throw error;
  return data ?? [];
}

export async function syncCompanyPlaybookEmbedding(
  supabase: SupabaseClient,
  playbook: {
    id: string;
    company_id?: string | null;
    stage?: string | null;
    title?: string | null;
    description?: string | null;
    guidance?: string | null;
    example?: string | null;
    objection_type?: string | null;
    priority?: number | null;
    is_active?: boolean | null;
  }
) {
  const content = [
    playbook.title ? `Title: ${playbook.title}` : null,
    playbook.description ? `Description: ${playbook.description}` : null,
    playbook.guidance ? `Guidance: ${playbook.guidance}` : null,
    playbook.example ? `Example: ${playbook.example}` : null,
    playbook.objection_type ? `Objection Type: ${playbook.objection_type}` : null,
  ]
    .filter(Boolean)
    .join("\n");

  if (!content.trim()) {
    throw new Error("Cannot sync company playbook embedding without content");
  }

  return upsertKnowledgeEmbedding(supabase, {
    companyId: playbook.company_id ?? null,
    userId: null,
    sourceType: "company_playbook",
    sourceId: playbook.id,
    stage: (playbook.stage as KnowledgeStage | null) ?? "general",
    title: playbook.title ?? null,
    content,
    metadata: {
      objection_type: playbook.objection_type ?? null,
      priority: playbook.priority ?? null,
      is_active: playbook.is_active ?? true,
    },
  });
}

export async function syncRepMemoryEmbedding(
  supabase: SupabaseClient,
  repMemory: {
    id: string;
    user_id: string;
    company_id?: string | null;
    avg_score?: number | null;
    intro_score?: number | null;
    discovery_score?: number | null;
    objection_score?: number | null;
    close_score?: number | null;
    strengths?: string[] | null;
    weaknesses?: string[] | null;
    coaching_focus?: string[] | null;
    call_count?: number | null;
  }
) {
  const content = [
    `Average score: ${repMemory.avg_score ?? "n/a"}`,
    `Intro score: ${repMemory.intro_score ?? "n/a"}`,
    `Discovery score: ${repMemory.discovery_score ?? "n/a"}`,
    `Objection score: ${repMemory.objection_score ?? "n/a"}`,
    `Close score: ${repMemory.close_score ?? "n/a"}`,
    `Strengths: ${(repMemory.strengths ?? []).join(", ") || "none"}`,
    `Weaknesses: ${(repMemory.weaknesses ?? []).join(", ") || "none"}`,
    `Coaching focus: ${(repMemory.coaching_focus ?? []).join(", ") || "none"}`,
    `Call count: ${repMemory.call_count ?? 0}`,
  ].join("\n");

  return upsertKnowledgeEmbedding(supabase, {
    companyId: repMemory.company_id ?? null,
    userId: repMemory.user_id,
    sourceType: "rep_memory",
    sourceId: repMemory.id,
    stage: "general",
    title: "Rep memory profile",
    content,
    metadata: {
      avg_score: repMemory.avg_score ?? null,
      call_count: repMemory.call_count ?? 0,
    },
  });
}

export { EMBEDDING_MODEL };