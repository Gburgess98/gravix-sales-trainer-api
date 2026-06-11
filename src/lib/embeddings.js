import OpenAI from "openai";
const EMBEDDING_MODEL = process.env.OPENAI_EMBEDDING_MODEL || "text-embedding-3-small";
function getOpenAIClient() {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
        throw new Error("OPENAI_API_KEY is missing");
    }
    return new OpenAI({ apiKey });
}
function cleanEmbeddingText(text) {
    return String(text || "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 8000);
}
async function createEmbedding(text) {
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
export async function upsertKnowledgeEmbedding(supabase, input) {
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
        if (existingErr)
            throw existingErr;
        if (existing?.id) {
            const { data, error } = await supabase
                .from("knowledge_embeddings")
                .update(payload)
                .eq("id", existing.id)
                .select()
                .single();
            if (error)
                throw error;
            return data;
        }
    }
    const { data, error } = await supabase
        .from("knowledge_embeddings")
        .insert(payload)
        .select()
        .single();
    if (error)
        throw error;
    return data;
}
export async function deleteKnowledgeEmbeddingBySource(supabase, args) {
    const { error } = await supabase
        .from("knowledge_embeddings")
        .delete()
        .eq("source_type", args.sourceType)
        .eq("source_id", args.sourceId);
    if (error)
        throw error;
}
export async function searchKnowledgeEmbeddings(supabase, input) {
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
    if (error)
        throw error;
    return data ?? [];
}
export async function syncCompanyPlaybookEmbedding(supabase, playbook) {
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
        stage: playbook.stage ?? "general",
        title: playbook.title ?? null,
        content,
        metadata: {
            objection_type: playbook.objection_type ?? null,
            priority: playbook.priority ?? null,
            is_active: playbook.is_active ?? true,
        },
    });
}
export async function syncRepMemoryEmbedding(supabase, repMemory) {
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
