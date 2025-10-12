import { getOpenAI, AI_MODEL, OPENAI_TIMEOUT_MS } from "./openai";
function sectionSchema() {
    return {
        type: "object",
        properties: {
            score: { type: "integer", minimum: 0, maximum: 100 },
            notes: { type: "string", maxLength: 300 },
        },
        required: ["score", "notes"],
        additionalProperties: false,
    };
}
const JSON_SCHEMA = {
    name: "SalesCallScore",
    schema: {
        type: "object",
        properties: {
            model: { type: "string" },
            overall: { type: "integer", minimum: 0, maximum: 100 },
            intro: sectionSchema(),
            discovery: sectionSchema(),
            objection: sectionSchema(),
            close: sectionSchema(),
        },
        required: ["model", "overall", "intro", "discovery", "objection", "close"],
        additionalProperties: false,
    },
    strict: true,
};
function clamp(n) {
    return Math.max(0, Math.min(100, Math.round(n)));
}
export function heuristicScoreFallback() {
    const pick = () => Math.max(55, Math.min(85, Math.round(70 + (Math.random() * 16 - 8))));
    const s = { score: pick(), notes: "Heuristic fallback based on minimal call metadata." };
    return {
        model: "heuristic:v1",
        overall: s.score,
        intro: { ...s },
        discovery: { ...s },
        objection: { ...s },
        close: { ...s },
    };
}
export async function scoreWithLLM(opts) {
    const { supabase, callId } = opts;
    try {
        // Minimal, schema-agnostic select
        const { data: call, error: callErr } = await supabase
            .from("calls")
            .select("id, filename")
            .eq("id", callId)
            .single();
        if (callErr || !call)
            throw new Error("call_not_found");
        // Build user prompt with plain strings (no backticks)
        const userLines = [
            'CALL META: filename="' + (call.filename || call.id) + '"',
            "TRANSCRIPT: (not available in MVP)",
            "",
            "Rubric guide:",
            "- Intro: pattern interrupt, clear reason, agenda set.",
            "- Discovery: deep questions, pain/impact, budget/timeline, authority.",
            "- Objection: isolates true objection, reframes value, tests commitment.",
            "- Close: clear next step, assumptive/binary ask, time/date locked.",
        ];
        const user = userLines.join("\n");
        const system = "You are a strict sales call evaluator. Score from 0–100 overall and for Intro, Discovery, Objection Handling, Close. Be concise. Output must match the provided JSON schema exactly.";
        const openai = getOpenAI();
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), OPENAI_TIMEOUT_MS);
        const resp = await openai.chat.completions.create({
            model: AI_MODEL,
            messages: [
                { role: "system", content: system },
                { role: "user", content: user },
            ],
            response_format: { type: "json_schema", json_schema: JSON_SCHEMA },
            temperature: 0.2,
        }, { signal: ctrl.signal });
        clearTimeout(timer);
        const raw = resp.choices?.[0]?.message?.content;
        if (!raw)
            throw new Error("no_model_content");
        const parsed = JSON.parse(raw);
        // Clamp & tidy
        parsed.model = AI_MODEL;
        parsed.overall = clamp(parsed.overall);
        ["intro", "discovery", "objection", "close"].forEach((k) => {
            parsed[k].score = clamp(parsed[k].score);
            parsed[k].notes = (parsed[k].notes || "").slice(0, 300);
        });
        const rubric = {
            intro: parsed.intro,
            discovery: parsed.discovery,
            objection: parsed.objection,
            close: parsed.close,
        };
        // Persist LLM result
        const { error: upErr } = await supabase
            .from("calls")
            .update({
            score_overall: parsed.overall,
            rubric,
            ai_model: parsed.model,
            scored_at: new Date().toISOString(),
        })
            .eq("id", callId);
        if (upErr)
            throw upErr;
        return parsed;
    }
    catch (err) {
        console.warn("[scoreWithLLM] LLM failed, using heuristic:", err?.status ?? "", err?.code ?? "", err?.message ?? err);
        const fb = heuristicScoreFallback();
        await supabase
            .from("calls")
            .update({
            score_overall: fb.overall,
            rubric: {
                intro: fb.intro,
                discovery: fb.discovery,
                objection: fb.objection,
                close: fb.close,
            },
            ai_model: fb.model,
            scored_at: new Date().toISOString(),
        })
            .eq("id", callId);
        return fb;
    }
}
