import { createClient } from "@supabase/supabase-js";
import { getOpenAI, AI_MODEL, OPENAI_TIMEOUT_MS } from "./openai";
import { postScoreSummary } from "./slack";
import { sendReviewFeedbackEmail } from "./email";
import { cleanTranscript, buildSegments, findNearestSegment } from "./transcript";
import { syncRepMemoryEmbedding, searchKnowledgeEmbeddings } from "./embeddings";
async function getScoringKnowledgeContext(supabase, args) {
    const query = String(args.transcript || "").trim();
    if (!query) {
        return {
            playbookText: "",
            repMemoryText: "",
        };
    }
    let playbookMatches = [];
    let repMatches = [];
    try {
        playbookMatches = await searchKnowledgeEmbeddings(supabase, {
            companyId: args.companyId ?? null,
            userId: null,
            query,
            sourceTypes: ["company_playbook"],
            matchCount: 3,
        });
    }
    catch (e) {
        console.warn("[knowledge] playbook search failed", e?.message || e);
    }
    try {
        repMatches = await searchKnowledgeEmbeddings(supabase, {
            companyId: args.companyId ?? null,
            userId: args.userId ?? null,
            query,
            sourceTypes: ["rep_memory"],
            matchCount: 2,
        });
    }
    catch (e) {
        console.warn("[knowledge] rep memory search failed", e?.message || e);
    }
    const playbookText = playbookMatches.length
        ? playbookMatches
            .map((m, i) => {
            return [
                `Playbook ${i + 1}: ${m.title || "Untitled"}`,
                `Stage: ${m.stage || "general"}`,
                `Content: ${m.content || ""}`,
            ].join("\n");
        })
            .join("\n\n")
        : "";
    const repMemoryText = repMatches.length
        ? repMatches
            .map((m, i) => {
            return [
                `Rep Memory ${i + 1}: ${m.title || "Rep profile"}`,
                `Content: ${m.content || ""}`,
            ].join("\n");
        })
            .join("\n\n")
        : "";
    return {
        playbookText,
        repMemoryText,
    };
}
export const RUBRIC_VERSION = "v1"; // bump when rubric changes
export const SCORING_PROMPT_VERSION = "v1";
export const SCORING_MODEL_VERSION = `${AI_MODEL}:${SCORING_PROMPT_VERSION}:${RUBRIC_VERSION}`;
const SKIP_SCORING_SIDE_EFFECTS = process.env.SKIP_SCORING_SIDE_EFFECTS === "1";
function stableHash(input) {
    let hash = 2166136261;
    for (let i = 0; i < input.length; i += 1) {
        hash ^= input.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(16).padStart(8, "0");
}
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
function stagesSchema() {
    return {
        type: "object",
        properties: {
            intro: sectionSchema(),
            discovery: sectionSchema(),
            objection: sectionSchema(),
            close: sectionSchema(),
        },
        required: ["intro", "discovery", "objection", "close"],
        additionalProperties: false,
    };
}
function momentsSchema() {
    return {
        type: "array",
        items: {
            type: "object",
            additionalProperties: false,
            properties: {
                timestamp: {
                    anyOf: [
                        { type: "number" },
                        { type: "null" }
                    ]
                },
                type: {
                    type: "string",
                    enum: ["objection", "mistake", "highlight", "closing_attempt"],
                },
                text: { type: "string", maxLength: 280 },
                severity: {
                    type: "string",
                    enum: ["low", "medium", "high"],
                },
            },
            required: ["timestamp", "type", "text", "severity"]
        },
    };
}
function suggestionsSchema() {
    return {
        type: "array",
        items: { type: "string", maxLength: 220 },
    };
}
const JSON_SCHEMA = {
    name: "SalesCallScore",
    schema: {
        type: "object",
        properties: {
            model: { type: "string" },
            overall: { type: "integer", minimum: 0, maximum: 100 },
            summary: { type: "string", maxLength: 220 },
            stages: stagesSchema(),
            moments: momentsSchema(),
            suggestions: suggestionsSchema(),
        },
        required: ["model", "overall", "summary", "stages", "moments", "suggestions"],
        additionalProperties: false,
    },
    strict: true,
};
function clamp(n) {
    return Math.max(0, Math.min(100, Math.round(n)));
}
function clampText(input, max = 300) {
    return String(input || "").trim().slice(0, max);
}
function normaliseSection(section, key) {
    if (!section || typeof section !== "object") {
        throw new Error(`invalid_score_schema:${key}`);
    }
    const score = Number(section.score);
    if (!Number.isFinite(score)) {
        throw new Error(`invalid_score_schema:${key}.score`);
    }
    const notes = clampText(section.notes, 300);
    if (!notes) {
        throw new Error(`invalid_score_schema:${key}.notes`);
    }
    return {
        score: clamp(score),
        notes,
    };
}
function normaliseStages(input) {
    const source = input?.stages && typeof input.stages === "object" ? input.stages : input;
    return {
        intro: normaliseSection(source?.intro, "intro"),
        discovery: normaliseSection(source?.discovery, "discovery"),
        objection: normaliseSection(source?.objection, "objection"),
        close: normaliseSection(source?.close, "close"),
    };
}
function normaliseMoments(input) {
    if (!Array.isArray(input))
        return [];
    return input
        .map((moment) => {
        const type = String(moment?.type || "").trim();
        const text = clampText(moment?.text, 280);
        const severity = String(moment?.severity || "").trim();
        const timestamp = Number(moment?.timestamp);
        if (!type || !text)
            return null;
        if (!["objection", "mistake", "highlight", "closing_attempt"].includes(type))
            return null;
        return {
            type: type,
            text,
            severity: ["low", "medium", "high"].includes(severity)
                ? severity
                : undefined,
            timestamp: Number.isFinite(timestamp) ? timestamp : undefined,
        };
    })
        .filter(Boolean);
}
function normaliseSuggestions(input) {
    if (!Array.isArray(input))
        return [];
    return input
        .map((x) => clampText(x, 220))
        .filter(Boolean)
        .slice(0, 6);
}
function normaliseVoiceScore(input) {
    const fallback = computeVoiceScore("", null);
    if (!input || typeof input !== "object")
        return fallback;
    return {
        clarity: clamp(Number(input.clarity ?? fallback.clarity)),
        confidence: clamp(Number(input.confidence ?? fallback.confidence)),
        filler_density: clamp(Number(input.filler_density ?? fallback.filler_density)),
        pace: clamp(Number(input.pace ?? fallback.pace)),
        overall: clamp(Number(input.overall ?? fallback.overall)),
    };
}
function normaliseStoredScore(input) {
    const stages = normaliseStages(input);
    const voice = normaliseVoiceScore(input?.voice ?? input?.rubric?._meta?.voice ?? null);
    return {
        model: clampText(input?.model, 120) || AI_MODEL,
        overall: clamp(Number(input?.overall ?? 0)),
        summary: clampText(input?.summary, 220) || "No summary available.",
        stages,
        moments: normaliseMoments(input?.moments),
        suggestions: normaliseSuggestions(input?.suggestions),
        voice,
    };
}
function parseAndValidateScoreResponse(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    }
    catch {
        throw new Error("invalid_score_schema:json_parse_failed");
    }
    if (!parsed || typeof parsed !== "object") {
        throw new Error("invalid_score_schema:root");
    }
    const overall = Number(parsed.overall);
    if (!Number.isFinite(overall)) {
        throw new Error("invalid_score_schema:overall");
    }
    const summary = clampText(parsed.summary, 220);
    if (!summary) {
        throw new Error("invalid_score_schema:summary");
    }
    return {
        model: clampText(parsed.model, 120) || AI_MODEL,
        overall: clamp(overall),
        summary,
        stages: normaliseStages(parsed.stages),
        moments: normaliseMoments(parsed.moments),
        suggestions: normaliseSuggestions(parsed.suggestions),
    };
}
function computeVoiceScore(transcript, durationSec) {
    const text = String(transcript || "").trim();
    const words = text ? text.split(/\s+/).filter(Boolean) : [];
    const wordCount = words.length;
    if (!wordCount || !durationSec || durationSec <= 0) {
        return {
            clarity: 60,
            confidence: 55,
            filler_density: 0,
            pace: 50,
            overall: 55,
        };
    }
    const minutes = durationSec / 60;
    const wpm = minutes > 0 ? wordCount / minutes : 0;
    const fillerMatches = text.match(/\b(um|uh|erm|er|like|you know|sort of|kind of|basically|literally)\b/gi) || [];
    const fillerCount = fillerMatches.length;
    const fillerDensityRaw = wordCount > 0 ? (fillerCount / wordCount) * 100 : 0;
    const sentenceParts = text
        .split(/[.!?\n]+/)
        .map((x) => x.trim())
        .filter(Boolean);
    const sentenceCount = sentenceParts.length || 1;
    const avgWordsPerSentence = wordCount / sentenceCount;
    const pacePenalty = Math.min(45, Math.abs(wpm - 145) * 0.45);
    const pace = clamp(100 - pacePenalty);
    const clarity = clamp(100 - fillerDensityRaw * 4 - Math.max(0, avgWordsPerSentence - 26) * 1.5);
    const confidence = clamp(55 + pace * 0.25 + clarity * 0.2 - fillerDensityRaw * 2.5);
    const fillerDensity = clamp(100 - fillerDensityRaw * 8);
    const overall = clamp((clarity + confidence + fillerDensity + pace) / 4);
    return {
        clarity,
        confidence,
        filler_density: fillerDensity,
        pace,
        overall,
    };
}
function detectMomentsFromTranscript(transcript, segments) {
    const sourceSegments = Array.isArray(segments) && segments.length > 0
        ? segments
        : String(transcript || "")
            .split(/\n+/)
            .map((text, i) => ({
            speaker: i % 2 === 0 ? "Speaker 1" : "Speaker 2",
            start_sec: i * 4,
            end_sec: i * 4 + 4,
            text,
        }));
    const moments = [];
    for (const seg of sourceSegments) {
        const text = String(seg?.text || "").trim();
        if (!text)
            continue;
        const lower = text.toLowerCase();
        const timestamp = typeof seg?.start_sec === "number" ? seg.start_sec : undefined;
        const matchedSegment = findNearestSegment(sourceSegments, timestamp);
        const alignedTimestamp = typeof matchedSegment?.start_sec === "number"
            ? matchedSegment.start_sec
            : timestamp;
        if (lower.includes("not interested") ||
            lower.includes("we're okay") ||
            lower.includes("we are okay") ||
            lower.includes("no time") ||
            lower.includes("outside help")) {
            moments.push({
                timestamp: alignedTimestamp,
                type: "objection",
                text,
                severity: "high",
            });
            continue;
        }
        if (lower.includes("does that mean") ||
            lower.includes("why") ||
            lower.includes("how") ||
            lower.includes("what happens")) {
            moments.push({
                timestamp: alignedTimestamp,
                type: "highlight",
                text,
                severity: "medium",
            });
            continue;
        }
        if (lower.includes("call me back") ||
            lower.includes("couple weeks") ||
            lower.includes("send you an email") ||
            lower.includes("revisit this")) {
            moments.push({
                timestamp: alignedTimestamp,
                type: "closing_attempt",
                text,
                severity: "medium",
            });
            continue;
        }
    }
    // --- DEDUPE + CLEAN ---
    const seen = new Set();
    const deduped = moments.filter((m) => {
        const key = `${m.type}-${m.text.toLowerCase().slice(0, 60)}`;
        if (seen.has(key))
            return false;
        seen.add(key);
        return true;
    });
    // Prioritise higher-severity moments first, then earlier timestamps.
    deduped.sort((a, b) => {
        const weight = { high: 3, medium: 2, low: 1 };
        const severityDiff = (weight[b.severity || "low"] || 1) - (weight[a.severity || "low"] || 1);
        if (severityDiff !== 0)
            return severityDiff;
        const aTs = typeof a.timestamp === "number" ? a.timestamp : Number.MAX_SAFE_INTEGER;
        const bTs = typeof b.timestamp === "number" ? b.timestamp : Number.MAX_SAFE_INTEGER;
        return aTs - bTs;
    });
    return deduped.slice(0, 6);
}
function buildSuggestionsFromAnalysis(args) {
    const suggestions = [];
    if (args.stages.discovery.score < 60) {
        suggestions.push("Ask 2 more discovery questions before pitching.");
    }
    if (args.stages.objection.score < 60) {
        suggestions.push("Slow down and isolate the real objection before rebutting.");
    }
    if (args.stages.close.score < 60) {
        suggestions.push("End with a clearer next step and a direct commitment ask.");
    }
    if (args.moments.some((m) => m.type === "objection")) {
        suggestions.push("Review the objection moments and rehearse a tighter value reframe.");
    }
    if (args.overall >= 70 && suggestions.length === 0) {
        suggestions.push("Keep the structure, but tighten follow-up questions to raise conversion odds.");
    }
    return suggestions.slice(0, 4);
}
async function updateCallScoreRow(supabase, callId, payload) {
    const nextPayload = { ...payload };
    // Preserve existing structured transcript / analysis data when scoring writes analysis_json.
    if (nextPayload.analysis_json) {
        try {
            const { data: existingRow, error: existingErr } = await supabase
                .from("calls")
                .select("analysis_json")
                .eq("id", callId)
                .maybeSingle();
            if (!existingErr) {
                nextPayload.analysis_json = {
                    ...(existingRow?.analysis_json ?? {}),
                    ...nextPayload.analysis_json,
                };
            }
        }
        catch {
            // best effort only — do not block scoring if merge lookup fails
        }
    }
    let { error } = await supabase.from("calls").update(nextPayload).eq("id", callId);
    if (!error)
        return;
    const msg = String(error?.message ?? "").toLowerCase();
    const missingVoiceCols = (msg.includes("voice_score") || msg.includes("voice_rubric")) &&
        (msg.includes("column") || msg.includes("schema cache") || msg.includes("could not find"));
    if (missingVoiceCols) {
        const retryPayload = { ...nextPayload };
        delete retryPayload.voice_score;
        delete retryPayload.voice_rubric;
        const retry = await supabase.from("calls").update(retryPayload).eq("id", callId);
        if (retry.error)
            throw retry.error;
        return;
    }
    throw error;
}
function uniqueStrings(items) {
    return Array.from(new Set(items.map((x) => String(x).trim()).filter(Boolean)));
}
function buildRepMemoryLabels(args) {
    const strengths = [];
    const weaknesses = [];
    const coachingFocus = [];
    const { stages, moments, voice } = args;
    const stageEntries = [
        { name: "Intro", score: stages.intro.score },
        { name: "Discovery", score: stages.discovery.score },
        { name: "Objection handling", score: stages.objection.score },
        { name: "Close", score: stages.close.score },
    ].sort((a, b) => b.score - a.score);
    // Always keep the rep's strongest 1-2 stage areas, even if the overall call was weak.
    for (const s of stageEntries.slice(0, 2)) {
        if (s.score >= 40)
            strengths.push(s.name);
    }
    if ((voice?.clarity ?? 0) >= 70)
        strengths.push("Clarity");
    if ((voice?.confidence ?? 0) >= 70)
        strengths.push("Confidence");
    if (stages.intro.score < 45)
        weaknesses.push("Intro");
    if (stages.discovery.score < 60)
        weaknesses.push("Discovery");
    if (stages.objection.score < 60)
        weaknesses.push("Objection handling");
    if (stages.close.score < 60)
        weaknesses.push("Close");
    if ((voice?.clarity ?? 100) < 55)
        weaknesses.push("Clarity");
    if ((voice?.confidence ?? 100) < 55)
        weaknesses.push("Confidence");
    if ((voice?.pace ?? 100) < 50)
        weaknesses.push("Pace");
    if (stages.discovery.score < 60) {
        coachingFocus.push("Ask more discovery questions before pitching");
    }
    if (stages.objection.score < 60) {
        coachingFocus.push("Slow down and isolate objections before rebutting");
    }
    if (stages.close.score < 60) {
        coachingFocus.push("End with a clearer next step and direct commitment ask");
    }
    if (moments.some((m) => m.type === "objection")) {
        coachingFocus.push("Rehearse objection handling around the flagged pushback moments");
    }
    if ((voice?.confidence ?? 100) < 60) {
        coachingFocus.push("Improve vocal confidence and certainty in delivery");
    }
    return {
        strengths: uniqueStrings(strengths).slice(0, 5),
        weaknesses: uniqueStrings(weaknesses).slice(0, 5),
        coachingFocus: uniqueStrings(coachingFocus).slice(0, 5),
    };
}
async function upsertRepMemory(supabase, input) {
    const { userId, companyId, callId, overall, stages, voice, moments } = input;
    const { data: existing, error: existingErr } = await supabase
        .from("rep_memory")
        .select("*")
        .eq("user_id", userId)
        .maybeSingle();
    if (existingErr)
        throw existingErr;
    const labels = buildRepMemoryLabels({
        overall,
        stages,
        moments,
        voice,
    });
    const nextCallCount = Number(existing?.call_count ?? 0) + 1;
    const rolling = (prev, next) => {
        const prevNum = Number(prev ?? 0);
        const countBefore = Math.max(0, nextCallCount - 1);
        if (countBefore === 0)
            return next;
        return Number((((prevNum * countBefore) + next) / nextCallCount).toFixed(2));
    };
    const avgScore = rolling(existing?.avg_score, overall);
    const introScore = rolling(existing?.intro_score, stages.intro.score);
    const discoveryScore = rolling(existing?.discovery_score, stages.discovery.score);
    const objectionScore = rolling(existing?.objection_score, stages.objection.score);
    const closeScore = rolling(existing?.close_score, stages.close.score);
    const trendOverall = existing?.avg_score != null ? Number((overall - Number(existing.avg_score)).toFixed(2)) : 0;
    const trendObjection = existing?.objection_score != null ? Number((stages.objection.score - Number(existing.objection_score)).toFixed(2)) : 0;
    const trendClose = existing?.close_score != null ? Number((stages.close.score - Number(existing.close_score)).toFixed(2)) : 0;
    const payload = {
        user_id: userId,
        company_id: companyId ?? null,
        avg_score: avgScore,
        intro_score: introScore,
        discovery_score: discoveryScore,
        objection_score: objectionScore,
        close_score: closeScore,
        trend_overall: trendOverall,
        trend_objection: trendObjection,
        trend_close: trendClose,
        filler_word_rate: voice?.filler_density ?? null,
        talk_ratio: null,
        strengths: labels.strengths,
        weaknesses: labels.weaknesses,
        coaching_focus: labels.coachingFocus,
        call_count: nextCallCount,
        last_call_id: callId,
        last_updated: new Date().toISOString(),
    };
    if (existing?.id) {
        const { data: updatedRow, error: updateErr } = await supabase
            .from("rep_memory")
            .update(payload)
            .eq("id", existing.id)
            .select()
            .single();
        if (updateErr)
            throw updateErr;
        try {
            await syncRepMemoryEmbedding(supabase, updatedRow);
        }
        catch (e) {
            console.warn("[rep_memory] embedding sync failed", e?.message || e);
        }
        return updatedRow;
    }
    const { data: insertedRow, error: insertErr } = await supabase
        .from("rep_memory")
        .insert(payload)
        .select()
        .single();
    if (insertErr)
        throw insertErr;
    try {
        await syncRepMemoryEmbedding(supabase, insertedRow);
    }
    catch (e) {
        console.warn("[rep_memory] embedding sync failed", e?.message || e);
    }
    return insertedRow;
}
function normaliseTranscriptForDeterminism(transcript) {
    return String(transcript || "")
        .replace(/\r\n/g, "\n")
        .replace(/\s+/g, " ")
        .trim();
}
function buildDeterministicPromptKey(params) {
    const transcript = normaliseTranscriptForDeterminism(params.transcript || "");
    const transcriptHash = stableHash(transcript || params.sha256 || params.callId);
    return {
        transcript,
        transcriptHash,
        key: [
            `rubric=${RUBRIC_VERSION}`,
            `prompt=${SCORING_PROMPT_VERSION}`,
            `model=${SCORING_MODEL_VERSION}`,
            `filename=${params.filename || params.callId}`,
            `sha256=${params.sha256 || "missing"}`,
            `transcriptHash=${transcriptHash}`,
        ].join("|"),
    };
}
function buildRubricWithMeta(args) {
    return {
        intro: args.intro,
        discovery: args.discovery,
        objection: args.objection,
        close: args.close,
        _meta: {
            rubric_version: RUBRIC_VERSION,
            prompt_version: SCORING_PROMPT_VERSION,
            model_version: args.modelVersion,
            call_sha256: args.callSha256,
            transcript_hash: args.transcriptHash,
            transcript_present: args.transcriptPresent,
        },
    };
}
async function readScoreCache(supabase, cacheKey) {
    try {
        const { data, error } = await supabase
            .from("score_cache")
            .select("result_json")
            .eq("cache_key", cacheKey)
            .maybeSingle();
        if (error) {
            const msg = String(error?.message ?? "").toLowerCase();
            const missing = (msg.includes("relation") && msg.includes("does not exist")) ||
                (msg.includes("could not find") && msg.includes("score_cache")) ||
                (msg.includes("schema cache") && msg.includes("score_cache"));
            if (missing)
                return null;
            console.warn("[score-cache] read failed:", error.message);
            return null;
        }
        const raw = data?.result_json;
        if (!raw)
            return null;
        return raw;
    }
    catch (e) {
        console.warn("[score-cache] read error:", e?.message || e);
        return null;
    }
}
async function writeScoreCache(supabase, args) {
    try {
        const { error } = await supabase.from("score_cache").upsert({
            cache_key: args.cacheKey,
            call_sha256: args.callSha256,
            transcript_hash: args.transcriptHash,
            rubric_version: RUBRIC_VERSION,
            prompt_version: SCORING_PROMPT_VERSION,
            model_version: SCORING_MODEL_VERSION,
            result_json: args.result,
            updated_at: new Date().toISOString(),
        }, { onConflict: "cache_key" });
        if (error) {
            const msg = String(error?.message ?? "").toLowerCase();
            const missing = (msg.includes("relation") && msg.includes("does not exist")) ||
                (msg.includes("could not find") && msg.includes("score_cache")) ||
                (msg.includes("schema cache") && msg.includes("score_cache"));
            if (!missing)
                console.warn("[score-cache] write failed:", error.message);
        }
    }
    catch (e) {
        console.warn("[score-cache] write error:", e?.message || e);
    }
}
export function heuristicScoreFallback() {
    const s = {
        score: 68,
        notes: "Deterministic fallback based on minimal call metadata.",
    };
    const voice = computeVoiceScore("", null);
    return {
        model: "heuristic:v1",
        overall: s.score,
        summary: "Solid overall structure, but a fuller transcript is needed for a reliable coaching summary.",
        stages: {
            intro: { ...s },
            discovery: { ...s },
            objection: { ...s },
            close: { ...s },
        },
        moments: [],
        suggestions: [],
        voice,
    };
}
// --- SCORING THRESHOLD HELPERS ---
async function getScoringThresholds(supabase) {
    try {
        const { data, error } = await supabase
            .from("admin_config")
            .select("low_score_threshold, critical_score_threshold")
            .limit(1)
            .maybeSingle();
        if (error)
            throw error;
        const low = Number(data?.low_score_threshold);
        const critical = Number(data?.critical_score_threshold);
        return {
            low: Number.isFinite(low) ? low : 65,
            critical: Number.isFinite(critical) ? critical : 45,
        };
    }
    catch {
        return { low: 65, critical: 45 };
    }
}
function buildScoreThresholdFlags(args) {
    const now = new Date().toISOString();
    const flags = [];
    const bandFor = (score) => {
        if (score <= args.thresholds.critical)
            return "critical";
        if (score <= args.thresholds.low)
            return "low";
        return null;
    };
    const timestampForSection = (section) => {
        const moments = Array.isArray(args.moments) ? args.moments : [];
        const match = moments.find((m) => {
            const type = String(m?.type || "").toLowerCase();
            const text = String(m?.text || "").toLowerCase();
            if (section === "overall")
                return true;
            if (section === "objection")
                return type === "objection" || text.includes("too expensive");
            if (section === "close")
                return type === "closing_attempt" || text.includes("call me back");
            if (section === "discovery")
                return text.includes("why") || text.includes("how");
            if (section === "intro")
                return text.includes("intro") || type === "mistake";
            return false;
        });
        return typeof match?.timestamp === "number" ? match.timestamp : null;
    };
    const pushFlag = (key, label, section, score, severity) => {
        flags.push({
            flag_key: key,
            type: key,
            category: "score",
            label,
            severity,
            section,
            score,
            threshold_triggered: severity,
            timestamp: timestampForSection(section),
            source: "scoring_engine",
            created_at: now,
        });
    };
    const overallBand = bandFor(args.overall);
    if (overallBand) {
        pushFlag("overall_low_score", "Overall score below threshold", "overall", args.overall, overallBand);
    }
    const sections = [
        { key: "intro", score: args.stages.intro.score },
        { key: "discovery", score: args.stages.discovery.score },
        { key: "objection", score: args.stages.objection.score },
        { key: "close", score: args.stages.close.score },
    ];
    for (const section of sections) {
        const band = bandFor(section.score);
        if (!band)
            continue;
        pushFlag(`${section.key}_weak`, `${section.key} performance below threshold`, section.key, section.score, band);
    }
    return flags;
}
function shouldCreateAssignment(args) {
    // Only create if critical
    if (args.thresholdBand !== "critical")
        return false;
    // Require at least one meaningful section failure (avoid noise)
    const meaningful = args.reviewFlags.some(f => f.severity === "critical" &&
        ["close", "objection", "discovery"].includes(f.section));
    return meaningful;
}
// 🔥 STEP 1 — NOISE FILTER FUNCTION (Day 66)
function isHighQualityFlag(args) {
    // ❌ Ignore low severity unless repeated
    if (args.severity === "low" && (args.failureCount || 0) < 2) {
        return false;
    }
    // ❌ Ignore weak sections unless critical
    const weakSections = ["intro"];
    if (weakSections.includes(args.section) && args.severity !== "critical") {
        return false;
    }
    // ❌ Ignore decent performance (not actually failing)
    if (typeof args.score === "number" && args.score > 65) {
        return false;
    }
    return true;
}
async function ensureCriticalCallAssignment(args) {
    const hasCriticalFlag = args.reviewFlags.some(f => f.severity === "critical");
    if (!shouldCreateAssignment({
        reviewFlags: args.reviewFlags,
        overall: args.overall,
        thresholdBand: args.thresholdBand,
    }))
        return null;
    // 🔥 NEW FILTER LAYER
    const validFlags = args.reviewFlags.filter(f => isHighQualityFlag({
        section: f.section,
        severity: f.severity,
        score: args.overall,
        failureCount: f.failureCount || 1,
    }));
    // ❌ No strong signal → no assignment
    if (validFlags.length === 0)
        return null;
    if (!hasCriticalFlag)
        return null;
    if (!args.assigneeUserId)
        return null;
    try {
        const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
        // Intelligent drill mapping based on flags
        const mapFlagsToDrillId = (flags) => {
            // 1. PRIORITISE CRITICAL FLAGS
            const criticalFlags = validFlags.filter(f => f.severity === "critical");
            // 2. PRIORITISE BY SECTION IMPORTANCE (closing > objection > discovery > intro)
            const priorityOrder = ["close", "objection", "discovery", "intro"];
            const sorted = (criticalFlags.length ? criticalFlags : flags).sort((a, b) => {
                return priorityOrder.indexOf(a.section) - priorityOrder.indexOf(b.section);
            });
            const target = sorted[0];
            if (!target)
                return "critical-call-review";
            switch (target.section) {
                case "close":
                    return "closing-drill";
                case "objection":
                    return "objection-handling-drill";
                case "discovery":
                    return "discovery-drill";
                case "intro":
                    return "intro-drill";
                default:
                    return "critical-call-review";
            }
        };
        const drillId = mapFlagsToDrillId(args.reviewFlags);
        const dedupeKey = `${args.callId}:${args.assigneeUserId}:${drillId}`;
        const existing = await svc
            .from("coach_assignments")
            .select("id,status")
            .eq("call_id", args.callId)
            .eq("assignee_user_id", args.assigneeUserId)
            .eq("drill_id", drillId)
            .order("created_at", { ascending: false })
            .limit(1)
            .maybeSingle();
        if (!existing.error && existing.data) {
            const status = String(existing.data?.status || "").toLowerCase();
            if (status !== "completed") {
                return { ok: true, skipped: true, reason: "existing_open_assignment", id: existing.data?.id ?? null };
            }
        }
        const payload = {
            call_id: args.callId,
            assignee_user_id: args.assigneeUserId,
            drill_id: drillId,
            notes: "Auto-created from critical flagged call.",
            org_id: args.orgId ?? null,
            status: "open",
            source: "flagged_call_auto",
            meta: {
                source: "flagged_call_auto",
                action_type: "assignment_created",
                assignment_origin: "flagged_call_auto",
                dedupe_key: dedupeKey,
                flagged_call: true,
                threshold_band: args.thresholdBand,
                needs_manager_review: true,
                review_flags: args.reviewFlags,
                score_overall: args.overall,
                flag_sections: args.reviewFlags.map(f => f.section),
                primary_flag: args.reviewFlags[0]?.type ?? null,
            },
        };
        const inserted = await svc
            .from("coach_assignments")
            .insert(payload)
            .select("id")
            .single();
        if (inserted.error) {
            const msg = String(inserted.error?.message ?? "").toLowerCase();
            const missingOrg = msg.includes("org_id") && msg.includes("schema cache");
            if (missingOrg) {
                const retryPayload = {
                    call_id: args.callId,
                    assignee_user_id: args.assigneeUserId,
                    drill_id: drillId,
                    notes: "Auto-created from critical flagged call.",
                    status: "open",
                    source: "flagged_call_auto",
                    meta: {
                        source: "flagged_call_auto",
                        action_type: "assignment_created",
                        assignment_origin: "flagged_call_auto",
                        dedupe_key: dedupeKey,
                        flagged_call: true,
                        threshold_band: args.thresholdBand,
                        needs_manager_review: true,
                        review_flags: args.reviewFlags,
                        score_overall: args.overall,
                    },
                };
                const retry = await svc
                    .from("coach_assignments")
                    .insert(retryPayload)
                    .select("id")
                    .single();
                if (retry.error)
                    throw retry.error;
                return { ok: true, skipped: false, id: retry.data?.id ?? null };
            }
            throw inserted.error;
        }
        try {
            await svc.from("crm_activities").insert({
                type: "coach_assignment_created",
                summary: "Auto-created critical call review assignment",
                org_id: args.orgId ?? null,
                rep_id: args.assigneeUserId,
                call_id: args.callId,
                source: args.source,
                meta: {
                    source: "flagged_call_auto",
                    action_type: "assignment_created",
                    assignment_origin: "flagged_call_auto",
                    dedupe_key: dedupeKey,
                    flagged_call: true,
                    threshold_band: args.thresholdBand,
                    review_flags: args.reviewFlags,
                    score_overall: args.overall,
                    coach_assignment_id: inserted.data?.id ?? null,
                    drill_id: drillId,
                },
            });
        }
        catch (e) {
            console.warn("[score] auto assignment crm_activities insert failed", e);
        }
        return { ok: true, skipped: false, id: inserted.data?.id ?? null };
    }
    catch (e) {
        console.warn("[score] auto critical assignment failed", e);
        return null;
    }
}
/** Write a score row into call_scores (non-blocking if table missing) */
async function writeScoreHistory(supabase, callId, model, overall, rubric) {
    try {
        const { data: callRow, error: callErr } = await supabase
            .from("calls")
            .select("user_id")
            .eq("id", callId)
            .maybeSingle();
        if (callErr) {
            console.warn("[score] call lookup for history failed:", callErr.message);
        }
        const { error } = await supabase.from("call_scores").insert({
            call_id: callId,
            user_id: callRow?.user_id ?? null,
            ai_model: model,
            rubric_version: RUBRIC_VERSION,
            overall,
            rubric,
        });
        if (error)
            console.warn("[score] call_scores insert failed:", error.message);
    }
    catch (e) {
        console.warn("[score] call_scores insert error:", e?.message || e);
    }
}
/** Best-effort Slack notifier (safe: silently skips if webhook unset) */
async function notifySlack(opts) {
    try {
        // Try to resolve a human name for the rep if you store it in profiles
        let repName = undefined;
        if (opts.repIdFallback) {
            try {
                const { data } = await opts.supabase
                    .from("profiles")
                    .select("full_name")
                    .eq("id", opts.repIdFallback)
                    .maybeSingle();
                repName = data?.full_name || opts.repIdFallback || undefined;
            }
            catch {
                repName = opts.repIdFallback || undefined;
            }
        }
        const WEB = (process.env.PUBLIC_WEB_BASE ||
            process.env.WEB_BASE_URL ||
            "http://localhost:3000").replace(/\/$/, "");
        await postScoreSummary({
            callId: opts.callId,
            overall: opts.scores.overall,
            sections: {
                intro: opts.scores.intro,
                discovery: opts.scores.discovery,
                objection: opts.scores.objection,
                close: opts.scores.close,
            },
            durationSec: typeof opts.durationSec === "number" ? opts.durationSec : undefined,
            repName,
            callUrl: `${WEB}/calls/${opts.callId}`,
            recentUrl: `${WEB}/recent-calls`,
            // webhookUrl optional — postScoreSummary will default to SLACK_WEBHOOK_URL
        });
        console.log("[score] Slack summary posted");
    }
    catch (err) {
        console.error("[score] Slack summary failed", err);
    }
}
async function lookupUserContactBestEffort(supabase, userId) {
    // PRIMARY: profiles keyed by user_id (this is what /v1/team/users reads from)
    const primarySelects = [
        "user_id,email,full_name",
        "user_id,email,full_name,name",
        "user_id,email,name",
        "user_id,email",
    ];
    for (const select of primarySelects) {
        try {
            const r = await supabase
                .from("profiles")
                .select(select)
                .eq("user_id", userId)
                .maybeSingle();
            if (r.error) {
                const msg = String(r.error?.message ?? "").toLowerCase();
                if ((msg.includes("relation") && msg.includes("does not exist")) ||
                    msg.includes("could not find the table") ||
                    (msg.includes("column") && msg.includes("does not exist")) ||
                    (msg.includes("schema cache") && msg.includes("column"))) {
                    continue;
                }
                continue;
            }
            const row = r.data;
            if (!row)
                continue;
            const email = String(row?.email ?? "").trim() || null;
            const name = String(row?.full_name ?? row?.name ?? "").trim() || null;
            if (email || name) {
                return { email, name };
            }
        }
        catch (e) {
            console.warn("[lookupUserContactBestEffort] profiles lookup failed", e);
        }
    }
    const tablePlans = [
        {
            table: "profiles",
            selects: [
                "id,email,full_name,name",
                "id,email,full_name",
                "id,email,name",
                "id,email",
            ],
            predicates: [{ col: "id", value: userId }],
            emailKeys: ["email"],
            nameKeys: ["full_name", "name"],
        },
        {
            table: "users",
            selects: [
                "id,email,full_name,name",
                "id,email,full_name",
                "id,email,name",
                "id,email",
            ],
            predicates: [{ col: "id", value: userId }],
            emailKeys: ["email"],
            nameKeys: ["full_name", "name"],
        },
        {
            table: "reps",
            selects: [
                "id,email,full_name,name",
                "id,email,full_name",
                "id,email,name",
                "id,email",
            ],
            predicates: [{ col: "id", value: userId }],
            emailKeys: ["email"],
            nameKeys: ["full_name", "name"],
        },
    ];
    for (const plan of tablePlans) {
        for (const select of plan.selects) {
            let q = supabase.from(plan.table).select(select).limit(1);
            for (const predicate of plan.predicates) {
                q = q.eq(predicate.col, predicate.value);
            }
            const r = await q.maybeSingle();
            if (r.error) {
                const msg = String(r.error?.message ?? "").toLowerCase();
                if ((msg.includes("relation") && msg.includes("does not exist")) ||
                    msg.includes("could not find the table") ||
                    (msg.includes("column") && msg.includes("does not exist")) ||
                    (msg.includes("schema cache") && msg.includes("column"))) {
                    continue;
                }
                continue;
            }
            const row = r.data;
            if (!row)
                continue;
            const email = plan.emailKeys
                .map((k) => String(row?.[k] ?? "").trim())
                .find(Boolean) || null;
            const name = plan.nameKeys
                .map((k) => String(row?.[k] ?? "").trim())
                .find(Boolean) || null;
            if (email || name) {
                return { email, name };
            }
        }
    }
    return { email: null, name: null };
}
async function resolveReviewFeedbackContextForWorker(args) {
    const { supabase, callId, repId } = args;
    let managerId = null;
    let assignmentId = null;
    const fallback = await supabase
        .from("assignments")
        .select("id,rep_id,manager_id,target_id,type,status,created_at")
        .eq("rep_id", repId)
        .eq("type", "call_review")
        .eq("target_id", callId)
        .order("created_at", { ascending: false })
        .limit(1)
        .maybeSingle();
    if (!fallback.error && fallback.data) {
        managerId = fallback.data.manager_id ? String(fallback.data.manager_id) : null;
        assignmentId = fallback.data.id ? String(fallback.data.id) : null;
    }
    return {
        repId,
        managerId,
        assignmentId,
    };
}
async function autoSendReviewFeedbackEmailForWorkerBestEffort(args) {
    try {
        const context = await resolveReviewFeedbackContextForWorker({
            supabase: args.supabase,
            callId: args.callId,
            repId: args.repId,
        });
        const repContact = await lookupUserContactBestEffort(args.supabase, context.repId);
        const managerContact = context.managerId
            ? await lookupUserContactBestEffort(args.supabase, context.managerId)
            : { email: null, name: null };
        console.log("[review feedback email] attempting", {
            callId: args.callId,
            repId: context.repId,
            managerId: context.managerId,
            repEmail: repContact.email,
            managerEmail: managerContact.email,
            assignmentId: context.assignmentId,
        });
        if (!repContact.email && !managerContact.email) {
            console.warn("[review feedback email] skipped: no email recipients", {
                callId: args.callId,
                repId: context.repId,
                managerId: context.managerId,
            });
            return;
        }
        const result = await sendReviewFeedbackEmail({
            repEmail: repContact.email,
            managerEmail: managerContact.email,
            repName: repContact.name,
            managerName: managerContact.name,
            callId: args.callId,
            scoreOverall: args.scoreOverall,
            voiceScore: args.voiceScore,
            summary: args.summary ?? null,
            weakClose: Boolean(args.reviewTags?.weak_close),
            fillerCount: Number(args.reviewTags?.filler_count ?? 0),
            fillerWords: Array.isArray(args.reviewTags?.filler_words) ? args.reviewTags.filler_words : [],
            assignmentId: context.assignmentId,
            appUrl: process.env.WEB_APP_URL || process.env.WEB_ORIGIN || "http://localhost:3000",
        });
        if (result?.ok) {
            console.log("[review feedback email] sent", {
                callId: args.callId,
                messageId: result.id ?? null,
                repEmail: repContact.email,
                managerEmail: managerContact.email,
            });
        }
        else {
            console.warn("[review feedback email] failed", {
                callId: args.callId,
                error: result?.error || "unknown_email_send_failure",
                repEmail: repContact.email,
                managerEmail: managerContact.email,
            });
        }
    }
    catch (e) {
        console.warn("[review feedback email] failed", e?.message || e);
    }
}
export async function scoreWithLLM(opts) {
    const { supabase, callId } = opts;
    const t0 = Date.now();
    try {
        // Pull minimal call meta (include duration for Slack; user_id to resolve rep)
        const { data: call, error: callErr } = await supabase
            .from("calls")
            .select("id, filename, user_id, org_id, duration_sec, sha256, transcript, analysis_json")
            .eq("id", callId)
            .single();
        if (callErr || !call)
            throw new Error("call_not_found");
        const rawTranscript = String(call?.transcript ?? "");
        const cleanedTranscript = cleanTranscript(rawTranscript);
        const storedSegments = Array.isArray(call?.analysis_json?.transcript?.segments)
            ? call?.analysis_json?.transcript?.segments
            : [];
        const cleanedSegments = storedSegments.length > 0 ? storedSegments : buildSegments(cleanedTranscript);
        const deterministic = buildDeterministicPromptKey({
            callId,
            filename: call?.filename ?? null,
            sha256: call?.sha256 ?? null,
            transcript: cleanedTranscript,
        });
        const cached = await readScoreCache(supabase, deterministic.key);
        if (cached) {
            const cachedScore = normaliseStoredScore(cached);
            const cachedModelVersion = String(cachedScore.model || SCORING_MODEL_VERSION);
            const voice = cachedScore.voice ?? computeVoiceScore(deterministic.transcript, call?.duration_sec ?? null);
            const transcriptSegments = cleanedSegments;
            const thresholds = await getScoringThresholds(supabase);
            const derivedMoments = cachedScore.moments?.length > 0
                ? cachedScore.moments
                : detectMomentsFromTranscript(deterministic.transcript, transcriptSegments);
            const derivedSuggestions = cachedScore.suggestions?.length > 0
                ? cachedScore.suggestions
                : buildSuggestionsFromAnalysis({
                    overall: cachedScore.overall,
                    stages: cachedScore.stages,
                    moments: derivedMoments,
                });
            const reviewFlags = buildScoreThresholdFlags({
                overall: cachedScore.overall,
                stages: cachedScore.stages,
                thresholds,
                moments: derivedMoments,
            });
            const thresholdBand = cachedScore.overall <= thresholds.critical
                ? "critical"
                : cachedScore.overall <= thresholds.low
                    ? "low"
                    : null;
            const rubric = buildRubricWithMeta({
                intro: cachedScore.stages.intro,
                discovery: cachedScore.stages.discovery,
                objection: cachedScore.stages.objection,
                close: cachedScore.stages.close,
                callSha256: call?.sha256 ?? null,
                transcriptHash: deterministic.transcriptHash,
                transcriptPresent: Boolean(deterministic.transcript),
                modelVersion: cachedModelVersion,
            });
            rubric._meta.voice = voice;
            const dbWriteStart = Date.now();
            await updateCallScoreRow(supabase, callId, {
                score_overall: cachedScore.overall,
                summary: cachedScore.summary,
                transcript: cleanedTranscript || null,
                voice_score: voice.overall,
                voice_rubric: voice,
                rubric,
                analysis_json: {
                    overall: cachedScore.overall,
                    stages: cachedScore.stages,
                    moments: derivedMoments,
                    suggestions: derivedSuggestions,
                    summary: cachedScore.summary,
                    voice,
                    review_flags: reviewFlags,
                    threshold_band: thresholdBand,
                    needs_manager_review: thresholdBand === "critical",
                    transcript: {
                        text: cleanedTranscript,
                        segments: cleanedSegments,
                    },
                },
                ai_model: cachedModelVersion,
                rubric_version: RUBRIC_VERSION,
                scored_at: new Date().toISOString(),
            });
            console.log("[perf] db_write_ms", Date.now() - dbWriteStart, { callId, path: "cache" });
            await writeScoreHistory(supabase, callId, cachedModelVersion, cachedScore.overall, rubric);
            // CRM Activity: record a score event for cache hit (best-effort; non-blocking)
            try {
                const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
                const { data: callRow } = await svc
                    .from('calls')
                    .select('id, account_id, contact_id, score_overall')
                    .eq('id', callId)
                    .single();
                const overall = typeof callRow?.score_overall === 'number' ? callRow.score_overall : cachedScore.overall;
                const summary = `Scored ${Math.round(overall)}`;
                await svc.from('crm_activities').insert({
                    type: 'score',
                    summary,
                    org_id: call?.org_id ?? null,
                    rep_id: call?.user_id ?? null,
                    call_id: callId,
                    source: 'scoring_engine_cache',
                    meta: {
                        action_type: 'score_recorded',
                        overall,
                        stage_scores: cachedScore.stages,
                        threshold_band: thresholdBand,
                        needs_manager_review: thresholdBand === 'critical',
                        review_flag_count: reviewFlags.length,
                        review_flags: reviewFlags,
                    },
                    account_id: callRow?.account_id ?? null,
                    contact_id: callRow?.contact_id ?? null,
                });
            }
            catch (e) {
                console.warn('[score] activity insert failed (cache)', e);
            }
            // CRM Activity: review_flag best-effort
            await ensureCriticalCallAssignment({
                supabase,
                callId,
                orgId: call?.org_id ?? null,
                assigneeUserId: call?.user_id ?? null,
                overall: cachedScore.overall,
                thresholdBand,
                reviewFlags,
                source: "scoring_engine",
            });
            if (!SKIP_SCORING_SIDE_EFFECTS) {
                const slackStart = Date.now();
                await notifySlack({
                    supabase,
                    callId,
                    repIdFallback: call?.user_id ?? null,
                    durationSec: call?.duration_sec ?? null,
                    scores: {
                        intro: cachedScore.stages.intro.score,
                        discovery: cachedScore.stages.discovery.score,
                        objection: cachedScore.stages.objection.score,
                        close: cachedScore.stages.close.score,
                        overall: cachedScore.overall,
                    },
                });
                console.log("[perf] slack_ms", Date.now() - slackStart, { callId });
                await autoSendReviewFeedbackEmailForWorkerBestEffort({
                    supabase,
                    callId,
                    repId: String(call?.user_id ?? ""),
                    scoreOverall: cachedScore.overall,
                    voiceScore: voice.overall,
                    summary: cachedScore.summary ?? null,
                    reviewTags: {
                        weak_close: cachedScore.stages.close.score < 60,
                        filler_count: 0,
                        filler_words: [],
                    },
                });
            }
            if (!SKIP_SCORING_SIDE_EFFECTS) {
                try {
                    const repMemoryStart = Date.now();
                    await upsertRepMemory(supabase, {
                        userId: String(call?.user_id),
                        companyId: call?.org_id ?? null,
                        callId,
                        overall: cachedScore.overall,
                        stages: cachedScore.stages,
                        voice,
                        moments: derivedMoments,
                    });
                    console.log("[perf] rep_memory_ms", Date.now() - repMemoryStart, { callId });
                }
                catch (e) {
                    console.warn("[rep_memory] cache-hit update failed", e?.message || e);
                }
            }
            console.log("[perf] total_score_ms", Date.now() - t0, { callId, path: "cache" });
            return {
                ...cachedScore,
                moments: derivedMoments,
                suggestions: derivedSuggestions,
                voice,
            };
        }
        const knowledge = await getScoringKnowledgeContext(supabase, {
            companyId: call?.org_id ?? null,
            userId: String(call?.user_id ?? ""),
            transcript: cleanedTranscript,
        });
        // Build prompt
        const userLines = [
            `CALL META: filename="${call.filename || call.id}"`,
            `CALL HASH: sha256="${(call.sha256 || "missing")}"`,
            `TRANSCRIPT HASH: stable="${deterministic.transcriptHash}"`,
            `DETERMINISM KEY: "${deterministic.key}"`,
            "TRANSCRIPT:",
            deterministic.transcript || "(not available in MVP)",
            "",
            "Rubric guide:",
            "- Intro: pattern interrupt, clear reason, agenda set.",
            "- Discovery: deep questions, pain/impact, budget/timeline, authority.",
            "- Objection: isolates true objection, reframes value, tests commitment.",
            "- Close: clear next step, assumptive/binary ask, time/date locked.",
            "Return a short coaching summary in 1-2 sentences (max 220 chars) that explains the main strength and main weakness of the call.",
            "Return stage scores under stages.intro, stages.discovery, stages.objection, and stages.close.",
            "Return moments as an array of notable call moments using timestamps when possible. Focus on objections, mistakes, highlights, and closing attempts.",
            "Return suggestions as an array of short actionable coaching suggestions based on the weakest stages and moments.",
            "For identical transcripts, hashes, rubric versions, and prompt versions, return identical stage scores, overall score, moments, suggestions, and materially consistent reasoning.",
        ];
        const user = userLines.join("\n");
        const system = `You are a strict sales call evaluator. Score from 0–100 overall and for Intro, Discovery, Objection Handling, Close. Be concise. Also provide a short coaching summary. Treat the transcript and determinism key as the source of truth. Identical calls scored under the same rubric and prompt version must return the same result. Output must match the provided JSON schema exactly.

Relevant company playbook:
${knowledge.playbookText || "None"}

Relevant rep memory:
${knowledge.repMemoryText || "None"}`;
        const openai = getOpenAI();
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), OPENAI_TIMEOUT_MS);
        const llmStart = Date.now();
        const resp = await openai.chat.completions.create({
            model: AI_MODEL,
            messages: [
                { role: "system", content: system },
                { role: "user", content: user },
            ],
            response_format: { type: "json_schema", json_schema: JSON_SCHEMA },
            temperature: 0,
        }, { signal: ctrl.signal });
        clearTimeout(timer);
        console.log("[perf] llm_ms", Date.now() - llmStart, { callId });
        const raw = resp.choices?.[0]?.message?.content;
        if (!raw)
            throw new Error("no_model_content");
        const validated = parseAndValidateScoreResponse(raw);
        const voice = computeVoiceScore(deterministic.transcript, call?.duration_sec ?? null);
        const thresholds = await getScoringThresholds(supabase);
        const parsed = {
            ...validated,
            model: AI_MODEL,
            voice,
        };
        const transcriptSegments = cleanedSegments;
        const derivedMoments = detectMomentsFromTranscript(deterministic.transcript, transcriptSegments);
        const derivedSuggestions = parsed.suggestions?.length > 0
            ? parsed.suggestions
            : buildSuggestionsFromAnalysis({
                overall: parsed.overall,
                stages: parsed.stages,
                moments: derivedMoments,
            });
        parsed.moments = derivedMoments;
        parsed.suggestions = derivedSuggestions;
        const reviewFlags = buildScoreThresholdFlags({
            overall: parsed.overall,
            stages: parsed.stages,
            thresholds,
            moments: derivedMoments,
        });
        const thresholdBand = parsed.overall <= thresholds.critical
            ? "critical"
            : parsed.overall <= thresholds.low
                ? "low"
                : null;
        const rubric = buildRubricWithMeta({
            intro: parsed.stages.intro,
            discovery: parsed.stages.discovery,
            objection: parsed.stages.objection,
            close: parsed.stages.close,
            callSha256: call?.sha256 ?? null,
            transcriptHash: deterministic.transcriptHash,
            transcriptPresent: Boolean(deterministic.transcript),
            modelVersion: SCORING_MODEL_VERSION,
        });
        rubric._meta.voice = voice;
        // Persist latest on calls
        const dbWriteStart = Date.now();
        await updateCallScoreRow(supabase, callId, {
            score_overall: parsed.overall,
            summary: parsed.summary,
            transcript: cleanedTranscript || null,
            voice_score: voice.overall,
            voice_rubric: voice,
            rubric,
            analysis_json: {
                overall: parsed.overall,
                stages: parsed.stages,
                moments: parsed.moments,
                suggestions: parsed.suggestions,
                summary: parsed.summary,
                voice,
                review_flags: reviewFlags,
                threshold_band: thresholdBand,
                needs_manager_review: thresholdBand === "critical",
                transcript: {
                    text: cleanedTranscript,
                    segments: cleanedSegments,
                },
            },
            ai_model: SCORING_MODEL_VERSION,
            rubric_version: RUBRIC_VERSION,
            scored_at: new Date().toISOString(),
        });
        console.log("[perf] db_write_ms", Date.now() - dbWriteStart, { callId, path: "llm" });
        // History row (non-blocking)
        await writeScoreHistory(supabase, callId, SCORING_MODEL_VERSION, parsed.overall, rubric);
        await writeScoreCache(supabase, {
            cacheKey: deterministic.key,
            callSha256: call?.sha256 ?? null,
            transcriptHash: deterministic.transcriptHash,
            result: parsed,
        });
        // CRM Activity: record a score event (best-effort; non-blocking)
        try {
            const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
            // fetch linkage for account/contact so the activity appears on tfheir timelines
            const { data: callRow } = await svc
                .from('calls')
                .select('id, account_id, contact_id, score_overall')
                .eq('id', callId)
                .single();
            const overall = typeof callRow?.score_overall === 'number' ? callRow.score_overall : parsed.overall;
            const summary = `Scored ${Math.round(overall)}`;
            await svc.from('crm_activities').insert({
                type: 'score',
                summary,
                org_id: call?.org_id ?? null,
                rep_id: call?.user_id ?? null,
                call_id: callId,
                source: 'scoring_engine',
                meta: {
                    action_type: 'score_recorded',
                    overall,
                    stage_scores: parsed.stages,
                    threshold_band: thresholdBand,
                    needs_manager_review: thresholdBand === 'critical',
                    review_flag_count: reviewFlags.length,
                    review_flags: reviewFlags,
                },
                account_id: callRow?.account_id ?? null,
                contact_id: callRow?.contact_id ?? null,
            });
        }
        catch (e) {
            console.warn('[score] activity insert failed', e);
        }
        // CRM Activity: review_flag (structured fallback)
        try {
            if (reviewFlags.length > 0) {
                const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
                await svc.from('crm_activities').insert(reviewFlags.map((flag) => ({
                    type: 'review_flag',
                    // ✅ CORE FIELDS
                    summary: `${flag.type} (${flag.severity})`,
                    org_id: memoryCall?.org_id ?? null,
                    rep_id: memoryCall?.user_id ?? null,
                    call_id: opts.callId,
                    source: 'scoring_engine_fallback',
                    // 🚀 NEW ANALYTICS FIELDS
                    flag_key: flag.type,
                    flag_category: 'score_threshold',
                    flag_severity: flag.severity,
                    flag_section: flag.section,
                    // 🧠 FLEXIBLE META
                    meta: {
                        score: flag.score,
                        timestamp: flag.timestamp ?? null,
                        threshold_band: flag.severity,
                        thresholds: {
                            low: thresholds.low,
                            critical: thresholds.critical,
                        },
                        needs_manager_review: flag.severity === 'critical',
                    },
                })));
            }
        }
        catch (e) {
            console.warn('[score] review_flag activity insert failed (fallback)', e);
        }
        await ensureCriticalCallAssignment({
            supabase,
            callId,
            orgId: call?.org_id ?? null,
            assigneeUserId: call?.user_id ?? null,
            overall: parsed.overall,
            thresholdBand,
            reviewFlags,
            source: "scoring_engine",
        });
        if (!SKIP_SCORING_SIDE_EFFECTS) {
            const slackStart = Date.now();
            await notifySlack({
                supabase,
                callId,
                repIdFallback: call?.user_id ?? null,
                durationSec: call?.duration_sec ?? null,
                scores: {
                    intro: parsed.stages.intro.score,
                    discovery: parsed.stages.discovery.score,
                    objection: parsed.stages.objection.score,
                    close: parsed.stages.close.score,
                    overall: parsed.overall,
                },
            });
            console.log("[perf] slack_ms", Date.now() - slackStart, { callId });
            await autoSendReviewFeedbackEmailForWorkerBestEffort({
                supabase,
                callId,
                repId: String(call?.user_id ?? ""),
                scoreOverall: parsed.overall,
                voiceScore: voice.overall,
                summary: parsed.summary ?? null,
                reviewTags: {
                    weak_close: parsed.stages.close.score < 60,
                    filler_count: 0,
                    filler_words: [],
                },
            });
        }
        if (!SKIP_SCORING_SIDE_EFFECTS) {
            try {
                const repMemoryStart = Date.now();
                await upsertRepMemory(supabase, {
                    userId: String(call?.user_id),
                    companyId: call?.org_id ?? null,
                    callId,
                    overall: parsed.overall,
                    stages: parsed.stages,
                    voice,
                    moments: parsed.moments,
                });
                console.log("[perf] rep_memory_ms", Date.now() - repMemoryStart, { callId });
            }
            catch (e) {
                console.warn("[rep_memory] success update failed", e?.message || e);
            }
        }
        console.log("[perf] total_score_ms", Date.now() - t0, { callId, path: "llm" });
        return parsed;
    }
    catch (err) {
        console.warn("[scoreWithLLM] LLM failed, using heuristic:", err?.status ?? "", err?.code ?? "", err?.message ?? err);
        const fb = heuristicScoreFallback();
        const fallbackVoice = computeVoiceScore("", null);
        const thresholds = await getScoringThresholds(opts.supabase);
        let transcriptSegments = [];
        let transcriptText = "";
        try {
            const { data: callRow } = await opts.supabase
                .from("calls")
                .select("transcript, analysis_json")
                .eq("id", opts.callId)
                .maybeSingle();
            transcriptText = cleanTranscript(String(callRow?.transcript || ""));
            transcriptSegments = Array.isArray(callRow?.analysis_json?.transcript?.segments)
                ? callRow?.analysis_json?.transcript?.segments
                : buildSegments(transcriptText);
        }
        catch { }
        const fallbackMoments = detectMomentsFromTranscript(transcriptText, transcriptSegments);
        const fallbackSuggestions = buildSuggestionsFromAnalysis({
            overall: fb.overall,
            stages: fb.stages,
            moments: fallbackMoments,
        });
        const reviewFlags = buildScoreThresholdFlags({
            overall: fb.overall,
            stages: fb.stages,
            thresholds,
            moments: fallbackMoments,
        });
        const thresholdBand = fb.overall <= thresholds.critical
            ? "critical"
            : fb.overall <= thresholds.low
                ? "low"
                : null;
        let memoryCall = null;
        try {
            const lookup = await opts.supabase
                .from("calls")
                .select("user_id, org_id")
                .eq("id", opts.callId)
                .maybeSingle();
            memoryCall = lookup.data ?? null;
        }
        catch { }
        const fallbackModelVersion = `${fb.model}:${SCORING_PROMPT_VERSION}:${RUBRIC_VERSION}`;
        const rubric = buildRubricWithMeta({
            intro: fb.stages.intro,
            discovery: fb.stages.discovery,
            objection: fb.stages.objection,
            close: fb.stages.close,
            callSha256: null,
            transcriptHash: null,
            transcriptPresent: false,
            modelVersion: fallbackModelVersion,
        });
        rubric._meta.voice = fallbackVoice;
        const dbWriteStart = Date.now();
        await updateCallScoreRow(opts.supabase, opts.callId, {
            score_overall: fb.overall,
            summary: fb.summary,
            voice_score: fallbackVoice.overall,
            voice_rubric: fallbackVoice,
            rubric,
            analysis_json: {
                overall: fb.overall,
                stages: fb.stages,
                moments: fallbackMoments,
                suggestions: fallbackSuggestions,
                summary: fb.summary,
                voice: fallbackVoice,
                review_flags: reviewFlags,
                threshold_band: thresholdBand,
                needs_manager_review: thresholdBand === "critical",
                transcript: {
                    text: transcriptText,
                    segments: transcriptSegments,
                },
            },
            ai_model: fallbackModelVersion,
            rubric_version: RUBRIC_VERSION,
            scored_at: new Date().toISOString(),
        });
        console.log("[perf] db_write_ms", Date.now() - dbWriteStart, { callId: opts.callId, path: "fallback" });
        // History row (non-blocking)
        await writeScoreHistory(opts.supabase, opts.callId, fallbackModelVersion, fb.overall, rubric);
        await writeScoreCache(opts.supabase, {
            cacheKey: stableHash(`fallback|${opts.callId}|${RUBRIC_VERSION}|${SCORING_PROMPT_VERSION}`),
            callSha256: null,
            transcriptHash: null,
            result: fb,
        });
        // CRM Activity: record a score event for fallback (best-effort)
        try {
            const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
            const { data: callRow } = await svc
                .from('calls')
                .select('id, account_id, contact_id, score_overall')
                .eq('id', opts.callId)
                .single();
            const overall = typeof callRow?.score_overall === 'number' ? callRow.score_overall : fb.overall;
            const summary = `Scored ${Math.round(overall)}`;
            await svc.from('crm_activities').insert({
                type: 'score',
                summary,
                org_id: memoryCall?.org_id ?? null,
                rep_id: memoryCall?.user_id ?? null,
                call_id: opts.callId,
                source: 'scoring_engine_fallback',
                meta: {
                    action_type: 'score_recorded',
                    overall,
                    stage_scores: fb.stages,
                    threshold_band: thresholdBand,
                    needs_manager_review: thresholdBand === 'critical',
                    review_flag_count: reviewFlags.length,
                    review_flags: reviewFlags,
                },
                account_id: callRow?.account_id ?? null,
                contact_id: callRow?.contact_id ?? null,
            });
        }
        catch (e) {
            console.warn('[score] activity insert failed (fallback)', e);
        }
        // CRM Activity: review_flag (structured + analytics-ready)
        try {
            if (reviewFlags.length > 0) {
                const svc = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
                await svc.from('crm_activities').insert(reviewFlags.map((flag) => ({
                    type: 'review_flag',
                    // ✅ CORE FIELDS
                    summary: `${flag.type} (${flag.severity})`,
                    org_id: call?.org_id ?? null,
                    rep_id: call?.user_id ?? null,
                    call_id: callId,
                    source: 'scoring_engine',
                    // 🚀 NEW ANALYTICS FIELDS
                    flag_key: flag.type,
                    flag_category: 'score_threshold',
                    flag_severity: flag.severity,
                    flag_section: flag.section,
                    // 🧠 FLEXIBLE META
                    meta: {
                        score: flag.score,
                        timestamp: flag.timestamp ?? null,
                        threshold_band: flag.severity,
                        thresholds: {
                            low: thresholds.low,
                            critical: thresholds.critical,
                        },
                        needs_manager_review: flag.severity === 'critical',
                    },
                })));
            }
        }
        catch (e) {
            console.warn('[score] review_flag activity insert failed', e);
        }
        await ensureCriticalCallAssignment({
            supabase: opts.supabase,
            callId: opts.callId,
            orgId: memoryCall?.org_id ?? null,
            assigneeUserId: memoryCall?.user_id ?? null,
            overall: fb.overall,
            thresholdBand,
            reviewFlags,
            source: "scoring_engine_fallback",
        });
        // Grab duration if present for Slack
        let durationSec = null;
        try {
            const { data } = await opts.supabase
                .from("calls")
                .select("duration_sec")
                .eq("id", opts.callId)
                .single();
            durationSec = data?.duration_sec ?? null;
        }
        catch { }
        if (!SKIP_SCORING_SIDE_EFFECTS) {
            const slackStart = Date.now();
            await notifySlack({
                supabase: opts.supabase,
                callId: opts.callId,
                repIdFallback: null,
                durationSec,
                scores: {
                    intro: fb.stages.intro.score,
                    discovery: fb.stages.discovery.score,
                    objection: fb.stages.objection.score,
                    close: fb.stages.close.score,
                    overall: fb.overall,
                },
            });
            console.log("[perf] slack_ms", Date.now() - slackStart, { callId: opts.callId });
            await autoSendReviewFeedbackEmailForWorkerBestEffort({
                supabase: opts.supabase,
                callId: opts.callId,
                repId: String(memoryCall?.user_id ?? ""),
                scoreOverall: fb.overall,
                voiceScore: fallbackVoice.overall,
                summary: fb.summary ?? null,
                reviewTags: {
                    weak_close: fb.stages.close.score < 60,
                    filler_count: 0,
                    filler_words: [],
                },
            });
        }
        if (!SKIP_SCORING_SIDE_EFFECTS) {
            try {
                const repMemoryStart = Date.now();
                await upsertRepMemory(opts.supabase, {
                    userId: String(memoryCall?.user_id),
                    companyId: memoryCall?.org_id ?? null,
                    callId: opts.callId,
                    overall: fb.overall,
                    stages: fb.stages,
                    voice: fallbackVoice,
                    moments: fallbackMoments,
                });
                console.log("[perf] rep_memory_ms", Date.now() - repMemoryStart, { callId: opts.callId });
            }
            catch (e) {
                console.warn("[rep_memory] fallback update failed", e?.message || e);
            }
        }
        console.log("[perf] total_score_ms", Date.now() - t0, { callId: opts.callId, path: "fallback" });
        return {
            ...fb,
            moments: fallbackMoments,
            suggestions: fallbackSuggestions,
            voice: fallbackVoice,
        };
    }
}
