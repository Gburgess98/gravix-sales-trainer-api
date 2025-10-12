// src/lib/openai.ts
import OpenAI from "openai";
export const AI_MODEL = process.env.AI_MODEL || "gpt-4o-mini";
export const OPENAI_TIMEOUT_MS = Number(process.env.OPENAI_TIMEOUT_MS || 8000);
let client = null;
export function getOpenAI() {
    const key = process.env.OPENAI_API_KEY;
    if (!key)
        throw new Error("OPENAI_API_KEY missing");
    if (!client)
        client = new OpenAI({ apiKey: key });
    return client;
}
