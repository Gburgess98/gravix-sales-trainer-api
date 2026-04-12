export type Segment = {
  speaker: "Rep" | "Prospect";
  text: string;
  start_sec: number;
  end_sec: number;
};

const FILLER_RE = /\b(uh|um|erm|er|ah)\b/gi;

function normaliseWhitespace(text: string): string {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\t/g, " ")
    .replace(/[ ]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function cleanupLine(line: string): string {
  let t = String(line || "").trim();
  if (!t) return "";

  // remove obvious fillers but do not over-strip useful words like "like"
  t = t.replace(FILLER_RE, " ");

  // collapse repeated punctuation / spaces
  t = t.replace(/[ ]{2,}/g, " ").trim();
  t = t.replace(/\s+([,?.!])/g, "$1");
  t = t.replace(/([,?.!]){2,}/g, "$1");

  // if line looks like sentence fragments stuck together, add soft sentence break
  t = t.replace(/([a-z0-9])\s+([A-Z])/g, "$1. $2");

  // capitalise first character
  if (t.length > 0) {
    t = t.charAt(0).toUpperCase() + t.slice(1);
  }

  // end with sentence punctuation if it looks like a sentence
  if (t && !/[.!?]$/.test(t)) {
    t += ".";
  }

  return t;
}

export function cleanTranscript(text: string): string {
  const raw = normaliseWhitespace(text);
  if (!raw) return "";

  const lines = raw
    .split("\n")
    .map((line) => cleanupLine(line))
    .filter(Boolean);

  // if uploader gave one giant block, preserve it as readable sentences
  if (lines.length <= 1) {
    return cleanupLine(raw);
  }

  return lines.join("\n");
}

function estimateDurationSec(text: string): number {
  const words = String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;

  // simple speaking-rate estimate
  // 150 wpm ≈ 2.5 words/sec
  const seconds = words > 0 ? words / 2.5 : 2;

  // keep segments sensible for UI
  return Math.max(2, Math.min(12, Math.round(seconds)));
}

function splitIntoUnits(cleanedText: string): string[] {
  const byLines = cleanedText
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);

  if (byLines.length > 1) return byLines;

  // fallback: split one big transcript block into sentence-ish chunks
  return cleanedText
    .split(/(?<=[.!?])\s+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

export function buildSegments(rawText: string): Segment[] {
  const cleaned = cleanTranscript(rawText);
  if (!cleaned) return [];

  const units = splitIntoUnits(cleaned);
  if (!units.length) return [];

  const segments: Segment[] = [];
  let cursor = 0;

  for (let i = 0; i < units.length; i += 1) {
    const text = units[i];
    const duration = estimateDurationSec(text);

    segments.push({
      speaker: i % 2 === 0 ? "Rep" : "Prospect",
      text,
      start_sec: cursor,
      end_sec: cursor + duration,
    });

    cursor += duration;
  }

  return segments;
}

export function findNearestSegment(
  segments: Segment[],
  timestamp?: number | null
): Segment | null {
  if (!Array.isArray(segments) || !segments.length) return null;
  if (!Number.isFinite(Number(timestamp))) return null;

  const ts = Number(timestamp);

  let best: Segment | null = null;
  let bestDistance = Number.POSITIVE_INFINITY;

  for (const seg of segments) {
    const start = Number(seg.start_sec ?? 0);
    const end = Number(seg.end_sec ?? start);
    const mid = start + (end - start) / 2;
    const dist = Math.abs(mid - ts);

    if (dist < bestDistance) {
      best = seg;
      bestDistance = dist;
    }
  }

  return best;
}