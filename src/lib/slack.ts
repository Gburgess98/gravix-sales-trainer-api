// src/lib/slack.ts

// ---------- Block Kit types ----------
export type SlackText =
  | { type: "mrkdwn"; text: string }
  | { type: "plain_text"; text: string; emoji?: boolean };

export type SlackBlock =
  | { type: "section"; text?: SlackText; fields?: SlackText[] }
  | { type: "header"; text: Extract<SlackText, { type: "plain_text" }> }
  | {
      type: "actions";
      elements: Array<{
        type: "button";
        text: { type: "plain_text"; text: string };
        url: string;
        style?: "primary" | "danger";
        action_id?: string;
      }>;
    }
  | { type: "context"; elements: SlackText[] }
  | { type: "divider" };

type SlackPayload = { text?: string; blocks?: SlackBlock[] };

// ---------- Low-level posters ----------
export async function postSlack(
  text: string,
  blocks?: SlackBlock[],
  explicitWebhook?: string
) {
  const url = explicitWebhook || process.env.SLACK_WEBHOOK_URL;
  if (!url) return { ok: false as const, skipped: true as const }; // non-fatal

  const body: SlackPayload = blocks?.length ? { text, blocks } : { text };
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const msg = await r.text().catch(() => r.statusText);
    throw new Error(`Slack webhook failed: HTTP ${r.status} ${msg}`);
  }
  return { ok: true as const };
}

export async function postSlackSummary(
  payload: { blocks: SlackBlock[]; text?: string },
  explicitWebhook?: string
) {
  const url = explicitWebhook || process.env.SLACK_WEBHOOK_URL;
  if (!url) return { ok: false as const, skipped: true as const }; // non-fatal

  const body: SlackPayload = { text: payload.text, blocks: payload.blocks };
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const msg = await r.text().catch(() => r.statusText);
    throw new Error(`Slack webhook failed: HTTP ${r.status} ${msg}`);
  }
  return { ok: true as const };
}

// =========================================================
// Rich “Call Scored” summary (Intro / Discovery / Objection / Close)
// =========================================================

export type RubricSection = {
  score?: number | null;
  notes?: string | null;
};

export type Rubric = {
  intro?: RubricSection | null;
  discovery?: RubricSection | null;
  objection?: RubricSection | null;
  close?: RubricSection | null;
};

export type ScoreSummaryInput = {
  callId: string;
  filename?: string | null;
  overall?: number | null;
  rubric?: Rubric | null;           // <— supports notes
  durationSec?: number | null;
  appUrlBase?: string | null;       // e.g. https://your-web.vercel.app
  // Optional cosmetics:
  repName?: string | null;
  contactName?: string | null;
  company?: string | null;
};

/** Build human-friendly text for a rubric section */
function secText(label: string, s?: RubricSection | null) {
  const score = typeof s?.score === "number" ? Math.round(s.score) : "—";
  const notes =
    (s?.notes ? String(s.notes).trim().slice(0, 700) : "—") // trim long notes
      || "—";
  return `*${label}*: ${score}\n${notes}`;
}

/** Compose Slack blocks for a scored call (with notes + action buttons) */
export function buildScoreBlocksFromRubric(input: ScoreSummaryInput) {
  const {
    callId,
    filename,
    overall,
    rubric,
    durationSec,
    repName,
    contactName,
    company,
  } = input;

  const overallStr = typeof overall === "number" ? `${Math.round(overall)}/100` : "—";
  const title = filename || `Call ${callId.slice(0, 8)}…`;
  const dur =
    typeof durationSec === "number"
      ? `${Math.floor(durationSec / 60)}m ${Math.floor(durationSec % 60)}s`
      : "—";

  const metaParts = [
    repName ? `*Rep*: ${repName}` : null,
    contactName ? `*Contact*: ${contactName}` : null,
    company ? `*Company*: ${company}` : null,
    `*Duration*: ${dur}`,
    `*Call ID*: \`${callId}\``,
  ].filter(Boolean);

  const blocks: SlackBlock[] = [
    { type: "header", text: { type: "plain_text", text: `Call scored: ${overallStr} • ${title}`, emoji: true } },
    { type: "section", text: { type: "mrkdwn", text: metaParts.join("  •  ") || "—" } },
    { type: "divider" },
    { type: "section", text: { type: "mrkdwn", text: secText("Intro", rubric?.intro) } },
    { type: "section", text: { type: "mrkdwn", text: secText("Discovery", rubric?.discovery) } },
    { type: "section", text: { type: "mrkdwn", text: secText("Objection", rubric?.objection) } },
    { type: "section", text: { type: "mrkdwn", text: secText("Close", rubric?.close) } },
  ];

  // Add action buttons if we can build URLs
  const base = input.appUrlBase || process.env.APP_URL_BASE || process.env.WEB_ORIGIN || null;
  if (base) {
    const openUrl = `${base.replace(/\/$/, "")}/calls/${callId}`;
    const crmUrl = `${base.replace(/\/$/, "")}/calls/${callId}?panel=crm`;
    blocks.push({ type: "divider" });
    blocks.push({
      type: "actions",
      elements: [
        { type: "button", text: { type: "plain_text", text: "Open Call" }, url: openUrl, action_id: "open_call" },
        { type: "button", text: { type: "plain_text", text: "Link / Review CRM" }, url: crmUrl, action_id: "open_crm" },
      ],
    });
  }

  return { blocks };
}

// ---------- Back-compat: your older numeric-only API ----------

export type SummaryInput = {
  callId: string;
  repName?: string;
  contactName?: string;
  company?: string;
  overallScore: number;
  section: { intro: number; discovery: number; objection: number; close: number };
  durationSec?: number;
  callUrl: string;
  recentUrl: string;
};

/** Legacy builder kept for compatibility with existing callers */
export function buildScoreBlocks(s: SummaryInput) {
  const dur =
    typeof s.durationSec === "number"
      ? `${Math.floor(s.durationSec / 60)}m ${s.durationSec % 60}s`
      : "—";

  const header = `*${Math.round(s.overallScore)}* / 100  •  Intro ${s.section.intro} | Disc ${s.section.discovery} | Obj ${s.section.objection} | Close ${s.section.close}`;

  const subtitleParts = [
    s.repName ? `Rep: ${s.repName}` : null,
    s.contactName ? `Contact: ${s.contactName}` : null,
    s.company ? `@ ${s.company}` : null,
    `Duration: ${dur}`,
  ].filter(Boolean);

  const blocks: SlackBlock[] = [
    { type: "header", text: { type: "plain_text", text: "Call Scored", emoji: true } },
    { type: "section", text: { type: "mrkdwn", text: header } },
    { type: "context", elements: [{ type: "mrkdwn", text: subtitleParts.join("  •  ") || "—" }] },
    {
      type: "section",
      fields: [
        { type: "mrkdwn", text: `*Intro*\n${s.section.intro}` },
        { type: "mrkdwn", text: `*Discovery*\n${s.section.discovery}` },
        { type: "mrkdwn", text: `*Objection*\n${s.section.objection}` },
        { type: "mrkdwn", text: `*Close*\n${s.section.close}` },
      ],
    },
    {
      type: "actions",
      elements: [
        { type: "button", text: { type: "plain_text", text: "Open Call" }, url: s.callUrl, action_id: "open_call" },
        { type: "button", text: { type: "plain_text", text: "Recent Calls" }, url: s.recentUrl, action_id: "open_recent" },
      ],
    },
  ];

  return { blocks };
}

// ---------- Single entry point for scoring code ----------
/**
 * New preferred entry:
 *   await postScoreSummary({
 *     callId, filename: call.filename, overall: call.score_overall,
 *     rubric: call.rubric, durationSec: call.duration_sec,
 *     appUrlBase: process.env.APP_URL_BASE
 *   });
 *
 * (Falls back to no-op if SLACK_WEBHOOK_URL is not set.)
 */
export async function postScoreSummary(input: ScoreSummaryInput) {
  const payload = buildScoreBlocksFromRubric(input);
  return postSlackSummary(payload);
}