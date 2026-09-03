export type SendEmailArgs = {
  to: string | string[];
  subject: string;
  html?: string;
  text?: string;
  from?: string;
  replyTo?: string | null;
  cc?: string | string[];
  bcc?: string | string[];
  tags?: Array<{ name: string; value: string }>;
};

export type ReviewFeedbackEmailArgs = {
  repEmail?: string | null;
  managerEmail?: string | null;
  repName?: string | null;
  managerName?: string | null;
  callId: string;
  scoreOverall: number;
  voiceScore: number;
  summary?: string | null;
  weakClose?: boolean;
  fillerCount?: number;
  fillerWords?: string[] | null;
  assignmentId?: string | null;
  appUrl?: string | null;
};

function asArray(value?: string | string[] | null): string[] {
  if (!value) return [];
  return Array.isArray(value)
    ? value.map((x) => String(x || "").trim()).filter(Boolean)
    : [String(value).trim()].filter(Boolean);
}

function compactText(value: unknown, max = 240): string {
  const s = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!s) return "";
  return s.length <= max ? s : `${s.slice(0, max - 1)}…`;
}

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normaliseScore(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.min(100, Math.round(value))) : 0;
}

function getBaseUrl(explicit?: string | null): string {
  return (
    explicit ||
    process.env.WEB_APP_URL ||
    process.env.WEB_ORIGIN ||
    "http://localhost:3000"
  );
}

export function buildReviewFeedbackText(args: ReviewFeedbackEmailArgs): string {
  const score = normaliseScore(args.scoreOverall);
  const voice = normaliseScore(args.voiceScore);
  const callUrl = `${getBaseUrl(args.appUrl)}/calls/${args.callId}`;
  const fillerWords = Array.isArray(args.fillerWords)
    ? args.fillerWords.map((x) => String(x || "").trim()).filter(Boolean).slice(0, 5)
    : [];

  return [
    `Review complete for call ${args.callId}`,
    args.repName ? `Rep: ${args.repName}` : null,
    args.managerName ? `Manager: ${args.managerName}` : null,
    args.assignmentId ? `Assignment: ${args.assignmentId}` : null,
    `Call score: ${score}`,
    `Voice score: ${voice}`,
    args.weakClose ? `Weak close: Yes` : null,
    Number(args.fillerCount ?? 0) > 0 ? `Filler count: ${Math.round(Number(args.fillerCount ?? 0))}` : null,
    fillerWords.length ? `Top fillers: ${fillerWords.join(", ")}` : null,
    args.summary ? `Summary: ${compactText(args.summary, 500)}` : null,
    `Open reviewed call: ${callUrl}`,
  ]
    .filter(Boolean)
    .join("\n");
}

export function buildReviewFeedbackHtml(args: ReviewFeedbackEmailArgs): string {
  const score = normaliseScore(args.scoreOverall);
  const voice = normaliseScore(args.voiceScore);
  const callUrl = `${getBaseUrl(args.appUrl)}/calls/${args.callId}`;
  const fillerWords = Array.isArray(args.fillerWords)
    ? args.fillerWords.map((x) => String(x || "").trim()).filter(Boolean).slice(0, 5)
    : [];

  const flags = [
    args.weakClose ? "Weak close" : null,
    Number(args.fillerCount ?? 0) > 0 ? `Fillers: ${Math.round(Number(args.fillerCount ?? 0))}` : null,
    fillerWords.length ? `Top fillers: ${fillerWords.join(", ")}` : null,
  ].filter(Boolean);

  return `
  <div style="font-family: Inter, Arial, sans-serif; background: #0a0a0a; color: #f5f5f5; padding: 24px;">
    <div style="max-width: 640px; margin: 0 auto; border: 1px solid #262626; border-radius: 16px; overflow: hidden; background: #111111;">
      <div style="padding: 20px 24px; border-bottom: 1px solid #262626;">
        <div style="font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: #a3a3a3;">Gravix Sales Trainer</div>
        <h1 style="margin: 8px 0 0; font-size: 20px; line-height: 1.3;">Reviewed call feedback</h1>
      </div>

      <div style="padding: 24px;">
        <div style="display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px;">
          <div style="min-width: 140px; padding: 12px 14px; border: 1px solid #262626; border-radius: 12px; background: #0a0a0a;">
            <div style="font-size: 12px; color: #a3a3a3;">Call score</div>
            <div style="margin-top: 4px; font-size: 24px; font-weight: 700;">${escapeHtml(score)}</div>
          </div>
          <div style="min-width: 140px; padding: 12px 14px; border: 1px solid #262626; border-radius: 12px; background: #0a0a0a;">
            <div style="font-size: 12px; color: #a3a3a3;">Voice score</div>
            <div style="margin-top: 4px; font-size: 24px; font-weight: 700;">${escapeHtml(voice)}</div>
          </div>
        </div>

        <div style="font-size: 14px; line-height: 1.6; color: #d4d4d4;">
          ${args.repName ? `<div><strong style="color:#fafafa;">Rep:</strong> ${escapeHtml(args.repName)}</div>` : ""}
          ${args.managerName ? `<div><strong style="color:#fafafa;">Manager:</strong> ${escapeHtml(args.managerName)}</div>` : ""}
          ${args.assignmentId ? `<div><strong style="color:#fafafa;">Assignment:</strong> ${escapeHtml(args.assignmentId)}</div>` : ""}
          <div><strong style="color:#fafafa;">Call:</strong> ${escapeHtml(args.callId)}</div>
        </div>

        ${flags.length ? `
          <div style="margin-top: 20px;">
            <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: #a3a3a3; margin-bottom: 8px;">Coaching flags</div>
            <div style="display: flex; gap: 8px; flex-wrap: wrap;">
              ${flags
                .map(
                  (flag) =>
                    `<span style="display:inline-block; padding:6px 10px; border-radius:999px; border:1px solid #3f3f46; background:#18181b; color:#e5e7eb; font-size:12px; font-weight:600;">${escapeHtml(flag)}</span>`
                )
                .join("")}
            </div>
          </div>
        ` : ""}

        ${args.summary ? `
          <div style="margin-top: 20px; padding: 16px; border: 1px solid #262626; border-radius: 12px; background: #0a0a0a;">
            <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: #a3a3a3; margin-bottom: 8px;">Summary</div>
            <div style="font-size: 14px; line-height: 1.6; color: #e5e5e5;">${escapeHtml(compactText(args.summary, 500))}</div>
          </div>
        ` : ""}

        <div style="margin-top: 24px;">
          <a href="${escapeHtml(callUrl)}" style="display:inline-block; padding:12px 16px; border-radius:10px; background:#ffffff; color:#111111; text-decoration:none; font-weight:700;">Open reviewed call</a>
        </div>
      </div>
    </div>
  </div>`;
}

export async function sendEmail(args: SendEmailArgs): Promise<{ ok: boolean; id?: string | null; error?: string }> {
  const apiKey = process.env.POSTMARK_SERVER_TOKEN || "";
  const from = args.from || process.env.EMAIL_FROM || "Gravix <noreply@gravixbots.com>";
  const to = asArray(args.to);
  const cc = asArray(args.cc);
  const bcc = asArray(args.bcc);

  if (!to.length) {
    return { ok: false, error: "missing_recipient" };
  }

  if (!apiKey) {
    console.warn("[email] POSTMARK_SERVER_TOKEN missing; skipping send", { to, subject: args.subject });
    return { ok: false, error: "missing_postmark_token" };
  }

  const payload: Record<string, unknown> = {
    from,
    to,
    subject: args.subject,
    html: args.html || undefined,
    text: args.text || undefined,
  };

  if (cc.length) payload.cc = cc;
  if (bcc.length) payload.bcc = bcc;
  if (args.replyTo) payload.reply_to = args.replyTo;
  if (args.tags?.length) payload.tags = args.tags;

  const res = await fetch("https://api.postmarkapp.com/email", {
    method: "POST",
    headers: {
      "X-Postmark-Server-Token": apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      From: from,
      To: to.join(","),
      Cc: cc.length ? cc.join(",") : undefined,
      Bcc: bcc.length ? bcc.join(",") : undefined,
      Subject: args.subject,
      HtmlBody: args.html || undefined,
      TextBody: args.text || undefined,
      Tag: args.tags?.[0]?.value || "gravix",
    }),
  });

  const json = (await res.json().catch(() => ({}))) as { MessageID?: string; Message?: string; ErrorCode?: number };

  if (!res.ok || json?.ErrorCode) {
    return {
      ok: false,
      error: String(json?.Message || `postmark_send_failed_${res.status}`),
    };
  }

  return { ok: true, id: json?.MessageID ?? null };
}

export async function sendReviewFeedbackEmail(args: ReviewFeedbackEmailArgs) {
  const recipients = [...asArray(args.repEmail), ...asArray(args.managerEmail)];
  if (!recipients.length) {
    return { ok: false, error: "missing_feedback_recipients" };
  }

  const subject = `Reviewed call feedback · score ${normaliseScore(args.scoreOverall)} · call ${args.callId}`;
  const html = buildReviewFeedbackHtml(args);
  const text = buildReviewFeedbackText(args);

  return sendEmail({
    to: recipients,
    subject,
    html,
    text,
    tags: [
      { name: "type", value: "review_feedback" },
      { name: "call_id", value: args.callId },
    ],
  });
}