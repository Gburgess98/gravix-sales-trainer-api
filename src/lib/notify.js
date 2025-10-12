import fetch from "node-fetch";
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const WEB_ORIGIN = process.env.WEB_ORIGIN || "http://localhost:3000";
export async function sendSlackOnScored(opts) {
    const { callId, score, model } = opts;
    const url = `${WEB_ORIGIN}/calls/${callId}`;
    const payload = {
        blocks: [
            { type: "section", text: { type: "mrkdwn", text: `*Call scored:* *${score}*/100\nModel: \`${model}\`` } },
            {
                type: "actions",
                elements: [{ type: "button", text: { type: "plain_text", text: "Open Call" }, url, style: "primary" }],
            },
        ],
    };
    try {
        await fetch(SLACK_WEBHOOK_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
    }
    catch (e) {
        console.error("Slack webhook failed", e);
    }
}
