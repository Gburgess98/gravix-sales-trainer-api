// No import needed; Node 18+ has global fetch
const WEBHOOK = process.env.SLACK_WEBHOOK_URL || "";
export async function postSlack(text, blocks) {
    if (!WEBHOOK)
        return; // silently skip in dev if unset
    const body = blocks ? { text, blocks } : { text };
    const res = await fetch(WEBHOOK, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    if (!res.ok) {
        const t = await res.text().catch(() => "");
        console.error("Slack post failed:", res.status, t);
    }
}
