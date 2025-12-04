export function buildScoreSummaryBlocks(args: {
  callId: string;
  rep?: string;
  overall: number;
  intro: number; discovery: number; pitch: number; objection: number; close: number;
  vps: number;
}) {
  const { callId, rep="Unknown Rep", overall, intro, discovery, pitch, objection, close, vps } = args;
  return [
    { type: "header", text: { type: "plain_text", text: `Call Scored: ${overall}` } },
    { type: "section", fields: [
      { type: "mrkdwn", text: `*Rep:*\n${rep}` },
      { type: "mrkdwn", text: `*VPS™:*\n${vps}` },
      { type: "mrkdwn", text: `*Intro:*\n${intro}` },
      { type: "mrkdwn", text: `*Discovery:*\n${discovery}` },
      { type: "mrkdwn", text: `*Pitch:*\n${pitch}` },
      { type: "mrkdwn", text: `*Objection:*\n${objection}` },
      { type: "mrkdwn", text: `*Close:*\n${close}` },
    ]},
    { type: "actions", elements: [
      { type: "button", text: { type: "plain_text", text: "Open Call" }, url: `https://gravix-sales-trainer-web.vercel.app/calls/${callId}?panel=coach` },
      { type: "button", text: { type: "plain_text", text: "Assign Drill" }, url: `https://gravix-sales-trainer-web.vercel.app/recent-calls?weakSpot=Close` },
    ]}
  ];
}