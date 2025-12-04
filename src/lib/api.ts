// add to src/lib/api.ts
const PROXY = "/api/proxy";
export async function getCall(callId: string) {
  const res = await fetch(
    `${(process.env.NEXT_PUBLIC_API_BASE ?? 'http://localhost:4000')}/v1/calls/${encodeURIComponent(callId)}`
  );
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json(); // { ok, call: {..., signedAudioUrl} }
}
// --- Rep overview (Rep Profile Page)
export async function getRepOverview(repId: string) {
  const url = `${PROXY}/v1/reps/${encodeURIComponent(repId)}/overview`;
  const res = await fetch(url, {
    method: 'GET',
    headers: { 'cache-control': 'no-cache' }
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`getRepOverview failed: ${res.status} ${text}`);
  }
  return res.json();
}
// ---- Rewards helpers (avoid duplicates) ----
export async function getRewards(userId: string) {
  const url = `${PROXY}/v1/rewards/${encodeURIComponent(userId)}`;
  // prefer jfetch if available
  try {
    // @ts-ignore - jfetch may exist in this project
    if (typeof jfetch === "function") {
      // @ts-ignore
      return await jfetch(url);
    }
  } catch {}
  const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`getRewards failed: ${res.status} ${text}`);
  }
  return res.json();
}

export async function selectTitle(userId: string, titleId: string) {
  const url = `${PROXY}/v1/rewards/${encodeURIComponent(userId)}/select-title`;
  try {
    // @ts-ignore
    if (typeof jfetch === "function") {
      // @ts-ignore
      return await jfetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ titleId })
      });
    }
  } catch {}
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ titleId })
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`selectTitle failed: ${res.status} ${text}`);
  }
  return res.json();
}

export async function listActiveBounties() {
  const url = `${PROXY}/v1/rewards/bounties/active`;
  try {
    // @ts-ignore
    if (typeof jfetch === "function") {
      // @ts-ignore
      return await jfetch(url);
    }
  } catch {}
  const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`listActiveBounties failed: ${res.status} ${text}`);
  }
  return res.json();
}

// ---------------------------
// Sparring sessions (Call Library / Dashboards)
// ---------------------------

export type SparringSession = {
  id: string;
  rep_id: string;
  persona_id: string | null;
  total: number | null;
  xp_awarded: number | null;
  created_at: string;
};

export async function listSparringSessions(limit: number = 20) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));

  const url = `${PROXY}/v1/sparring/sessions?${params.toString()}`;

  // Prefer jfetch if it's available in this project
  try {
    // @ts-ignore
    if (typeof jfetch === "function") {
      // @ts-ignore
      return await jfetch(url);
    }
  } catch {}

  const res = await fetch(url, {
    method: "GET",
    headers: { "cache-control": "no-cache" },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`listSparringSessions failed: ${res.status} ${text}`);
  }

  const json = await res.json();
  // API shape: { ok: true, sessions: [...] }
  return (json.sessions ?? []) as SparringSession[];
}