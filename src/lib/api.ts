// add to src/lib/api.ts
export async function getCall(callId: string) {
  const res = await fetch(
    `${(process.env.NEXT_PUBLIC_API_BASE ?? 'http://localhost:4000')}/v1/calls/${encodeURIComponent(callId)}`
  );
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json(); // { ok, call: {..., signedAudioUrl} }
}