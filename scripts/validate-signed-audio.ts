// signed-audio decision proof (network-free). Run: npm run validate:signed-audio
// Covers: existing audio; no audio reference; object missing from storage; genuine storage failure; owner / manager /
// cross-tenant access with no existence oracle; no path / provider text in any response; route wiring.
import * as fs from "node:fs";
import * as path from "node:path";
import { resolveSignedAudio, classifyStorageError, type CallAudioRow, type SignResult, type SignedAudioDeps } from "../src/lib/signedAudio";

let failures = 0;
const check = (label: string, ok: boolean, detail = "") => { console.log(`${ok ? "OK  " : "FAIL"}  ${label}${!ok && detail ? ` — ${detail}` : ""}`); if (!ok) failures++; };

const OWNER = "00000000-0000-4000-8000-0000000000a1", MANAGER = "00000000-0000-4000-8000-0000000000b1",
  OTHER_TENANT = "00000000-0000-4000-8000-0000000000c1", REP2 = "00000000-0000-4000-8000-0000000000a2";
const ORG = "org-1", OTHER_ORG = "org-2";
const SECRET_PATH = "tenant-1/private/folder/secret-recording.mp3";
const CALLS: Record<string, CallAudioRow> = {
  "call-audio": { user_id: OWNER, org_id: ORG, audio_path: SECRET_PATH },
  "call-noaudio": { user_id: OWNER, org_id: ORG, audio_path: null },
  "call-missingobj": { user_id: OWNER, org_id: ORG, audio_path: "gone/object.mp3" },
  "call-storagefail": { user_id: OWNER, org_id: ORG, audio_path: "flaky/object.mp3" },
};
const ORG_OF: Record<string, string> = { [OWNER]: ORG, [MANAGER]: ORG, [REP2]: ORG, [OTHER_TENANT]: OTHER_ORG };
const logs: string[] = [];
function deps(over: Partial<SignedAudioDeps> = {}): SignedAudioDeps {
  return {
    ttlSec: 300,
    getCall: async (id) => CALLS[id] ?? null,
    // mirrors canAccessCall: owner, or same org + manager visibility; never cross-org
    canAccess: async (req, owner, org) => req === owner || (!!org && ORG_OF[req] === org && req === MANAGER),
    sign: async (p): Promise<SignResult> =>
      p === SECRET_PATH ? { ok: true, url: "https://signed.example/x?token=t" }
        : p === "gone/object.mp3" ? { ok: false, reason: classifyStorageError({ name: "StorageApiError", status: 400, statusCode: "404", message: "Object not found" }) }
        : { ok: false, reason: classifyStorageError({ name: "StorageUnknownError", status: 502, message: "upstream connect error host=db.internal" }) },
    log: (e, d) => logs.push(`${e} ${JSON.stringify(d)}`),
    ...over,
  };
}
const run = (who: string, id: string, d = deps()) => resolveSignedAudio(d, who, id);
const bodyText = (r: { body: unknown }) => JSON.stringify(r.body);

(async () => {
  // 1 existing audio
  let r = await run(OWNER, "call-audio");
  check("existing audio → 200 with url + ttl", r.status === 200 && (r.body as any).ok === true && typeof (r.body as any).url === "string" && (r.body as any).ttl === 300);
  // 2 no audio reference
  r = await run(OWNER, "call-noaudio");
  check("call with no audio reference → 404 no_audio", r.status === 404 && (r.body as any).error === "no_audio", bodyText(r));
  // 3 referenced object missing from storage
  r = await run(OWNER, "call-missingobj");
  check("object missing from storage → 404 no_audio", r.status === 404 && (r.body as any).error === "no_audio", bodyText(r));
  check("  …and nothing about the path or provider in the body", !/gone|object|Object not found|storage/i.test(bodyText(r).replace("no_audio", "")), bodyText(r));
  // 4 genuine storage failure
  r = await run(OWNER, "call-storagefail");
  check("storage/provider failure → 500 audio_unavailable (not converted to 404)", r.status === 500 && (r.body as any).error === "audio_unavailable", bodyText(r));
  check("  …generic body: no provider text, host or path", !/upstream|db\.internal|flaky|object\.mp3|connect/i.test(bodyText(r)), bodyText(r));
  r = await run(OWNER, "call-audio", deps({ sign: async () => { throw new Error("socket hang up secret-host"); } }));
  check("thrown storage exception → 500 audio_unavailable, message not exposed", r.status === 500 && !/socket|secret-host/.test(bodyText(r)), bodyText(r));
  r = await run(OWNER, "call-audio", deps({ getCall: async () => { throw new Error("pg: connection refused 10.0.0.5"); } }));
  check("database error on lookup → 500 audio_unavailable, message not exposed", r.status === 500 && !/pg|10\.0\.0\.5|refused/.test(bodyText(r)), bodyText(r));
  r = await run(OWNER, "call-audio", deps({ canAccess: async () => { throw new Error("lookup exploded"); } }));
  check("access-lookup error fails closed (500, no audio)", r.status === 500 && !(r.body as any).url, bodyText(r));
  // classifier
  const cls = (e: unknown) => classifyStorageError(e);
  check("classifier: Supabase 'Object not found' (statusCode 404) → missing", cls({ status: 400, statusCode: "404", message: "Object not found" }) === "missing");
  check("classifier: HTTP 404 → missing", cls({ status: 404, message: "x" }) === "missing");
  check("classifier: 500 / 502 / 401 / network / unknown / null → failure",
    [{ status: 500, statusCode: "500", message: "boom" }, { status: 502 }, { status: 401, statusCode: "401", message: "Invalid JWT" }, { message: "fetch failed" }, {}, null, "str", undefined].every((e) => cls(e) === "failure"));
  check("classifier: 'Bucket not found' is a failure (misconfiguration, not a missing recording)", cls({ status: 400, statusCode: "400", message: "Bucket not found" }) === "failure");
  // 5 authorisation
  r = await run(OWNER, "call-audio");
  check("owner can request own call", r.status === 200);
  r = await run(MANAGER, "call-audio");
  check("manager can request an in-tenant call", r.status === 200);
  r = await run(REP2, "call-audio");
  check("another rep in the tenant is denied (404 not_found)", r.status === 404 && (r.body as any).error === "not_found", bodyText(r));
  r = await run(OTHER_TENANT, "call-audio");
  check("cross-tenant caller is denied", r.status === 404 && (r.body as any).error === "not_found", bodyText(r));
  // no existence/audio oracle for denied callers
  const missing = await run(OTHER_TENANT, "no-such-call");
  const sameShape = async (id: string) => { const x = await run(OTHER_TENANT, id); return x.status === missing.status && bodyText(x) === bodyText(missing); };
  check("denied caller: unknown call, call with audio, no audio, missing object, failing storage are byte-identical",
    (await Promise.all(["call-audio", "call-noaudio", "call-missingobj", "call-storagefail"].map(sameShape))).every(Boolean));
  const mgrNoAudio = await run(MANAGER, "call-noaudio");
  check("manager sees no_audio only for an in-tenant call", mgrNoAudio.status === 404 && (mgrNoAudio.body as any).error === "no_audio");
  check("denied cross-tenant caller never gets no_audio / audio_unavailable / 200", !(await Promise.all(["call-noaudio", "call-missingobj", "call-storagefail"].map((id) => run(OTHER_TENANT, id)))).some((x) => x.status !== 404 || (x.body as any).error !== "not_found"));
  // responses never contain a stored path
  const all = await Promise.all(["call-noaudio", "call-missingobj", "call-storagefail"].flatMap((id) => [OWNER, MANAGER, OTHER_TENANT].map((u) => run(u, id))));
  check("no response body anywhere contains a storage path", !all.some((x) => /\.mp3|\//.test(bodyText(x))));
  check("diagnostics are logged server-side without the storage path", logs.length > 0 && !logs.some((l) => /object\.mp3|secret-recording|upstream/.test(l)), logs[0]);

  // 6 route wiring
  const src = fs.readFileSync(path.join(__dirname, "../src/routes/calls.ts"), "utf8");
  const h = src.slice(src.indexOf('router.get("/:id/signed-audio"'), src.indexOf("POST /v1/calls/:id/score"));
  check("route delegates to resolveSignedAudio with the real canAccessCall", /resolveSignedAudio\(/.test(h) && /canAccess:\s*canAccessCall/.test(h));
  check("route no longer returns raw provider/exception text (sign_failed / e.message)", !/sign_failed|e\.message|signErr/.test(h));
  check("route no longer maps call lookup errors to 404 or returns 403 forbidden", !/forbidden|no_audio_path/.test(h));

  console.log(failures ? `\n${failures} FAILED` : "\nsigned-audio proof PASSED");
  process.exit(failures ? 1 : 0);
})();
