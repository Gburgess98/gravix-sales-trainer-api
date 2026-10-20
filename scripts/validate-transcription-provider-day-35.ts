// Go Live Day 35 — F-34-1: explicit, fail-closed transcription provider. Network-free.
//
// Proves (loopback stubs only — OPENAI_BASE_URL points at a local recorder, Supabase at a
// local PostgREST/storage stub; no real key, provider, DB, storage or audio):
//   table     the pure resolver/assert contract across env maps
//   boot      the real server module: production + unset / invalid / openai-without-key
//             REFUSE before listen with a stable message that never echoes the supplied
//             value or key; explicit production stub, staging-shaped env and non-production
//             unset BOOT
//   e2e       POST /v1/upload/finalize with OPENAI_API_KEY present:
//               unset (non-prod)            -> stub: ZERO provider requests, one fewer audio download
//               unset (NODE_ENV=production + APP_ENV=staging, the real staging shape) -> stub, same
//               explicit stub               -> stub, same
//               explicit openai (+ key)     -> exactly one /audio/transcriptions request
//             /v1/version reports the resolved provider and exposes no key; scoring provider
//             block and upload/finalize contracts are unchanged
// Non-vacuity: each source mutation of a COPY of src/ MUST fail the named check.
//
// Run: npm run validate:transcription-provider   (tsx; child processes + loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import * as crypto from "node:crypto";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
type Check = { id: string; pass: boolean; detail?: string };

const REP = "aaaaaaaa-0000-4000-8000-000000000003";
const PLANTED_KEY = "sk-planted-secret-key-d35";
const PLANTED_BAD = "planted-bad-provider-d35";
const PROXY_SECRET = "planted-proxy-secret-d35";

// ───────────── table: pure contract ─────────────
async function tableSuite(root: string): Promise<Check[]> {
  const m = await import(path.join(root, "src/lib/transcriptionProvider.ts"));
  const res = (env: Record<string, string | undefined>) => { try { return m.resolveTranscriptionProvider(env); } catch (e: any) { return `ERR:${e?.code}`; } };
  const asrt = (env: Record<string, string | undefined>) => { try { return m.assertTranscriptionProviderConfig(env); } catch (e: any) { return `ERR:${e?.code}`; } };
  const K = { OPENAI_API_KEY: PLANTED_KEY };
  const checks: Array<[string, boolean, string]> = [
    ["table-nonprod-unset-with-key-is-stub", res({ NODE_ENV: "test", ...K }) === "stub" && res({ ...K }) === "stub" && res({ NODE_ENV: "development", ...K }) === "stub", ""],
    ["table-whitespace-is-unset", res({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "  \t ", ...K }) === "stub" && res({ NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "   " }) === "ERR:transcription_provider_required_in_production", ""],
    ["table-explicit-stub-with-key-is-stub", res({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "stub", ...K }) === "stub" && res({ NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "stub" }) === "stub", ""],
    ["table-explicit-openai-selected", res({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "openai", ...K }) === "openai" && res({ TRANSCRIPTION_PROVIDER: " OpenAI ", ...K }) === "openai", ""],
    ["table-production-unset-refused-never-inferred-from-key", res({ NODE_ENV: "production" }) === "ERR:transcription_provider_required_in_production" && res({ NODE_ENV: "production", ...K }) === "ERR:transcription_provider_required_in_production", ""],
    ["table-production-staging-marker-unset-is-stub", res({ NODE_ENV: "production", APP_ENV: "staging", ...K }) === "stub", ""],
    ["table-invalid-values-refused-in-every-environment", ["openai2", "whisper", "true", "1", "none", PLANTED_BAD].every((v) => res({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: v, ...K }) === "ERR:transcription_provider_invalid" && res({ NODE_ENV: "production", TRANSCRIPTION_PROVIDER: v, ...K }) === "ERR:transcription_provider_invalid"), ""],
    ["table-openai-requires-nonempty-key", ["", "   ", undefined].every((k) => asrt({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "openai", OPENAI_API_KEY: k }) === "ERR:transcription_provider_openai_requires_key") && asrt({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "openai", ...K }) === "openai", ""],
    ["table-stub-needs-no-key", asrt({ NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "stub" }) === "stub", ""],
    ["table-version-label-never-throws", m.describeTranscriptionProvider({ NODE_ENV: "production" }) === "unresolved" && m.describeTranscriptionProvider({ NODE_ENV: "test" }) === "stub", ""],
    ["table-error-messages-never-echo-value-or-key", (() => { try { m.assertTranscriptionProviderConfig({ NODE_ENV: "test", TRANSCRIPTION_PROVIDER: PLANTED_BAD, ...K }); return false; } catch (e: any) { return !String(e.message).includes(PLANTED_BAD) && !String(e.message).includes(PLANTED_KEY); } })(), ""],
  ];
  return checks.map(([id, pass, detail]) => ({ id, pass, detail }));
}

// ───────────── boot: real server module in child processes ─────────────
const BOOT_CASES: Array<{ id: string; env: Record<string, string | undefined>; refuse: boolean; code?: RegExp }> = [
  { id: "boot-production-unset-refused", env: { NODE_ENV: "production", OPENAI_API_KEY: PLANTED_KEY }, refuse: true, code: /TRANSCRIPTION_PROVIDER must be set explicitly/ },
  { id: "boot-invalid-provider-refused", env: { NODE_ENV: "test", TRANSCRIPTION_PROVIDER: PLANTED_BAD, OPENAI_API_KEY: PLANTED_KEY }, refuse: true, code: /TRANSCRIPTION_PROVIDER is invalid/ },
  { id: "boot-production-invalid-provider-refused", env: { NODE_ENV: "production", TRANSCRIPTION_PROVIDER: PLANTED_BAD, OPENAI_API_KEY: PLANTED_KEY }, refuse: true, code: /TRANSCRIPTION_PROVIDER is invalid/ },
  { id: "boot-openai-without-key-refused", env: { NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "openai", OPENAI_API_KEY: "" }, refuse: true, code: /requires OPENAI_API_KEY/ },
  { id: "boot-production-openai-blank-key-refused", env: { NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "openai", OPENAI_API_KEY: "   " }, refuse: true, code: /requires OPENAI_API_KEY/ },
  { id: "boot-production-explicit-stub-boots", env: { NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "stub" }, refuse: false },
  { id: "boot-production-explicit-openai-with-key-boots", env: { NODE_ENV: "production", TRANSCRIPTION_PROVIDER: "openai", OPENAI_API_KEY: PLANTED_KEY }, refuse: false },
  { id: "boot-staging-shaped-env-unset-boots", env: { NODE_ENV: "production", APP_ENV: "staging", OPENAI_API_KEY: PLANTED_KEY }, refuse: false },
  { id: "boot-nonproduction-unset-boots", env: { NODE_ENV: "test", OPENAI_API_KEY: PLANTED_KEY }, refuse: false },
];
function bootSuite(root: string, stubUrl: string): Check[] {
  const tsx = path.join(ROOT, "node_modules/.bin/tsx");
  return BOOT_CASES.map((c) => {
    const env: Record<string, string> = { PATH: process.env.PATH || "", HOME: process.env.HOME || "", SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder", GRAVIX_API_NO_LISTEN: "1" };
    for (const [k, v] of Object.entries(c.env)) if (v !== undefined) env[k] = v;
    if (env.OPENAI_API_KEY === undefined) env.OPENAI_API_KEY = "validator-placeholder"; // other modules construct clients at import
    const p = spawnSync(tsx, [__filename, "--boot", root], { env, cwd: EMPTY_CWD, encoding: "utf8", timeout: 60_000 });
    const text = `${p.stdout || ""}${p.stderr || ""}`;
    const leaked = text.includes(PLANTED_BAD) || text.includes(PLANTED_KEY);
    const refused = p.status !== 0 && !!c.code && c.code.test(text);
    const booted = p.status === 0 && /BOOT_OK/.test(text);
    return { id: c.id, pass: !leaked && (c.refuse ? refused : booted), detail: `exit=${p.status} leaked=${leaked} ${c.refuse ? "expected refuse" : "expected boot"}` };
  });
}

// ───────────── e2e: finalize with a key present ─────────────
type Rec = { sb: string[]; patches: Array<{ table: string; body: string }>; openai: string[] };
function wavBytes(): Buffer { const n = 8000; const d = Buffer.alloc(n * 2); const h = Buffer.alloc(44); h.write("RIFF", 0); h.writeUInt32LE(36 + d.length, 4); h.write("WAVEfmt ", 8); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22); h.writeUInt32LE(8000, 24); h.writeUInt32LE(16000, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34); h.write("data", 36); h.writeUInt32LE(d.length, 40); return Buffer.concat([h, d]); }
function startStubs(rec: Rec): Promise<{ sb: http.Server; oa: http.Server }> {
  const sb = http.createServer((req, res) => {
    const chunks: Buffer[] = []; req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const u = new URL(req.url || "/", "http://stub"); const body = Buffer.concat(chunks).toString("utf8");
      const send = (c: number, b: unknown) => { res.writeHead(c, { "content-type": "application/json" }); res.end(JSON.stringify(b)); };
      rec.sb.push(`${req.method} ${u.pathname}`);
      if (u.pathname === "/storage/v1/bucket") return send(200, []);
      if (u.pathname.startsWith("/storage/v1/object/sign/")) return send(200, { signedURL: `/object/sign/calls/x?token=t` });
      if (u.pathname.startsWith("/storage/v1/object/") && req.method === "GET") { res.writeHead(200, { "content-type": "audio/wav" }); return res.end(wavBytes()); }
      if (u.pathname.startsWith("/storage/")) return send(200, { Key: "stub" });
      if (u.pathname.startsWith("/auth/")) return send(400, { error: "invalid_grant" });
      const table = u.pathname.replace("/rest/v1/", "");
      if (req.method === "PATCH") { rec.patches.push({ table, body }); res.writeHead(204); return res.end(); }
      if (req.method === "POST") { res.writeHead(201); return res.end(); }
      return send(String(req.headers["accept"] || "").includes("vnd.pgrst.object") ? 406 : 200, String(req.headers["accept"] || "").includes("vnd.pgrst.object") ? { code: "PGRST116", message: "none", details: "", hint: null } : []);
    });
  });
  const oa = http.createServer((req, res) => {
    req.on("data", () => {}); req.on("end", () => { rec.openai.push(`${req.method} ${req.url}`); res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ text: "planted transcript alpha. planted transcript beta." })); });
  });
  return Promise.all([new Promise<http.Server>((r) => sb.listen(0, "127.0.0.1", () => r(sb))), new Promise<http.Server>((r) => oa.listen(0, "127.0.0.1", () => r(oa)))]).then(([a, b]) => ({ sb: a, oa: b }));
}
async function serve(root: string, env: Record<string, string>): Promise<{ base: string; kill: () => void; child: ChildProcess }> {
  const child = spawn(path.join(ROOT, "node_modules/.bin/tsx"), [__filename, "--serve", root], { env, cwd: EMPTY_CWD, stdio: ["ignore", "pipe", "pipe"] });
  const port: number = await new Promise((resolve, reject) => {
    let buf = ""; const t = setTimeout(() => reject(new Error("child server did not start: " + buf.slice(-200))), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); const m = buf.match(/SERVING_PORT=(\d+)/); if (m) { clearTimeout(t); resolve(Number(m[1])); } });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", (c) => { clearTimeout(t); reject(new Error(`child exited ${c}: ${buf.slice(-200)}`)); });
  });
  return { base: `http://127.0.0.1:${port}`, child, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } } };
}
async function call(base: string, method: string, p: string, headers: Record<string, string> = {}, body?: unknown) {
  const ctl = new AbortController(); const timer = setTimeout(() => ctl.abort(), 8000);
  try { const r = await fetch(base + p, { method, headers: { ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers }, body: body !== undefined ? JSON.stringify(body) : undefined, signal: ctl.signal }); const raw = await r.text(); let j: any = raw; try { j = JSON.parse(raw); } catch { /* text */ } return { status: r.status, body: j, raw }; }
  catch { return { status: 0, body: null, raw: "" }; } finally { clearTimeout(timer); }
}
type Scenario = { id: string; env: Record<string, string | undefined>; expectProvider: "stub" | "openai" };
const SCENARIOS: Scenario[] = [
  { id: "nonprod-unset-key-present", env: { NODE_ENV: "test" }, expectProvider: "stub" },
  { id: "staging-shaped-unset-key-present", env: { NODE_ENV: "production", APP_ENV: "staging" }, expectProvider: "stub" },
  { id: "explicit-stub-key-present", env: { NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "stub" }, expectProvider: "stub" },
  { id: "explicit-openai-key-present", env: { NODE_ENV: "test", TRANSCRIPTION_PROVIDER: "openai" }, expectProvider: "openai" },
];
async function e2eSuite(root: string, sbUrl: string, oaUrl: string, rec: Rec): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  for (const sc of SCENARIOS) {
    let app: Awaited<ReturnType<typeof serve>> | null = null;
    try {
      rec.sb.length = 0; rec.patches.length = 0; rec.openai.length = 0;
      const env: Record<string, string> = { PATH: process.env.PATH || "", HOME: process.env.HOME || "", GRAVIX_API_NO_LISTEN: "1", SUPABASE_URL: sbUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder", DEFAULT_ORG_ID: "dddddddd-0000-4000-8000-00000000000a", SCORING_PROVIDER: "stub", SKIP_SCORING_SIDE_EFFECTS: "1", PROXY_SHARED_SECRET: PROXY_SECRET, OPENAI_API_KEY: PLANTED_KEY, OPENAI_BASE_URL: `${oaUrl}/v1` };
      for (const [k, v] of Object.entries(sc.env)) if (v !== undefined) env[k] = v;
      app = await serve(root, env);
      const h = { "x-user-id": REP, "x-proxy-secret": PROXY_SECRET };
      const path1 = `${REP}/${crypto.randomUUID()}.wav`;
      const fin = await call(app.base, "POST", "/v1/upload/finalize", h, { path: path1, filename: "d35-synthetic.wav", mime: "audio/wav", size: wavBytes().length });
      add(`e2e-${sc.id}-finalize-contract-unchanged`, fin.status === 200 && fin.body?.ok === true && typeof fin.body?.callId === "string" && typeof fin.body?.jobId === "string", `status=${fin.status}`);
      // wait for the transcribe job to reach a terminal state
      let model = ""; for (let i = 0; i < 30 && !model; i++) { await new Promise((r) => setTimeout(r, 250)); const done = rec.patches.find((p) => p.table === "jobs" && /"status":"succeeded"/.test(p.body) && /"result":\{"model"/.test(p.body)); if (done) model = JSON.parse(done.body).result.model; }
      const audioHits = rec.openai.filter((x) => x.startsWith("POST") && x.includes("/audio/transcriptions")).length;
      const otherHits = rec.openai.length - audioHits;
      const downloads = rec.sb.filter((x) => x.startsWith("GET /storage/v1/object/") && !x.includes("/sign/")).length;
      if (sc.expectProvider === "stub") {
        add(`e2e-${sc.id}-stub-no-provider-request`, model === "whisper-stub" && audioHits === 0, `model=${model} providerRequests=${audioHits}`);
        add(`e2e-${sc.id}-stub-no-audio-download-for-egress`, downloads === 1, `downloads=${downloads} (finalize's own duration read = 1)`);
      } else {
        add(`e2e-${sc.id}-openai-branch-selected`, model === "gpt-4o-mini-transcribe" && audioHits === 1, `model=${model} providerRequests=${audioHits}`);
        add(`e2e-${sc.id}-openai-downloads-audio-once-more`, downloads === 2, `downloads=${downloads}`);
      }
      add(`e2e-${sc.id}-no-other-provider-egress`, otherHits === 0, `otherProviderRequests=${otherHits}`);
      const v = await call(app.base, "GET", "/v1/version");
      add(`e2e-${sc.id}-version-reports-provider-no-secret`, v.status === 200 && v.body?.transcription_provider === sc.expectProvider && !v.raw.includes(PLANTED_KEY) && !/OPENAI_API_KEY|api[_-]?key|sk-/i.test(v.raw), `reported=${v.body?.transcription_provider}`);
      add(`e2e-${sc.id}-scoring-block-unchanged`, v.body?.scoring?.provider === "stub" && v.body?.scoring?.contract === "v1" && v.body?.scoring?.side_effects_disabled === true, JSON.stringify(v.body?.scoring));
      if (sc.id === "nonprod-unset-key-present") {
        const anon = await call(app.base, "POST", "/v1/upload/finalize", {}, { path: path1, filename: "x.wav" });
        add("e2e-anonymous-finalize-still-401", anon.status === 401, `status=${anon.status}`);
      }
    } catch (e: any) { add(`e2e-${sc.id}-executed`, false, String(e?.message || e).slice(0, 240)); }
    finally { app?.kill(); }
  }
  return out;
}

// ───────────── mutation harness (COPY of src/) ─────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string, repl: string) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}
const SRV = "src/server.ts", LIB = "src/lib/transcriptionProvider.ts";
type Mutation = { name: string; file: string; find: string; repl: string; suite: "table" | "boot" | "e2e"; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "restore `if (OPENAI_API_KEY)` selection in simulateTranscription", file: SRV, find: 'if (resolveTranscriptionProvider() === "openai") {', repl: 'if (String(process.env.OPENAI_API_KEY || "").trim()) {', suite: "e2e", expect: "e2e-nonprod-unset-key-present-stub-no-provider-request" },
  { name: "stub branch still reaches the provider", file: SRV, find: 'if (resolveTranscriptionProvider() === "openai") {', repl: 'if (resolveTranscriptionProvider() !== "none") {', suite: "e2e", expect: "e2e-explicit-stub-key-present-stub-no-provider-request" },
  { name: "unset provider defaults to openai (non-production)", file: LIB, find: "    return \"stub\";\n  }\n  if (raw === \"stub\"", repl: "    return \"openai\";\n  }\n  if (raw === \"stub\"", suite: "table", expect: "table-nonprod-unset-with-key-is-stub" },
  { name: "invalid provider falls back to openai", file: LIB, find: 'throw new TranscriptionProviderConfigError("transcription_provider_invalid");\n}', repl: 'return "openai";\n}', suite: "table", expect: "table-invalid-values-refused-in-every-environment" },
  { name: "production unset no longer refused", file: LIB, find: 'if (isProductionEnv(env)) throw new TranscriptionProviderConfigError("transcription_provider_required_in_production");', repl: "", suite: "table", expect: "table-production-unset-refused-never-inferred-from-key" },
  { name: "boot guard call removed from server.ts", file: SRV, find: "assertTranscriptionProviderConfig(process.env);", repl: "", suite: "boot", expect: "boot-production-unset-refused" },
  { name: "openai allowed without a key", file: LIB, find: 'if (provider === "openai" && String(env.OPENAI_API_KEY ?? "").trim() === "") {', repl: 'if (false) {', suite: "boot", expect: "boot-openai-without-key-refused" },
  { name: "error message echoes the supplied provider value", file: LIB, find: '? "FATAL: TRANSCRIPTION_PROVIDER is invalid — allowed values are \'stub\' or \'openai\' (value not shown)."', repl: '? "FATAL: TRANSCRIPTION_PROVIDER is invalid: " + String(process.env.TRANSCRIPTION_PROVIDER)', suite: "boot", expect: "boot-invalid-provider-refused" },
  { name: "/v1/version omits the transcription provider", file: SRV, find: "transcription_provider: describeTranscriptionProvider(),", repl: "", suite: "e2e", expect: "e2e-explicit-stub-key-present-version-reports-provider-no-secret" },
  { name: "/v1/version leaks the key", file: SRV, find: "transcription_provider: describeTranscriptionProvider(),", repl: "transcription_provider: describeTranscriptionProvider(), debug_key: process.env.OPENAI_API_KEY,", suite: "e2e", expect: "e2e-nonprod-unset-key-present-version-reports-provider-no-secret" },
];

function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) { console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${(!c.pass || process.env.VALIDATOR_VERBOSE) && c.detail ? " — " + c.detail : ""}`); if (!c.pass) bad++; }
  return bad;
}

async function main() {
  if (process.argv[2] === "--boot" || process.argv[2] === "--serve") {
    process.env.GRAVIX_API_NO_LISTEN = "1";
    const mod = await import(path.join(process.argv[3], "src/server.ts"));
    if (process.argv[2] === "--boot") { console.log("BOOT_OK"); process.exit(0); }
    const srv = http.createServer(mod.app);
    srv.listen(0, "127.0.0.1", () => console.log(`SERVING_PORT=${(srv.address() as AddressInfo).port}`));
    return;
  }
  console.log("Go Live Day 35 — transcription provider safety (F-34-1), loopback stubs only\n");
  const rec: Rec = { sb: [], patches: [], openai: [] };
  const { sb, oa } = await startStubs(rec);
  const sbUrl = `http://127.0.0.1:${(sb.address() as AddressInfo).port}`;
  const oaUrl = `http://127.0.0.1:${(oa.address() as AddressInfo).port}`;
  let failures = 0;
  try {
    failures += report("Resolver contract (pure):", await tableSuite(ROOT));
    failures += report("\nBoot (real server module, child processes):", bootSuite(ROOT, sbUrl));
    failures += report("\nEnd-to-end finalize, OPENAI_API_KEY present (loopback OpenAI recorder):", await e2eSuite(ROOT, sbUrl, oaUrl, rec));
    console.log("\nMutation proof — each mutation of a COPY must fail the named check:");
    for (const m of MUTATIONS) {
      let dir = "";
      try {
        dir = copyTree(ROOT);
        mutate(dir, m.file, m.find, m.repl);
        const checks = m.suite === "table" ? await tableSuite(dir) : m.suite === "boot" ? bootSuite(dir, sbUrl) : await e2eSuite(dir, sbUrl, oaUrl, rec);
        const hit = checks.find((c) => c.id === m.expect);
        const controlId = m.suite === "boot" ? "boot-nonproduction-unset-boots" : m.suite === "table" ? "table-stub-needs-no-key" : "e2e-nonprod-unset-key-present-finalize-contract-unchanged";
        const control = checks.find((c) => c.id === controlId);
        const caught = !!hit && !hit.pass && !!control?.pass; // control keeps a dead harness from counting
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found / harness dead)"}`);
        if (!caught) failures++;
      } catch (e: any) { console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++; }
      finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally { sb.close(); oa.close(); fs.rmSync(EMPTY_CWD, { recursive: true, force: true }); }
  if (failures) { console.log(`\n❌ transcription provider validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ transcription provider validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
