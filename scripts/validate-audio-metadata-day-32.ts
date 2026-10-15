// Go Live Day 32 — F-30-1: music-metadata dependency repair, network-free guard.
//
// Proves, with synthetic bytes only (no real audio, secrets, DB, network or AI):
//   1. versions   declared + locked music-metadata sit outside EVERY affected range from
//                 the five advisories (GHSA-f94x-6692-553q, -53v6-4h7p-p4gj, -jjpr-9cvf-cq55,
//                 -5gfj-9q3v-qfp3, -8j4c-6x6g-rq3j)
//   2. legit      small valid WAV + MPEG fixtures still parse with sane durations
//   3. hostile    MP4 stsd / APEv2 / ID3v2 / EBML / DSF fixtures are handled inside strict
//                 child-process time / RSS bounds and the child survives (no late crash);
//                 the ID3v2 + APEv2 oversize-declaration inputs must be REJECTED (the
//                 pre-fix 11.12.3 *parsed* them — verified in a throwaway pre-fix tree)
//   4. controls   the bounded-runner CAN detect a hang, a crash-after-result and a memory
//                 blow-up (planted children) — so "bounded" is not vacuous
//   5. end-to-end the real POST /v1/upload (child-process app, loopback stub) returns for
//                 every hostile fixture and /health stays 200; a legit WAV records its
//                 duration; anonymous uploads still 401
//
// Honest limit: the MP4-stsd hang, EBML/APE allocation and DSF crash could NOT be made to
// misbehave on 11.12.3 through parseBuffer in a bounded test, so those classes are guarded
// by "bounded + survives" plus the version floor, not by a pre-fix failure.
//
// Run: npm run validate:audio-metadata   (tsx; child processes + loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
type Check = { id: string; pass: boolean; detail?: string };

// ───────────── synthetic fixtures ─────────────
const u32 = (n: number) => [(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255];
const le32 = (n: number) => [n & 255, (n >>> 8) & 255, (n >>> 16) & 255, (n >>> 24) & 255];
const le16 = (n: number) => [n & 255, (n >>> 8) & 255];
const str = (s: string) => [...s].map((c) => c.charCodeAt(0));
const box = (type: string, ...payload: number[][]) => { const body = payload.flat(); return [...u32(8 + body.length), ...str(type), ...body]; };
type Fx = { name: string; mime: string; build: () => number[] };
const LEGIT: Fx[] = [
  { name: "wav", mime: "audio/wav", build: () => { const d = new Array(16000).fill(0); return [...str("RIFF"), ...le32(36 + d.length), ...str("WAVE"), ...str("fmt "), ...le32(16), ...le16(1), ...le16(1), ...le32(8000), ...le32(16000), ...le16(2), ...le16(16), ...str("data"), ...le32(d.length), ...d]; } },
  { name: "mp3", mime: "audio/mpeg", build: () => Array.from({ length: 20 }, () => [0xff, 0xfb, 0x90, 0x00, ...new Array(413).fill(0)]).flat() },
];
const HOSTILE: Array<Fx & { mustReject: boolean }> = [
  { name: "mp4-stsd-zero-entries", mime: "audio/mp4", mustReject: true, build: () => { const stsd = box("stsd", [0, 0, 0, 0], u32(0xfffffff0), new Array(4096).fill(0)); const hdlr = box("hdlr", [0, 0, 0, 0], u32(0), str("soun"), new Array(12).fill(0), [0]); const mdhd = box("mdhd", [0, 0, 0, 0], u32(0), u32(0), u32(44100), u32(44100), [0, 0, 0, 0]); return [...box("ftyp", str("M4A "), u32(0), str("M4A "), str("mp42")), ...box("moov", box("trak", box("mdia", mdhd, hdlr, box("minf", box("stbl", stsd)))))]; } },
  { name: "id3v2-huge-declared-size", mime: "audio/mpeg", mustReject: true, build: () => [...str("ID3"), 4, 0, 0, 0x7f, 0x7f, 0x7f, 0x7f, ...new Array(64).fill(0)] },
  { name: "apev2-huge-item", mime: "audio/mpeg", mustReject: true, build: () => { const item = [...le32(0x0fffffff), ...le32(0), ...str("Title"), 0, 65]; return [...new Array(2000).fill(0), ...item, ...str("APETAGEX"), ...le32(2000), ...le32(item.length + 32), ...le32(1), ...le32(0), ...new Array(8).fill(0)]; } },
  { name: "ebml-huge-leaf", mime: "audio/webm", mustReject: true, build: () => [0x1a, 0x45, 0xdf, 0xa3, 0x84, 0x42, 0x82, 0x81, 0x77, 0x18, 0x53, 0x80, 0x67, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x15, 0x49, 0xa9, 0x66, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x4d, 0x80, 0x1f, 0xff, 0xff, 0xff, ...new Array(32).fill(65)] },
  { name: "dsf-unknown-chunk-size-lt-header", mime: "application/octet-stream", mustReject: false, build: () => { const fmt = [...str("fmt "), ...le32(52), 0, 0, 0, 0, ...le32(1), ...le32(0), ...le32(2), ...le32(2), ...le32(2822400), ...le32(1), ...le32(1000), 0, 0, 0, 0, ...le32(4096), ...le32(0)]; const junk = [...str("junk"), ...le32(5), 0, 0, 0, 0]; return [...str("DSD "), ...le32(28), 0, 0, 0, 0, ...le32(28 + fmt.length + junk.length), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ...fmt, ...junk]; } },
];

// ───────────── bounded child runner ─────────────
const PARSE_CHILD = `
const [b64, mime] = process.argv.slice(1);
const mm = await import("music-metadata");
const t0 = Date.now(); let out;
try { const m = await mm.parseBuffer(Buffer.from(b64, "base64"), mime || undefined); out = { status: "parsed", duration: m?.format?.duration ?? null }; }
catch (e) { out = { status: "rejected", err: String(e?.name || "Error") }; }
out.ms = Date.now() - t0; out.rssMB = Math.round(process.resourceUsage().maxRSS / 1024);
console.log("RESULT " + JSON.stringify(out));
await new Promise((r) => setTimeout(r, 400)); // let a detached rejection fire (the DSF crash class)
console.log("SURVIVED");
`;
type Run = { kind: "ok" | "hang" | "crash" | "crash-after-result" | "memory"; result: any; wallMs: number };
function classify(p: ReturnType<typeof spawnSync>, wallMs: number, timeoutMs: number, rssLimitMB: number): Run {
  const out = String(p.stdout || "");
  const m = out.match(/RESULT (\{.*\})/);
  const result = m ? JSON.parse(m[1]) : null;
  const timedOut = (p.error as any)?.code === "ETIMEDOUT" || (p.signal === "SIGKILL" && wallMs >= timeoutMs - 50);
  if (timedOut) return { kind: "hang", result, wallMs };
  if (!result) return { kind: "crash", result, wallMs };
  if (!/SURVIVED/.test(out)) return { kind: "crash-after-result", result, wallMs };
  if ((result.rssMB ?? 0) > rssLimitMB) return { kind: "memory", result, wallMs };
  return { kind: "ok", result, wallMs };
}
function runChild(root: string, code: string, args: string[], timeoutMs = 8000, rssLimitMB = 200): Run {
  const t0 = Date.now();
  const p = spawnSync(process.execPath, ["--max-old-space-size=128", "--input-type=module", "-e", code, ...args], { cwd: root, timeout: timeoutMs, killSignal: "SIGKILL", encoding: "utf8", env: { PATH: process.env.PATH || "", HOME: process.env.HOME || "" } });
  return classify(p, Date.now() - t0, timeoutMs, rssLimitMB);
}
const parseFixture = (root: string, fx: Fx) => runChild(root, PARSE_CHILD, [Buffer.from(fx.build()).toString("base64"), fx.mime]);

// ───────────── version checks ─────────────
const AFFECTED: Array<[string, (v: number[]) => boolean]> = [
  ["GHSA-f94x-6692-553q  <11.16.0", (v) => lt(v, [11, 16, 0])],
  ["GHSA-53v6-4h7p-p4gj  <=11.12.3", (v) => !lt([11, 12, 3], v)],
  ["GHSA-jjpr-9cvf-cq55  <=11.12.3", (v) => !lt([11, 12, 3], v)],
  ["GHSA-5gfj-9q3v-qfp3  <11.16.0", (v) => lt(v, [11, 16, 0])],
  ["GHSA-8j4c-6x6g-rq3j  <=11.14.0", (v) => !lt([11, 14, 0], v)],
];
function ver(s: string): number[] { return s.replace(/^[^\d]*/, "").split(".").map((n) => parseInt(n, 10) || 0); }
function lt(a: number[], b: number[]): boolean { for (let i = 0; i < 3; i++) { if ((a[i] || 0) !== (b[i] || 0)) return (a[i] || 0) < (b[i] || 0); } return false; }
function lockedVersions(root: string): string[] {
  const lock = JSON.parse(fs.readFileSync(path.join(root, "package-lock.json"), "utf8")).packages as Record<string, { version?: string }>;
  return Object.entries(lock).filter(([k]) => k === "node_modules/music-metadata" || k.endsWith("/node_modules/music-metadata")).map(([, v]) => String(v.version));
}
function versionSuite(root: string): Check[] {
  const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  const declared = String(pkg.dependencies?.["music-metadata"] || "");
  const locked = lockedVersions(root);
  const installed = (() => { try { return JSON.parse(fs.readFileSync(path.join(root, "node_modules/music-metadata/package.json"), "utf8")).version as string; } catch { return ""; } })();
  const outside = (v: string) => AFFECTED.every(([, inRange]) => !inRange(ver(v)));
  const bad = (v: string) => AFFECTED.filter(([, inRange]) => inRange(ver(v))).map(([n]) => n.split(" ")[0]).join(",");
  return [
    { id: "declared-range-floor-outside-all-affected-ranges", pass: !!declared && outside(declared), detail: `declared=${declared} affected-by=${bad(declared)}` },
    { id: "locked-version-outside-all-affected-ranges", pass: locked.length > 0 && locked.every(outside), detail: `locked=${locked.join(",")} affected-by=${locked.map(bad).join(",")}` },
    { id: "installed-version-outside-all-affected-ranges", pass: installed !== "" && outside(installed), detail: `installed=${installed}` },
  ];
}

// ───────────── parser behaviour ─────────────
function parserSuite(root: string): Check[] {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const wav = parseFixture(root, LEGIT[0]);
  add("legit-wav-parses-with-duration", wav.kind === "ok" && wav.result.status === "parsed" && Math.abs(wav.result.duration - 1) < 0.05, `${wav.kind} ${JSON.stringify(wav.result)}`);
  const mp3 = parseFixture(root, LEGIT[1]);
  add("legit-mpeg-parses-with-duration", mp3.kind === "ok" && mp3.result.status === "parsed" && mp3.result.duration > 0.3 && mp3.result.duration < 0.8, `${mp3.kind} ${JSON.stringify(mp3.result)}`);
  for (const fx of HOSTILE) {
    const r = parseFixture(root, fx);
    const bounded = r.kind === "ok" && r.result.ms < 3000 && r.wallMs < 6000;
    add(`hostile-${fx.name}-bounded-and-survives`, bounded, `${r.kind} ${JSON.stringify(r.result)} wall=${r.wallMs}ms`);
    if (fx.mustReject) add(`hostile-${fx.name}-rejected-not-parsed`, r.kind === "ok" && r.result.status === "rejected", `${r.kind} ${JSON.stringify(r.result)}`);
    const after = parseFixture(root, LEGIT[0]);
    add(`healthy-after-${fx.name}`, after.kind === "ok" && after.result.status === "parsed", after.kind);
  }
  return out;
}

// ───────────── planted controls (the runner must be able to FAIL) ─────────────
function controlSuite(root: string): Check[] {
  const hang = runChild(root, `for (;;) {}`, [], 1500);
  const crashLate = runChild(root, `console.log('RESULT ' + JSON.stringify({status:'parsed',ms:1,rssMB:50})); await new Promise((r)=>setTimeout(r,100)); Promise.reject(new Error('planted')); await new Promise((r)=>setTimeout(r,400)); console.log('SURVIVED');`, [], 4000);
  const crash = runChild(root, `process.exit(7)`, [], 4000);
  const mem = runChild(root, `const b = Buffer.alloc(420*1024*1024, 1); console.log('RESULT ' + JSON.stringify({status:'parsed',ms:1,rssMB:Math.round(process.resourceUsage().maxRSS/1024)})); await new Promise((r)=>setTimeout(r,50)); console.log('SURVIVED'+b.length);`, [], 8000, 200);
  const fine = runChild(root, `console.log('RESULT ' + JSON.stringify({status:'parsed',ms:1,rssMB:50})); await new Promise((r)=>setTimeout(r,50)); console.log('SURVIVED');`, [], 4000);
  return [
    { id: "control-runner-detects-hang", pass: hang.kind === "hang", detail: hang.kind },
    { id: "control-runner-detects-crash-after-result", pass: crashLate.kind === "crash-after-result", detail: crashLate.kind },
    { id: "control-runner-detects-crash", pass: crash.kind === "crash", detail: crash.kind },
    { id: "control-runner-detects-memory-blowup", pass: mem.kind === "memory", detail: `${mem.kind} rss=${mem.result?.rssMB}` },
    { id: "control-runner-accepts-healthy-child", pass: fine.kind === "ok", detail: fine.kind },
  ];
}

// ───────────── end-to-end: real POST /v1/upload ─────────────
const REP = "aaaaaaaa-0000-4000-8000-000000000003";
type Rec = { method: string; url: string; body: string };
function startStub(records: Rec[]): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const chunks: Buffer[] = []; req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const u = new URL(req.url || "/", "http://stub");
      if (req.method !== "GET") records.push({ method: req.method || "", url: u.pathname, body: u.pathname.startsWith("/storage/") ? "<bytes>" : Buffer.concat(chunks).toString("utf8") });
      const send = (c: number, b: unknown) => { res.writeHead(c, { "content-type": "application/json" }); res.end(JSON.stringify(b)); };
      if (u.pathname === "/storage/v1/bucket") return send(200, []);
      if (u.pathname.startsWith("/storage/")) return send(200, { Key: "stub" });
      if (u.pathname.startsWith("/auth/")) return send(400, { error: "invalid_grant" });
      if (req.method === "POST") { res.writeHead(201); return res.end(); }
      return send(200, String(req.headers["accept"] || "").includes("vnd.pgrst.object") ? {} : []);
    });
  });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}
async function serve(root: string, stubUrl: string): Promise<{ base: string; kill: () => void; child: ChildProcess }> {
  const env: Record<string, string> = { PATH: process.env.PATH || "", HOME: process.env.HOME || "", NODE_ENV: "test", GRAVIX_API_NO_LISTEN: "1", SUPABASE_URL: stubUrl, SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder", OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder", DEFAULT_ORG_ID: "dddddddd-0000-4000-8000-00000000000a" };
  const child = spawn(path.join(ROOT, "node_modules/.bin/tsx"), [__filename, "--serve", root], { env, cwd: EMPTY_CWD, stdio: ["ignore", "pipe", "pipe"] });
  const port: number = await new Promise((resolve, reject) => {
    let buf = ""; const t = setTimeout(() => reject(new Error("child server did not start: " + buf.slice(-200))), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); const m = buf.match(/SERVING_PORT=(\d+)/); if (m) { clearTimeout(t); resolve(Number(m[1])); } });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", (c) => { clearTimeout(t); reject(new Error(`child exited ${c}: ${buf.slice(-200)}`)); });
  });
  return { base: `http://127.0.0.1:${port}`, child, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } } };
}
async function call(base: string, method: string, p: string, headers: Record<string, string> = {}, body?: FormData, timeout = 6000) {
  const ctl = new AbortController(); const t0 = Date.now(); const timer = setTimeout(() => ctl.abort(), timeout);
  try { const r = await fetch(base + p, { method, headers, body, signal: ctl.signal }); const text = await r.text(); let j: any = text; try { j = JSON.parse(text); } catch { /* text */ } return { status: r.status, body: j, ms: Date.now() - t0 }; }
  catch { return { status: 0, body: null, ms: Date.now() - t0 }; } finally { clearTimeout(timer); }
}
const form = (fx: Fx) => { const fd = new FormData(); fd.append("file", new Blob([new Uint8Array(fx.build())], { type: fx.mime }), `${fx.name}.bin`); return fd; };

async function e2eSuite(root: string, stubUrl: string, records: Rec[]): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Awaited<ReturnType<typeof serve>> | null = null;
  try {
    app = await serve(root, stubUrl);
    records.length = 0;
    const legit = await call(app.base, "POST", "/v1/upload", { "x-user-id": REP }, form(LEGIT[0]));
    const ins = records.find((r) => r.url === "/rest/v1/calls" && r.method === "POST");
    const dur = ins ? JSON.parse(ins.body || "{}").duration_sec : undefined;
    add("e2e-legit-wav-upload-records-duration", legit.status === 200 && dur === 1, `status=${legit.status} duration_sec=${dur}`);
    const anon = await call(app.base, "POST", "/v1/upload", {}, form(LEGIT[0]));
    add("e2e-anonymous-upload-still-401", anon.status === 401, `status=${anon.status}`);
    for (const fx of HOSTILE) {
      const r = await call(app.base, "POST", "/v1/upload", { "x-user-id": REP }, form(fx), 7000);
      const health = await call(app.base, "GET", "/health", {}, undefined, 3000);
      add(`e2e-hostile-${fx.name}-returns-and-server-healthy`, r.status === 200 && r.ms < 5000 && health.status === 200, `upload=${r.status} ${r.ms}ms health=${health.status}`);
      if (r.status === 0 || health.status !== 200) { app.kill(); app = await serve(root, stubUrl); }
    }
    await new Promise((r) => setTimeout(r, 500)); // a detached rejection would kill the app by now
    const final = await call(app.base, "GET", "/health", {}, undefined, 3000);
    add("e2e-server-still-healthy-after-all-hostile-uploads", final.status === 200, `health=${final.status}`);
  } catch (e: any) {
    add("e2e-suite-executed", false, String(e?.message || e).slice(0, 300));
  } finally { app?.kill(); }
  return out;
}

// ───────────── mutation harness (COPY of the tree) ─────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "package-lock.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string | RegExp, repl: string) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  const ok = typeof find === "string" ? t.includes(find) : find.test(t);
  if (!ok) throw new Error(`mutation anchor not found in ${file}: ${String(find).slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}
type Mutation = { name: string; file: string; find: string | RegExp; repl: string; suite: "version" | "e2e"; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "declared floor lowered to the vulnerable range", file: "package.json", find: /"music-metadata": "\^[0-9.]+"/, repl: '"music-metadata": "^11.12.3"', suite: "version", expect: "declared-range-floor-outside-all-affected-ranges" },
  { name: "lockfile pins a vulnerable music-metadata", file: "package-lock.json", find: /("node_modules\/music-metadata": \{\s*"version": ")[0-9.]+(")/, repl: "$111.15.0$2", suite: "version", expect: "locked-version-outside-all-affected-ranges" },
  { name: "upload duration parsing hangs (planted infinite loop)", file: "src/server.ts", find: "const meta = await mm.parseBuffer(f.buffer, f.mimetype);", repl: "if ((f.originalname || '').includes('mp4-stsd')) { for (;;) {} } const meta = await mm.parseBuffer(f.buffer, f.mimetype);", suite: "e2e", expect: "e2e-hostile-mp4-stsd-zero-entries-returns-and-server-healthy" },
  { name: "late detached rejection after parse (planted crash class)", file: "src/server.ts", find: "const meta = await mm.parseBuffer(f.buffer, f.mimetype);", repl: "const meta = await mm.parseBuffer(f.buffer, f.mimetype); setTimeout(() => { Promise.reject(new Error('planted')); }, 50);", suite: "e2e", expect: "e2e-server-still-healthy-after-all-hostile-uploads" },
  { name: "anonymous upload gate bypassed", file: "src/routeAuthPolicy.ts", find: 'const PUBLIC_KEYS = new Set(PUBLIC_ROUTES.map((r) => `${r.method} ${normalise(r.path)}`));', repl: 'const PUBLIC_KEYS = new Set([...PUBLIC_ROUTES.map((r) => `${r.method} ${normalise(r.path)}`), "POST /v1/upload"]);', suite: "e2e", expect: "e2e-anonymous-upload-still-401" },
];

function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) { console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${(!c.pass || process.env.VALIDATOR_VERBOSE) && c.detail ? " — " + c.detail : ""}`); if (!c.pass) bad++; }
  return bad;
}

async function main() {
  if (process.argv[2] === "--serve") {
    process.env.GRAVIX_API_NO_LISTEN = "1";
    const mod = await import(path.join(process.argv[3], "src/server.ts"));
    const srv = http.createServer(mod.app);
    srv.listen(0, "127.0.0.1", () => console.log(`SERVING_PORT=${(srv.address() as AddressInfo).port}`));
    return;
  }
  console.log("Go Live Day 32 — audio metadata dependency repair (F-30-1), child processes + loopback only\n");
  const records: Rec[] = [];
  const stub = await startStub(records);
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`;
  let failures = 0;
  try {
    failures += report("Versions vs the five advisories:", versionSuite(ROOT));
    failures += report("\nParser behaviour (bounded child processes, 128MB heap cap):", parserSuite(ROOT));
    failures += report("\nPlanted controls (the bounded runner must detect these):", controlSuite(ROOT));
    failures += report("\nEnd-to-end POST /v1/upload (real app, loopback stub):", await e2eSuite(ROOT, stubUrl, records));
    console.log("\nMutation proof — each mutation of a COPY must fail the named check:");
    for (const m of MUTATIONS) {
      let dir = "";
      try {
        dir = copyTree(ROOT);
        mutate(dir, m.file, m.find, m.repl);
        const checks = m.suite === "version" ? versionSuite(dir) : await e2eSuite(dir, stubUrl, records);
        const hit = checks.find((c) => c.id === m.expect);
        const control = m.suite === "e2e" ? checks.find((c) => c.id === "e2e-legit-wav-upload-records-duration") : undefined;
        const caught = !!hit && !hit.pass && (m.suite === "version" || !!control?.pass || m.expect === "e2e-anonymous-upload-still-401");
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found / harness dead)"}`);
        if (!caught) failures++;
      } catch (e: any) {
        console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++;
      } finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally {
    stub.close();
    fs.rmSync(EMPTY_CWD, { recursive: true, force: true });
  }
  if (failures) { console.log(`\n❌ audio metadata validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ audio metadata validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
