// Go Live Day 28 — dependency-gate repair: behavioural, loopback-only checks.
//
// Proves, against the REAL Express app (booted in a CHILD process so a hostile
// request that pins its event loop cannot freeze this validator):
//
//   upload     POST /v1/upload multipart behaviour is preserved on the patched multer
//              (identity, mime gating, size cap, storage/DB writes owned by the caller)
//   DoS        crafted multipart field names (GHSA-535w sparse-array, GHSA-72gw nesting,
//              GHSA-wc9g malformed names) are rejected fast and the server stays healthy.
//              NOTE: multer >=2.2 limits default to Infinity — the version bump alone
//              does NOT stop the sparse-array vector; the limits in server.ts do.
//   deps       patched floors for multer / body-parser / qs / ip-address / brace-expansion,
//              body-parser JSON cap and express-rate-limit (ip-address) still enforce.
//
// No real secret/DB/network: SUPABASE_* point at a 127.0.0.1 stub, keys are placeholders,
// the child runs from an empty directory (server.ts loads ./.env via dotenv).
// Non-vacuity: each mutation of a COPY of src/ + package.json MUST fail the named check.
//
// Run: npm run validate:upload-dependencies   (tsx, loopback only)

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import * as crypto from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { AddressInfo } from "node:net";

const ROOT = path.resolve(__dirname, "..");
const EMPTY_CWD = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-nocfg-"));
type Check = { id: string; pass: boolean; detail?: string };

const REP = "aaaaaaaa-0000-4000-8000-000000000003";

// ───────────── loopback PostgREST + storage stub that records writes ─────────────
type Rec = { method: string; url: string; body: string };
function startStub(records: Rec[]): Promise<http.Server> {
  const srv = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const u = new URL(req.url || "/", "http://stub");
      const body = Buffer.concat(chunks).toString("utf8");
      if (req.method !== "GET") records.push({ method: req.method || "", url: u.pathname, body: u.pathname.startsWith("/storage/") ? `<${chunks.length ? Buffer.concat(chunks).length : 0} bytes>` : body });
      const wantsObject = String(req.headers["accept"] || "").includes("vnd.pgrst.object");
      if (u.pathname.startsWith("/auth/")) { res.writeHead(400, { "content-type": "application/json" }); return res.end(JSON.stringify({ error: "invalid_grant", error_description: "stub" })); }
      if (u.pathname === "/storage/v1/bucket") { res.writeHead(200, { "content-type": "application/json" }); return res.end("[]"); }
      if (u.pathname.startsWith("/storage/")) { res.writeHead(200, { "content-type": "application/json" }); return res.end(JSON.stringify({ Key: "stub" })); }
      if (req.method === "POST") { res.writeHead(201); return res.end(); }
      if (wantsObject) { res.writeHead(406, { "content-type": "application/json" }); return res.end(JSON.stringify({ code: "PGRST116", message: "none", details: "", hint: null })); }
      res.writeHead(200, { "content-type": "application/json" });
      res.end("[]");
    });
  });
  return new Promise((r) => srv.listen(0, "127.0.0.1", () => r(srv)));
}

// ───────────── child-process app server ─────────────
type Served = { base: string; kill: () => void; child: ChildProcess };
async function serve(root: string, stubUrl: string, extraEnv: Record<string, string> = {}): Promise<Served> {
  const tsx = path.join(ROOT, "node_modules/.bin/tsx");
  const env: Record<string, string> = {
    PATH: process.env.PATH || "", HOME: process.env.HOME || "",
    NODE_ENV: "test", GRAVIX_API_NO_LISTEN: "1", SUPABASE_URL: stubUrl,
    SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder",
    OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder",
    DEFAULT_ORG_ID: "dddddddd-0000-4000-8000-00000000000a",
    ...extraEnv,
  };
  const child = spawn(tsx, [__filename, "--serve", root], { env, cwd: EMPTY_CWD, stdio: ["ignore", "pipe", "pipe"] });
  if (process.env.VALIDATOR_VERBOSE) child.stderr!.on("data", (d) => { const t = String(d); if (/Error|crash|Uncaught/i.test(t)) console.log("   child stderr:", t.slice(0, 300)); });
  const port: number = await new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(() => reject(new Error("child server did not start: " + buf.slice(-300))), 60_000);
    child.stdout!.on("data", (d) => { buf += String(d); const m = buf.match(/SERVING_PORT=(\d+)/); if (m) { clearTimeout(t); resolve(Number(m[1])); } });
    child.stderr!.on("data", (d) => { buf += String(d); });
    child.on("exit", (c) => { clearTimeout(t); reject(new Error(`child exited ${c}: ${buf.slice(-300)}`)); });
  });
  return { base: `http://127.0.0.1:${port}`, child, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } } };
}

type Resp = { status: number; body: any; ms: number; timedOut?: boolean };
async function call(base: string, method: string, p: string, opts: { headers?: Record<string, string>; body?: BodyInit; timeout?: number } = {}): Promise<Resp> {
  const ctl = new AbortController();
  const t0 = Date.now();
  const timer = setTimeout(() => ctl.abort(), opts.timeout ?? 6000);
  try {
    const r = await fetch(base + p, { method, headers: opts.headers, body: opts.body, signal: ctl.signal });
    const text = await r.text();
    let j: any = text; try { j = JSON.parse(text); } catch { /* keep text */ }
    return { status: r.status, body: j, ms: Date.now() - t0 };
  } catch (e: any) {
    if (process.env.VALIDATOR_VERBOSE && e?.name !== "AbortError") console.log("   fetch error:", e?.cause?.code || e?.message);
    return { status: 0, body: null, ms: Date.now() - t0, timedOut: e?.name === "AbortError" };
  } finally { clearTimeout(timer); }
}
const as = (uid: string) => ({ "x-user-id": uid });
const errOf = (r: Resp) => (r.body && typeof r.body === "object" ? r.body.error : undefined);
function form(parts: Array<[string, string | { name: string; type: string; data: Buffer }]>): FormData {
  const fd = new FormData();
  for (const [k, v] of parts) typeof v === "string" ? fd.append(k, v) : fd.append(k, new Blob([new Uint8Array(v.data)], { type: v.type }), v.name);
  return fd;
}
const file = (data: Buffer, type = "audio/wav", name = "call.wav") => ({ name, type, data });

// ───────────── upload + DoS suite ─────────────
async function uploadSuite(root: string, stubUrl: string, records: Rec[]): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  let app: Served | null = null;
  try {
    app = await serve(root, stubUrl);
    const base = app.base;
    const data = crypto.randomBytes(2048);

    records.length = 0;
    let r = await call(base, "POST", "/v1/upload", { headers: as(REP), body: form([["file", file(data)]]) });
    const sha = crypto.createHash("sha256").update(data).digest("hex");
    add("upload-happy-path", r.status === 200 && r.body?.ok === true && r.body.size === data.length && r.body.sha256 === sha && r.body.mime === "audio/wav" && r.body.kind === "audio", `status=${r.status} ${errOf(r) ?? ""}`);
    const storageWrite = records.find((x) => x.url.startsWith("/storage/"));
    const callInsert = records.find((x) => x.url === "/rest/v1/calls" && x.method === "POST");
    add("upload-write-owned-by-caller", !!storageWrite && decodeURIComponent(storageWrite.url).includes(`/${REP}/`) && !!callInsert && JSON.parse(callInsert.body || "{}").user_id === REP, `storage=${storageWrite?.url.slice(0, 60)} callInsert=${!!callInsert}`);

    r = await call(base, "POST", "/v1/upload", { headers: as(REP), body: form([["file", file(Buffer.from('{"a":1}'), "application/json", "x.json")]]) });
    add("upload-json-kind", r.status === 200 && r.body?.kind === "json", `status=${r.status}`);
    r = await call(base, "POST", "/v1/upload", { headers: as(REP), body: form([["file", file(data, "text/html", "x.html")]]) });
    add("upload-unsupported-mime-415", r.status === 415 && errOf(r) === "unsupported_file_type", `status=${r.status}`);
    r = await call(base, "POST", "/v1/upload", { headers: as(REP), body: form([["note", "no file"]]) });
    add("upload-no-file-400", r.status === 400 && errOf(r) === "No file uploaded", `status=${r.status}`);
    r = await call(base, "POST", "/v1/upload", { body: form([["file", file(data)]]) });
    add("upload-anonymous-401", r.status === 401, `status=${r.status}`);
    r = await call(base, "POST", "/v1/upload", { headers: as(REP), body: form([["other", file(data)]]) });
    add("upload-unexpected-field-400", r.status === 400 && errOf(r) === "upload_limit_exceeded" && r.body?.code === "LIMIT_UNEXPECTED_FILE", `status=${r.status} code=${r.body?.code}`);
    r = await call(base, "POST", "/v1/upload", { headers: as(REP), timeout: 30_000, body: form([["file", file(Buffer.alloc(50 * 1024 * 1024 + 1, 1))]]) });
    add("upload-oversize-413", r.status === 413 && errOf(r) === "file_too_large", `status=${r.status}`);

    // hostile field names — must be rejected quickly, and the server must stay responsive
    const hostile: Array<[string, string[], string]> = [
      ["sparse-array-index-GHSA-535w", ["a[4294967294]", "a[x]"], "upload-rejects-sparse-array-dos"],
      ["deep-nesting-GHSA-72gw", ["a[b][c][d][e]"], "upload-rejects-deep-nesting"],
      ["malformed-names-GHSA-wc9g", ["a[4294967294]", "a[]"], "upload-rejects-malformed-field-names"],
    ];
    for (const [label, names, id] of hostile) {
      const fd = new FormData();
      for (const n of names) fd.append(n, "v");
      fd.append("file", new Blob([new Uint8Array(data)], { type: "audio/wav" }), "c.wav");
      const x = await call(base, "POST", "/v1/upload", { headers: as(REP), body: fd, timeout: 5000 });
      const health = await call(base, "GET", "/health", { timeout: 3000 });
      add(id, x.status === 400 && x.ms < 3000 && health.status === 200, `${label}: status=${x.status}${x.timedOut ? " TIMEOUT (event loop pinned)" : ""} ${x.ms}ms health=${health.status}`);
      if (x.timedOut) { app.kill(); app = await serve(root, stubUrl); }
    }
  } catch (e: any) {
    add("upload-suite-executed", false, String(e?.message || e).slice(0, 300));
  } finally { app?.kill(); }
  return out;
}

// ───────────── dependency suite ─────────────
function ver(v: string): number[] { return v.replace(/^[^\d]*/, "").split(".").map((n) => parseInt(n, 10) || 0); }
function gte(a: string, b: string): boolean { const x = ver(a), y = ver(b); for (let i = 0; i < 3; i++) { if ((x[i] || 0) !== (y[i] || 0)) return (x[i] || 0) > (y[i] || 0); } return true; }
function installedVersions(root: string, name: string): string[] {
  const lock = JSON.parse(fs.readFileSync(path.join(root, "package-lock.json"), "utf8")).packages as Record<string, { version?: string }>;
  return Object.entries(lock).filter(([k]) => k === `node_modules/${name}` || k.endsWith(`/node_modules/${name}`)).map(([, v]) => String(v.version));
}
async function depSuite(root: string, stubUrl: string): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  const multerRange = String(pkg.dependencies?.multer || "");
  add("multer-declared-floor-2.4.0", gte(multerRange, "2.4.0"), `package.json multer=${multerRange}`);
  const FLOORS: Array<[string, string]> = [["multer", "2.4.0"], ["body-parser", "2.3.0"], ["qs", "6.16.0"], ["ip-address", "10.7.1"]];
  for (const [n, f] of FLOORS) { const v = installedVersions(root, n); add(`lock-${n}-patched`, v.length > 0 && v.every((x) => gte(x, f)), `locked=${v.join(",")} floor=${f}`); }
  const be = installedVersions(root, "brace-expansion");
  add("lock-brace-expansion-patched", be.length > 0 && be.every((x) => (x.startsWith("1.") ? gte(x, "1.1.21") : x.startsWith("5.") ? gte(x, "5.0.12") : false)), `locked=${be.join(",")}`);
  add("legacy-ts-node-dev-chain-removed", !pkg.devDependencies?.["ts-node-dev"] && !pkg.dependencies?.["ts-node-dev"] && installedVersions(root, "braces").length === 0 && installedVersions(root, "chokidar").length === 0, `braces=${installedVersions(root, "braces")} chokidar=${installedVersions(root, "chokidar")}`);
  // Day 28: start:dev is an alias of the hardened `dev` script; nothing may boot the legacy app.
  add("start-dev-aliases-hardened-dev-script", pkg.scripts?.["start:dev"] === "npm run dev" && pkg.scripts?.dev === "tsx watch src/server.ts", `start:dev=${pkg.scripts?.["start:dev"]} dev=${pkg.scripts?.dev}`);
  add("no-npm-script-boots-legacy-entry", !Object.values(pkg.scripts || {}).some((v) => /src\/index|dist\/index/.test(String(v))), "");

  let app: Served | null = null;
  try {
    app = await serve(root, stubUrl);
    // body-parser: express.json() default 100kb cap still enforced
    const big = JSON.stringify({ pad: "x".repeat(200 * 1024) });
    const j = await call(app.base, "POST", "/v1/auth/logout", { headers: { "content-type": "application/json", ...as(REP) }, body: big });
    // Known pre-existing quirk (reported, not changed here): the global error handler maps the
    // body-parser 413 to a generic 500. The dependency behaviour under test is that the
    // oversized body is REJECTED (never parsed/accepted) and the server stays up.
    const jh = await call(app.base, "GET", "/health");
    add("body-parser-json-cap-enforced", (j.status === 413 || j.status === 500) && jh.status === 200, `status=${j.status} health=${jh.status}`);
    // express-rate-limit (ip-address): auth limiter 10/15min per IP
    const statuses: number[] = [];
    for (let i = 0; i < 12; i++) statuses.push((await call(app.base, "POST", "/v1/auth/login", { headers: { "content-type": "application/json" }, body: JSON.stringify({ email: "a@example.com", password: "x" }) })).status);
    add("rate-limit-auth-429-after-10", statuses.slice(0, 10).every((s) => s !== 429) && statuses[10] === 429 && statuses[11] === 429, `statuses=${statuses.join(",")}`);
  } catch (e: any) {
    add("dep-suite-executed", false, String(e?.message || e).slice(0, 300));
  } finally { app?.kill(); }
  return out;
}

// ───────────── dev-script startup behaviour ─────────────
// Runs the REAL `npm run start:dev` (synthetic env, stub services, empty cwd) and checks
// which app answers: the hardened server must require identity on the routes the legacy
// app exposed anonymously.
function freePort(): Promise<number> {
  return new Promise((resolve) => { const s = http.createServer(); s.listen(0, "127.0.0.1", () => { const p = (s.address() as AddressInfo).port; s.close(() => resolve(p)); }); });
}
async function devStartSuite(root: string, stubUrl: string): Promise<Check[]> {
  const out: Check[] = [];
  const add = (id: string, pass: boolean, detail = "") => out.push({ id, pass, detail });
  const port = await freePort();
  const env: Record<string, string> = {
    PATH: process.env.PATH || "", HOME: process.env.HOME || "", PORT: String(port), NODE_ENV: "development",
    SUPABASE_URL: stubUrl, WEB_ORIGIN: "http://localhost:3000", DEFAULT_ORG_ID: "dddddddd-0000-4000-8000-00000000000a",
    SUPABASE_SERVICE_ROLE_KEY: "validator-placeholder", SUPABASE_ANON_KEY: "validator-placeholder",
    OPENAI_API_KEY: "validator-placeholder", ANTHROPIC_API_KEY: "validator-placeholder",
  };
  // npm runs scripts with cwd = the package root, and server.ts calls dotenv (reads ./.env).
  // So run from a throw-away COPY of the tree (src + package.json, no .env) — never the repo root.
  const runDir = copyTree(root);
  const child = spawn("npm", ["run", "start:dev"], { env, cwd: runDir, stdio: "ignore", detached: true });
  const base = `http://127.0.0.1:${port}`;
  try {
    let up: Resp = { status: 0, body: null, ms: 0 };
    for (let i = 0; i < 60 && up.status !== 200; i++) { up = await call(base, "GET", "/health", { timeout: 1500 }); if (up.status !== 200) await new Promise((r) => setTimeout(r, 750)); }
    add("start-dev-boots-and-serves-health", up.status === 200, `status=${up.status}`);
    const env1 = await call(base, "GET", "/v1/env-check", { timeout: 4000 });
    add("start-dev-env-check-not-anonymous", env1.status === 401, `status=${env1.status}`);
    const db = await call(base, "GET", "/v1/db/now", { timeout: 4000 });
    add("start-dev-db-now-not-anonymous", db.status === 401, `status=${db.status}`);
    const own = await call(base, "GET", "/v1/version", { timeout: 4000 });
    add("start-dev-is-hardened-server", own.status === 200 && own.body?.ok === true && own.body?.version !== undefined, `status=${own.status}`);
  } catch (e: any) {
    add("dev-start-suite-executed", false, String(e?.message || e).slice(0, 200));
  } finally {
    try { process.kill(-child.pid!, "SIGKILL"); } catch { /* gone */ }
    fs.rmSync(runDir, { recursive: true, force: true });
  }
  return out;
}

// ───────────── mutation harness ─────────────
function copyTree(root: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gravix-mut-"));
  fs.cpSync(path.join(root, "src"), path.join(dir, "src"), { recursive: true });
  for (const f of ["package.json", "package-lock.json", "tsconfig.json"]) fs.copyFileSync(path.join(root, f), path.join(dir, f));
  fs.symlinkSync(path.join(root, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}
function mutate(dir: string, file: string, find: string, repl: string) {
  const p = path.join(dir, file); const t = fs.readFileSync(p, "utf8");
  if (!t.includes(find)) throw new Error(`mutation anchor not found in ${file}: ${find.slice(0, 70)}`);
  fs.writeFileSync(p, t.replace(find, repl));
}
type Mutation = { name: string; file: string; find: string; repl: string; suite: "upload" | "deps" | "dev"; expect: string };
const MUTATIONS: Mutation[] = [
  { name: "sparse-array index limit removed", file: "src/server.ts", find: ", fieldArrayIndexLimit: 100", repl: "", suite: "upload", expect: "upload-rejects-sparse-array-dos" },
  { name: "field nesting limit removed", file: "src/server.ts", find: ", fieldNestingDepth: 1", repl: "", suite: "upload", expect: "upload-rejects-deep-nesting" },
  { name: "file size cap removed", file: "src/server.ts", find: "fileSize: MAX_UPLOAD_BYTES, ", repl: "", suite: "upload", expect: "upload-oversize-413" },
  { name: "upload loses identity guard (default-deny bypass)", file: "src/routeAuthPolicy.ts", find: 'const PUBLIC_KEYS = new Set(PUBLIC_ROUTES.map((r) => `${r.method} ${normalise(r.path)}`));', repl: 'const PUBLIC_KEYS = new Set([...PUBLIC_ROUTES.map((r) => `${r.method} ${normalise(r.path)}`), "POST /v1/upload"]);', suite: "upload", expect: "upload-anonymous-401" },
  { name: "mime allow-list opened to text/html", file: "src/server.ts", find: '"application/octet-stream", // generic binary', repl: '"application/octet-stream", "text/html", // generic binary', suite: "upload", expect: "upload-unsupported-mime-415" },
  { name: "multer floor lowered in package.json", file: "package.json", find: '"multer": "^2.4.0"', repl: '"multer": "^2.0.2"', suite: "deps", expect: "multer-declared-floor-2.4.0" },
  { name: "ts-node-dev re-added", file: "package.json", find: '"typescript": "^5.9.2"', repl: '"typescript": "^5.9.2", "ts-node-dev": "^2.0.0"', suite: "deps", expect: "legacy-ts-node-dev-chain-removed" },
  { name: "start:dev points back at legacy app (tsx)", file: "package.json", find: '"start:dev": "npm run dev"', repl: '"start:dev": "tsx watch src/index.ts"', suite: "deps", expect: "start-dev-aliases-hardened-dev-script" },
  { name: "start:dev reverted to ts-node-dev legacy", file: "package.json", find: '"start:dev": "npm run dev"', repl: '"start:dev": "ts-node-dev --respawn --transpile-only src/index.ts"', suite: "deps", expect: "no-npm-script-boots-legacy-entry" },
  { name: "start:dev behaviourally boots legacy app", file: "package.json", find: '"start:dev": "npm run dev"', repl: '"start:dev": "tsx watch src/index.ts"', suite: "dev", expect: "start-dev-env-check-not-anonymous" },
  { name: "dev script itself points at legacy app", file: "package.json", find: '"dev": "tsx watch src/server.ts"', repl: '"dev": "tsx watch src/index.ts"', suite: "dev", expect: "start-dev-db-now-not-anonymous" },
  { name: "auth rate limiter removed", file: "src/server.ts", find: 'app.use("/v1/auth/login",          authRateLimit);', repl: "", suite: "deps", expect: "rate-limit-auth-429-after-10" },
];

function report(title: string, checks: Check[]): number {
  console.log(title);
  let bad = 0;
  for (const c of checks) { console.log(`  ${c.pass ? "✅" : "❌"}  ${c.id}${c.detail && (!c.pass || process.env.VALIDATOR_VERBOSE) ? " — " + c.detail : ""}`); if (!c.pass) bad++; }
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
  console.log("Go Live Day 28 — upload / dependency-gate hardening (behavioural, loopback only)\n");
  const records: Rec[] = [];
  const stub = await startStub(records);
  const stubUrl = `http://127.0.0.1:${(stub.address() as AddressInfo).port}`;
  let failures = 0;
  try {
    failures += report("Dependency floors + behaviour:", await depSuite(ROOT, stubUrl));
    failures += report("\nnpm run start:dev startup + access (real script, stub services):", await devStartSuite(ROOT, stubUrl));
    failures += report("\nUpload behaviour + hostile multipart (child-process app):", await uploadSuite(ROOT, stubUrl, records));
    console.log("\nMutation proof — each mutation of a COPY must fail the named check:");
    for (const m of MUTATIONS) {
      let dir = "";
      try {
        dir = copyTree(ROOT);
        mutate(dir, m.file, m.find, m.repl);
        const checks = m.suite === "upload" ? await uploadSuite(dir, stubUrl, records) : m.suite === "dev" ? await devStartSuite(dir, stubUrl) : await depSuite(dir, stubUrl);
        const hit = checks.find((c) => c.id === m.expect);
        // a mutant only counts as caught if the harness itself still works (control check passes)
        const control = checks.find((c) => c.id === (m.suite === "upload" ? "upload-happy-path" : m.suite === "dev" ? "start-dev-boots-and-serves-health" : "lock-qs-patched"));
        const caught = !!hit && !hit.pass && (m.expect === "upload-happy-path" || !!control?.pass);
        console.log(`  ${caught ? "✅" : "❌"}  ${m.name} ⇒ ${m.expect}${caught ? "" : hit ? " (check still PASSED — vacuous)" : " (check id not found)"}`);
        if (!caught) failures++;
      } catch (e: any) {
        console.log(`  ❌  ${m.name} — harness error: ${e?.message}`); failures++;
      } finally { if (dir) fs.rmSync(dir, { recursive: true, force: true }); }
    }
  } finally {
    stub.close();
    fs.rmSync(EMPTY_CWD, { recursive: true, force: true });
  }
  if (failures) { console.log(`\n❌ upload/dependency validation FAILED (${failures})`); process.exit(1); }
  console.log("\n✅ upload/dependency validation passed");
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
