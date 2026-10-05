// Go Live Day 26 (G4) — static, network-free route authorization guard.
//
// Parses the API source with the TypeScript compiler API (no server boot, no
// DB, no network) and proves, against the REAL route graph:
//
//   1. entry points  — production boots only src/server.ts
//   2. ordering      — identity resolution < default-deny < every customer router
//   3. inventory     — array paths, app.head, mount prefixes, nested routers,
//                      factory routers; unmounted files are declared + unreachable
//   4. allowlist     — every PUBLIC_ROUTES entry matches a real route, is not
//                      shadowed by an earlier param route, and service/self-auth
//                      public routes carry their own verified guard
//   5. guards        — every `require*` guard in use is a known, structurally
//                      verified guard (fail-closed, correct tier set)
//   6. admin routes  — each /v1/admin|internal|cron route has the exact expected
//                      protection; the four inline-scope routes are verified
//   7. wiring        — check:release runs validate:authz-remediation + this guard
//
// Non-vacuity: after the real tree passes, in-memory mutations of the source
// each MUST make the analyser fail on the intended check (nothing is written
// to disk).
//
// Run: npm run validate:route-authz   (tsx, no DB, no network)

import * as ts from "typescript";
import * as fs from "node:fs";
import * as path from "node:path";

type Files = Map<string, string>;
type Fail = { check: string; msg: string };
type Route = {
  method: string; // GET | POST | ... | HEAD | ALL
  path: string; // normalised full path
  guards: string[]; // identifiers/calls between path and handler (+ router-level use())
  file: string;
  line: number;
  order: number[];
  handler: ts.Node | null;
  preGate: boolean; // registered before app.use(requireIdentityByDefault)
};
type Report = { fails: Fail[]; info: string[]; stats: Record<string, number>; routes: Route[] };

const ROOT = path.resolve(__dirname, "..");
const SERVER = "src/server.ts";
const POLICY = "src/routeAuthPolicy.ts";
const EXCLUDED_UNMOUNTED = ["src/routes/callsPins.ts.ts", "src/routes/personas.ts"];
const METHODS = new Set(["get", "post", "put", "patch", "delete", "head", "options", "all"]);

// ───────────────────────── helpers ─────────────────────────

const parseCache = new Map<string, ts.SourceFile>();
function parse(file: string, text: string): ts.SourceFile {
  const k = file + "\0" + text;
  let sf = parseCache.get(k);
  if (!sf) {
    sf = ts.createSourceFile(file, text, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
    parseCache.set(k, sf);
  }
  return sf;
}

/** Comment-free, quote-normalised token string for structural matching. */
function tok(text: string): string {
  const sc = ts.createScanner(ts.ScriptTarget.ES2022, true, ts.LanguageVariant.Standard, text);
  const out: string[] = [];
  for (let t = sc.scan(); t !== ts.SyntaxKind.EndOfFileToken; t = sc.scan()) {
    out.push(
      t === ts.SyntaxKind.StringLiteral || t === ts.SyntaxKind.NoSubstitutionTemplateLiteral
        ? JSON.stringify(sc.getTokenValue())
        : sc.getTokenText()
    );
  }
  return out.join(" ");
}
const has = (tokens: string, snippet: string) => tokens.includes(tok(snippet));

function walk(node: ts.Node, cb: (n: ts.Node) => void) {
  cb(node);
  ts.forEachChild(node, (c) => walk(c, cb));
}
const lineOf = (n: ts.Node) => n.getSourceFile().getLineAndCharacterOfPosition(n.getStart()).line + 1;

function strLits(n: ts.Node | undefined): string[] | null {
  if (!n) return null;
  if (ts.isStringLiteral(n) || ts.isNoSubstitutionTemplateLiteral(n)) return [n.text];
  if (ts.isArrayLiteralExpression(n)) {
    const out: string[] = [];
    for (const e of n.elements) {
      if (!(ts.isStringLiteral(e) || ts.isNoSubstitutionTemplateLiteral(e))) return null;
      out.push(e.text);
    }
    return out;
  }
  return null;
}

function normPath(p: string): string {
  const s = ("/" + p).replace(/\/+/g, "/").toLowerCase();
  return s.length > 1 ? s.replace(/\/+$/, "") : s;
}
const joinPath = (a: string, b: string) => normPath(a + "/" + b);

function patternMatches(pattern: string, concrete: string): boolean {
  const a = pattern.split("/");
  const b = concrete.split("/");
  if (a.length !== b.length) return false;
  return a.every((seg, i) => (seg.startsWith(":") ? b[i].length > 0 : seg === b[i]));
}

function resolveImport(files: Files, from: string, spec: string): string | null {
  if (!spec.startsWith(".")) return null;
  const base = path.posix.join(path.posix.dirname(from), spec);
  for (const c of [base, base + ".ts", base + "/index.ts", base.replace(/\.js$/, ".ts")]) {
    if (files.has(c)) return c;
  }
  return null;
}

type Imp = { file: string | null; imported: string; spec: string };
function importMap(files: Files, file: string, sf: ts.SourceFile): Map<string, Imp> {
  const m = new Map<string, Imp>();
  for (const st of sf.statements) {
    if (!ts.isImportDeclaration(st) || !ts.isStringLiteral(st.moduleSpecifier)) continue;
    const spec = st.moduleSpecifier.text;
    const target = resolveImport(files, file, spec);
    const c = st.importClause;
    if (!c) continue;
    if (c.name) m.set(c.name.text, { file: target, imported: "default", spec });
    if (c.namedBindings && ts.isNamedImports(c.namedBindings)) {
      for (const el of c.namedBindings.elements) {
        m.set(el.name.text, { file: target, imported: (el.propertyName ?? el.name).text, spec });
      }
    }
    if (c.namedBindings && ts.isNamespaceImport(c.namedBindings)) {
      m.set(c.namedBindings.name.text, { file: target, imported: "*", spec });
    }
  }
  return m;
}

/** Side-effect imports (`import "./x"`) also make a file reachable. */
function sideEffectImports(files: Files, file: string, sf: ts.SourceFile): string[] {
  const out: string[] = [];
  for (const st of sf.statements) {
    if (ts.isImportDeclaration(st) && !st.importClause && ts.isStringLiteral(st.moduleSpecifier)) {
      const t = resolveImport(files, file, st.moduleSpecifier.text);
      if (t) out.push(t);
    }
  }
  return out;
}

function isRouterInit(e: ts.Expression | undefined): boolean {
  if (!e || !ts.isCallExpression(e)) return false;
  const c = e.expression;
  return (ts.isIdentifier(c) && c.text === "Router") || (ts.isPropertyAccessExpression(c) && c.name.text === "Router");
}

function routerVars(scope: ts.Node, topLevelOnly: boolean): Set<string> {
  const out = new Set<string>();
  const visit = (n: ts.Node) => {
    if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && isRouterInit(n.initializer)) out.add(n.name.text);
  };
  if (topLevelOnly && ts.isSourceFile(scope)) {
    for (const st of scope.statements) if (ts.isVariableStatement(st)) st.declarationList.declarations.forEach(visit);
  } else {
    walk(scope, visit);
  }
  return out;
}

function findFunction(sf: ts.SourceFile, name: string): ts.FunctionDeclaration | ts.VariableDeclaration | null {
  for (const st of sf.statements) {
    if (ts.isFunctionDeclaration(st) && st.name?.text === name) return st;
    if (ts.isVariableStatement(st)) {
      for (const d of st.declarationList.declarations) {
        if (ts.isIdentifier(d.name) && d.name.text === name) return d;
      }
    }
  }
  return null;
}

function guardNames(args: readonly ts.Expression[]): string[] {
  const out: string[] = [];
  const add = (e: ts.Expression) => {
    if (ts.isIdentifier(e)) out.push(e.text);
    else if (ts.isArrayLiteralExpression(e)) e.elements.forEach(add);
    else if (ts.isCallExpression(e)) out.push(e.expression.getText() + "()");
    else if (ts.isPropertyAccessExpression(e)) out.push(e.getText());
  };
  args.forEach(add);
  return out;
}

// ───────────────────────── analyser ─────────────────────────

function analyse(files: Files, configFiles: Files): Report {
  const fails: Fail[] = [];
  const info: string[] = [];
  const stats: Record<string, number> = {};
  const fail = (check: string, msg: string) => fails.push({ check, msg });
  const routes: Route[] = [];

  const serverText = files.get(SERVER);
  if (!serverText) {
    fail("entry-points", `${SERVER} missing`);
    return { fails, info, stats, routes };
  }
  const sSf = parse(SERVER, serverText);
  const sImports = importMap(files, SERVER, sSf);

  // ── server.ts: locate app + ordered registrations ──
  type Reg = { node: ts.CallExpression; name: string; pos: number; stmtTop: boolean };
  const regs: Reg[] = [];
  walk(sSf, (n) => {
    if (
      ts.isCallExpression(n) &&
      ts.isPropertyAccessExpression(n.expression) &&
      ts.isIdentifier(n.expression.expression) &&
      n.expression.expression.text === "app" &&
      (METHODS.has(n.expression.name.text) || n.expression.name.text === "use" || n.expression.name.text === "route")
    ) {
      const top = ts.isExpressionStatement(n.parent) && ts.isSourceFile(n.parent.parent);
      regs.push({ node: n, name: n.expression.name.text, pos: n.getStart(), stmtTop: top });
    }
  });
  regs.sort((a, b) => a.pos - b.pos);

  for (const r of regs) {
    if (!r.stmtTop) fail("server-structure", `${SERVER}:${lineOf(r.node)} app.${r.name}() registered outside a top-level statement`);
    if (r.name === "route") fail("inventory", `${SERVER}:${lineOf(r.node)} app.route() is not analysable`);
  }

  // identity middleware (A), default-deny (D)
  const useRegs = regs.filter((r) => r.name === "use");
  const identityRegs = useRegs.filter((r) => r.node.arguments.length === 1 && has(tok(r.node.arguments[0].getText()), "resolveIdentity("));
  const denyRegs = useRegs.filter(
    (r) => r.node.arguments.length === 1 && ts.isIdentifier(r.node.arguments[0]) && r.node.arguments[0].text === "requireIdentityByDefault"
  );
  const impersonationRegs = useRegs.filter((r) => r.node.arguments.length === 1 && r.node.arguments[0].getText().includes("x-impersonated-user-id"));
  const A = identityRegs[0];
  const D = denyRegs[0];
  if (identityRegs.length !== 1) fail("identity-middleware", `expected exactly 1 resolveIdentity middleware in ${SERVER}, found ${identityRegs.length}`);
  else {
    const t = tok(A.node.arguments[0].getText());
    if (!has(t, "verifyClaims(token)") || !has(t, "resolveIdentity(req, verified?.sub ?? null)")) {
      fail("identity-middleware", "identity middleware must resolve identity from a VERIFIED claims sub only");
    }
  }
  if (denyRegs.length === 0) fail("default-deny-missing", "app.use(requireIdentityByDefault) is not registered in server.ts");
  if (denyRegs.length > 1) fail("default-deny-missing", "app.use(requireIdentityByDefault) registered more than once");
  const denyImport = sImports.get("requireIdentityByDefault");
  if (!denyImport || denyImport.file !== POLICY || denyImport.imported !== "requireIdentityByDefault") {
    fail("default-deny-missing", "requireIdentityByDefault must be imported from ./routeAuthPolicy");
  }
  if (A && D && !(A.pos < D.pos)) fail("default-deny-order", "default-deny is registered BEFORE identity resolution");
  if (A && impersonationRegs[0] && !(A.pos < impersonationRegs[0].pos)) {
    fail("default-deny-order", "impersonation middleware must come after identity resolution");
  }
  const denyPos = D ? D.pos : Number.POSITIVE_INFINITY;
  const isPre = (r: Reg) => r.pos < denyPos;

  // ── public allowlist (AST-parsed from routeAuthPolicy.ts) ──
  const policyText = files.get(POLICY);
  const publicList: { method: string; path: string; reason: string }[] = [];
  if (!policyText) fail("allowlist", `${POLICY} missing`);
  else {
    const pSf = parse(POLICY, policyText);
    let found = false;
    walk(pSf, (n) => {
      if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && n.name.text === "PUBLIC_ROUTES" && n.initializer) {
        found = true;
        const arr = ts.isAsExpression(n.initializer) ? n.initializer.expression : n.initializer;
        if (!ts.isArrayLiteralExpression(arr)) return fail("allowlist", "PUBLIC_ROUTES must be an array literal");
        for (const el of arr.elements) {
          if (!ts.isObjectLiteralExpression(el)) {
            fail("allowlist", "PUBLIC_ROUTES contains a non-literal entry");
            continue;
          }
          const o: Record<string, string> = {};
          for (const p of el.properties) {
            if (ts.isPropertyAssignment(p) && ts.isIdentifier(p.name) && ts.isStringLiteral(p.initializer)) o[p.name.text] = p.initializer.text;
          }
          publicList.push({ method: (o.method || "").toUpperCase(), path: o.path || "", reason: o.reason || "" });
        }
      }
    });
    if (!found) fail("allowlist", "PUBLIC_ROUTES not found");
    const fn = findFunction(pSf, "requireIdentityByDefault");
    const ft = fn ? tok(fn.getText()) : "";
    if (!fn || !has(ft, "isPublicRoute(req.method, req.path)") || !has(ft, "res.status(401)")) {
      fail("allowlist", "requireIdentityByDefault must consult isPublicRoute(req.method, req.path) and answer 401");
    }
  }
  const seenPub = new Set<string>();
  for (const e of publicList) {
    const k = `${e.method} ${normPath(e.path)}`;
    if (seenPub.has(k)) fail("public-entry-duplicate", `duplicate PUBLIC_ROUTES entry ${k}`);
    seenPub.add(k);
    if (!e.reason.trim()) fail("allowlist", `PUBLIC_ROUTES entry ${k} has no reason`);
    if (!e.path.startsWith("/") || /[:*()?{}]/.test(e.path)) fail("allowlist", `PUBLIC_ROUTES entry ${k} must be a literal path`);
    if (!/^(GET|HEAD|POST)$/.test(e.method)) fail("allowlist", `PUBLIC_ROUTES entry ${k} uses an unexpected method`);
  }

  // ── inventory ──
  const reached = new Set<string>();
  // Mounts that pass a router FACTORY without calling it (Express would treat the
  // factory as plain middleware and never run its routes). Day 27: the only prior
  // exception (/v1/rewards) was repaired, so NO uncalled mount is tolerated.
  const DECLARED_UNCALLED = new Set<string>();
  const seenUncalled = new Set<string>();
  function classify(imps: Map<string, Imp>, t: ts.Expression | undefined): { file: string; factory: string | null; varName: string | null; uncalled: boolean } | null {
    if (!t) return null;
    if (ts.isIdentifier(t)) {
      const im = imps.get(t.text);
      if (!im?.file?.startsWith("src/routes/")) return null;
      if (im.imported === "default") return { file: im.file, factory: null, varName: null, uncalled: false };
      const def = findFunction(parse(im.file, files.get(im.file)!), im.imported);
      if (def && ts.isFunctionDeclaration(def)) return { file: im.file, factory: im.imported, varName: null, uncalled: true };
      return { file: im.file, factory: null, varName: im.imported, uncalled: false };
    }
    if (ts.isCallExpression(t) && ts.isIdentifier(t.expression)) {
      const im = imps.get(t.expression.text);
      if (!im?.file?.startsWith("src/routes/")) return null;
      return { file: im.file, factory: im.imported, varName: null, uncalled: false };
    }
    return null;
  }
  function noteUncalled(prefix: string, factory: string, loc: string) {
    const k = `${prefix}|${factory}`;
    seenUncalled.add(k);
    if (!DECLARED_UNCALLED.has(k)) fail("factory-uncalled", `${loc} mounts router factory ${factory} WITHOUT calling it — Express would run it as middleware and never serve its routes`);
    else info.push(`${loc} mounts ${factory} uncalled at ${prefix}: declared exception`);
  }

  function addRoutes(file: string, scope: ts.Node, vars: Set<string>, prefix: string, baseOrder: number[], serverPre: boolean, seen: string[]) {
    const sf = scope.getSourceFile();
    const imps = importMap(files, file, sf);
    const mw: { pos: number; names: string[] }[] = [];
    const calls: ts.CallExpression[] = [];
    walk(scope, (n) => {
      if (
        ts.isCallExpression(n) &&
        ts.isPropertyAccessExpression(n.expression) &&
        ts.isIdentifier(n.expression.expression) &&
        vars.has(n.expression.expression.text) &&
        (METHODS.has(n.expression.name.text) || ["use", "route"].includes(n.expression.name.text))
      ) {
        calls.push(n);
      }
    });
    calls.sort((a, b) => a.getStart() - b.getStart());
    for (const c of calls) {
      const m = (c.expression as ts.PropertyAccessExpression).name.text;
      const args = c.arguments;
      const loc = `${file}:${lineOf(c)}`;
      if (m === "route") {
        fail("inventory", `${loc} router.route() is not analysable`);
        continue;
      }
      if (m === "use") {
        const ps = strLits(args[0]);
        const target = ps ? args[1] : args[0];
        const cl = classify(imps, target);
        if (cl && ps) {
          for (const p of ps) {
            if (cl.uncalled) noteUncalled(joinPath(prefix, p), cl.factory!, loc);
            mountRouter(cl.file, cl.factory, cl.varName, joinPath(prefix, p), [...baseOrder, c.getStart()], serverPre, seen);
          }
        } else if (!ps) {
          mw.push({ pos: c.getStart(), names: guardNames(args) });
        }
        continue;
      }
      const ps = strLits(args[0]);
      if (!ps) {
        fail("inventory", `${loc} route path is not a string literal / array of literals`);
        continue;
      }
      const mids = args.slice(1, Math.max(1, args.length - 1));
      const handler = args.length > 1 ? args[args.length - 1] : null;
      const routerGuards = mw.filter((x) => x.pos < c.getStart()).flatMap((x) => x.names);
      for (const p of ps) {
        const full = joinPath(prefix, p);
        if (/[*()?{}]/.test(full)) fail("inventory", `${loc} unsupported path syntax ${full}`);
        routes.push({
          method: m.toUpperCase(),
          path: full,
          guards: [...routerGuards, ...guardNames(mids)],
          file,
          line: lineOf(c),
          order: [...baseOrder, c.getStart()],
          handler,
          preGate: serverPre,
        });
      }
    }
  }

  function mountRouter(file: string, factory: string | null, varName: string | null, prefix: string, order: number[], serverPre: boolean, seen: string[]) {
    const id = file + "#" + (factory ?? varName ?? "*");
    if (seen.includes(id)) return fail("inventory", `router mount cycle at ${file}`);
    reached.add(file);
    const sf = parse(file, files.get(file)!);
    const next = [...seen, id];
    const before = routes.length;
    if (factory) {
      const fn = findFunction(sf, factory);
      if (!fn) return fail("inventory", `${file}: factory ${factory}() not found`);
      const vars = routerVars(fn, false);
      if (!vars.size) fail("inventory", `${file}: factory ${factory}() creates no Router`);
      addRoutes(file, fn, vars, prefix, order, serverPre, next);
    } else {
      const all = routerVars(sf, true);
      const vars = varName ? new Set([varName]) : all;
      if (!vars.size) fail("inventory", `${file}: no top-level Router found`);
      addRoutes(file, sf, vars, prefix, order, serverPre, next);
    }
    if (routes.length === before && !files.get(file)!.includes("router.use(\"/")) fail("inventory", `${file} is mounted but yielded no routes`);
  }

  for (const r of regs) {
    const args = r.node.arguments;
    const pre = isPre(r);
    if (r.name === "use") {
      const ps = strLits(args[0]);
      const target = ps ? args[1] : args[0];
      const cl = classify(sImports, target);
      if (cl) {
        if (!ps) fail("inventory", `${SERVER}:${lineOf(r.node)} router mounted without a path prefix`);
        for (const p of ps ?? ["/"]) {
          if (pre) fail("pre-default-deny-mount", `${SERVER}:${lineOf(r.node)} router ${cl.file} is mounted BEFORE default-deny`);
          if (cl.uncalled) noteUncalled(normPath(p), cl.factory!, `${SERVER}:${lineOf(r.node)}`);
          mountRouter(cl.file, cl.factory, cl.varName, normPath(p), [r.pos], pre, []);
        }
      } else if (pre && target && /express\.static|\bstatic\(/.test(target.getText())) {
        fail("pre-default-deny-route", `${SERVER}:${lineOf(r.node)} static file serving registered before default-deny`);
      } else if (pre && args.length && ts.isArrowFunction(args[args.length - 1]) && r !== A && r !== impersonationRegs[0]) {
        const t = tok(args[args.length - 1].getText());
        if (/res \. (json|send|sendFile|end)\b/.test(t)) fail("pre-default-deny-route", `${SERVER}:${lineOf(r.node)} inline middleware before default-deny writes a response`);
      }
      continue;
    }
    // direct route
    if (r.name === "options" && args[0] && ts.isRegularExpressionLiteral(args[0]) && args[0].text === "/.*/" && args.length === 2 && /^cors\(/.test(args[1].getText())) {
      continue; // CORS preflight: answers OPTIONS with CORS headers only (OPTIONS is exempt in isPublicRoute)
    }
    const ps = strLits(args[0]);
    if (!ps) {
      fail("inventory", `${SERVER}:${lineOf(r.node)} route path is not a string literal / array of literals`);
      continue;
    }
    const mids = args.slice(1, Math.max(1, args.length - 1));
    for (const p of ps) {
      routes.push({
        method: r.name.toUpperCase(),
        path: normPath(p),
        guards: guardNames(mids),
        file: SERVER,
        line: lineOf(r.node),
        order: [r.pos],
        handler: args.length > 1 ? args[args.length - 1] : null,
        preGate: pre,
      });
    }
  }
  for (const k of DECLARED_UNCALLED) if (!seenUncalled.has(k)) fail("factory-uncalled", `declared uncalled-factory exception ${k} is stale — remove it from the guard`);
  routes.sort((a, b) => {
    for (let i = 0; i < Math.max(a.order.length, b.order.length); i++) {
      const d = (a.order[i] ?? -1) - (b.order[i] ?? -1);
      if (d) return d;
    }
    return 0;
  });
  stats.routes = routes.length;
  stats.public = publicList.length;

  // pre-gate routes must be public (exact method+path)
  for (const r of routes) {
    if (!r.preGate) continue;
    if (!publicList.some((e) => e.method === r.method && normPath(e.path) === r.path)) {
      fail("pre-default-deny-route", `${r.file}:${r.line} ${r.method} ${r.path} is registered before default-deny and is not in PUBLIC_ROUTES`);
    }
  }

  // ── public entries vs inventory ──
  for (const e of publicList) {
    const p = normPath(e.path);
    const exact = routes.filter((r) => (r.method === e.method || r.method === "ALL") && r.path === p);
    if (!exact.length) {
      fail("public-entry-unmatched", `PUBLIC_ROUTES ${e.method} ${e.path} matches no real route`);
      continue;
    }
    const effective = routes.find((r) => (r.method === e.method || r.method === "ALL" || (e.method === "HEAD" && r.method === "GET")) && patternMatches(r.path, p));
    if (effective && effective.path !== p) {
      fail("public-entry-shadowed", `PUBLIC_ROUTES ${e.method} ${e.path} is first handled by param route ${effective.method} ${effective.path} (${effective.file}:${effective.line})`);
    }
    if (e.method === "GET" || e.method === "HEAD") {
      const h = exact[0].handler;
      if (h && has(tok(h.getText()), ".from (")) {
        fail("public-entry-data", `public ${e.method} ${e.path} handler queries the database (${exact[0].file}:${exact[0].line})`);
      }
    }
  }
  // service / self-verifying public routes
  const cronRoute = routes.find((r) => r.method === "POST" && r.path === "/v1/cron/crm/auto-assign");
  if (publicList.some((e) => e.path === "/v1/cron/crm/auto-assign") && !(cronRoute && cronRoute.guards.includes("requireCron"))) {
    fail("service-guard", "public POST /v1/cron/crm/auto-assign must carry requireCron");
  }
  const meRoute = routes.find((r) => r.method === "GET" && r.path === "/v1/auth/me");
  if (publicList.some((e) => e.path === "/v1/auth/me") && !(meRoute?.handler && has(tok(meRoute.handler.getText()), "auth.getUser(token)"))) {
    fail("service-guard", "public GET /v1/auth/me must verify its own Bearer token (auth.getUser(token))");
  }

  // ── reachability / unmounted files ──
  const routeFiles = [...files.keys()].filter((f) => /^src\/routes\/[^/]+\.ts$/.test(f));
  for (const f of routeFiles) {
    const excluded = EXCLUDED_UNMOUNTED.includes(f);
    if (excluded && reached.has(f)) fail("excluded-file-reachable", `${f} must stay unmounted but is mounted`);
    if (!excluded && !reached.has(f)) fail("unmounted-undeclared", `${f} is not mounted by any router and is not a declared exclusion`);
  }
  for (const f of EXCLUDED_UNMOUNTED) if (!files.has(f)) fail("unmounted-undeclared", `declared exclusion ${f} no longer exists — update the guard`);
  for (const [f, text] of files) {
    if (!f.startsWith("src/")) continue;
    const sf = parse(f, text);
    for (const [local, im] of importMap(files, f, sf)) {
      if (im.file && EXCLUDED_UNMOUNTED.includes(im.file)) fail("excluded-file-reachable", `${f} imports excluded unmounted file ${im.file} (as ${local})`);
      if (im.imported === "repsRoutes" && im.file === "src/routes/assignments.ts") fail("excluded-file-reachable", `${f} imports unmounted factory repsRoutes()`);
    }
    for (const t of sideEffectImports(files, f, sf)) {
      if (EXCLUDED_UNMOUNTED.includes(t)) fail("excluded-file-reachable", `${f} side-effect-imports excluded unmounted file ${t}`);
    }
  }

  // ── entry points ──
  const bootText: string[] = [];
  let pkg: any = {};
  try {
    pkg = JSON.parse(configFiles.get("package.json") ?? "{}");
  } catch {
    fail("entry-points", "package.json unreadable");
  }
  if (pkg.scripts?.start) bootText.push(String(pkg.scripts.start));
  if (pkg.main) bootText.push(String(pkg.main));
  for (const f of ["railway.json", "nixpacks.toml", "Procfile", "Dockerfile"]) {
    const t = configFiles.get(f);
    if (t === undefined) continue;
    if (f === "railway.json") {
      try {
        const j = JSON.parse(t);
        if (j.deploy?.startCommand) bootText.push(String(j.deploy.startCommand));
      } catch {
        fail("entry-points", "railway.json unreadable");
      }
    } else bootText.push(t.split("\n").filter((l) => /cmd|CMD|web:|start/i.test(l) && !l.trim().startsWith("#")).join("\n"));
  }
  if (!bootText.length) fail("entry-points", "no production start command found");
  for (const b of bootText) {
    if (/src\/index|dist\/index/.test(b)) fail("entry-points", `a production start command boots the legacy src/index.ts app: ${b.trim()}`);
  }
  if (!bootText.some((b) => b.includes("src/server.ts"))) fail("entry-points", "no production start command boots src/server.ts");
  for (const [f, text] of files) {
    if (!f.startsWith("src/") || f === SERVER || f === "src/index.ts") continue;
    if (/\bexpress\(\)/.test(text)) fail("entry-points", `${f} creates a second express() app`);
    if ([...importMap(files, f, parse(f, text)).values()].some((i) => i.file === "src/index.ts")) fail("entry-points", `${f} imports src/index.ts`);
  }
  if (files.has("src/index.ts") && /\bexpress\(\)/.test(files.get("src/index.ts")!)) {
    info.push("src/index.ts is a legacy, UNAUTHENTICATED dev entry (start:dev only; exposes /v1/env-check, /v1/db/now). Not booted by production commands; consider deleting it.");
  }

  // ── guards ──
  const KNOWN = new Set(["requireIdentity", "requireAdmin", "requireCron", "requireManager", "requireUserId", "requireAuth", "requireSuperAdmin", "requirePartnerAdmin", "requireInternal"]);
  function guardDef(file: string, name: string): { file: string; node: ts.Node } | null {
    const sf = parse(file, files.get(file)!);
    const local = findFunction(sf, name);
    if (local) return { file, node: local };
    const im = importMap(files, file, sf).get(name);
    if (im?.file) {
      const f2 = findFunction(parse(im.file, files.get(im.file)!), im.imported === "default" ? name : im.imported);
      if (f2) return { file: im.file, node: f2 };
    }
    return null;
  }
  const TIERS_MGR = new Set(["Manager", "Admin", "Owner", "PartnerAdmin", "SuperAdmin"]);
  function verifyGuard(name: string, file: string): string[] {
    const def = guardDef(file, name);
    if (!def) return [`${name} (used in ${file}) has no resolvable definition`];
    const t = tok(def.node.getText());
    const p: string[] = [];
    const defText = files.get(def.file)!;
    const litsOf = (src: string) => [...src.matchAll(/"(SalesRep|Manager|Admin|Owner|PartnerAdmin|SuperAdmin)"/g)].map((m) => m[1]);
    const setDef = (id: string) => {
      const m = defText.match(new RegExp(id + "\\s*=\\s*new Set(?:<[^>]*>)?\\(\\[([^\\]]*)\\]"));
      return m ? litsOf(m[1]) : null;
    };
    const tierLits =
      name === "requirePartnerAdmin" ? (setDef("PARTNER_ADMIN_TIERS") ?? []) :
      name === "requireManager" ? (setDef("MANAGER_TIERS") ?? setDef("MANAGER_ROLES") ?? litsOf(def.node.getText())) :
      litsOf(def.node.getText());
    const nexts = (t.match(/\bnext \( \)/g) || []).length;
    switch (name) {
      case "requireIdentity":
        if (!/\b401\b/.test(t) || !has(t, "if (!uid)")) p.push("must answer 401 when no uid");
        break;
      case "requireUserId":
        if (!/\b401\b/.test(t) || !has(t, "if (!uid)")) p.push("must answer 401 when no uid");
        break;
      case "requireAuth":
        if (!has(t, "requireUserId(req, res, next)")) p.push("must delegate to requireUserId");
        break;
      case "requireAdmin":
        // Day 27: env gate (exact "true" only) THEN verified-SuperAdmin delegation; no direct next().
        if (nexts !== 0 || !has(t, 'if (process.env.ALLOW_ADMIN_ENDPOINTS !== "true") return res.status(403)') || !has(t, "return requireSuperAdmin(req, res, next)")) {
          p.push('must be an env gate (403 unless ALLOW_ADMIN_ENDPOINTS === "true") that delegates to requireSuperAdmin and never calls next() itself');
        }
        break;
      case "requireCron":
        if (!has(t, "cron_secret_not_configured") || !has(t, "crypto.timingSafeEqual") || !has(t, 'req.header("x-cron-secret")') || nexts !== 1) {
          p.push("must fail closed without a secret and compare x-cron-secret timing-safely");
        }
        break;
      case "requireInternal":
        if (!has(t, "canAccessInternalPortal(internalUser)") || !has(t, "internal_access_required") || !has(t, "res.status(403)")) {
          p.push("must 403 unless canAccessInternalPortal(internalUser)");
        }
        break;
      case "requireSuperAdmin":
        if (!has(t, 'tier !== "SuperAdmin"') || !has(t, "res.status(403)") || tierLits.some((x) => x !== "SuperAdmin")) {
          p.push('must admit tier "SuperAdmin" only');
        }
        break;
      case "requirePartnerAdmin": {
        const set = [...new Set(tierLits)].sort().join(",");
        if (set !== "PartnerAdmin,SuperAdmin" || !has(t, "!PARTNER_ADMIN_TIERS.has(tier)")) p.push("must admit PartnerAdmin + SuperAdmin only");
        break;
      }
      case "requireManager": {
        const bad = tierLits.filter((x) => !TIERS_MGR.has(x));
        if (bad.length || !tierLits.length || !/\b403\b/.test(t)) p.push("tier set must be a subset of Manager/Admin/Owner/PartnerAdmin/SuperAdmin (never SalesRep) with a 403 path");
        break;
      }
    }
    return p;
  }
  const usedGuards = new Set<string>();
  for (const r of routes) {
    for (const g of r.guards) {
      if (!/^require[A-Z]/.test(g)) continue;
      usedGuards.add(`${r.file}|${g}`);
      if (!KNOWN.has(g)) fail("guard-unknown", `${r.file}:${r.line} uses unverified guard ${g}`);
    }
  }
  for (const k of usedGuards) {
    const [f, g] = k.split("|");
    if (!KNOWN.has(g)) continue;
    for (const m of verifyGuard(g, f)) fail("guard-weak", `${g} (${f}): ${m}`);
  }
  stats.guards = usedGuards.size;

  // ── admin / internal / cron route protection ──
  const ADMIN: Record<string, string[]> = {
    requireSuperAdmin: [
      "POST /v1/admin/force-score/:id", "GET /v1/admin/status", "POST /v1/admin/test-slack", "POST /v1/admin/send-slack",
      "GET /v1/admin/preview-slack", "POST /v1/admin/post-score-demo", "GET /v1/admin/whoami", "GET /v1/admin/whoami-org",
      "GET /v1/admin/super/health", "GET /v1/admin/super/partners", "POST /v1/admin/super/impersonate",
      "POST /v1/admin/super/stop-impersonation", "GET /v1/admin/organisations", "GET /v1/admin/platform",
      "GET /v1/admin/support/users", "GET /v1/admin/support/companies", "GET /v1/admin/support/impersonation-history",
      "GET /v1/admin/super/audit", "GET /v1/admin/super/licences",
    ],
    requirePartnerAdmin: [
      "GET /v1/admin/partner/health", "GET /v1/admin/partner/companies", "GET /v1/admin/partner/users",
      "GET /v1/admin/partner/licences", "GET /v1/admin/context/options",
    ],
    requireManager: [
      "PATCH /v1/admin/org-settings", "POST /v1/admin/users", "GET /v1/admin/users", "GET /v1/admin/usage",
      "GET /v1/admin/config", "PATCH /v1/admin/config", "GET /v1/admin/reps", "PATCH /v1/admin/reps/:id",
      "GET /v1/admin/persona-config", "PATCH /v1/admin/persona-config",
    ],
    requireUserId: ["GET /v1/admin/org-settings"],
    INLINE: ["GET /v1/admin/users/:id", "PATCH /v1/admin/users/:id", "GET /v1/admin/companies/:id", "PATCH /v1/admin/companies/:id"],
  };
  const SERVER_ADMIN = [
    "POST /v1/admin/score/:id", "POST /v1/admin/force-score/:id", "GET /v1/admin/preview-slack",
    "POST /v1/admin/post-slack", "POST /v1/admin/digest/daily", "GET /v1/admin/index-hints",
  ];
  const expected = new Map<string, string>();
  for (const [g, list] of Object.entries(ADMIN)) for (const k of list) expected.set(`src/routes/admin.ts|${k}`, g);
  for (const k of SERVER_ADMIN) expected.set(`${SERVER}|${k}`, "requireAdmin");
  const seenExpected = new Set<string>();
  for (const r of routes) {
    if (r.path === "/v1/admin" || r.path.startsWith("/v1/admin/")) {
      const key = `${r.file}|${r.method} ${r.path}`;
      const want = expected.get(key);
      if (!want) {
        fail("admin-unclassified", `${r.file}:${r.line} ${r.method} ${r.path} is an /v1/admin route with no declared protection — classify it in the guard`);
      } else {
        seenExpected.add(key);
        if (want === "INLINE") {
          if (r.guards.some((g) => /^require/.test(g))) fail("admin-guard", `${r.method} ${r.path}: inline-scope route unexpectedly mixes a guard (${r.guards.join(",")})`);
        } else if (!r.guards.includes(want)) {
          fail("admin-guard", `${r.file}:${r.line} ${r.method} ${r.path} must be guarded by ${want} (has: ${r.guards.join(",") || "none"})`);
        }
      }
    }
    if (r.path.startsWith("/v1/internal/") && !r.guards.includes("requireInternal")) {
      fail("admin-guard", `${r.file}:${r.line} ${r.method} ${r.path} must be guarded by requireInternal`);
    }
    if (r.path.startsWith("/v1/cron/") && !r.guards.includes("requireCron")) {
      fail("service-guard", `${r.file}:${r.line} ${r.method} ${r.path} must be guarded by requireCron`);
    }
  }
  for (const k of expected.keys()) if (!seenExpected.has(k)) fail("admin-table-stale", `expected admin route not found in router: ${k.replace("|", " ")}`);
  for (const r of routes) {
    if (r.file === "src/routes/admin.ts" && r.path.startsWith("/v1/admin/")) {
      const first = routes.find((x) => x.method === r.method && patternMatches(x.path, r.path));
      if (first && first !== r) {
        info.push(`${r.method} ${r.path}: adminRouter [${r.guards.filter((g) => /^require/.test(g)).join(",")}] is SHADOWED by earlier ${first.file}:${first.line} [${first.guards.join(",") || "no guard"}] — effective guard is the earlier one`);
      }
    }
  }
  info.push("server.ts /v1/admin/* routes use requireAdmin: closed unless ALLOW_ADMIN_ENDPOINTS==='true' AND the caller is a verified-tier SuperAdmin (Day 27); production boot refuses the flag entirely.");

  // inline-scope routes
  const adminText = files.get("src/routes/admin.ts");
  const adminSf = adminText ? parse("src/routes/admin.ts", adminText) : null;
  const INLINE: Record<string, string> = { "/v1/admin/users/:id": "assertUserEditScope", "/v1/admin/companies/:id": "assertCompanyEditScope" };
  for (const r of routes) {
    const helper = r.file === "src/routes/admin.ts" ? INLINE[r.path] : undefined;
    if (!helper || !r.handler) continue;
    const ht = tok(r.handler.getText());
    const iCall = ht.indexOf(`await ${helper} (`);
    const iDb = ht.search(/\.from \( "(reps|companies)" \)/);
    const callOk = iCall >= 0 && (iDb < 0 || iCall < iDb);
    const afterCall = iCall >= 0 ? ht.slice(iCall) : "";
    const denyOk = /if \( ! scope \. ok \) return res \. status \( scope \. status \)/.test(afterCall);
    const actorOk = has(ht, 'req.header("x-user-id")') && /if \( ! actorId \) return res \. status \( 401 \)/.test(ht);
    if (!callOk || !denyOk || !actorOk) {
      fail("inline-scope", `${r.method} ${r.path} (${r.file}:${r.line}) must authenticate the actor, call ${helper}() BEFORE any data access, and return scope.status when !scope.ok`);
    }
  }
  if (adminSf) {
    for (const [name, mgrCheck] of [["assertUserEditScope", "actorCo !== targetCo"], ["assertCompanyEditScope", "actorCo !== targetCompanyId"]] as const) {
      const fn = findFunction(adminSf, name);
      if (!fn) {
        fail("inline-scope-helper", `${name} not found`);
        continue;
      }
      const t = tok(fn.getText());
      const body = (fn as ts.FunctionDeclaration).body;
      const last = body && body.statements[body.statements.length - 1];
      const lastT = last ? tok(last.getText()) : "";
      const okTrues = (t.match(/ok : true ,/g) || []).length;
      const gated = (t.match(/if \( actorTier === "(SuperAdmin|Manager|PartnerAdmin)" \)/g) || []).length;
      if (
        !/^return \{ ok : false , status : 403/.test(lastT) ||
        gated !== 3 ||
        okTrues !== 3 ||
        !has(t, mgrCheck) ||
        !has(t, "partner_id !== partnerId") ||
        !has(t, "actor_not_found") ||
        !has(t, 'if (actorTier === "SuperAdmin") return { ok: true, actorTier }')
      ) {
        fail("inline-scope-helper", `${name} must default-deny (final 403), allow only SuperAdmin globally, scope Manager by company and PartnerAdmin by partner`);
      }
    }
  }

  // ── header-reading guards depend on resolveIdentity stripping spoofable headers ──
  const trust = files.get("src/identityTrust.ts");
  if (!trust) fail("header-strip", "src/identityTrust.ts missing");
  else {
    const tsf = parse("src/identityTrust.ts", trust);
    const fn = findFunction(tsf, "resolveIdentity");
    const ft = fn ? tok(fn.getText()) : "";
    if (!has(ft, "if (!identityHeadersTrusted(req)) { for (const h of SPOOFABLE_IDENTITY_HEADERS) delete req.headers[h]; }")) {
      fail("header-strip", "resolveIdentity must strip SPOOFABLE_IDENTITY_HEADERS from req.headers when identityHeadersTrusted(req) is false");
    }
    const spoof = new Set<string>();
    walk(tsf, (n) => {
      if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && n.name.text === "SPOOFABLE_IDENTITY_HEADERS" && n.initializer) {
        const arr = ts.isAsExpression(n.initializer) ? n.initializer.expression : n.initializer;
        for (const s of strLits(arr) ?? []) spoof.add(s.toLowerCase());
      }
    });
    for (const f of ["src/middleware/requireManager.ts", "src/middleware/requireSuperAdmin.ts", "src/middleware/requirePartnerAdmin.ts", "src/routes/admin.ts"]) {
      const text = files.get(f);
      if (!text) continue;
      for (const m of text.matchAll(/\.header\(\s*["']([^"']*user-id[^"']*)["']\s*\)/gi)) {
        if (!spoof.has(m[1].toLowerCase())) fail("header-strip", `${f} reads identity header "${m[1]}" which resolveIdentity does not strip when untrusted`);
      }
    }
  }
  const setsAuthUserId = [...files].some(([f, t]) => (f === SERVER || f.startsWith("src/middleware/")) && /authUserId\s*=/.test(t));
  if (!setsAuthUserId && files.get("src/routes/internal.ts")?.includes("req.authUserId")) {
    info.push("requireInternal reads req.authUserId, which no global middleware sets → /v1/internal/* answers 403 for everyone (fails closed; the internal portal is unreachable).");
  }

  // ── ALLOW_ADMIN_ENDPOINTS must not be enabled by committed config ──
  for (const [f, t] of configFiles) {
    if (/ALLOW_ADMIN_ENDPOINTS["']?\s*[:=]\s*["']?true/i.test(t)) fail("admin-env", `${f} enables ALLOW_ADMIN_ENDPOINTS`);
  }

  // ── release wiring ──
  const scripts = pkg.scripts ?? {};
  const cr = String(scripts["check:release"] ?? "");
  for (const s of ["validate:authz-remediation", "validate:route-authz", "validate:admin-hardening"]) {
    if (!scripts[s]) fail("release-wiring", `package.json has no "${s}" script`);
    if (!cr.includes(`npm run ${s}`)) fail("release-wiring", `check:release does not run ${s}`);
  }
  if (!cr.includes("npm run validate:identity-boundary")) fail("release-wiring", "check:release lost validate:identity-boundary");

  return { fails, info, stats, routes };
}

// ───────────────────────── runner ─────────────────────────

function loadFiles(): { files: Files; config: Files } {
  const files: Files = new Map();
  const walkDir = (dir: string) => {
    for (const e of fs.readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
      const rel = path.posix.join(dir, e.name);
      if (e.isDirectory()) walkDir(rel);
      else if (e.name.endsWith(".ts")) files.set(rel, fs.readFileSync(path.join(ROOT, rel), "utf8"));
    }
  };
  walkDir("src");
  const config: Files = new Map();
  // Committed config only — never .env (secrets).
  for (const f of ["package.json", "railway.json", "nixpacks.toml", "Procfile", "Dockerfile", ".env.example", ".env.staging.example"]) {
    if (fs.existsSync(path.join(ROOT, f))) config.set(f, fs.readFileSync(path.join(ROOT, f), "utf8"));
  }
  return { files, config };
}

function mutate(base: Files, file: string, find: string, repl: string, nth = 1): Files {
  const text = base.get(file);
  if (text === undefined) throw new Error(`mutation target missing: ${file}`);
  let idx = -1;
  for (let i = 0; i < nth; i++) {
    idx = text.indexOf(find, idx + 1);
    if (idx < 0) throw new Error(`mutation anchor not found (#${nth}) in ${file}: ${find.slice(0, 60)}`);
  }
  const out = new Map(base);
  out.set(file, text.slice(0, idx) + repl + text.slice(idx + find.length));
  return out;
}

let failures = 0;
function ok(name: string, pass: boolean, detail = "") {
  console.log(`  ${pass ? "✅" : "❌"}  ${name}${detail ? " — " + detail : ""}`);
  if (!pass) failures++;
}

async function main() {
  const { files, config } = loadFiles();
  console.log("Go Live Day 26 (G4) — static route authorization guard\n");

  const rep = analyse(files, config);
  const CHECKS: [string, string][] = [
    ["entry-points", "production boots only src/server.ts"],
    ["identity-middleware", "identity resolved from verified claims only"],
    ["default-deny-missing", "default-deny registered once, from routeAuthPolicy"],
    ["default-deny-order", "identity resolution → default-deny ordering"],
    ["pre-default-deny-mount", "no customer router mounted before default-deny"],
    ["pre-default-deny-route", "only PUBLIC routes registered before default-deny"],
    ["factory-uncalled", "router factories are mounted called (declared exceptions only)"],
    ["inventory", "route inventory fully analysable (arrays, head, prefixes, nesting, factories)"],
    ["server-structure", "app registrations are top-level statements"],
    ["unmounted-undeclared", "every routes/*.ts mounted or a declared exclusion"],
    ["excluded-file-reachable", "callsPins.ts.ts / personas.ts / repsRoutes() unreachable"],
    ["allowlist", "PUBLIC_ROUTES well-formed"],
    ["public-entry-duplicate", "PUBLIC_ROUTES has no duplicates"],
    ["public-entry-unmatched", "every PUBLIC_ROUTES entry matches a real route"],
    ["public-entry-shadowed", "no public entry is first handled by a param route"],
    ["public-entry-data", "public GET/HEAD handlers do not query the database"],
    ["service-guard", "cron + self-verifying public routes carry their own guard"],
    ["guard-unknown", "only known require* guards in use"],
    ["guard-weak", "each guard is structurally fail-closed / correct tier set"],
    ["admin-unclassified", "every /v1/admin route is classified"],
    ["admin-table-stale", "every classified admin route exists"],
    ["admin-guard", "admin/internal routes carry the exact expected guard"],
    ["inline-scope", "users/:id + companies/:id GET/PATCH: scope check precedes data access"],
    ["inline-scope-helper", "scope helpers default-deny and tenant-scope"],
    ["header-strip", "header-reading guards rely on stripped spoofable headers"],
    ["admin-env", "committed config never enables ALLOW_ADMIN_ENDPOINTS"],
    ["release-wiring", "check:release runs authz-remediation + route-authz"],
  ];
  for (const [id, label] of CHECKS) {
    const f = rep.fails.filter((x) => x.check === id);
    ok(label, f.length === 0, f.map((x) => x.msg).join(" | "));
  }
  const unknown = rep.fails.filter((x) => !CHECKS.some(([id]) => id === x.check));
  ok("no uncategorised failures", unknown.length === 0, unknown.map((x) => x.msg).join(" | "));
  console.log(`\n  inventory: ${rep.stats.routes} routes (${rep.routes.filter((r) => r.preGate).length} pre-gate), ${rep.stats.public} public entries, ${rep.stats.guards} guard uses verified`);

  // runtime policy behaviour (network-free)
  const policy = await import("../src/routeAuthPolicy");
  const rt = JSON.stringify(policy.PUBLIC_ROUTES.map((r: any) => [r.method, r.path]));
  const astList: string[][] = [];
  walk(parse(POLICY, files.get(POLICY)!), (n) => {
    if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && n.name.text === "PUBLIC_ROUTES" && n.initializer) {
      const arr = ts.isAsExpression(n.initializer) ? n.initializer.expression : n.initializer;
      if (!ts.isArrayLiteralExpression(arr)) return;
      for (const el of arr.elements) {
        if (!ts.isObjectLiteralExpression(el)) continue;
        const o: Record<string, string> = {};
        for (const p of el.properties) if (ts.isPropertyAssignment(p) && ts.isIdentifier(p.name) && ts.isStringLiteral(p.initializer)) o[p.name.text] = p.initializer.text;
        astList.push([o.method, o.path]);
      }
    }
  });
  ok("AST-parsed PUBLIC_ROUTES equals runtime export", rt === JSON.stringify(astList));
  const probe = (method: string, p: string, userId?: string) => {
    let status = 0;
    let nexted = false;
    const res: any = { status: (s: number) => ((status = s), res), json: () => res };
    policy.requireIdentityByDefault({ method, path: p, userId } as any, res, () => (nexted = true));
    return { status, nexted };
  };
  ok("runtime: anonymous protected route ⇒ 401", probe("GET", "/v1/calls").status === 401 && !probe("GET", "/v1/calls").nexted);
  ok("runtime: anonymous PUBLIC route ⇒ next()", probe("GET", "/v1/health").nexted && probe("HEAD", "/health").nexted);
  ok("runtime: identified protected route ⇒ next()", probe("GET", "/v1/calls", "u1").nexted);
  ok("runtime: case/trailing-slash variants of a protected path stay denied", probe("GET", "/V1/CALLS/").status === 401);
  ok("runtime: case/trailing-slash variants of a public path stay public (matches Express routing)", probe("GET", "/V1/Health/").nexted);

  console.log("\nInformational (policy questions left unresolved, per task scope):");
  for (const i of [...new Set(rep.info)]) console.log("  ℹ️  " + i);

  // ── mutation proof ──
  console.log("\nMutation proof — each in-memory mutation MUST be caught by the intended check:");
  const DENY = "app.use(requireIdentityByDefault);";
  const withCfg = (name: string, text: string | undefined, fn: (t: string) => string) => new Map(config).set(name, fn(text ?? ""));
  const M: { name: string; expect: string; run: () => Files; cfg?: Files }[] = [
    { name: "delete default-deny", expect: "default-deny-missing", run: () => mutate(files, SERVER, DENY, "") },
    {
      name: "default-deny moved after customer routers",
      expect: "pre-default-deny-mount",
      run: () => mutate(mutate(files, SERVER, DENY, ""), SERVER, 'app.use("/v1/calls", callsRouter);', 'app.use("/v1/calls", callsRouter);\n' + DENY),
    },
    { name: "customer router mounted before default-deny", expect: "pre-default-deny-mount", run: () => mutate(files, SERVER, DENY, 'app.use("/v1/team", teamRoutes);\n' + DENY) },
    {
      name: "default-deny before identity resolution",
      expect: "default-deny-order",
      run: () => mutate(mutate(files, SERVER, DENY, ""), SERVER, "const verifyClaims = defaultClaimsVerifier();", DENY + "\nconst verifyClaims = defaultClaimsVerifier();"),
    },
    { name: "data route registered before default-deny", expect: "pre-default-deny-route", run: () => mutate(files, SERVER, DENY, 'app.get("/v1/leak", (_req, res) => res.json({}));\n' + DENY) },
    {
      name: "bogus PUBLIC_ROUTES entry",
      expect: "public-entry-unmatched",
      run: () => mutate(files, POLICY, '{ method: "GET", path: "/v1/version"', '{ method: "GET", path: "/v1/nope", reason: "x" },\n  { method: "GET", path: "/v1/version"'),
    },
    { name: "public entry path renamed", expect: "public-entry-unmatched", run: () => mutate(files, POLICY, "/v1/crm/health", "/v1/crm/healthz") },
    { name: "app.head registration removed (HEAD handling)", expect: "public-entry-unmatched", run: () => mutate(files, SERVER, 'app.head(["/health", "/v1/health"], (_req, res) => res.status(200).end());', "") },
    { name: "array path shrunk (array-path handling)", expect: "public-entry-unmatched", run: () => mutate(files, SERVER, 'app.get(["/health", "/v1/health"]', 'app.get(["/health"]') },
    { name: "mount prefix changed (prefix handling)", expect: "public-entry-unmatched", run: () => mutate(files, SERVER, 'app.use("/v1/crm", crmRouter)', 'app.use("/v2/crm", crmRouter)') },
    { name: "nested intelligence router unmounted", expect: "unmounted-undeclared", run: () => mutate(files, "src/routes/intelligence.ts", 'router.use("/scorecards", scorecardsRouter);', "") },
    { name: "factory router assignmentsRoutes() unmounted", expect: "unmounted-undeclared", run: () => mutate(files, SERVER, 'app.use("/v1/assignments", assignmentsRoutes());', "") },
    { name: "factory router rewardsRoutes unmounted", expect: "unmounted-undeclared", run: () => mutate(files, SERVER, 'app.use("/v1/rewards", rewardsRoutes());', "") },
    { name: "rewards factory regresses to uncalled mount", expect: "factory-uncalled", run: () => mutate(files, SERVER, 'app.use("/v1/rewards", rewardsRoutes());', 'app.use("/v1/rewards", rewardsRoutes);') },
    { name: "assignments factory mounted uncalled", expect: "factory-uncalled", run: () => mutate(files, SERVER, 'app.use("/v1/assignments", assignmentsRoutes());', 'app.use("/v1/assignments", assignmentsRoutes);') },
    {
      name: "excluded callsPins.ts.ts imported",
      expect: "excluded-file-reachable",
      run: () => mutate(files, SERVER, 'import callsRouter from "./routes/calls";', 'import callsRouter from "./routes/calls";\nimport cp from "./routes/callsPins.ts";\nvoid cp;'),
    },
    { name: "excluded routes/personas.ts side-effect imported", expect: "excluded-file-reachable", run: () => mutate(files, "src/routes/sparring.ts", "const router = express.Router();", 'import "./personas";\nconst router = express.Router();') },
    { name: "param route shadows a public path", expect: "public-entry-shadowed", run: () => mutate(files, SERVER, 'app.get("/v1/version"', 'app.get("/v1/:x", (_q, r) => r.json({}));\napp.get("/v1/version"') },
    { name: "admin route loses requireSuperAdmin", expect: "admin-guard", run: () => mutate(files, "src/routes/admin.ts", 'adminRouter.get("/super/health", requireSuperAdmin,', 'adminRouter.get("/super/health",') },
    { name: "requireSuperAdmin widened to Manager", expect: "guard-weak", run: () => mutate(files, "src/middleware/requireSuperAdmin.ts", 'tier !== "SuperAdmin"', 'tier !== "SuperAdmin" && tier !== "Manager"') },
    { name: "requireAdmin env gate made fail-open", expect: "guard-weak", run: () => mutate(files, SERVER, 'process.env.ALLOW_ADMIN_ENDPOINTS !== "true"', 'process.env.ALLOW_ADMIN_ENDPOINTS === "false"') },
    { name: "requireAdmin drops SuperAdmin delegation (any identity passes)", expect: "guard-weak", run: () => mutate(files, SERVER, "return requireSuperAdmin(req, res, next);", "return next();") },
    { name: "server.ts admin route loses requireAdmin", expect: "admin-guard", run: () => mutate(files, SERVER, 'app.post("/v1/admin/post-slack", requireAdmin,', 'app.post("/v1/admin/post-slack",') },
    { name: "unclassified new admin route", expect: "admin-unclassified", run: () => mutate(files, "src/routes/admin.ts", "export default adminRouter;", 'adminRouter.get("/danger", async (_q: any, r: any) => r.json({}));\nexport default adminRouter;') },
    {
      name: "users/:id GET loses scope check",
      expect: "inline-scope",
      run: () => mutate(files, "src/routes/admin.ts", "const scope = await assertUserEditScope(actorId, targetId, supa);\n    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });", "", 1),
    },
    {
      name: "companies/:id PATCH loses scope check",
      expect: "inline-scope",
      run: () => mutate(files, "src/routes/admin.ts", "const scope = await assertCompanyEditScope(actorId, companyId, supa);\n    if (!scope.ok) return res.status(scope.status).json({ ok: false, error: scope.error });", "", 2),
    },
    {
      name: "user scope helper falls through to allow",
      expect: "inline-scope-helper",
      run: () => mutate(files, "src/routes/admin.ts", '  return { ok: false, status: 403, error: "forbidden_scope" };\n}\n\nadminRouter.get("/users/:id"', '  return { ok: true, actorTier };\n}\n\nadminRouter.get("/users/:id"'),
    },
    { name: "company scope helper drops Manager company check", expect: "inline-scope-helper", run: () => mutate(files, "src/routes/admin.ts", "if (actorCo !== targetCompanyId)", "if (false)") },
    { name: "requireManager admits SalesRep", expect: "guard-weak", run: () => mutate(files, "src/middleware/requireManager.ts", 'new Set<string>(["Manager",', 'new Set<string>(["SalesRep", "Manager",') },
    { name: "requirePartnerAdmin admits Manager", expect: "guard-weak", run: () => mutate(files, "src/middleware/requirePartnerAdmin.ts", 'new Set(["PartnerAdmin", "SuperAdmin"])', 'new Set(["PartnerAdmin", "SuperAdmin", "Manager"])') },
    { name: "cron route loses requireCron", expect: "service-guard", run: () => mutate(files, SERVER, 'app.post("/v1/cron/crm/auto-assign", requireCron,', 'app.post("/v1/cron/crm/auto-assign",') },
    { name: "internal route loses requireInternal", expect: "admin-guard", run: () => mutate(files, "src/routes/internal.ts", 'router.get("/tmcs", requireInternal,', 'router.get("/tmcs",') },
    { name: "production boots legacy src/index.ts", expect: "entry-points", run: () => files, cfg: withCfg("railway.json", config.get("railway.json"), (t) => t.replace("src/server.ts", "src/index.ts")) },
    { name: "committed config enables ALLOW_ADMIN_ENDPOINTS", expect: "admin-env", run: () => files, cfg: withCfg("nixpacks.toml", config.get("nixpacks.toml"), (t) => t + '\n[variables]\nALLOW_ADMIN_ENDPOINTS = "true"\n') },
    { name: "spoofable-header stripping removed", expect: "header-strip", run: () => mutate(files, "src/identityTrust.ts", "for (const h of SPOOFABLE_IDENTITY_HEADERS) delete req.headers[h];", "") },
    { name: "check:release drops validate:route-authz", expect: "release-wiring", run: () => files, cfg: withCfg("package.json", config.get("package.json"), (t) => t.replace(" && npm run validate:route-authz", "")) },
    { name: "check:release drops validate:admin-hardening", expect: "release-wiring", run: () => files, cfg: withCfg("package.json", config.get("package.json"), (t) => t.replace(" && npm run validate:admin-hardening", "")) },
    { name: "check:release drops validate:authz-remediation", expect: "release-wiring", run: () => files, cfg: withCfg("package.json", config.get("package.json"), (t) => t.replace(" && npm run validate:authz-remediation", "")) },
  ];
  for (const m of M) {
    try {
      const r = analyse(m.run(), m.cfg ?? config);
      const hit = r.fails.some((x) => x.check === m.expect);
      ok(`${m.name} ⇒ ${m.expect}`, hit, hit ? "" : `not caught (got: ${[...new Set(r.fails.map((x) => x.check))].join(",") || "no failures"})`);
    } catch (e: any) {
      ok(`${m.name} ⇒ ${m.expect}`, false, `mutation harness error: ${e?.message}`);
    }
  }

  console.log(failures ? `\n❌ ${failures} check(s) failed` : "\n✅ route authorization guard passed");
  process.exit(failures ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
