/**
 * validate-schema-selects.ts
 *
 * Catches Supabase column drift against the live database, on both the read
 * side (`.select()` strings) and the write side (`.insert()`/`.upsert()`
 * payload keys).
 *
 * Why this exists: PostgREST select strings are plain strings, so TypeScript
 * cannot check them. A column that does not exist fails at query time with
 * PostgREST 42703, and three consecutive days found that bug in production
 * code:
 *   Day 237A — objection evidence selected calls.rep_id (it is user_id):
 *              hard 500 on every evidence write with a call_id
 *   Day 237B — accounts.ts selected calls.score / calls.rep_id: two of the
 *              three routes SWALLOWED the error via `data || []` and served
 *              a plausible empty state, reporting zero calls for an account
 *              that had three
 *   Day 238  — reps.ts selected intro_score/discovery_score/objection_score/
 *              close_score: both rep trend routes 500 for every rep
 *
 * The Day 237B case is the reason this is a validator and not a habit:
 * the failure mode is often silent, so it cannot be found by watching for
 * 500s. The only reliable check is to diff the select strings against the
 * real schema — which is what this does.
 *
 * Schema source: PostgREST's OpenAPI document at {SUPABASE_URL}/rest/v1/.
 * That lists every exposed table and view with its full column set, so it
 * stays correct for empty tables — unlike sampling a row and reading its
 * keys, which silently sees no columns when a table happens to be empty.
 *
 * Write payloads (Day 243). Day 242 found three write-side bugs the
 * select-only scan could not see, one of which — coach_assignments getting
 * source/meta — made POST /v1/coach/assign answer 400 for every request.
 * Writes fail loudly at insert time, but this codebase routinely wraps them
 * in warn-only or try/catch handlers, so they go unnoticed just as easily.
 * Supported: object literals, array-of-object form, and `.insert(payload)`
 * where payload is a const object literal bound exactly once in the file
 * (the shape the dead coach/assign route used). Aliases do NOT apply to
 * writes — a payload key must be a real column. Nested objects are not
 * descended into, since a jsonb column's contents are free-form.
 *
 * Limitations, reported rather than guessed at: dynamic select arguments
 * and unresolvable payload identifiers are counted as unverified, spreads
 * are noted while explicit keys are still checked, computed keys are
 * skipped, and tables absent from PostgREST are ignored. RPCs and raw SQL
 * are out of scope. It checks that keys EXIST, not that required columns
 * are PRESENT — Day 243 hit crm_activities.title/user_id NOT NULL
 * violations that only surfaced by running the insert.
 *
 * Usage: npm run validate:schema-selects
 * Exit code 1 if any select or write payload names a column its table does
 * not have.
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";

const SUPA_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "");
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";

const SCAN_DIRS = ["src", "scripts"];
const REPO_ROOT = path.resolve(__dirname, "..");

// PostgREST select keywords that are not columns.
const NON_COLUMN_TOKENS = new Set(["*", "count", "sum", "avg", "min", "max"]);

/**
 * Drift that already existed when this validator landed (Day 239).
 *
 * These are NOT false positives and NOT suppressed noise — every one was
 * confirmed against the live database, each returning PostgREST 42703. They
 * are baselined so the validator can gate NEW drift from today while the
 * backlog is worked down deliberately, because fixing them is not a rename:
 *   users.full_name        lives on `profiles`, so it needs a join, not a
 *                          column swap
 *   profiles.id            `profiles` is keyed by user_id and has no id
 *   companies.settings     the column is `settings_json`
 *   call_scores.score      / .total_score — the column is `overall`
 *   coach_assignments.source/.meta        those columns are on `assignments`
 *   contacts.name/.company/.role          contacts has first_name/last_name/title
 *   reps.full_name/.user_id               reps has name and is keyed by id
 *   crm_accounts.user_id, assignments.org_id, admin_config thresholds
 *                          — no equivalent column exists at all
 *
 * Each needs its consumers checked before patching (Day 237B showed these
 * often degrade silently rather than throwing, so the caller's handling
 * matters as much as the select). Entries are keyed file|table|column so
 * they survive line moves. Removing an entry once fixed is required — a
 * baseline listing drift that no longer exists fails as stale.
 *
 * Day 240 cleared all three src/server.ts entries (profiles.id,
 * profiles.display_name, users.full_name): profiles is keyed by user_id
 * with the name in full_name, and the `users` fallback was impossible
 * because that table has no name column — it now reads reps.name, the
 * name source the rest of the app already uses. 33 → 24 findings.
 *
 * Day 241 cleared all six src/routes/dashboard.ts entries: the same name
 * hydration on the live leaderboard, plus coach_assignments.source/meta
 * (provenance columns that table does not have — they belong to the
 * separate `assignments` table) and assignments.org_id (that table is
 * scoped by office_id/company_id and has no org column). 24 → 18.
 *
 * Day 242 cleared coach.ts (the same coach_assignments.source/meta pair)
 * and the call_scores family in calls.ts/crm.ts, where `score` and
 * `total_score` are both the column `overall`. 18 → 13.
 *
 * Day 243 added write-payload scanning (see the header) and fixed the four
 * scoring.ts crm_activities inserts it found. Read baseline unchanged at 13.
 *
 * Day 244 cleared the companies.settings family in admin.ts and
 * sparring.ts — the column is settings_json. GET /v1/admin/persona-config
 * answered a misleading 404, PATCH 500'd, and sparring silently fell back
 * to an empty buyer persona. 13 read -> 9. Note the PATCH also wrote
 * `settings` via .update(), which this validator still does not scan:
 * only .insert()/.upsert() payloads are checked.
 */
const KNOWN_DRIFT = new Set([
  "src/lib/scoring.ts|admin_config|low_score_threshold",
  "src/lib/scoring.ts|admin_config|critical_score_threshold",
  "src/routes/accounts.ts|contacts|name",
  "src/routes/accounts.ts|contacts|company",
  "src/routes/accounts.ts|contacts|role",
  "src/routes/accounts.ts|users|full_name",
  "src/routes/crm.ts|crm_accounts|user_id",
  "src/routes/crm.ts|reps|full_name",
  "src/routes/crm.ts|reps|user_id",
]);

const driftKey = (f: { file: string; table: string; column: string }) =>
  `${f.file}|${f.table}|${f.column}`;

/**
 * Write-side drift that already existed when payload scanning landed
 * (Day 243). Same contract as KNOWN_DRIFT: confirmed real, baselined only
 * so new drift can be gated, and a listed entry that stops reproducing
 * fails as stale. Keyed file|table|column|op so a read and a write bug on
 * the same column stay distinct.
 */
const KNOWN_WRITE_DRIFT = new Set<string>([
  // crm_accounts is (id, org_id, name, domain, created_at) — it has no
  // user_id and no owner column of any kind. These inserts surface the
  // error, so CRM account creation currently answers 500.
  //
  // Deferred rather than patched because every option is a semantic
  // decision, not a rename: dropping user_id records no owner at all,
  // and org_id is a different concept, not a substitute. The read side
  // of the same drift (crm.ts|crm_accounts|user_id) is likewise still
  // baselined, so the whole ownership model needs deciding at once.
  "src/routes/crm.ts|crm_accounts|user_id|insert",
]);

type Op = "select" | "insert" | "upsert";

type Finding = {
  file: string;
  line: number;
  table: string;
  column: string;
  snippet: string;
  op: Op;
};

type Skip = {
  file: string;
  line: number;
  table: string;
  reason: string;
};

const writeDriftKey = (f: { file: string; table: string; column: string; op: Op }) =>
  `${f.file}|${f.table}|${f.column}|${f.op}`;

const isWrite = (f: Finding) => f.op !== "select";
// Reads and writes carry separate baselines, so route each finding to its own.
const baselineHas = (f: Finding) =>
  isWrite(f) ? KNOWN_WRITE_DRIFT.has(writeDriftKey(f)) : KNOWN_DRIFT.has(driftKey(f));

// ── Schema ──────────────────────────────────────────────────────────────────

async function loadSchema(): Promise<Map<string, Set<string>>> {
  const res = await fetch(`${SUPA_URL}/rest/v1/`, {
    headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` },
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    throw new Error(`PostgREST schema fetch failed: ${res.status} ${res.statusText}`);
  }
  const doc: any = await res.json();
  const definitions = doc.definitions ?? doc.components?.schemas ?? {};

  const schema = new Map<string, Set<string>>();
  for (const [table, def] of Object.entries<any>(definitions)) {
    const props = def?.properties ?? {};
    schema.set(table, new Set(Object.keys(props)));
  }
  return schema;
}

// ── Source scanning ─────────────────────────────────────────────────────────

function walk(dir: string, out: string[] = []): string[] {
  if (!fs.existsSync(dir)) return out;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "node_modules" || entry.name === "dist" || entry.name === ".git") continue;
      walk(full, out);
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      out.push(full);
    }
  }
  return out;
}

/**
 * Blank out comments, preserving length so every offset and line number
 * downstream stays valid. Without this the scanner reads commented-out
 * queries as live code — reps.ts carries a commented `.from("calls")
 * .select("id,created_at,final_score,outcome")` example that is not, and
 * must not be reported as, a defect.
 */
function blankComments(src: string): string {
  const out = src.split("");
  let i = 0;
  let state: "code" | "line" | "block" | "single" | "double" | "template" = "code";

  while (i < src.length) {
    const ch = src[i];
    const next = src[i + 1];

    if (state === "code") {
      if (ch === "/" && next === "/") { state = "line"; out[i] = " "; out[i + 1] = " "; i += 2; continue; }
      if (ch === "/" && next === "*") { state = "block"; out[i] = " "; out[i + 1] = " "; i += 2; continue; }
      if (ch === "'") state = "single";
      else if (ch === '"') state = "double";
      else if (ch === "`") state = "template";
      i++;
      continue;
    }

    if (state === "line") {
      if (ch === "\n") { state = "code"; i++; continue; }
      out[i] = " ";
      i++;
      continue;
    }

    if (state === "block") {
      if (ch === "*" && next === "/") { state = "code"; out[i] = " "; out[i + 1] = " "; i += 2; continue; }
      if (ch !== "\n") out[i] = " ";
      i++;
      continue;
    }

    // Inside a string literal: honour escapes, leave contents untouched.
    if (ch === "\\") { i += 2; continue; }
    if (
      (state === "single" && ch === "'") ||
      (state === "double" && ch === '"') ||
      (state === "template" && ch === "`")
    ) {
      state = "code";
    }
    i++;
  }

  return out.join("");
}

/**
 * Read a string literal starting at `start` (which must be a quote char).
 * Returns the raw contents and the index just past the closing quote, or
 * null when the literal is not a simple one (unterminated).
 */
function readLiteral(src: string, start: number): { raw: string; end: number; quote: string } | null {
  const quote = src[start];
  if (quote !== '"' && quote !== "'" && quote !== "`") return null;
  let i = start + 1;
  let raw = "";
  while (i < src.length) {
    const ch = src[i];
    if (ch === "\\") { raw += src[i + 1] ?? ""; i += 2; continue; }
    if (ch === quote) return { raw, end: i + 1, quote };
    raw += ch;
    i++;
  }
  return null;
}

/**
 * Read a balanced {...} starting at `start`, which must be an opening brace.
 * String contents are honoured so a brace inside a literal cannot end it.
 */
function readObjectLiteral(src: string, start: number): { body: string; end: number } | null {
  if (src[start] !== "{") return null;
  let depth = 0;
  let i = start;
  let quote: string | null = null;

  while (i < src.length) {
    const ch = src[i];
    if (quote) {
      if (ch === "\\") { i += 2; continue; }
      if (ch === quote) quote = null;
      i++;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === "`") { quote = ch; i++; continue; }
    if (ch === "{") depth++;
    if (ch === "}") {
      depth--;
      if (depth === 0) return { body: src.slice(start + 1, i), end: i + 1 };
    }
    i++;
  }
  return null;
}

/**
 * Top-level keys of an object literal body. Nested objects and arrays are
 * skipped entirely — for a jsonb column like `meta`, only `meta` itself is
 * a real column and its contents are free-form.
 */
function topLevelKeys(body: string): { keys: string[]; hasSpread: boolean; hasComputed: boolean } {
  const keys: string[] = [];
  let hasSpread = false;
  let hasComputed = false;

  let depth = 0;
  let quote: string | null = null;
  let atKeyPosition = true;
  let token = "";

  const flush = () => {
    const t = token.trim();
    token = "";
    if (!t) return;
    // Shorthand property (`status,`) is a key too.
    if (/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(t)) keys.push(t);
  };

  for (let i = 0; i < body.length; i++) {
    const ch = body[i];

    if (quote) {
      if (ch === "\\") { i++; continue; }
      if (ch === quote) quote = null;
      else if (depth === 0 && atKeyPosition) token += ch;
      continue;
    }

    if (ch === '"' || ch === "'" || ch === "`") {
      // A quoted key at depth 0 — collect its contents, ignore its quotes.
      if (depth === 0 && atKeyPosition) token = "";
      quote = ch;
      continue;
    }

    if (ch === "{" || ch === "[" || ch === "(") { depth++; continue; }
    if (ch === "}" || ch === "]" || ch === ")") { depth--; continue; }

    if (depth !== 0) continue;

    if (ch === ":") {
      // Everything before the colon at depth 0 was the key.
      const t = token.trim();
      token = "";
      atKeyPosition = false;
      if (!t) continue;
      if (/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(t)) keys.push(t);
      continue;
    }

    if (ch === ",") {
      if (atKeyPosition) flush(); // shorthand
      token = "";
      atKeyPosition = true;
      continue;
    }

    if (atKeyPosition) {
      if (ch === "." && body.slice(i, i + 3) === "...") { hasSpread = true; atKeyPosition = false; token = ""; i += 2; continue; }
      if (ch === "[") { hasComputed = true; atKeyPosition = false; token = ""; continue; }
      token += ch;
    }
  }
  if (atKeyPosition) flush();

  return { keys, hasSpread, hasComputed };
}

/**
 * Resolve `.insert(payload)` where payload is a const object literal declared
 * in the same file. Day 242's dead coach/assign route used exactly this shape
 * (`const assignmentPayload = {...}` then `.insert(assignmentPayload)`), so
 * without this the scanner would miss the very bugs that motivated it.
 */
function resolveNamedPayload(src: string, name: string, callIndex: number): string | null {
  // Scope-blind resolution is dangerous: crm.ts has both `const payload = {…}`
  // and `for (const payload of activityAttempts)`, and matching the first
  // blindly attributed one payload's keys to three unrelated tables. Resolve
  // only when the name is bound exactly once in the file, so there is no
  // other binding it could refer to.
  const bindingRe = new RegExp(`\\b(?:const|let|var)\\s+${name}\\b`, "g");
  const bindings = [...src.matchAll(bindingRe)];
  if (bindings.length !== 1) return null;

  const decl = bindings[0];
  if (decl.index === undefined || decl.index > callIndex) return null;

  // That single binding must actually be an object literal assignment.
  const assignRe = new RegExp(`^\\s*(?::[^=]+)?=\\s*\\{`);
  const after = src.slice(decl.index + decl[0].length);
  if (!assignRe.test(after)) return null;

  const brace = src.indexOf("{", decl.index + decl[0].length);
  const obj = readObjectLiteral(src, brace);
  return obj ? obj.body : null;
}

/** Split a select list on top-level commas only (parens = embeds). */
function splitColumns(list: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";
  for (const ch of list) {
    if (ch === "(") depth++;
    if (ch === ")") depth--;
    if (ch === "," && depth === 0) { parts.push(current); current = ""; continue; }
    current += ch;
  }
  parts.push(current);
  return parts.map((p) => p.trim()).filter(Boolean);
}

/**
 * Resolve one select entry to the base column it actually reads, or null
 * when it is not a plain column reference.
 *
 * Handles the forms this codebase uses:
 *   score:score_overall   alias      -> score_overall   (Day 237B fix shape)
 *   rubric->stages        JSON path  -> rubric
 *   created_at::text      cast       -> created_at
 *   contacts(id,name)     embed      -> null (skipped)
 *   *, count              keyword    -> null (skipped)
 */
function resolveColumn(entry: string): string | null {
  let token = entry.trim();
  if (!token) return null;

  // Alias: take the right-hand side, which is the real column.
  if (token.includes(":")) {
    const idx = token.indexOf(":");
    token = token.slice(idx + 1).trim();
  }

  // Embedded relationship or aggregate — not a column of this table.
  if (token.includes("(")) return null;

  // JSON path and casts read a real base column.
  token = token.split("->")[0].trim();
  token = token.split("::")[0].trim();

  // Modifiers such as !inner / !left on embeds.
  token = token.split("!")[0].trim();

  if (!token || NON_COLUMN_TOKENS.has(token)) return null;
  // Anything left that is not a bare identifier is out of scope for this
  // deliberately small parser — reported as skipped, never failed.
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(token)) return null;

  return token;
}

function lineOf(src: string, index: number): number {
  return src.slice(0, index).split("\n").length;
}

function scanFile(
  file: string,
  schema: Map<string, Set<string>>,
  findings: Finding[],
  skips: Skip[]
): number {
  const src = blankComments(fs.readFileSync(file, "utf8"));
  const rel = path.relative(REPO_ROOT, file);
  let checked = 0;

  const fromRe = /\.from\(\s*(['"`])([A-Za-z0-9_]+)\1\s*\)/g;
  let m: RegExpExecArray | null;

  while ((m = fromRe.exec(src))) {
    const table = m[2];
    const afterFrom = m.index + m[0].length;
    const line = lineOf(src, m.index);

    // Look ahead for this chain's .select(, stopping at the next .from( so
    // two adjacent queries can never be attributed to each other.
    const nextFrom = src.indexOf(".from(", afterFrom);
    const limit = nextFrom === -1 ? src.length : nextFrom;
    const window = src.slice(afterFrom, limit);

    // ── Write payloads (Day 243) ─────────────────────────────────────────
    // Runs before the select branch below, which continues when a chain has
    // no .select() — an insert-only chain must still be checked.
    for (const op of ["insert", "upsert"] as const) {
      const opIdx = window.indexOf(`.${op}(`);
      if (opIdx === -1) continue;

      let j = afterFrom + opIdx + `.${op}(`.length;
      while (j < src.length && /\s/.test(src[j])) j++;
      if (src[j] === "[") { // .insert([{...}]) — array form
        j++;
        while (j < src.length && /\s/.test(src[j])) j++;
      }
      const opLine = lineOf(src, j);

      let body: string | null = null;
      if (src[j] === "{") {
        body = readObjectLiteral(src, j)?.body ?? null;
      } else {
        // .insert(identifier) — resolve a const object literal in this file.
        const ident = /^[A-Za-z_$][A-Za-z0-9_$]*/.exec(src.slice(j, j + 80))?.[0];
        if (ident) body = resolveNamedPayload(src, ident, j);
        if (!body) {
          skips.push({ file: rel, line: opLine, table, reason: `${op} payload is not a resolvable object literal (dynamic)` });
          continue;
        }
      }
      if (body === null) {
        skips.push({ file: rel, line: opLine, table, reason: `${op} payload could not be parsed` });
        continue;
      }

      const writeKnown = schema.get(table);
      if (!writeKnown) {
        skips.push({ file: rel, line: opLine, table, reason: "table not exposed in PostgREST schema (view, RPC or fixture)" });
        continue;
      }

      const { keys, hasSpread, hasComputed } = topLevelKeys(body);
      if (hasSpread) {
        skips.push({ file: rel, line: opLine, table, reason: `${op} payload contains a spread (explicit keys still checked)` });
      }
      if (hasComputed) {
        skips.push({ file: rel, line: opLine, table, reason: `${op} payload contains a computed key (skipped)` });
      }

      checked++;
      const writeSnippet = keys.join(", ").slice(0, 70);
      for (const key of keys) {
        // No aliasing on writes — a payload key must be a real column.
        if (!writeKnown.has(key)) {
          findings.push({ file: rel, line: opLine, table, column: key, snippet: writeSnippet, op });
        }
      }
    }

    const selIdx = window.indexOf(".select(");
    if (selIdx === -1) continue;

    const argStart = afterFrom + selIdx + ".select(".length;
    // Skip whitespace before the argument.
    let i = argStart;
    while (i < src.length && /\s/.test(src[i])) i++;

    const selectLine = lineOf(src, i);

    if (src[i] === ")") continue; // .select() with no args — returns all columns

    const literal = readLiteral(src, i);
    if (!literal) {
      skips.push({ file: rel, line: selectLine, table, reason: "select argument is not a string literal (dynamic)" });
      continue;
    }
    if (literal.quote === "`" && literal.raw.includes("${")) {
      skips.push({ file: rel, line: selectLine, table, reason: "template literal with interpolation (dynamic)" });
      continue;
    }

    const known = schema.get(table);
    if (!known) {
      skips.push({ file: rel, line: selectLine, table, reason: "table not exposed in PostgREST schema (view, RPC or fixture)" });
      continue;
    }

    checked++;
    const snippet = literal.raw.replace(/\s+/g, " ").trim().slice(0, 70);

    for (const entry of splitColumns(literal.raw)) {
      const column = resolveColumn(entry);
      if (column === null) continue; // embed / keyword / unparseable — never a failure
      if (!known.has(column)) {
        findings.push({ file: rel, line: selectLine, table, column, snippet, op: "select" });
      }
    }
  }

  return checked;
}

// ── Stale .js sibling report (Day 237B follow-up, informational) ─────────────

function staleJsSiblings(): string[] {
  const stale: string[] = [];
  const walkJs = (dir: string) => {
    if (!fs.existsSync(dir)) return;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === "node_modules" || entry.name === "dist") continue;
        walkJs(full);
      } else if (entry.isFile() && entry.name.endsWith(".js")) {
        if (fs.existsSync(full.replace(/\.js$/, ".ts"))) {
          stale.push(path.relative(REPO_ROOT, full));
        }
      }
    }
  };
  walkJs(path.join(REPO_ROOT, "src"));
  return stale;
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nSupabase select schema validation\n");

  if (!SUPA_URL || !SERVICE_KEY) {
    console.error("  SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.");
    process.exit(1);
  }

  const schema = await loadSchema();
  console.log(`  Schema: ${schema.size} tables/views from PostgREST\n`);

  const files = SCAN_DIRS.flatMap((d) => walk(path.join(REPO_ROOT, d)));
  const findings: Finding[] = [];
  const skips: Skip[] = [];
  let checked = 0;
  for (const file of files) checked += scanFile(file, schema, findings, skips);

  console.log(`  Scanned ${files.length} TypeScript files, ${checked} literal select(s) + write payload(s).`);

  // Skips are expected and not failures — printed so the blind spots stay visible.
  if (skips.length) {
    const byReason = new Map<string, number>();
    for (const s of skips) byReason.set(s.reason, (byReason.get(s.reason) ?? 0) + 1);
    console.log(`  Not verified (${skips.length}):`);
    for (const [reason, n] of [...byReason].sort((a, b) => b[1] - a[1])) {
      console.log(`    ${String(n).padStart(3)}  ${reason}`);
    }
  }

  const stale = staleJsSiblings();
  if (stale.length) {
    console.log(`\n  Note: ${stale.length} compiled .js sibling(s) under src/ shadow a .ts file.`);
    console.log("        Not scanned (tsx resolves .ts first, proven Day 237B) but they");
    console.log("        carry stale copies of these selects. Candidates for deletion.");
  }

  // Split findings against the baselines: known debt is reported but does
  // not fail; anything new does. Reads and writes are gated separately.
  const newFindings = findings.filter((f) => !baselineHas(f));
  const seenReadKeys = new Set(findings.filter((f) => !isWrite(f)).map(driftKey));
  const seenWriteKeys = new Set(findings.filter(isWrite).map(writeDriftKey));
  const staleBaseline = [
    ...[...KNOWN_DRIFT].filter((k) => !seenReadKeys.has(k)),
    ...[...KNOWN_WRITE_DRIFT].filter((k) => !seenWriteKeys.has(k)),
  ];

  const knownCount = findings.length - newFindings.length;
  if (knownCount) {
    console.log(`\n  Known pre-existing drift (baselined, still real bugs): ${knownCount}`);
    const byFile = new Map<string, number>();
    for (const f of findings) {
      if (baselineHas(f)) byFile.set(f.file, (byFile.get(f.file) ?? 0) + 1);
    }
    for (const [file, n] of [...byFile].sort((a, b) => b[1] - a[1])) {
      console.log(`    ${String(n).padStart(3)}  ${file}`);
    }
  }

  // A baseline entry with no matching finding means the drift was fixed (or
  // the code moved). Failing here stops the list quietly rotting into a
  // permanent suppression.
  if (staleBaseline.length) {
    console.log(`\n  ✗ ${staleBaseline.length} stale baseline entr(y/ies) — drift no longer present, remove from KNOWN_DRIFT/KNOWN_WRITE_DRIFT:\n`);
    for (const k of staleBaseline) console.log(`    ${k}`);
    console.log(`\n  Schema validation FAILED\n`);
    process.exit(1);
  }

  // Truncate as well as pad — padEnd alone lets a long value overflow and
  // shunt every later column out of alignment.
  const w = (s: string, n: number) => (s.length > n - 2 ? s.slice(0, n - 3) + "…" : s).padEnd(n);

  function reportSection(title: string, rows: Finding[], lastCol: string) {
    if (!rows.length) return;
    console.log(`\n  ✗ ${rows.length} ${title}:\n`);
    console.log(`    ${w("FILE:LINE", 38)}${w("TABLE", 22)}${w("INVALID COLUMN", 26)}${lastCol}`);
    console.log(`    ${"-".repeat(116)}`);
    for (const f of rows) {
      console.log(`    ${w(`${f.file}:${f.line}`, 38)}${w(f.table, 22)}${w(f.column, 26)}${f.snippet}`);
    }
  }

  if (newFindings.length) {
    reportSection("invalid select(s)", newFindings.filter((f) => !isWrite(f)), "SELECT");
    reportSection("invalid write payload key(s)", newFindings.filter(isWrite), "PAYLOAD KEYS");
    console.log(`\n  Schema validation FAILED\n`);
    process.exit(1);
  }

  console.log(
    knownCount
      ? `\n  ✓ no NEW drift (${knownCount} baselined bug(s) outstanding — see KNOWN_DRIFT/KNOWN_WRITE_DRIFT)`
      : `\n  ✓ every literal select and write payload resolves against the live schema`
  );
  console.log("  Schema validation PASSED\n");
}

main().catch((e) => {
  console.error("\n  Validator crashed:", e?.message || e, "\n");
  process.exit(1);
});
