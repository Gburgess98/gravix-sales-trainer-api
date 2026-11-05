import { readFileSync } from "node:fs";
import { join } from "node:path";

/** The application's own UUID predicate, extracted from the source the API runs (src/routes/crm.ts `UUID_RE`). Never a copy. */
const RE_SRC = /const\s+UUID_RE\s*=\s*\n?\s*\/(\^\[0-9a-f\]\{8\}[^/]+\$)\/i\s*;/;
export function extractAppUuidPattern(file = "src/routes/crm.ts", root = join(__dirname, "..")): string | null {
  return RE_SRC.exec(readFileSync(join(root, file), "utf8"))?.[1] ?? null;
}
export function appUuidPredicate(): RegExp {
  const src = extractAppUuidPattern();
  if (!src) throw new Error("could not extract the application UUID predicate from src/routes/crm.ts");
  return new RegExp(src, "i");
}
