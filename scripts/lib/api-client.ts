// Lightweight HTTP + assertion helpers for API regression tests.
// No test framework — just fetch + checks.

export type Check = { label: string; passed: boolean };

export type TestResult = {
  suite: string;
  name: string;
  passed: boolean;
  durationMs: number;
  checks: Check[];
  skipped?: boolean;
  skipReason?: string;
  error?: string;
};

export type ReqOpts = {
  userId?: string;
  orgId?: string;
  method?: string;
  body?: unknown;
};

export async function apiReq(
  base: string,
  path: string,
  opts: ReqOpts = {}
): Promise<{ status: number; body: unknown }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.userId) headers["x-user-id"] = opts.userId;
  if (opts.orgId) headers["x-org-id"] = opts.orgId;

  const res = await fetch(`${base}${path}`, {
    method: opts.method ?? (opts.body ? "POST" : "GET"),
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
    signal: AbortSignal.timeout(10_000),
  });

  let body: unknown = null;
  try {
    body = await res.json();
  } catch { /* empty body */ }

  return { status: res.status, body };
}

// --- assertion helpers ---

export function c(label: string, passed: boolean): Check {
  return { label, passed };
}

export function statusIs(actual: number, expected: number): Check {
  return c(`status ${expected}`, actual === expected);
}

export function statusOk(actual: number): Check {
  return c("status 2xx", actual >= 200 && actual < 300);
}

export function statusNot5xx(actual: number): Check {
  return c("not 5xx", actual < 500);
}

export function statusIsAuth(actual: number): Check {
  return c("status 401 or 403", actual === 401 || actual === 403);
}

export function hasField(body: unknown, field: string): Check {
  const has = !!body && typeof body === "object" && field in (body as object);
  return c(`has "${field}"`, has);
}

export function fieldIsArray(body: unknown, field: string): Check {
  const arr = (body as Record<string, unknown>)?.[field];
  return c(`"${field}" is array`, Array.isArray(arr));
}

export function isOk(body: unknown): Check {
  return c('body.ok === true', (body as Record<string, unknown>)?.ok === true);
}
