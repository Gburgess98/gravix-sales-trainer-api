// src/lib/retry.ts
type RetryOpts = {
  attempts?: number;    // total attempts incl. first (default 3)
  baseMs?: number;      // base backoff (default 300)
  maxMs?: number;       // max backoff (default 3_000)
  retryOn?: (res: Response | null, err: unknown, attempt: number) => boolean;
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function fetchJsonWithRetry<T = any>(
  url: string,
  init?: RequestInit,
  opts: RetryOpts = {}
): Promise<T> {
  const attempts = opts.attempts ?? 3;
  const baseMs = opts.baseMs ?? 300;
  const maxMs = opts.maxMs ?? 3000;

  let lastErr: unknown = null;

  for (let i = 1; i <= attempts; i++) {
    let res: Response | null = null;
    try {
      res = await fetch(url, init);
      const shouldRetry =
        opts.retryOn
          ? opts.retryOn(res, null, i)
          : (!res.ok && (res.status >= 500 || res.status === 429));

      if (shouldRetry && i < attempts) {
        const delay = Math.min(maxMs, Math.round(baseMs * Math.pow(2, i - 1) * (0.5 + Math.random())));
        await sleep(delay);
        continue;
      }

      // parse (even for non-2xx so caller can surface message)
      const json = await res.json().catch(() => ({}));
      if (!res.ok || json?.ok === false) {
        const msg = json?.error || `HTTP ${res.status}`;
        throw new Error(msg);
      }
      return json as T;
    } catch (err) {
      lastErr = err;
      const shouldRetry =
        opts.retryOn
          ? opts.retryOn(res, err, i)
          : (res === null && i < attempts); // network error/timeouts

      if (shouldRetry && i < attempts) {
        const delay = Math.min(maxMs, Math.round(baseMs * Math.pow(2, i - 1) * (0.5 + Math.random())));
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }
  // fallback (should never reach)
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}