// src/lib/http.ts
type RetryOpts = {
  retries?: number;
  baseMs?: number;
  maxMs?: number;
  jitter?: boolean;
  retryOn?: (res: Response | null, err: any) => boolean;
};

export async function withRetry<T>(fn: () => Promise<T>, opts: RetryOpts = {}): Promise<T> {
  const {
    retries = 3,
    baseMs = 250,
    maxMs = 2000,
    jitter = true,
    retryOn = (res, err) => {
      if (err) return true;
      if (!res) return true;
      // retry on 429, 5xx
      return res.status === 429 || (res.status >= 500 && res.status < 600);
    },
  } = opts;

  let attempt = 0;
  let lastErr: any = null;

  while (attempt <= retries) {
    try {
      return await fn();
    } catch (err: any) {
      lastErr = err;
    }

    const delayBase = Math.min(maxMs, baseMs * Math.pow(2, attempt));
    const delay = jitter ? Math.round(delayBase * (0.75 + Math.random() * 0.5)) : delayBase;

    await new Promise((r) => setTimeout(r, delay));
    attempt++;
  }
  throw lastErr ?? new Error("withRetry: failed");
}

export async function fetchJson<T = any>(url: string, init: RequestInit = {}, retry = true): Promise<T> {
  const runner = async () => {
    const r = await fetch(url, init);
    if (!r.ok) {
      const body = await r.text().catch(() => "");
      const err = new Error(`HTTP ${r.status} on ${url}: ${body || r.statusText}`);
      (err as any).status = r.status;
      throw err;
    }
    return (await r.json()) as T;
  };
  return retry ? withRetry(runner) : runner();
}