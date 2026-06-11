export async function withRetry(fn, opts = {}) {
    const { retries = 3, baseMs = 250, maxMs = 2000, jitter = true, retryOn = (res, err) => {
        if (err)
            return true;
        if (!res)
            return true;
        // retry on 429, 5xx
        return res.status === 429 || (res.status >= 500 && res.status < 600);
    }, } = opts;
    let attempt = 0;
    let lastErr = null;
    while (attempt <= retries) {
        try {
            return await fn();
        }
        catch (err) {
            lastErr = err;
        }
        const delayBase = Math.min(maxMs, baseMs * Math.pow(2, attempt));
        const delay = jitter ? Math.round(delayBase * (0.75 + Math.random() * 0.5)) : delayBase;
        await new Promise((r) => setTimeout(r, delay));
        attempt++;
    }
    throw lastErr ?? new Error("withRetry: failed");
}
export async function fetchJson(url, init = {}, retry = true) {
    const runner = async () => {
        const r = await fetch(url, init);
        if (!r.ok) {
            const body = await r.text().catch(() => "");
            const err = new Error(`HTTP ${r.status} on ${url}: ${body || r.statusText}`);
            err.status = r.status;
            throw err;
        }
        return (await r.json());
    };
    return retry ? withRetry(runner) : runner();
}
