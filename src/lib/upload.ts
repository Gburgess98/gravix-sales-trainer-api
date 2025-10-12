// src/lib/upload.ts
import { createClient } from "@supabase/supabase-js";

// API base + dev test user header (same as you already use elsewhere)
const API = process.env.NEXT_PUBLIC_API_URL!;
const TEST_UID = process.env.NEXT_PUBLIC_TEST_UID!;

// Supabase client (browser) – used only to fetch a user/anon token for the Storage upload request
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const SUPABASE_ANON = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
const supa = createClient(SUPABASE_URL, SUPABASE_ANON);

/** Internal helper: POST the file to the Supabase signed URL with a progress callback. */
async function postToSignedUrlWithProgress(
  signedUrl: string,
  file: File,
  authToken: string,
  onProgress?: (pct: number) => void
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", signedUrl);
    xhr.setRequestHeader("Authorization", `Bearer ${authToken}`);
    xhr.setRequestHeader("x-upsert", "true");

    if (xhr.upload && onProgress) {
      xhr.upload.onprogress = (ev) => {
        if (ev.lengthComputable) {
          const pct = Math.round((ev.loaded / ev.total) * 100);
          onProgress(pct);
        }
      };
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) return resolve();
      reject(new Error(`storage_upload_failed ${xhr.status}: ${xhr.responseText || ""}`));
    };
    xhr.onerror = () => reject(new Error("storage_upload_network_error"));

    const fd = new FormData();
    fd.append("file", file, file.name);
    xhr.send(fd);
  });
}

/**
 * Upload a call using the production-safe "signed URL → storage → finalize" pattern.
 * Keeps the same return shape your page expects ({ ok, callId, jobId, ... }).
 *
 * Pass an optional onProgress callback to receive 0..100 during the Storage upload.
 */
export async function uploadCall(
  file: File,
  opts?: { onProgress?: (pct: number) => void }
) {
  // 1) Ask the API for a signed upload URL (x-user-id header required for now)
  const initRes = await fetch(`${API}/v1/upload/signed`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-user-id": TEST_UID,
    },
    body: JSON.stringify({
      filename: file.name,
      mime: file.type || "application/octet-stream",
      size: file.size,
    }),
  });

  const initJson = await initRes.json().catch(() => ({}));
  if (!initRes.ok || initJson?.ok === false) {
    throw new Error(initJson?.error || `signed_init_failed (${initRes.status})`);
  }
  const { url: signedUrl, path } = initJson as {
    ok: true;
    url: string;
    path: string;
    id: string;
    kind: "audio" | "json";
  };

  // 2) Upload the file to Supabase Storage using the signed URL (must include Authorization)
  //    Use the user's session token if logged in, otherwise fall back to anon (works for private buckets with signed URLs).
  const { data } = await supa.auth.getSession();
  const token = data.session?.access_token || SUPABASE_ANON;

  // 🔄 Use XHR so we can surface progress to the UI
  await postToSignedUrlWithProgress(signedUrl, file, token, opts?.onProgress);
  
  // inside uploadCall() just before finalize
try {
  await postToSignedUrlWithProgress(signedUrl, file, token, opts?.onProgress);
} catch (e) {
  // one opportunistic retry for transient network hiccups
  await postToSignedUrlWithProgress(signedUrl, file, token, opts?.onProgress);
}
  // 3) Finalize with the API (creates DB row + transcribe job → score job)
  const finRes = await fetch(`${API}/v1/upload/finalize`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-user-id": TEST_UID,
    },
    body: JSON.stringify({
      path,
      filename: file.name,
      mime: file.type || "application/octet-stream",
      size: file.size,
    }),
  });

  const finJson = await finRes.json().catch(() => ({}));
  if (!finRes.ok || finJson?.ok === false) {
    throw new Error(finJson?.error || `finalize_failed (${finRes.status})`);
  }

  // Keep the shape your page.tsx uses
  return {
    ...finJson,        // { ok, callId, jobId }
    path,              // target path (your UI shows this as "Target")
    storagePath: path, // alias for backward-compat with your UI
  };
}