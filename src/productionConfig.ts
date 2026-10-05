// Go Live Day 27 (G5) — production boot refusal for the legacy admin flag.
//
// Contract (explicit, documented, tested by scripts/validate-admin-hardening-day-27.ts):
//   - Applies only when NODE_ENV is exactly "production" (the same exact match
//     identityHeadersTrusted() and getUserId() use — "Production" is NOT production).
//   - ALLOW_ADMIN_ENDPOINTS must be ABSENT or EMPTY/whitespace-only. Any other value
//     — "true", "TRUE", "false", "0", "no", "off" — refuses boot. A false-like string
//     is refused too: a non-empty value is a mis-set deploy variable that one
//     typo/trim change from "true", and requireAdmin() only ever opens on the
//     exact string "true".
//   - The refusal throws BEFORE the server listens (server.ts runs it ahead of the
//     boot block). The message names the variable, never its value.
//   - Outside production the flag keeps its dev behaviour (requireAdmin still
//     needs exact "true" AND a verified SuperAdmin).

export type EnvLike = Record<string, string | undefined>;

export function assertProductionAdminFlagUnset(env: EnvLike = process.env): void {
  if (env.NODE_ENV !== "production") return;
  const raw = env.ALLOW_ADMIN_ENDPOINTS;
  if (raw === undefined || String(raw).trim() === "") return;
  throw new Error(
    "FATAL: ALLOW_ADMIN_ENDPOINTS is set in production — refusing to boot. " +
      "Unset the variable (empty/absent is the only accepted production value)."
  );
}
