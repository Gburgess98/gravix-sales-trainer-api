import { getAdminConfig } from "./adminConfig";

let cache: {
  value: {
    streakThreshold: number;
    xpMultiplier: number;
    comebackBonus: number;
  };
  expiresAt: number;
} | null = null;

export async function getScoringConfig() {
  const now = Date.now();

  if (cache && cache.expiresAt > now) return cache.value;

  const cfg = await getAdminConfig();

  const value = {
    streakThreshold: cfg.streak_threshold,
    xpMultiplier: Number(cfg.xp_multiplier),
    comebackBonus: cfg.comeback_bonus,
  };

  cache = { value, expiresAt: now + 30_000 }; // 30 sec cache
  return value;
}