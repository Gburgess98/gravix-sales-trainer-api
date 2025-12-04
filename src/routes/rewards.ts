// api/src/routes/rewards.ts
import { Router } from "express";
import { createClient } from "@supabase/supabase-js";

export function rewardsRoutes() {
  const r = Router();

  // GET earned cosmetics for a user (badges, titles, selected)
  r.get("/rewards/:userId", async (req, res) => {
    const { userId } = req.params;
    try {
      const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_ANON_KEY!);
      const { data: badges } = await supa
        .from("user_badges")
        .select("badge_id, earned_at, badges(label, icon)")
        .eq("user_id", userId);

      const { data: titles } = await supa
        .from("user_titles")
        .select("title_id, unlocked_at, titles(label, icon, bg_class)")
        .eq("user_id", userId);

      const { data: selected } = await supa
        .from("user_selected_title")
        .select("title_id")
        .eq("user_id", userId)
        .maybeSingle();

      res.set("Cache-Control", "public, max-age=15");
      res.json({
        ok: true,
        badges: (badges || []).map((x: any) => ({
          id: x.badge_id,
          label: x.badges?.label,
          icon: x.badges?.icon,
          earned_at: x.earned_at
        })),
        titles: (titles || []).map((x: any) => ({
          id: x.title_id,
          label: x.titles?.label,
          icon: x.titles?.icon,
          bg_class: x.titles?.bg_class,
          unlocked_at: x.unlocked_at
        })),
        selectedTitleId: selected?.title_id ?? null
      });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "rewards_fetch_failed" });
    }
  });

  // POST select title (equip)
  r.post("/rewards/:userId/select-title", expressJson(), async (req, res) => {
    const { userId } = req.params;
    const { titleId } = req.body || {};
    if (!titleId) return res.status(400).json({ ok: false, error: "titleId_required" });

    try {
      const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);
      // guard: must be unlocked
      const unlocked = await supa.from("user_titles").select("title_id").eq("user_id", userId).eq("title_id", titleId).maybeSingle();
      if (!unlocked.data) return res.status(403).json({ ok: false, error: "title_not_unlocked" });

      const upsert = await supa.from("user_selected_title").upsert({ user_id: userId, title_id: titleId, selected_at: new Date().toISOString() }, { onConflict: "user_id" }).select("title_id").maybeSingle();
      if (upsert.error) throw upsert.error;

      res.json({ ok: true, selectedTitleId: upsert.data?.title_id || titleId });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "select_failed" });
    }
  });

  // GET active bounties (basic)
  r.get("/rewards/bounties/active", async (_req, res) => {
    try {
      const now = new Date().toISOString();
      const supa = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_ANON_KEY!);
      const { data, error } = await supa
        .from("bounties")
        .select("id, name, description, prize_money, currency, starts_at, ends_at, title_id, titles(label, icon)")
        .or(`starts_at.is.null,starts_at.lte.${now}`)
        .or(`ends_at.is.null,ends_at.gte.${now}`);
      if (error) throw error;
      res.set("Cache-Control", "public, max-age=15");
      res.json({
        ok: true,
        items: (data || []).map((b: any) => ({
          id: b.id,
          name: b.name,
          description: b.description,
          prize: b.prize_money,
          currency: b.currency,
          window: { starts_at: b.starts_at, ends_at: b.ends_at },
          title: { id: b.title_id, label: b.titles?.label, icon: b.titles?.icon }
        }))
      });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || "bounties_fetch_failed" });
    }
  });

  return r;
}

function expressJson() {
  const express = require("express");
  return express.json();
}