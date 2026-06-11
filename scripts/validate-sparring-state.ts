/**
 * validate-sparring-state.ts — Tier 2A Day 101
 * Unit assertions for the pure conversation state manager.
 * Usage: npx tsx scripts/validate-sparring-state.ts
 */

import {
  createInitialSparringState,
  updateSparringState,
  inferStageFromText,
  inferObjectionType,
  summariseStateForPrompt,
  coerceState,
  normaliseDifficulty,
} from "../src/sparring/state";

let failures = 0;
function check(label: string, ok: boolean) {
  console.log(`${ok ? "OK  " : "FAIL"}  ${label}`);
  if (!ok) failures += 1;
}

// ── Initial state ──
const init = createInitialSparringState({ personaId: "price_sensitive", difficulty: "normal" });
check("initial stage is opening", init.stage === "opening");
check("legacy 'normal' maps to standard", init.difficulty === "standard");
check("price persona starts sceptical", init.buyerMood === "sceptical");
check("standard base pressure 35", init.pressureLevel === 35);
check("nightmare base pressure 70", createInitialSparringState({ difficulty: "nightmare" }).pressureLevel === 70);
check("normaliseDifficulty defaults standard", normaliseDifficulty("weird") === "standard");

// ── Determinism ──
const a = updateSparringState(init, { repText: "Tell me about your current process", turnScore: 60 });
const b = updateSparringState(init, { repText: "Tell me about your current process", turnScore: 60 });
check("updates are deterministic", JSON.stringify(a) === JSON.stringify(b));

// ── Stage transitions ──
check("discovery question advances stage", a.stage === "discovery");
const pitched = updateSparringState(a, { repText: "Our platform helps you cut admin time in half", turnScore: 70 });
check("pitch language advances stage", pitched.stage === "pitch");
const backwards = updateSparringState(pitched, { repText: "Tell me about your team", turnScore: 60 });
check("stages never regress", backwards.stage === "pitch");
check("inferStageFromText close signals", inferStageFromText("shall we sign the contract", "pitch") === "close");
check("ended stays ended", inferStageFromText("hello", "ended") === "ended");

// ── Pressure ramping by difficulty ──
const weakStd = updateSparringState(createInitialSparringState({ difficulty: "standard" }), { repText: "um maybe", turnScore: 30 });
const weakNightmare = updateSparringState(createInitialSparringState({ difficulty: "nightmare" }), { repText: "um maybe", turnScore: 30 });
check("weak turn raises pressure", weakStd.pressureLevel > 35);
check("nightmare ramps harder than standard", (weakNightmare.pressureLevel - 70) > (weakStd.pressureLevel - 35));
const strong = updateSparringState(weakStd, { repText: "Here is exactly how we save you 10 hours a week, with proof", turnScore: 85 });
check("strong turn relieves pressure", strong.pressureLevel < weakStd.pressureLevel);

// ── Objection lifecycle ──
const objState = updateSparringState(init, { repText: "anything", turnScore: 50, newObjectionText: "This is too expensive for us" });
check("new objection activates", objState.objectionState.active === true);
check("objection type inferred (price)", objState.objectionState.type === "price");
const resolved = updateSparringState(objState, { repText: "Compared to the 10 hours saved weekly, it pays for itself in a month", turnScore: 80 });
check("strong answer resolves objection", resolved.objectionState.resolved === true && !resolved.objectionState.active);
check("inferObjectionType competitor", inferObjectionType("we already use another provider") === "competitor");
check("inferObjectionType null on neutral text", inferObjectionType("good morning") === null);

// ── Buyer move + mood bounds ──
const frustrated = updateSparringState(init, { repText: "uh", turnScore: 20, emotional: { anger: 80, boredom: 20, trust: 25 } });
check("high anger → frustrated mood", frustrated.buyerMood === "frustrated");
const bored = updateSparringState(init, { repText: "ok", turnScore: 40, emotional: { anger: 10, boredom: 80, trust: 40 } });
check("high boredom → close_window move", bored.nextBuyerMove === "close_window");
check("active objection → push_back move", objState.nextBuyerMove === "push_back");

// ── Ended + clamps + prompt ──
const ended = updateSparringState(init, { repText: "bye", turnScore: 50, endedThisTurn: true });
check("ended turn sets stage ended + close_window", ended.stage === "ended" && ended.nextBuyerMove === "close_window");
const clamped = updateSparringState({ ...init, trustLevel: 250 as any }, { repText: "x", turnScore: 500 as any });
check("values clamp to 0–100", clamped.trustLevel <= 100 && clamped.pressureLevel <= 100);
const prompt = summariseStateForPrompt(resolved);
check("prompt summary contains directives", prompt.includes("CONVERSATION STATE") && prompt.includes("Your next move"));

// ── Coercion ──
const rebuilt = coerceState({ garbage: true }, { personaId: "p", difficulty: "hard" });
check("corrupt state rebuilds from input", rebuilt.stage === "opening" && rebuilt.difficulty === "hard");
const kept = coerceState(resolved, { personaId: "p", difficulty: "easy" });
check("valid state passes through coercion", kept.objectionState.resolved === true);

console.log(failures === 0 ? "\nSparring state validation PASSED" : `\nSparring state validation FAILED (${failures})`);
process.exit(failures === 0 ? 0 : 1);
