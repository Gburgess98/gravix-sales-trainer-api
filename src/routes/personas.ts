// api/src/routes/personas.ts
import express from "express";
import { PERSONAS } from "../personas";

export const personasRouter = express.Router();

personasRouter.get("/", async (_req, res) => {
  return res.json({ ok: true, personas: PERSONAS });
});