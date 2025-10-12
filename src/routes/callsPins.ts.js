// src/routes/callsPins.ts
import { Router } from "express";
import pg from "pg";
const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes("render.com") ||
        process.env.DATABASE_URL?.includes("supabase.co")
        ? { rejectUnauthorized: false }
        : undefined,
});
const router = Router({ mergeParams: true }); // allows access to :id from parent
// GET /v1/calls/:id/pins
router.get("/", async (req, res) => {
    try {
        const callId = req.params.id;
        const { rows } = await pool.query(`select id, call_id, user_id, t_sec, label, note, color, created_at
       from call_pins
       where call_id = $1
       order by t_sec asc, created_at asc`, [callId]);
        res.json({ pins: rows });
    }
    catch (e) {
        console.error("GET pins error", e);
        res.status(500).json({ error: "Internal" });
    }
});
// POST /v1/calls/:id/pins
router.post("/", async (req, res) => {
    try {
        const callId = req.params.id;
        const { userId, t_sec, label, note, color } = req.body ?? {};
        if (typeof t_sec !== "number" || t_sec < 0) {
            return res.status(400).json({ error: "t_sec must be >= 0" });
        }
        if (!userId)
            return res.status(400).json({ error: "userId required" });
        const { rows } = await pool.query(`insert into call_pins (call_id, user_id, t_sec, label, note, color)
       values ($1, $2, $3, $4, $5, coalesce($6,'amber'))
       returning id, call_id, user_id, t_sec, label, note, color, created_at`, [callId, userId, t_sec, label ?? null, note ?? null, color ?? null]);
        res.status(201).json({ pin: rows[0] });
    }
    catch (e) {
        console.error("POST pin error", e);
        res.status(500).json({ error: "Internal" });
    }
});
// DELETE /v1/calls/:id/pins/:pinId
router.delete("/:pinId", async (req, res) => {
    try {
        const callId = req.params.id;
        const pinId = req.params.pinId;
        await pool.query(`delete from call_pins where id = $1 and call_id = $2`, [
            pinId,
            callId,
        ]);
        res.status(204).end();
    }
    catch (e) {
        console.error("DELETE pin error", e);
        res.status(500).json({ error: "Internal" });
    }
});
export default router; // <-- IMPORTANT
