import { Router } from "express";
import { createClient } from "@supabase/supabase-js";
import { canImpersonateUsers, canAccessInternalPortal, buildImpersonationContext, } from "../lib/support";
import { auditImpersonationStarted, writeAuditEvent, } from "../lib/audit";
const router = Router();
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function getInternalUser(userId) {
    if (!userId)
        return null;
    const { data } = await supabase
        .from("internal_users")
        .select("id, email, role, is_internal")
        .eq("id", userId)
        .maybeSingle();
    if (!data)
        return null;
    return {
        id: String(data.id),
        email: String(data.email),
        role: String(data.role),
        is_internal: Boolean(data.is_internal),
    };
}
async function requireInternal(req, res, next) {
    try {
        const userId = String(req.authUserId || "").trim();
        const internalUser = await getInternalUser(userId);
        if (!internalUser || !canAccessInternalPortal(internalUser)) {
            return res.status(403).json({
                ok: false,
                error: "internal_access_required",
            });
        }
        req.internalUser = internalUser;
        next();
    }
    catch (err) {
        return res.status(500).json({
            ok: false,
            error: err?.message || "internal_guard_failed",
        });
    }
}
// --------------------------------------------------
// INTERNAL HEALTH
// --------------------------------------------------
router.get("/health", requireInternal, async (req, res) => {
    res.json({
        ok: true,
        internal: true,
        user: req.internalUser,
    });
});
// --------------------------------------------------
// INTERNAL TMCS
// --------------------------------------------------
router.get("/tmcs", requireInternal, async (_req, res) => {
    const { data, error } = await supabase
        .from("tmcs")
        .select("*")
        .order("created_at", { ascending: false });
    if (error) {
        return res.status(500).json({
            ok: false,
            error: error.message,
        });
    }
    return res.json({
        ok: true,
        tmcs: data || [],
    });
});
// --------------------------------------------------
// INTERNAL COMPANIES
// --------------------------------------------------
router.get("/companies", requireInternal, async (_req, res) => {
    const { data, error } = await supabase
        .from("companies")
        .select("*")
        .order("created_at", { ascending: false });
    if (error) {
        return res.status(500).json({
            ok: false,
            error: error.message,
        });
    }
    return res.json({
        ok: true,
        companies: data || [],
    });
});
// --------------------------------------------------
// INTERNAL OFFICES
// --------------------------------------------------
router.get("/offices", requireInternal, async (_req, res) => {
    const { data, error } = await supabase
        .from("offices")
        .select("*")
        .order("created_at", { ascending: false });
    if (error) {
        return res.status(500).json({
            ok: false,
            error: error.message,
        });
    }
    return res.json({
        ok: true,
        offices: data || [],
    });
});
// --------------------------------------------------
// INTERNAL USERS
// --------------------------------------------------
router.get("/users", requireInternal, async (_req, res) => {
    const { data, error } = await supabase
        .from("users")
        .select("id,email,role,office_id,company_id,is_admin")
        .order("created_at", { ascending: false });
    if (error) {
        return res.status(500).json({
            ok: false,
            error: error.message,
        });
    }
    return res.json({
        ok: true,
        users: data || [],
    });
});
// --------------------------------------------------
// IMPERSONATION FOUNDATION
// --------------------------------------------------
router.post("/impersonate", requireInternal, async (req, res) => {
    const internalUser = req.internalUser;
    if (!canImpersonateUsers(internalUser)) {
        return res.status(403).json({
            ok: false,
            error: "super_admin_required",
        });
    }
    const targetUserId = String(req.body?.target_user_id || "").trim();
    if (!targetUserId) {
        return res.status(400).json({
            ok: false,
            error: "target_user_id_required",
        });
    }
    const context = buildImpersonationContext(internalUser.id, targetUserId);
    await writeAuditEvent(supabase, auditImpersonationStarted(internalUser.id, targetUserId));
    return res.json({
        ok: true,
        impersonation: context,
    });
});
export default router;
