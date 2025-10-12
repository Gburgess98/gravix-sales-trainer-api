export function requireUserId(req, res, next) {
    const userId = req.header("x-user-id");
    if (!userId)
        return res.status(401).json({ error: "Missing x-user-id" });
    req.userId = userId;
    next();
}
