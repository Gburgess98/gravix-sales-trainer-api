// 🔥 CONTEXT BUILDER (Day 66)
export async function buildAIContext({ supa, repId, }) {
    // --- REP WEAKNESSES ---
    const { data: flags } = await supa
        .from("crm_activities")
        .select("meta, created_at")
        .eq("type", "review_flag")
        .eq("rep_id", repId)
        .limit(50);
    const sectionCounts = {};
    for (const row of flags || []) {
        const section = row?.meta?.flag_section || "general";
        sectionCounts[section] = (sectionCounts[section] || 0) + 1;
    }
    const topWeaknesses = Object.entries(sectionCounts)
        .map(([section, count]) => ({ section, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 3);
    // --- COMPANY CONTEXT ---
    const { data: companyFlags } = await supa
        .from("crm_activities")
        .select("meta")
        .eq("type", "review_flag")
        .limit(200);
    const companySectionCounts = {};
    for (const row of companyFlags || []) {
        const section = row?.meta?.flag_section || "general";
        companySectionCounts[section] =
            (companySectionCounts[section] || 0) + 1;
    }
    const topCompanyWeakness = Object.entries(companySectionCounts)
        .sort((a, b) => b[1] - a[1])[0]?.[0] || null;
    return {
        rep: {
            topWeaknesses,
        },
        company: {
            topWeakness: topCompanyWeakness,
        },
    };
}
