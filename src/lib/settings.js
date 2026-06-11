import { auditSettingsUpdated, writeAuditEvent, } from "./audit";
function safeObject(input) {
    if (!input || typeof input !== "object") {
        return {};
    }
    return input;
}
// --------------------------------------------------
// DEEP MERGE
// --------------------------------------------------
export function mergeSettings(base, override) {
    const output = { ...base };
    for (const key of Object.keys(override || {})) {
        const baseValue = base?.[key];
        const overrideValue = override?.[key];
        if (typeof baseValue === "object" &&
            typeof overrideValue === "object" &&
            !Array.isArray(baseValue) &&
            !Array.isArray(overrideValue)) {
            output[key] = mergeSettings(baseValue, overrideValue);
        }
        else {
            output[key] = overrideValue;
        }
    }
    return output;
}
export function resolveBranding(tmcSettings, companySettings, officeSettings) {
    return {
        ...(tmcSettings?.branding || {}),
        ...(companySettings?.branding || {}),
        ...(officeSettings?.branding || {}),
    };
}
// --------------------------------------------------
// SETTINGS INHERITANCE
// --------------------------------------------------
export async function resolveInheritedSettings(supa, companyId, officeId) {
    let finalSettings = {};
    // --------------------------------------------------
    // LOAD COMPANY
    // --------------------------------------------------
    let company = null;
    if (companyId) {
        const companyResult = await supa
            .from("companies")
            .select("id, name, slug, tmc_id, settings_json")
            .eq("id", companyId)
            .maybeSingle();
        company = companyResult.data;
    }
    // --------------------------------------------------
    // LOAD TMC
    // --------------------------------------------------
    let tmc = null;
    if (company?.tmc_id) {
        const tmcResult = await supa
            .from("tmcs")
            .select("id, name, slug, settings_json")
            .eq("id", company.tmc_id)
            .maybeSingle();
        tmc = tmcResult.data;
    }
    // --------------------------------------------------
    // LOAD OFFICE
    // --------------------------------------------------
    let office = null;
    if (officeId) {
        const officeResult = await supa
            .from("offices")
            .select("id, name, slug, settings_json")
            .eq("id", officeId)
            .maybeSingle();
        office = officeResult.data;
    }
    // --------------------------------------------------
    // APPLY INHERITANCE
    // --------------------------------------------------
    finalSettings = mergeSettings(finalSettings, safeObject(tmc?.settings_json));
    finalSettings = mergeSettings(finalSettings, safeObject(company?.settings_json));
    finalSettings = mergeSettings(finalSettings, safeObject(office?.settings_json));
    const branding = resolveBranding(safeObject(tmc?.settings_json), safeObject(company?.settings_json), safeObject(office?.settings_json));
    return {
        tmc,
        company,
        office,
        branding,
        settings: finalSettings,
    };
}
export function buildEffectiveIdentity(resolved) {
    return {
        tmc_name: resolved.tmc?.name || null,
        company_name: resolved.branding?.company_name ||
            resolved.company?.name ||
            null,
        office_name: resolved.office?.name || null,
        app_name: resolved.branding?.app_name ||
            "Gravix",
        support_email: resolved.branding?.support_email ||
            "support@gravix.ai",
        primary_color: resolved.branding?.primary_color ||
            "#2563eb",
        logo_url: resolved.branding?.logo_url || null,
        domain: resolved.branding?.domain || null,
    };
}
export async function auditSettingsChange(supa, actorId, targetType, targetId, metadata) {
    return writeAuditEvent(supa, auditSettingsUpdated(actorId, targetType, targetId, metadata));
}
