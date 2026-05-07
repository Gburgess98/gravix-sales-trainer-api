import {
  auditSettingsUpdated,
  writeAuditEvent,
} from "./audit";
export type SettingsObject = Record<string, any>;

export type BrandingConfig = {
  company_name?: string;
  app_name?: string;
  logo_url?: string;
  primary_color?: string;
  support_email?: string;
  domain?: string;
};

function safeObject(input: any): SettingsObject {
  if (!input || typeof input !== "object") {
    return {};
  }

  return input;
}

// --------------------------------------------------
// DEEP MERGE
// --------------------------------------------------

export function mergeSettings(
  base: SettingsObject,
  override: SettingsObject
): SettingsObject {
  const output: SettingsObject = { ...base };

  for (const key of Object.keys(override || {})) {
    const baseValue = base?.[key];
    const overrideValue = override?.[key];

    if (
      typeof baseValue === "object" &&
      typeof overrideValue === "object" &&
      !Array.isArray(baseValue) &&
      !Array.isArray(overrideValue)
    ) {
      output[key] = mergeSettings(
        baseValue,
        overrideValue
      );
    } else {
      output[key] = overrideValue;
    }
  }

  return output;
}

export function resolveBranding(
  tmcSettings?: SettingsObject,
  companySettings?: SettingsObject,
  officeSettings?: SettingsObject
): BrandingConfig {
  return {
    ...(tmcSettings?.branding || {}),
    ...(companySettings?.branding || {}),
    ...(officeSettings?.branding || {}),
  };
}

// --------------------------------------------------
// SETTINGS INHERITANCE
// --------------------------------------------------

export async function resolveInheritedSettings(
  supa: any,
  companyId?: string | null,
  officeId?: string | null
) {
  let finalSettings: SettingsObject = {};

  // --------------------------------------------------
  // LOAD COMPANY
  // --------------------------------------------------

  let company: any = null;

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

  let tmc: any = null;

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

  let office: any = null;

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

  finalSettings = mergeSettings(
    finalSettings,
    safeObject(tmc?.settings_json)
  );

  finalSettings = mergeSettings(
    finalSettings,
    safeObject(company?.settings_json)
  );

  finalSettings = mergeSettings(
    finalSettings,
    safeObject(office?.settings_json)
  );

  const branding = resolveBranding(
    safeObject(tmc?.settings_json),
    safeObject(company?.settings_json),
    safeObject(office?.settings_json)
  );

  return {
    tmc,
    company,
    office,
    branding,
    settings: finalSettings,
  };
}

export function buildEffectiveIdentity(
  resolved: {
    tmc?: any;
    company?: any;
    office?: any;
    branding?: BrandingConfig;
  }
) {
  return {
    tmc_name: resolved.tmc?.name || null,
    company_name:
      resolved.branding?.company_name ||
      resolved.company?.name ||
      null,

    office_name:
      resolved.office?.name || null,

    app_name:
      resolved.branding?.app_name ||
      "Gravix",

    support_email:
      resolved.branding?.support_email ||
      "support@gravix.ai",

    primary_color:
      resolved.branding?.primary_color ||
      "#2563eb",

    logo_url:
      resolved.branding?.logo_url || null,

    domain:
      resolved.branding?.domain || null,
  };
}

export async function auditSettingsChange(
  supa: any,
  actorId: string,
  targetType: string,
  targetId: string,
  metadata?: Record<string, any>
) {
  return writeAuditEvent(
    supa,
    auditSettingsUpdated(
      actorId,
      targetType,
      targetId,
      metadata
    )
  );
}