/**
 * validate-intelligence-scorecards-day-219b.ts
 *
 * Days 219B + 220 — Scorecard Studio data layer + lifecycle completion
 * (/v1/intelligence/scorecards).
 *
 * Day 220 additions:
 *   ✓ fork active version → draft n+1 copying weights/criteria/call
 *     types/origin; second fork → 409 draft_already_exists; forking a
 *     draft → 400; rep 403; cross-company 404
 *   ✓ editing the forked draft never alters the active snapshot
 *   ✓ activation conflict still blocks without { replace_conflicts: true };
 *     with the flag it supersedes the conflicting version, drops the
 *     replaced scorecard to draft and reports the replacement list
 *   ✓ archive marks without deleting (versions/snapshots survive) and an
 *     archived scorecard rejects edit/save/fork/activate with 409
 *   ✓ audit rows for create/save-draft/fork/replace/archive lifecycle
 *
 * Day 219B coverage:
 *   ✓ endpoints answer 503 scorecard_studio_not_migrated until
 *     sql/20260714b_scorecard_studio.sql is applied (fail-soft)
 *   ✓ gates: no identity rejected, SalesRep → 403 on read and write
 *   ✓ manager lists (empty) scorecards + read-only Gravix default card
 *   ✓ create draft → four fixed stages, default weights totalling 100
 *   ✓ duplicate name (case-insensitive) → 409
 *   ✓ save draft version: criteria land in fixed stages, sort order
 *     normalised; invalid stage / emphasis / critical-without-pass-fail /
 *     bad weights → 400
 *   ✓ activation: weights-total-100 enforced, call type or company default
 *     required, version snapshotted + immutable, previous active superseded,
 *     one-active-per-call-type conflict → 409, audit event written
 *   ✓ cross-company manager cannot read/update another company's scorecard
 *     (404, no existence leak) and their list never contains it
 *
 * Self-cleaning: cross-company fixtures and every scorecard row created
 * during the run are removed at the end (child rows first — the only
 * delete path anywhere is this validator's own fixtures).
 *
 * Requirements: server running (npm run dev), UFC demo seed applied,
 * sql/20260714b_scorecard_studio.sql applied (otherwise reports MIGRATION
 * PENDING after proving the fail-soft behaviour).
 * Usage: npm run validate:intelligence-scorecards
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import crypto from "node:crypto";

const API_BASE    = (process.env.API_BASE ?? "http://localhost:4000").replace(/\/$/, "");
const SUPA_URL    = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supa = createClient(SUPA_URL, SERVICE_KEY, { auth: { persistSession: false } });

type Check = { label: string; passed: boolean; detail?: string };
const checks: Check[] = [];
function c(label: string, passed: boolean, detail?: string) {
  checks.push({ label, passed, detail });
  console.log(`  ${passed ? "✓" : "✗"} ${label}${detail && !passed ? ` — ${detail}` : ""}`);
}

function uid(ns: string, name: string): string {
  const h = crypto.createHash("sha256").update(`${ns}::${name}`).digest("hex");
  const v = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${v}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

async function hit(method: string, path: string, userId?: string, body?: object) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (userId) headers["x-user-id"] = userId;
  const init: RequestInit = { method, headers, signal: AbortSignal.timeout(15_000) };
  if (body) init.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, init);
  let data: any = null;
  try { data = await res.json(); } catch { /* ignore */ }
  return { status: res.status, data };
}

async function repIdByEmail(email: string): Promise<string | null> {
  const { data } = await supa.from("reps").select("id").eq("email", email).maybeSingle();
  return (data as any)?.id ?? null;
}

const BASE = "/v1/intelligence/scorecards";
const STAGES = ["intro", "discovery", "objection", "close"] as const;

// Cross-company fixtures — synthetic company whose scorecards must stay invisible.
const XCOMPANY_ID = uid("DAY219B", "cross-company");
const XMANAGER_ID = uid("DAY219B", "cross-manager");

const RUN_TAG = `Day219B ${new Date().toISOString().slice(0, 10)}`;

function stagesPayload(weights: [number, number, number, number], extra?: Partial<Record<string, any>>) {
  const [intro, discovery, objection, close] = weights;
  return {
    intro: { weight: intro, criteria: [] },
    discovery: {
      weight: discovery,
      criteria: [
        {
          label: "Asked about training goals before pitching",
          description: "Any open goal question before membership or price talk.",
          scoring_guidance: "Full: asked and answered before the pitch.",
          emphasis: "major",
          pass_fail: true,
          critical: true,
          coaching_prompt: "Get one real goal out of them before you talk memberships.",
          good_example: "What would you want to be different in three months?",
          weak_example: "Quoting the All-Access price in the first two minutes.",
        },
        { label: "Mapped who makes the decision", emphasis: "standard" },
      ],
    },
    objection: { weight: objection, criteria: [{ label: "Isolated the objection before answering" }] },
    close: { weight: close, criteria: [{ label: "Asked for a firm, dated next step", emphasis: "major" }] },
    ...(extra ?? {}),
  };
}

async function seedCrossCompanyFixtures(orgId: string) {
  const { error: compErr } = await supa
    .from("companies")
    .upsert(
      {
        id: XCOMPANY_ID,
        tmc_id: "5055e1b6-fb33-45c0-959a-d7dd45f98a13", // demo partner (same TMC, different company)
        partner_id: "5055e1b6-fb33-45c0-959a-d7dd45f98a13",
        name: "Day219B Cross Company (validator)",
        slug: "day219b-cross-company-validator",
      },
      { onConflict: "id" }
    );
  if (compErr) throw new Error(`fixture company upsert failed: ${compErr.message}`);

  const { error: repErr } = await supa.from("reps").upsert(
    [{ id: XMANAGER_ID, name: "Day219B X Manager", tier: "Manager", org_id: orgId, company_id: XCOMPANY_ID }],
    { onConflict: "id" }
  );
  if (repErr) throw new Error(`fixture reps upsert failed: ${repErr.message}`);
}

async function scorecardIds(companyIds: string[]): Promise<Set<string>> {
  const { data } = await supa.from("scorecards").select("id").in("company_id", companyIds);
  return new Set(((data ?? []) as any[]).map((r) => String(r.id)));
}

async function cleanup(companyIds: string[], preExisting: Set<string>) {
  const { data } = await supa.from("scorecards").select("id").in("company_id", companyIds);
  const created = ((data ?? []) as any[]).map((r) => String(r.id)).filter((id) => !preExisting.has(id));
  if (created.length) {
    const { data: versions } = await supa
      .from("scorecard_versions").select("id").in("scorecard_id", created);
    const versionIds = ((versions ?? []) as any[]).map((v) => String(v.id));
    if (versionIds.length) {
      await supa.from("scorecard_criteria").delete().in("scorecard_version_id", versionIds);
      await supa.from("scorecard_stage_weights").delete().in("scorecard_version_id", versionIds);
      await supa.from("scorecard_versions").delete().in("id", versionIds);
    }
    await supa.from("scorecards").delete().in("id", created);
  }
  await supa.from("reps").delete().eq("id", XMANAGER_ID);
  await supa.from("companies").delete().eq("id", XCOMPANY_ID);
  return created.length;
}

async function main() {
  console.log("\n  Scorecard Studio Validator — Day 219B\n");

  const danaId = await repIdByEmail("dana.white@ufcelite.demo");
  const nateId = await repIdByEmail("nate.diaz@ufcelite.demo");
  if (!danaId || !nateId) {
    console.error("  ✗ UFC demo seed not found (run npm run seed:demo first)");
    process.exit(1);
  }

  // ── Migration preflight ───────────────────────────────────────────────────
  const { error: tableErr } = await supa.from("scorecards").select("id").limit(1);
  if (tableErr && /could not find the table/i.test(tableErr.message)) {
    console.log("  ⚠ scorecards tables missing — proving fail-soft behaviour only.\n");
    const r = await hit("GET", BASE, danaId);
    c("GET answers 503 scorecard_studio_not_migrated before migration",
      r.status === 503 && r.data?.error === "scorecard_studio_not_migrated",
      `got ${r.status} ${JSON.stringify(r.data)?.slice(0, 120)}`);
    const asRep = await hit("POST", BASE, nateId, { name: "x" });
    c("SalesRep still 403 before migration (gate before table)", asRep.status === 403, `got ${asRep.status}`);
    console.log("\n  MIGRATION PENDING — apply sql/20260714b_scorecard_studio.sql in the");
    console.log("  Supabase SQL editor, then re-run npm run validate:intelligence-scorecards\n");
    process.exit(checks.every((x) => x.passed) ? 2 : 1);
  }
  if (tableErr) {
    console.error(`  ✗ scorecards preflight failed: ${tableErr.message}`);
    process.exit(1);
  }

  const { data: danaUser } = await supa.from("users").select("company_id").eq("id", danaId).maybeSingle();
  const { data: danaRep } = await supa.from("reps").select("org_id, company_id").eq("id", danaId).single();
  const ufcCompanyId = String((danaUser as any)?.company_id ?? (danaRep as any)?.company_id);
  const orgId = (danaRep as any).org_id as string;

  const preExisting = await scorecardIds([ufcCompanyId, XCOMPANY_ID]);
  if (preExisting.size) {
    console.log(`  ⚠ ${preExisting.size} pre-existing scorecard row(s) — left untouched.\n`);
  }
  await seedCrossCompanyFixtures(orgId);

  try {
    // ── Gate checks ──────────────────────────────────────────────────────────
    // No-identity may resolve via the DEV_TEST_UID dev fallback — assert it
    // is rejected either way (401 in prod, 403 in dev), same as Day 212/218.
    const noAuth = await hit("GET", BASE);
    c("GET without identity rejected", noAuth.status === 401 || noAuth.status === 403, `got ${noAuth.status}`);
    const repGet = await hit("GET", BASE, nateId);
    c("GET as SalesRep → 403", repGet.status === 403, `got ${repGet.status}`);
    const repPost = await hit("POST", BASE, nateId, { name: "Rep card" });
    c("POST as SalesRep → 403", repPost.status === 403, `got ${repPost.status}`);

    // ── List (empty) + default card ──────────────────────────────────────────
    const emptyList = await hit("GET", BASE, danaId);
    c("manager GET list → 200 ok", emptyList.status === 200 && emptyList.data?.ok === true, `got ${emptyList.status}`);
    if (!preExisting.size) {
      c("empty company lists no scorecards", Array.isArray(emptyList.data?.items) && emptyList.data.items.length === 0,
        `got ${emptyList.data?.items?.length}`);
    }
    c("list carries the read-only Gravix default card",
      emptyList.data?.default_rubric?.read_only === true &&
      emptyList.data?.default_rubric?.version === "v1" &&
      (emptyList.data?.default_rubric?.stages ?? []).length === 4,
      JSON.stringify(emptyList.data?.default_rubric)?.slice(0, 120));

    // ── Create draft ─────────────────────────────────────────────────────────
    const name = `Discovery Call — ${RUN_TAG}`;
    const created = await hit("POST", BASE, danaId, { name, description: "Validator fixture." });
    c("manager POST creates draft scorecard", created.status === 201 && created.data?.scorecard?.status === "draft",
      `got ${created.status}`);
    const cardId = String(created.data?.scorecard?.id ?? "");
    const draftVersion = created.data?.draft_version;
    const weightStages = (draftVersion?.stage_weights ?? []).map((w: any) => w.stage).sort();
    c("draft version 1 contains exactly the four fixed stages",
      draftVersion?.version === 1 && weightStages.join(",") === ["close", "discovery", "intro", "objection"].join(","),
      JSON.stringify(weightStages));
    const defaultTotal = (draftVersion?.stage_weights ?? []).reduce((s: number, w: any) => s + w.weight, 0);
    c("default stage weights total 100", defaultTotal === 100, `got ${defaultTotal}`);

    const dupe = await hit("POST", BASE, danaId, { name: name.toUpperCase() });
    c("duplicate name (case-insensitive) → 409", dupe.status === 409 && dupe.data?.error === "scorecard_name_taken",
      `got ${dupe.status}`);
    const noName = await hit("POST", BASE, danaId, {});
    c("create without name → 400", noName.status === 400, `got ${noName.status}`);

    // ── Save draft version ───────────────────────────────────────────────────
    const versionId = String(draftVersion?.id ?? "");
    const versionPath = `${BASE}/${cardId}/versions/${versionId}`;

    const badStage = await hit("PUT", versionPath, danaId, {
      stages: { ...stagesPayload([10, 35, 30, 25]), pitch: { weight: 0, criteria: [] } },
    });
    c("invalid stage rejected → 400 invalid_stage", badStage.status === 400 && badStage.data?.error === "invalid_stage",
      `got ${badStage.status} ${badStage.data?.error}`);

    const badEmphasis = await hit("PUT", versionPath, danaId, {
      stages: stagesPayload([10, 35, 30, 25], {
        intro: { weight: 10, criteria: [{ label: "x", emphasis: "huge" }] },
      }),
    });
    c("invalid emphasis rejected → 400 invalid_emphasis",
      badEmphasis.status === 400 && JSON.stringify(badEmphasis.data).includes("invalid_emphasis"),
      `got ${badEmphasis.status} ${badEmphasis.data?.error}`);

    const badCritical = await hit("PUT", versionPath, danaId, {
      stages: stagesPayload([10, 35, 30, 25], {
        intro: { weight: 10, criteria: [{ label: "x", critical: true }] },
      }),
    });
    c("critical without pass_fail rejected → 400",
      badCritical.status === 400 && JSON.stringify(badCritical.data).includes("critical_requires_pass_fail"),
      `got ${badCritical.status}`);

    const badWeight = await hit("PUT", versionPath, danaId, {
      stages: stagesPayload([10, 35, 30, 25], { close: { weight: 250, criteria: [] } }),
    });
    c("out-of-range stage weight rejected → 400",
      badWeight.status === 400 && JSON.stringify(badWeight.data).includes("invalid_stage_weight"),
      `got ${badWeight.status}`);

    const badCallType = await hit("PUT", versionPath, danaId, {
      call_types: ["walk_in"],
      stages: stagesPayload([10, 35, 30, 25]),
    });
    c("invalid call type rejected → 400",
      badCallType.status === 400 && JSON.stringify(badCallType.data).includes("invalid_call_type"),
      `got ${badCallType.status}`);

    const draftSave = await hit("PUT", versionPath, danaId, {
      call_types: ["discovery"],
      stages: stagesPayload([10, 35, 30, 24]), // deliberately 99 — saving is never blocked
    });
    c("manager saves draft criteria into fixed stages (weights 99 allowed on save)",
      draftSave.status === 200 && (draftSave.data?.version?.criteria ?? []).length === 4,
      `got ${draftSave.status} ${(draftSave.data?.version?.criteria ?? []).length} criteria`);
    const discoveryCriteria = (draftSave.data?.version?.criteria ?? []).filter((cr: any) => cr.stage === "discovery");
    c("criteria sort order normalised within stage",
      discoveryCriteria.length === 2 && discoveryCriteria[0].sort_order === 0 && discoveryCriteria[1].sort_order === 1,
      JSON.stringify(discoveryCriteria.map((cr: any) => cr.sort_order)));

    const repSave = await hit("PUT", versionPath, nateId, {
      stages: stagesPayload([10, 35, 30, 25]),
    });
    c("PUT version as SalesRep → 403", repSave.status === 403, `got ${repSave.status}`);

    // ── Activation rules ─────────────────────────────────────────────────────
    const actAt99 = await hit("POST", `${BASE}/${cardId}/activate`, danaId);
    c("activation with weights totalling 99 → 400 weights_must_total_100",
      actAt99.status === 400 && JSON.stringify(actAt99.data).includes("weights_must_total_100"),
      `got ${actAt99.status} ${actAt99.data?.error}`);

    await hit("PUT", versionPath, danaId, { call_types: [], stages: stagesPayload([10, 35, 30, 25]) });
    const actNoType = await hit("POST", `${BASE}/${cardId}/activate`, danaId);
    c("activation without call type or company default → 400",
      actNoType.status === 400 && JSON.stringify(actNoType.data).includes("call_type_or_company_default_required"),
      `got ${actNoType.status}`);

    await hit("PUT", versionPath, danaId, { call_types: ["discovery"], stages: stagesPayload([10, 35, 30, 25]) });
    const repActivate = await hit("POST", `${BASE}/${cardId}/activate`, nateId);
    c("activate as SalesRep → 403", repActivate.status === 403, `got ${repActivate.status}`);

    const activate = await hit("POST", `${BASE}/${cardId}/activate`, danaId, { activation_note: "Validator activation." });
    c("manager activates valid draft → 200 active v1",
      activate.status === 200 && activate.data?.version?.status === "active" && activate.data?.version?.version === 1,
      `got ${activate.status} ${JSON.stringify(activate.data)?.slice(0, 140)}`);
    const snapshotStages = activate.data?.version?.snapshot?.stages ?? [];
    c("activated version carries the deterministic snapshot",
      snapshotStages.length === 4 &&
      snapshotStages.map((s: any) => s.stage).join(",") === "intro,discovery,objection,close" &&
      snapshotStages.reduce((sum: number, s: any) => sum + s.weight, 0) === 100,
      JSON.stringify(snapshotStages)?.slice(0, 140));

    const editActive = await hit("PUT", versionPath, danaId, { stages: stagesPayload([25, 25, 25, 25]) });
    c("active version is immutable → 409 version_immutable",
      editActive.status === 409 && editActive.data?.error === "version_immutable",
      `got ${editActive.status}`);

    // One active per call type across the company.
    const rival = await hit("POST", BASE, danaId, { name: `Rival — ${RUN_TAG}` });
    const rivalId = String(rival.data?.scorecard?.id ?? "");
    const rivalVersionId = String(rival.data?.draft_version?.id ?? "");
    await hit("PUT", `${BASE}/${rivalId}/versions/${rivalVersionId}`, danaId, {
      call_types: ["discovery"],
      stages: stagesPayload([25, 25, 25, 25]),
    });
    const conflict = await hit("POST", `${BASE}/${rivalId}/activate`, danaId);
    c("second activation on the same call type → 409 call_type_conflict",
      conflict.status === 409 && conflict.data?.error === "call_type_conflict" &&
      conflict.data?.conflicts?.[0]?.scorecard_id === cardId,
      `got ${conflict.status} ${JSON.stringify(conflict.data)?.slice(0, 140)}`);

    // Audit trail.
    const { data: auditRows } = await supa
      .from("audit_events")
      .select("id, action, metadata")
      .eq("action", "activate_scorecard_version")
      .eq("actor_user_id", danaId)
      .order("created_at", { ascending: false })
      .limit(1);
    const audit = (auditRows ?? [])[0] as any;
    c("activation writes an audit_events row",
      Boolean(audit) && audit?.metadata?.scorecard_id === cardId && audit?.metadata?.version === 1,
      JSON.stringify(audit)?.slice(0, 140));

    // ── Day 220: fork active version → new draft ─────────────────────────────
    const forkPath = `${BASE}/${cardId}/versions/${versionId}/fork`;

    const repFork = await hit("POST", forkPath, nateId);
    c("fork as SalesRep → 403", repFork.status === 403, `got ${repFork.status}`);
    const xFork = await hit("POST", forkPath, XMANAGER_ID);
    c("cross-company fork → 404", xFork.status === 404, `got ${xFork.status}`);

    const fork = await hit("POST", forkPath, danaId);
    c("fork active version → 201 draft v2",
      fork.status === 201 && fork.data?.version?.version === 2 && fork.data?.version?.status === "draft",
      `got ${fork.status} v${fork.data?.version?.version} ${fork.data?.version?.status}`);
    const forkedVersionId = String(fork.data?.version?.id ?? "");
    const forkWeightTotal = (fork.data?.version?.stage_weights ?? []).reduce((s: number, w: any) => s + w.weight, 0);
    c("fork copies weights/criteria/call_types/origin",
      forkWeightTotal === 100 &&
      (fork.data?.version?.criteria ?? []).length === 4 &&
      JSON.stringify(fork.data?.version?.call_types) === JSON.stringify(["discovery"]) &&
      fork.data?.version?.origin === "manual",
      JSON.stringify({ forkWeightTotal, criteria: (fork.data?.version?.criteria ?? []).length }));

    const forkAgain = await hit("POST", forkPath, danaId);
    c("second fork while a draft exists → 409 draft_already_exists",
      forkAgain.status === 409 && forkAgain.data?.error === "draft_already_exists" &&
      forkAgain.data?.draft_version_id === forkedVersionId,
      `got ${forkAgain.status} ${forkAgain.data?.error}`);
    const forkDraft = await hit("POST", `${BASE}/${cardId}/versions/${forkedVersionId}/fork`, danaId);
    c("forking a draft version → 400 cannot_fork_draft",
      forkDraft.status === 400 && forkDraft.data?.error === "cannot_fork_draft",
      `got ${forkDraft.status} ${forkDraft.data?.error}`);

    // Editing the forked draft must never touch the active snapshot.
    const editFork = await hit("PUT", `${BASE}/${cardId}/versions/${forkedVersionId}`, danaId, {
      call_types: ["discovery"],
      stages: stagesPayload([25, 25, 25, 25]),
    });
    c("forked draft is editable", editFork.status === 200, `got ${editFork.status}`);
    const detailAfterFork = await hit("GET", `${BASE}/${cardId}`, danaId);
    const activeAfterFork = (detailAfterFork.data?.versions ?? []).find((v: any) => v.status === "active");
    const activeSnapshotWeights = (activeAfterFork?.snapshot?.stages ?? []).map((s: any) => s.weight);
    c("active snapshot unchanged after editing the forked draft",
      activeAfterFork?.version === 1 && JSON.stringify(activeSnapshotWeights) === JSON.stringify([10, 35, 30, 25]),
      JSON.stringify(activeSnapshotWeights));

    // ── Day 220: explicit replacement activation ─────────────────────────────
    // (rival's earlier bare activation already proved the 409 without flag)
    const xReplace = await hit("POST", `${BASE}/${cardId}/activate`, XMANAGER_ID, { replace_conflicts: true });
    c("cross-company activate with replace flag → 404", xReplace.status === 404, `got ${xReplace.status}`);

    const replaceActivate = await hit("POST", `${BASE}/${rivalId}/activate`, danaId, { replace_conflicts: true });
    c("activation with replace_conflicts supersedes the conflict → 200",
      replaceActivate.status === 200 && replaceActivate.data?.version?.status === "active",
      `got ${replaceActivate.status} ${JSON.stringify(replaceActivate.data)?.slice(0, 140)}`);
    c("replacement list names the superseded scorecard/version",
      (replaceActivate.data?.replaced ?? []).length === 1 &&
      replaceActivate.data?.replaced?.[0]?.scorecard_id === cardId &&
      replaceActivate.data?.replaced?.[0]?.version === 1 &&
      replaceActivate.data?.replaced?.[0]?.reason === "call_type",
      JSON.stringify(replaceActivate.data?.replaced));
    const { data: replacedVersionRow } = await supa
      .from("scorecard_versions").select("status").eq("id", versionId).single();
    const { data: replacedCardRow } = await supa
      .from("scorecards").select("status").eq("id", cardId).single();
    c("replaced version superseded + scorecard dropped to draft (history kept)",
      (replacedVersionRow as any)?.status === "superseded" && (replacedCardRow as any)?.status === "draft",
      JSON.stringify({ version: (replacedVersionRow as any)?.status, card: (replacedCardRow as any)?.status }));

    // ── Day 220: archive ─────────────────────────────────────────────────────
    const xArchive = await hit("POST", `${BASE}/${rivalId}/archive`, XMANAGER_ID);
    c("cross-company archive → 404", xArchive.status === 404, `got ${xArchive.status}`);
    const repArchive = await hit("POST", `${BASE}/${rivalId}/archive`, nateId);
    c("archive as SalesRep → 403", repArchive.status === 403, `got ${repArchive.status}`);

    const archive = await hit("POST", `${BASE}/${rivalId}/archive`, danaId);
    c("manager archives scorecard → 200 archived",
      archive.status === 200 && archive.data?.scorecard?.status === "archived" &&
      Boolean(archive.data?.scorecard?.archived_at),
      `got ${archive.status} ${archive.data?.scorecard?.status}`);
    const { data: rivalVersions } = await supa
      .from("scorecard_versions").select("id, status, snapshot").eq("scorecard_id", rivalId);
    c("archive preserves versions + snapshots (nothing deleted)",
      (rivalVersions ?? []).length === 1 && (rivalVersions as any[])[0].status === "superseded" &&
      ((rivalVersions as any[])[0].snapshot?.stages ?? []).length === 4,
      JSON.stringify((rivalVersions ?? []).map((v: any) => v.status)));

    const archivedEdit = await hit("PUT", `${BASE}/${rivalId}`, danaId, { name: `Rival renamed — ${RUN_TAG}` });
    c("archived scorecard rejects metadata edits → 409", archivedEdit.status === 409, `got ${archivedEdit.status}`);
    const archivedSave = await hit("PUT", `${BASE}/${rivalId}/versions/${rivalVersionId}`, danaId, {
      stages: stagesPayload([25, 25, 25, 25]),
    });
    c("archived scorecard rejects version saves → 409", archivedSave.status === 409, `got ${archivedSave.status}`);
    const archivedActivate = await hit("POST", `${BASE}/${rivalId}/activate`, danaId);
    c("archived scorecard rejects activation → 409", archivedActivate.status === 409, `got ${archivedActivate.status}`);
    const archivedFork = await hit("POST", `${BASE}/${rivalId}/versions/${String((rivalVersions as any[])[0].id)}/fork`, danaId);
    c("archived scorecard rejects fork → 409", archivedFork.status === 409, `got ${archivedFork.status}`);
    const archiveAgain = await hit("POST", `${BASE}/${rivalId}/archive`, danaId);
    c("archiving twice → 409", archiveAgain.status === 409, `got ${archiveAgain.status}`);

    // ── Day 220: lifecycle audit trail ───────────────────────────────────────
    for (const action of [
      "create_scorecard",
      "save_scorecard_draft",
      "fork_scorecard_version",
      "replace_scorecard_conflicts",
      "archive_scorecard",
    ]) {
      const { data: rows } = await supa
        .from("audit_events")
        .select("id, metadata")
        .eq("action", action)
        .eq("actor_user_id", danaId)
        .order("created_at", { ascending: false })
        .limit(1);
      const row = (rows ?? [])[0] as any;
      const scoped =
        row?.metadata?.company_id === ufcCompanyId ||
        row?.metadata?.scorecard_id === cardId ||
        row?.metadata?.scorecard_id === rivalId;
      c(`audit row exists for ${action}`, Boolean(row) && scoped, JSON.stringify(row)?.slice(0, 120));
    }

    // ── Cross-company isolation ──────────────────────────────────────────────
    const xList = await hit("GET", BASE, XMANAGER_ID);
    const xIds = new Set(((xList.data?.items ?? []) as any[]).map((i) => String(i.id)));
    c("cross-company manager list excludes UFC scorecards",
      xList.status === 200 && !xIds.has(cardId) && !xIds.has(rivalId),
      `got ${xList.status}, ${xIds.size} items`);

    const xGet = await hit("GET", `${BASE}/${cardId}`, XMANAGER_ID);
    c("cross-company GET by id → 404 (no existence leak)", xGet.status === 404, `got ${xGet.status}`);
    const xPut = await hit("PUT", `${BASE}/${cardId}`, XMANAGER_ID, { name: "Hijacked" });
    c("cross-company PUT by id → 404", xPut.status === 404, `got ${xPut.status}`);
    const xActivate = await hit("POST", `${BASE}/${cardId}/activate`, XMANAGER_ID);
    c("cross-company activate by id → 404", xActivate.status === 404, `got ${xActivate.status}`);
    const xSave = await hit("PUT", versionPath, XMANAGER_ID, { stages: stagesPayload([25, 25, 25, 25]) });
    c("cross-company version save → 404", xSave.status === 404, `got ${xSave.status}`);

    const { data: ufcCard } = await supa.from("scorecards").select("name").eq("id", cardId).single();
    c("UFC scorecard untouched by cross-company attempts", (ufcCard as any)?.name === name,
      JSON.stringify(ufcCard));
  } finally {
    const removed = await cleanup([ufcCompanyId, XCOMPANY_ID], preExisting);
    console.log(`\n  Cleanup: removed ${removed} scorecard fixture(s) + cross-company fixtures.`);
  }

  const failed = checks.filter((x) => !x.passed);
  console.log(`\n  ${checks.length - failed.length}/${checks.length} checks passed`);
  if (failed.length) {
    console.log("  Scorecard Studio validation FAILED\n");
    process.exit(1);
  }
  console.log("  Scorecard Studio validation PASSED\n");
}

main().catch((e) => {
  console.error("\n  ✗ Validator crashed:", e?.message || e);
  process.exit(1);
});
