/** Day 299 — dynamic critical-assignment payload contract. Offline, no side effects. */
import fs from 'node:fs';
import path from 'node:path';

const scoring = fs.readFileSync(path.join(process.cwd(), 'src/lib/scoring.ts'), 'utf8');
const schemaValidator = fs.readFileSync(path.join(process.cwd(), 'scripts/validate-schema-selects.ts'), 'utf8');
const start = scoring.indexOf('async function ensureCriticalCallAssignment');
const end = scoring.indexOf('\nasync function', start + 20);
const body = scoring.slice(start, end > start ? end : undefined);
const payloadStart = body.indexOf('const payload = {');
const payloadEnd = body.indexOf('\n    };', payloadStart);
const payload = body.slice(payloadStart, payloadEnd > payloadStart ? payloadEnd + 7 : undefined);
let failures = 0;
const gate = (label: string, ok: boolean) => {
  console.log(`${ok ? '✓' : '✗'} ${label}`);
  if (!ok) failures++;
};

console.log('Day 299 — critical coach-assignment schema contract\n');
gate('critical-assignment function found', start >= 0);
gate('side-effect safety gate remains before persistence',
  /SKIP_SCORING_SIDE_EFFECTS[\s\S]{0,250}skipped:\s*true/.test(body));
gate('dedupe still uses real coach_assignments columns',
  /\.from\(["']coach_assignments["']\)[\s\S]*?\.eq\(["']call_id["'][\s\S]*?\.eq\(["']assignee_user_id["'][\s\S]*?\.eq\(["']drill_id["']/.test(body));
gate('insert writes canonical columns',
  /const payload\s*=\s*\{[\s\S]*?call_id:[\s\S]*?assignee_user_id:[\s\S]*?drill_id:[\s\S]*?notes:[\s\S]*?org_id:[\s\S]*?status:[\s\S]*?\};/.test(body));
gate('coach_assignments insert does not write nonexistent source',
  !/source\s*:/.test(payload));
gate('coach_assignments insert does not write nonexistent meta',
  !/meta\s*:/.test(payload));
gate('obsolete org_id-missing retry removed (org_id exists)',
  !/missingOrg|retryPayload/.test(body));
gate('provenance remains on crm_activities meta',
  /\.from\(["']crm_activities["']\)\.insert\([\s\S]*?meta:\s*\{[\s\S]*?assignment_origin:\s*["']flagged_call_auto/.test(body));
gate('detailed audit inventory remains opt-in',
  /SCHEMA_AUDIT_DETAILS\s*===\s*["']1["']/.test(schemaValidator));

console.log(`\n${failures === 0 ? 'PASS' : 'FAIL'} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
