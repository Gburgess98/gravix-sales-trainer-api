/** Staging-only schema proof for the Day-299 canonical coach_assignments payload. */
import { createClient } from '@supabase/supabase-js';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ref = (url || '').match(/https:\/\/([a-z0-9]+)\./)?.[1];
if (process.env.APP_ENV !== 'staging' || ref !== process.env.EXPECTED_STAGING_SUPABASE_REF || ref === process.env.PROD_SUPABASE_REF || !key) {
  console.error('GUARD: refusing non-staging target'); process.exit(1);
}

const CALL = '00000000-2990-4000-8000-000000000001';
const REP = '00000000-2990-4000-8000-000000000002';
const s = createClient(url, key, { auth: { persistSession: false } });
let assignmentId = null;
let pass = false;

try {
  const { data: company, error: companyError } = await s.from('companies').select('id').limit(1).maybeSingle();
  if (companyError || !company?.id) throw new Error('no staging company available for synthetic FK-safe proof');
  const companyId = company.id;
  await s.from('coach_assignments').delete().eq('call_id', CALL);
  await s.from('calls').delete().eq('id', CALL);
  const { error: callError } = await s.from('calls').insert({
    id: CALL, user_id: REP, org_id: companyId, company_id: companyId,
    filename: 'DAY299-critical-assignment-proof.wav',
    storage_path: 'synthetic/day299-critical-assignment-proof.wav',
    audio_path: 'synthetic/day299-critical-assignment-proof.wav', status: 'scored',
    score_overall: 20, created_at: new Date().toISOString(),
  });
  if (callError) throw callError;

  const { data, error } = await s.from('coach_assignments').insert({
    call_id: CALL,
    assignee_user_id: REP,
    drill_id: 'critical-call-review',
    notes: 'DAY299 canonical payload proof',
    org_id: companyId,
    status: 'open',
  }).select('id,call_id,assignee_user_id,drill_id,org_id,status').single();
  if (error) throw error;
  assignmentId = data?.id || null;
  pass = Boolean(assignmentId && data.call_id === CALL && data.assignee_user_id === REP && data.status === 'open');
  console.log(`${pass ? '✓' : '✗'} canonical coach_assignments payload inserts and reads back`);
} finally {
  if (assignmentId) await s.from('coach_assignments').delete().eq('id', assignmentId);
  await s.from('coach_assignments').delete().eq('call_id', CALL);
  await s.from('calls').delete().eq('id', CALL);
  const { data: assignmentResidue } = await s.from('coach_assignments').select('id').eq('call_id', CALL);
  const { data: callResidue } = await s.from('calls').select('id').eq('id', CALL);
  const clean = (assignmentResidue || []).length === 0 && (callResidue || []).length === 0;
  console.log(`${clean ? '✓' : '✗'} zero DAY299 residue`);
  pass = pass && clean;
}

console.log(`\n${pass ? 'PASS' : 'FAIL'}`);
process.exit(pass ? 0 : 1);
