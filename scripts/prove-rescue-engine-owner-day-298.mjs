/** Staging-only deployed proof for Day 298. Synthetic data; fail-safe cleanup. */
import { createClient } from '@supabase/supabase-js';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const anon = process.env.SUPABASE_ANON_KEY;
const ref = (url || '').match(/https:\/\/([a-z0-9]+)\./)?.[1];
const web = process.env.STAGING_WEB_BASE || 'https://gravix-sales-trainer-web-staging.vercel.app';
const email = (process.env.STAGING_QA_EMAIL || '').toLowerCase();
const password = process.env.STAGING_QA_PASSWORD;

if (process.env.APP_ENV !== 'staging' || ref !== process.env.EXPECTED_STAGING_SUPABASE_REF || ref === process.env.PROD_SUPABASE_REF) {
  console.error('GUARD: refusing non-staging target'); process.exit(1);
}
if (!key || !anon || !email || !password) { console.error('Missing guarded staging inputs'); process.exit(1); }

const OWNED = '00000000-2980-4000-8000-000000000001';
const UNOWNED = '00000000-2980-4000-8000-000000000002';
const FOREIGN = '00000000-2980-4000-8000-000000000003';
const ids = [OWNED, UNOWNED, FOREIGN];
const s = createClient(url, key, { auth: { persistSession: false } });

const gates = [];
const gate = (name, ok) => { gates.push(ok); console.log(`${ok ? '✓' : '✗'} ${name}`); };

try {
  const { data: profile } = await s.from('profiles').select('user_id').eq('email', email).maybeSingle();
  const authId = profile?.user_id;
  if (!authId) throw new Error('guarded QA profile absent');
  const { data: rep } = await s.from('reps').select('id,company_id,name,email').eq('id', authId).maybeSingle();
  if (!rep?.company_id) throw new Error('guarded QA rep bridge absent');
  const { data: usersRow } = await s.from('users').select('id').eq('id', authId).maybeSingle();

  await s.from('accounts').delete().in('id', ids);
  const now = new Date().toISOString();
  const foreignCompany = '00000000-2980-4000-8000-000000000099';
  const { error: insertError } = await s.from('accounts').insert([
    { id: OWNED, org_id: rep.company_id, name: 'DAY298 Owned Rescue', domain: 'day298-owned.invalid', owner_id: authId, created_at: now, updated_at: now },
    { id: UNOWNED, org_id: rep.company_id, name: 'DAY298 Unowned Rescue', domain: 'day298-unowned.invalid', owner_id: null, created_at: now, updated_at: now },
    { id: FOREIGN, org_id: foreignCompany, name: 'DAY298 Foreign Rescue', domain: 'day298-foreign.invalid', owner_id: authId, created_at: now, updated_at: now },
  ]);
  if (insertError) throw insertError;

  const pub = createClient(url, anon, { auth: { persistSession: false } });
  const { data: signed, error: signError } = await pub.auth.signInWithPassword({ email, password });
  const token = signed?.session?.access_token;
  if (signError || !token) throw new Error('guarded QA sign-in failed');

  const get = async (id) => {
    const response = await fetch(`${web}/api/proxy/v1/accounts/${id}/rescue-engine`, {
      headers: { authorization: `Bearer ${token}` },
    });
    let body = {}; try { body = await response.json(); } catch {}
    return { status: response.status, body };
  };

  const owned = await get(OWNED);
  const unowned = await get(UNOWNED);
  const foreign = await get(FOREIGN);

  gate('QA identity is auth-first (no public.users row)', !usersRow);
  gate('owned rescue returns 200 through deployed WEB proxy', owned.status === 200 && owned.body?.ok === true);
  gate('owned rescue returns canonical reps owner id', owned.body?.owner?.id === authId);
  gate('owned rescue returns a usable owner name', Boolean(owned.body?.owner?.full_name));
  gate('unowned rescue remains 200 with null owner', unowned.status === 200 && unowned.body?.owner === null);
  gate('foreign-company rescue remains non-leaking 404', foreign.status === 404 && foreign.body?.error === 'account_not_found');
} finally {
  await s.from('accounts').delete().in('id', ids);
  const { data: residue } = await s.from('accounts').select('id').in('id', ids);
  gate('zero DAY298 account residue', (residue || []).length === 0);
}

const pass = gates.every(Boolean);
console.log(`\n${pass ? 'PASS' : 'FAIL'} — ${gates.filter(Boolean).length}/${gates.length}`);
process.exit(pass ? 0 : 1);
