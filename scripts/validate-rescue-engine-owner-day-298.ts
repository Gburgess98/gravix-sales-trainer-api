/**
 * Day 298 — rescue-engine owner schema drift regression.
 *
 * accounts.owner_id is a reps.id. The old rescue path queried users.full_name,
 * which is absent from the schema and made owned-account rescue requests fail.
 * The canonical Day-284 resolver is company-scoped and already returns the
 * response shape the rescue UI expects.
 */
import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const accounts = fs.readFileSync(path.join(root, 'src/routes/accounts.ts'), 'utf8');
const validator = fs.readFileSync(path.join(root, 'scripts/validate-schema-selects.ts'), 'utf8');
const start = accounts.indexOf("router.get('/:id/rescue-engine'");
const end = accounts.indexOf("router.", start + 20);
const rescue = accounts.slice(start, end > start ? end : undefined);

let failures = 0;
const gate = (label: string, ok: boolean) => {
  console.log(`${ok ? '✓' : '✗'} ${label}`);
  if (!ok) failures += 1;
};

console.log('Day 298 — rescue-engine owner uses canonical reps contract\n');

gate('rescue route found', start >= 0);
gate('owner lookup uses resolveOwnerRep(owner_id, requester company)',
  /resolveOwnerRep\(\s*account\.owner_id\s*,\s*requester\.company_id\s*\)/.test(rescue));
gate('rescue route does not query the users table',
  !/\.from\(\s*['"]users['"]\s*\)/.test(rescue));
gate('rescue route does not select users.full_name',
  !/select\([^)]*full_name[^)]*\)/.test(rescue));
gate('canonical resolver reads reps and enforces company scope',
  /function resolveOwnerRep[\s\S]*?\.from\(\s*['"]reps['"]\s*\)[\s\S]*?rep\.company_id[\s\S]*?companyId/.test(accounts));
gate('owner response consumes the resolved object directly',
  /owner:\s*ownerRes\s*\|\|\s*null/.test(rescue));
gate('final users.full_name known-drift baseline removed',
  !/["']src\/routes\/accounts\.ts\|users\|full_name["']/.test(validator));
gate('schema validator still fails stale and new drift',
  /staleBaseline\.length[\s\S]{0,500}process\.exit\(1\)/.test(validator) &&
  /newFindings\.length[\s\S]{0,400}process\.exit\(1\)/.test(validator));

console.log(`\n${failures === 0 ? 'PASS' : 'FAIL'} — ${failures} gate failure(s).`);
process.exit(failures === 0 ? 0 : 1);
