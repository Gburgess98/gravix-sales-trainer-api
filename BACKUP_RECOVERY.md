# Backup and Recovery — Gravix Sales Trainer API

**Last updated:** 2026-06-09

---

## What Needs Protecting

| Asset | Location | Criticality |
|---|---|---|
| PostgreSQL database | Supabase managed Postgres | Critical |
| Call audio files | Supabase Storage (`calls` bucket) | High |
| Environment variables | Railway / Vercel | High |
| Application code | GitHub `main` branch | Medium |

---

## Database Backups

### Automatic backups (Supabase managed)

Supabase Pro and above provides:
- **Daily backups** retained for 7 days (Free), 30 days (Pro)
- **Point-in-time recovery (PITR)** on Pro+ plans — restore to any second in the last 7 days
- Backups are stored in a different availability zone from the primary

**Check your plan:** Supabase Dashboard → Settings → Backups

### Manual backup procedure

Run before any destructive migration or schema change:

```bash
# Requires pg_dump and the DATABASE_URL from Supabase Settings → Database
pg_dump "$DATABASE_URL" \
  --no-owner \
  --no-acl \
  --format=custom \
  --file="gravix-backup-$(date +%Y%m%d-%H%M%S).dump"
```

Store the dump file in a separate location (S3, local encrypted drive) — not in the repo.

### What to back up manually

```bash
# Schema only (safe to commit to docs/)
pg_dump "$DATABASE_URL" --schema-only --no-owner > docs/schema-$(date +%Y%m%d).sql

# Data for critical tables
pg_dump "$DATABASE_URL" --data-only \
  --table=public.reps \
  --table=public.companies \
  --table=public.partners \
  > docs/seed-data-$(date +%Y%m%d).sql
```

---

## Supabase Storage Backups

Call audio files in the `calls` bucket are not automatically backed up by Supabase on the Free plan.

### Manual bucket backup

```bash
# Install Supabase CLI first: npm install -g supabase
# Then download all files in the bucket:
supabase storage download --project-ref <project-ref> --bucket calls --output ./backups/calls/
```

Alternatively, use the Supabase Management API to list and download objects.

### Recovery point objective (RPO)

| Asset | Current RPO | Target RPO |
|---|---|---|
| Database (Supabase Pro) | 24 hours (daily backup) | 1 hour (PITR) |
| Call audio | None | 24 hours |
| Config / env vars | Manual | On change |

---

## Recovery Procedures

### Restore from Supabase backup

1. Supabase Dashboard → Settings → Backups
2. Select the backup point
3. Click "Restore" — this creates a new project or restores in-place
4. Update `SUPABASE_URL` in Railway/Vercel if a new project was created
5. Verify via `npm run validate:auth` and `npm run validate:lighthouse`

### Restore from manual pg_dump

```bash
pg_restore \
  --dbname "$DATABASE_URL" \
  --no-owner \
  --clean \
  --if-exists \
  gravix-backup-YYYYMMDD-HHMMSS.dump
```

**Warning:** `--clean` drops and recreates all objects. Run against a staging Supabase project first.

### Environment variable recovery

All env vars should be documented (without values) in `.env.example` in the repo. Values should be stored in a password manager (1Password, Bitwarden) under a shared Gravix vault entry. If Railway is lost:

1. Create a new Railway service
2. Restore vars from the password manager
3. Re-link the GitHub repo
4. Trigger a deploy

### Code recovery

Code is on GitHub. If the main branch is lost:

```bash
# Restore from a local clone
git push --force origin main
```

---

## Disaster Recovery Runbook

### Full environment loss (worst case)

1. **Database** — restore from Supabase backup or pg_dump
2. **Supabase project** — create a new project, restore dump, copy Storage bucket files
3. **Railway/Vercel** — create new service, set env vars from password manager, connect to GitHub, deploy
4. **DNS** — update CNAME to point to new Railway service URL (if using custom domain)
5. **Verify** — run all validation scripts against the new environment

Expected RTO (Recovery Time Objective): 4–8 hours for full restoration, < 1 hour for database-only.

---

## Backup Verification Schedule

| Task | Frequency | Owner |
|---|---|---|
| Confirm Supabase auto-backup running | Monthly | Engineering |
| Manual pg_dump | Before each major release | Engineering |
| Test restore to staging | Quarterly | Engineering |
| Verify env vars in password manager | After any change | Engineering |
| Review RPO / RTO targets | Annually | Engineering |
