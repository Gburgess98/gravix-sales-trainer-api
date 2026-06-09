-- 20260609_licence_pools.sql
-- Licence pool foundation.
--
-- licence_pools  — one row per partner, tracks total purchased seats
-- company_licences — one row per company, tracks how many seats are allocated
--
-- available = pool.purchased - SUM(company_licences.allocated for that partner)
--
-- No Stripe, no invoicing. Read-only from the API.
-- Safe to run multiple times (IF NOT EXISTS / ON CONFLICT).

-- ── 1. licence_pools ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.licence_pools (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_id  UUID        NOT NULL REFERENCES public.partners(id) ON DELETE CASCADE,
  plan_type   TEXT        NOT NULL DEFAULT 'standard',
  purchased   INTEGER     NOT NULL DEFAULT 0 CHECK (purchased >= 0),
  reserved    INTEGER     NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  renewed_at  TIMESTAMPTZ,
  expires_at  TIMESTAMPTZ
);

-- One pool per partner
CREATE UNIQUE INDEX IF NOT EXISTS idx_licence_pools_partner
  ON public.licence_pools(partner_id);

CREATE INDEX IF NOT EXISTS idx_licence_pools_expires
  ON public.licence_pools(expires_at);

-- ── 2. company_licences ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.company_licences (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_id  UUID        NOT NULL REFERENCES public.partners(id) ON DELETE CASCADE,
  company_id  UUID        NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  allocated   INTEGER     NOT NULL DEFAULT 0 CHECK (allocated >= 0),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One allocation row per company
CREATE UNIQUE INDEX IF NOT EXISTS idx_company_licences_company
  ON public.company_licences(company_id);

CREATE INDEX IF NOT EXISTS idx_company_licences_partner
  ON public.company_licences(partner_id);

-- ── 3. Seed: Gravix partner pool ─────────────────────────────────────────────
-- Gravix partner id is 5055e1b6-fb33-45c0-959a-d7dd45f98a13 (see 20260605_partner_foundation.sql)

INSERT INTO public.licence_pools (partner_id, plan_type, purchased, reserved)
VALUES ('5055e1b6-fb33-45c0-959a-d7dd45f98a13', 'standard', 50, 0)
ON CONFLICT (partner_id) DO NOTHING;

-- ── 4. Seed: one allocation row per company under Gravix ─────────────────────
-- allocated = 5 as a starting placeholder; adjust per-company as needed.

INSERT INTO public.company_licences (partner_id, company_id, allocated)
SELECT
  c.partner_id,
  c.id,
  5
FROM public.companies c
WHERE c.partner_id = '5055e1b6-fb33-45c0-959a-d7dd45f98a13'
ON CONFLICT (company_id) DO NOTHING;
