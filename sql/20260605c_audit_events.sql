-- 20260605c_audit_events.sql
-- Audit event log for the partner hierarchy.
-- New, clean table — does not replace the existing audit_logs table.
-- Safe to run multiple times (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS public.audit_events (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_user_id   UUID        NOT NULL,
  target_user_id  UUID        NULL,
  action          TEXT        NOT NULL,
  entity_type     TEXT        NULL,
  entity_id       UUID        NULL,
  metadata        JSONB       NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_created_at     ON public.audit_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_actor_user_id  ON public.audit_events(actor_user_id);
CREATE INDEX IF NOT EXISTS idx_audit_events_target_user_id ON public.audit_events(target_user_id) WHERE target_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_events_action         ON public.audit_events(action);
