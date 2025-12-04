-- api/sql/20251106_idx_xp_events.sql
BEGIN;
CREATE INDEX IF NOT EXISTS idx_xp_events_rep_created
  ON public.xp_events (rep_id, created_at DESC);
COMMIT;