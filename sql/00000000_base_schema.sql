


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public";






CREATE OR REPLACE FUNCTION "public"."calls_set_defaults"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  if (NEW.storage_path is null or NEW.storage_path = '') then
    NEW.storage_path := NEW.audio_path;
  end if;

  if (NEW.filename is null or NEW.filename = '') then
    NEW.filename := gravix_basename(NEW.audio_path);
  end if;

  if (NEW.mime_type is null or NEW.mime_type = '') then
    -- basic guess by extension; good enough for MVP
    if right(lower(NEW.audio_path), 4) = '.mp3' then
      NEW.mime_type := 'audio/mpeg';
    elsif right(lower(NEW.audio_path), 4) = '.wav' then
      NEW.mime_type := 'audio/wav';
    elsif right(lower(NEW.audio_path), 4) = '.m4a' then
      NEW.mime_type := 'audio/mp4';
    else
      NEW.mime_type := 'application/octet-stream';
    end if;
  end if;

  -- numeric fallbacks
  if NEW.filesize_bytes is null then NEW.filesize_bytes := 0; end if;
  if NEW.duration_ms   is null then NEW.duration_ms   := 0; end if;
  if NEW.sample_rate_hz is null then NEW.sample_rate_hz := 0; end if;
  if NEW.channels      is null then NEW.channels      := 1; end if;

  return NEW;
end;
$$;


ALTER FUNCTION "public"."calls_set_defaults"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."calls_set_filename"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  if NEW.filename is null or NEW.filename = '' then
    NEW.filename := gravix_basename(NEW.audio_path);
  end if;
  return NEW;
end;
$$;


ALTER FUNCTION "public"."calls_set_filename"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."calls_set_storage_path"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  if (NEW.storage_path is null or NEW.storage_path = '') then
    NEW.storage_path := NEW.audio_path;
  end if;
  return NEW;
end;
$$;


ALTER FUNCTION "public"."calls_set_storage_path"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."fn_activity_on_assignment"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  insert into public.activities(type, summary, call_id, created_at)
  values ('assign_created',
          coalesce('Assigned drill: ' || new.drill_id, 'Assignment created'),
          new.call_id,
          now());
  return new;
end;
$$;


ALTER FUNCTION "public"."fn_activity_on_assignment"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."gravix_basename"("p" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    AS $$
  select split_part(p, '/', array_length(string_to_array(p, '/'), 1));
$$;


ALTER FUNCTION "public"."gravix_basename"("p" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."handle_new_user"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
BEGIN

  INSERT INTO public.reps (
      id,
      name,
      tier,
      xp,
      org_id,
      created_at
  )
  VALUES (
      NEW.id,
      COALESCE(
          NEW.raw_user_meta_data->>'full_name',
          split_part(NEW.email,'@',1)
      ),
      'SalesRep',
      0,
      '89f61a54-dc76-4ce8-b408-500afd5bdcdb',
      now()
  );

  RETURN NEW;

END;
$$;


ALTER FUNCTION "public"."handle_new_user"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."increment_rep_xp"("p_delta" integer, "p_rep_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
begin
  update reps set xp = coalesce(xp,0) + p_delta where id = p_rep_id;
end;
$$;


ALTER FUNCTION "public"."increment_rep_xp"("p_delta" integer, "p_rep_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."match_knowledge_embeddings"("query_embedding" "public"."vector", "match_count" integer DEFAULT 8, "filter_company_id" "uuid" DEFAULT NULL::"uuid", "filter_user_id" "uuid" DEFAULT NULL::"uuid", "filter_source_types" "text"[] DEFAULT NULL::"text"[], "filter_stage" "text" DEFAULT NULL::"text") RETURNS TABLE("id" "uuid", "company_id" "uuid", "user_id" "uuid", "source_type" "text", "source_id" "uuid", "stage" "text", "title" "text", "content" "text", "metadata" "jsonb", "similarity" double precision)
    LANGUAGE "sql"
    AS $$
  select
    ke.id,
    ke.company_id,
    ke.user_id,
    ke.source_type,
    ke.source_id,
    ke.stage,
    ke.title,
    ke.content,
    ke.metadata,
    1 - (ke.embedding <=> query_embedding) as similarity
  from public.knowledge_embeddings ke
  where
    (filter_company_id is null or ke.company_id = filter_company_id)
    and (filter_user_id is null or ke.user_id = filter_user_id)
    and (filter_source_types is null or ke.source_type = any(filter_source_types))
    and (filter_stage is null or ke.stage = filter_stage)
  order by ke.embedding <=> query_embedding
  limit match_count;
$$;


ALTER FUNCTION "public"."match_knowledge_embeddings"("query_embedding" "public"."vector", "match_count" integer, "filter_company_id" "uuid", "filter_user_id" "uuid", "filter_source_types" "text"[], "filter_stage" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_sparring_personas_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  new.updated_at = now();
  return new;
end;
$$;


ALTER FUNCTION "public"."set_sparring_personas_updated_at"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."set_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  new.updated_at = now();
  return new;
end;
$$;


ALTER FUNCTION "public"."set_updated_at"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."tg_set_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  new.updated_at = now();
  return new;
end
$$;


ALTER FUNCTION "public"."tg_set_updated_at"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."trg_assign_created"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  insert into activities (
    org_id,
    actor_user_id,
    call_id,
    type,
    summary,
    created_at
  )
  values (
    new.org_id,
    new.assignee_user_id,
    new.call_id,
    'coach_assignment',
    concat('📝 Drill assigned: ', coalesce(new.drill_id, 'drill')),
    now()
  );
  return new;
end;
$$;


ALTER FUNCTION "public"."trg_assign_created"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."trg_assign_status"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
declare
  msg text;
begin
  if new.status = 'completed' and (old.status is distinct from new.status) then
    msg := concat('✅ Drill completed: ', coalesce(new.drill_id, 'drill'));
  elsif new.status = 'open' and (old.status is distinct from new.status) then
    msg := concat('↩️ Drill reopened: ', coalesce(new.drill_id, 'drill'));
  else
    return new;
  end if;

  insert into activities (
    org_id,
    actor_user_id,
    call_id,
    type,
    summary,
    created_at
  )
  values (
    new.org_id,
    new.assignee_user_id,
    new.call_id,
    'coach_assignment',
    msg,
    now()
  );

  return new;
end;
$$;


ALTER FUNCTION "public"."trg_assign_status"() OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."account_ai_summaries" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "account_id" "uuid" NOT NULL,
    "summary" "text",
    "health_status" "text",
    "churn_risk" numeric DEFAULT 0,
    "next_best_action" "text",
    "manager_notes" "text",
    "generated_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."account_ai_summaries" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."account_ai_tasks" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "account_id" "uuid" NOT NULL,
    "title" "text" NOT NULL,
    "description" "text",
    "category" "text",
    "urgency" "text" DEFAULT 'medium'::"text",
    "status" "text" DEFAULT 'open'::"text",
    "assigned_to" "uuid",
    "escalation_source" "text",
    "generated_by" "uuid",
    "due_at" timestamp with time zone,
    "completed_at" timestamp with time zone,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."account_ai_tasks" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."account_coaching_actions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "account_id" "uuid" NOT NULL,
    "company_id" "uuid",
    "office_id" "uuid",
    "action_type" "text",
    "title" "text" NOT NULL,
    "description" "text",
    "urgency" "text" DEFAULT 'medium'::"text",
    "status" "text" DEFAULT 'open'::"text",
    "assigned_to" "uuid",
    "linked_escalation_id" "uuid",
    "linked_task_id" "uuid",
    "replay_call_id" "uuid",
    "sparring_scenario" "text",
    "manager_notes" "text",
    "due_at" timestamp with time zone,
    "completed_at" timestamp with time zone,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb",
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."account_coaching_actions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."account_escalations" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "account_id" "uuid" NOT NULL,
    "company_id" "uuid",
    "office_id" "uuid",
    "severity" "text" DEFAULT 'high'::"text",
    "status" "text" DEFAULT 'open'::"text",
    "escalation_reason" "text",
    "intervention_required" boolean DEFAULT true,
    "assigned_manager_id" "uuid",
    "triggered_by" "uuid",
    "workflow_stage" "text",
    "metadata" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."account_escalations" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."accounts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "domain" "text",
    "owner_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."accounts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."activities" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "type" "text" NOT NULL,
    "call_id" "uuid",
    "contact_id" "uuid",
    "account_id" "uuid",
    "opportunity_id" "uuid",
    "summary" "text",
    "data" "jsonb",
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "actor_user_id" "uuid",
    "payload" "jsonb" DEFAULT '{}'::"jsonb",
    CONSTRAINT "activities_type_check" CHECK (("type" = ANY (ARRAY['call_uploaded'::"text", 'score_posted'::"text", 'assignment_create'::"text", 'assignment_complete'::"text", 'sparring_session'::"text"])))
);


ALTER TABLE "public"."activities" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."admin_config" (
    "id" boolean DEFAULT true NOT NULL,
    "streak_threshold" integer DEFAULT 3 NOT NULL,
    "xp_multiplier" numeric DEFAULT 1.0 NOT NULL,
    "comeback_bonus" integer DEFAULT 50 NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."admin_config" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."assignments" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "rep_id" "uuid" NOT NULL,
    "type" "text" NOT NULL,
    "target_id" "text",
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "due_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "manager_id" "uuid" NOT NULL,
    "title" "text" DEFAULT ''::"text" NOT NULL,
    "completed_by" "text",
    "office_id" "text",
    "company_id" "uuid",
    "source" "text",
    "meta" "jsonb",
    CONSTRAINT "assignments_status_check" CHECK (("status" = ANY (ARRAY['pending'::"text", 'assigned'::"text", 'completed'::"text"]))),
    CONSTRAINT "assignments_type_check" CHECK (("type" = ANY (ARRAY['call_review'::"text", 'sparring'::"text", 'custom'::"text"])))
);


ALTER TABLE "public"."assignments" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."audit_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "target_user_id" "uuid",
    "action" "text" NOT NULL,
    "entity_type" "text",
    "entity_id" "uuid",
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."audit_events" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."audit_logs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "action" "text" NOT NULL,
    "actor_id" "uuid" NOT NULL,
    "actor_type" "text" NOT NULL,
    "target_type" "text",
    "target_id" "text",
    "company_id" "uuid",
    "office_id" "uuid",
    "metadata" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."audit_logs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."badges" (
    "id" "text" NOT NULL,
    "label" "text" NOT NULL,
    "icon" "text" NOT NULL,
    "description" "text",
    "rule" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."badges" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."bounties" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "title_id" "text",
    "name" "text" NOT NULL,
    "description" "text",
    "prize_money" numeric(10,2),
    "currency" "text" DEFAULT 'GBP'::"text",
    "starts_at" timestamp with time zone,
    "ends_at" timestamp with time zone,
    "org_id" "uuid",
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."bounties" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."call_chunks" (
    "id" bigint NOT NULL,
    "call_id" "uuid",
    "start_ms" integer,
    "end_ms" integer,
    "speaker" "text",
    "text" "text",
    "embedding" "public"."vector"(1536)
);


ALTER TABLE "public"."call_chunks" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."call_chunks_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."call_chunks_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."call_chunks_id_seq" OWNED BY "public"."call_chunks"."id";



CREATE TABLE IF NOT EXISTS "public"."call_manager_reviews" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "manager_id" "uuid" NOT NULL,
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "status" "text" DEFAULT 'reviewed'::"text" NOT NULL,
    "note" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."call_manager_reviews" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."call_pins" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "t_sec" numeric(10,3) NOT NULL,
    "label" "text",
    "note" "text",
    "color" "text" DEFAULT 'amber'::"text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "call_pins_t_sec_check" CHECK (("t_sec" >= (0)::numeric))
);


ALTER TABLE "public"."call_pins" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."call_scores" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "rubric_version" "text",
    "ai_model" "text",
    "overall" integer,
    "rubric" "jsonb",
    "user_id" "uuid" NOT NULL
);


ALTER TABLE "public"."call_scores" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."calls" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "filename" "text",
    "storage_path" "text" NOT NULL,
    "mime_type" "text" DEFAULT 'audio/mpeg'::"text",
    "size_bytes" bigint DEFAULT 0,
    "sha256" "text",
    "duration_seconds" numeric,
    "kind" "text",
    "status" "text" DEFAULT 'uploaded'::"text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "org_id" "uuid" NOT NULL,
    "title" "text",
    "audio_path" "text" NOT NULL,
    "transcript_path" "text",
    "filesize_bytes" bigint DEFAULT 0,
    "duration_ms" integer DEFAULT 0,
    "sample_rate_hz" integer DEFAULT 0,
    "channels" integer DEFAULT 1,
    "checksum" "text",
    "score_overall" numeric,
    "rubric" "jsonb",
    "ai_model" "text",
    "scored_at" timestamp with time zone,
    "updated_at" timestamp with time zone,
    "duration_sec" integer,
    "rubric_version" "text",
    "account_id" "uuid",
    "contact_id" "uuid",
    "opportunity_id" "uuid",
    "customer_name" "text",
    "rep_name" "text",
    "tags" "text"[],
    "summary" "text",
    "flags" "text"[],
    "transcript" "text",
    "analysis_json" "jsonb",
    "company_id" "uuid",
    "office_id" "uuid",
    CONSTRAINT "calls_kind_check" CHECK (("kind" = ANY (ARRAY['audio'::"text", 'json'::"text"])))
);


ALTER TABLE "public"."calls" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."coach_assignments" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "assignee_user_id" "uuid",
    "drill_id" "text" NOT NULL,
    "notes" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "status" "text" DEFAULT 'open'::"text" NOT NULL,
    "completed_at" timestamp with time zone,
    "org_id" "uuid"
);


ALTER TABLE "public"."coach_assignments" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."coach_notes" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "notes" "text",
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."coach_notes" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."companies" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "tmc_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "slug" "text" NOT NULL,
    "settings_json" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "partner_id" "uuid",
    "website" "text",
    "industry" "text",
    "phone_number" "text",
    "address" "text",
    "is_active" boolean DEFAULT true NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."companies" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."company_context" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "version" integer DEFAULT 0 NOT NULL,
    "context" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "compiled_context" "text",
    "created_by" "uuid",
    "updated_by" "uuid",
    "published_by" "uuid",
    "published_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "archived_at" timestamp with time zone,
    CONSTRAINT "company_context_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'published'::"text", 'archived'::"text"])))
);


ALTER TABLE "public"."company_context" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."company_licences" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "partner_id" "uuid" NOT NULL,
    "company_id" "uuid" NOT NULL,
    "allocated" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "company_licences_allocated_check" CHECK (("allocated" >= 0))
);


ALTER TABLE "public"."company_licences" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."company_playbook" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "stage" "text",
    "title" "text",
    "description" "text",
    "example" "text",
    "embedding" "public"."vector"(1536),
    "created_at" timestamp with time zone DEFAULT "now"(),
    "guidance" "text",
    "objection_type" "text",
    "priority" integer DEFAULT 100 NOT NULL,
    "is_active" boolean DEFAULT true NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."company_playbook" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."contacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "account_id" "uuid",
    "first_name" "text",
    "last_name" "text",
    "email" "text",
    "phone" "text",
    "title" "text",
    "owner_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "last_contacted_at" timestamp with time zone,
    "tags" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL
);


ALTER TABLE "public"."contacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_accounts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "name" "text",
    "domain" "text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "company_id" "uuid"
);


ALTER TABLE "public"."crm_accounts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_actions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "contact_id" "text" NOT NULL,
    "status" "text" DEFAULT 'open'::"text" NOT NULL,
    "type" "text" NOT NULL,
    "title" "text" NOT NULL,
    "due_at" timestamp with time zone,
    "importance" "text" DEFAULT 'normal'::"text" NOT NULL,
    "completed_at" timestamp with time zone,
    "meta" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."crm_actions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_activities" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "type" "text" NOT NULL,
    "title" "text" NOT NULL,
    "status" "text" DEFAULT 'open'::"text",
    "due_at" timestamp with time zone,
    "opportunity_id" "uuid",
    "contact_id" "uuid",
    "account_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "org_id" "uuid",
    "rep_id" "uuid",
    "call_id" "uuid",
    "source" "text",
    "meta" "jsonb"
);


ALTER TABLE "public"."crm_activities" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_auto_assign_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "mode" "text" NOT NULL,
    "started_at" timestamp with time zone NOT NULL,
    "finished_at" timestamp with time zone,
    "totals" "jsonb",
    "reps" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "requester_id" "uuid",
    "org_id" "uuid",
    "requested_by" "uuid",
    "source" "text",
    "preview" "jsonb",
    CONSTRAINT "crm_auto_assign_runs_mode_check" CHECK (("mode" = ANY (ARRAY['dry_run'::"text", 'execute'::"text"])))
);


ALTER TABLE "public"."crm_auto_assign_runs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_call_links" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "contact_id" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."crm_call_links" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_contact_notes" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "contact_id" "text" NOT NULL,
    "body" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "author_id" "uuid",
    "author_name" "text",
    "importance" "text" DEFAULT 'normal'::"text" NOT NULL,
    CONSTRAINT "crm_contact_notes_body_nonempty" CHECK (("length"(TRIM(BOTH FROM "body")) > 0)),
    CONSTRAINT "crm_contact_notes_importance_check" CHECK (("importance" = ANY (ARRAY['normal'::"text", 'important'::"text", 'critical'::"text"])))
);


ALTER TABLE "public"."crm_contact_notes" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_contacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "user_id" "uuid",
    "email" "text",
    "first_name" "text",
    "last_name" "text",
    "company" "text",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "last_contacted_at" timestamp with time zone
);


ALTER TABLE "public"."crm_contacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_demo_call_links" (
    "user_id" "uuid" NOT NULL,
    "call_id" "text" NOT NULL,
    "contact_id" "uuid" NOT NULL,
    "account_id" "uuid",
    "opportunity_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."crm_demo_call_links" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."crm_opportunities" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "org_id" "uuid",
    "name" "text" NOT NULL,
    "title" "text",
    "stage" "text" DEFAULT 'new'::"text" NOT NULL,
    "amount" numeric,
    "currency" "text",
    "account_id" "uuid",
    "contact_id" "uuid",
    "account_name" "text",
    "contact_email" "text",
    "close_date" "date",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."crm_opportunities" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."internal_users" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "email" "text" NOT NULL,
    "role" "text" NOT NULL,
    "is_internal" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"(),
    CONSTRAINT "internal_users_role_check" CHECK (("role" = ANY (ARRAY['support'::"text", 'super_admin'::"text"])))
);


ALTER TABLE "public"."internal_users" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "kind" "text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "attempts" integer DEFAULT 0 NOT NULL,
    "error" "text",
    "result" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "jobs_kind_check" CHECK (("kind" = ANY (ARRAY['transcribe'::"text", 'score'::"text"]))),
    CONSTRAINT "jobs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'processing'::"text", 'succeeded'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."jobs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."knowledge_embeddings" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid",
    "user_id" "uuid",
    "source_type" "text" NOT NULL,
    "source_id" "uuid",
    "stage" "text",
    "title" "text",
    "content" "text" NOT NULL,
    "embedding" "public"."vector"(1536) NOT NULL,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "knowledge_embeddings_source_type_check" CHECK (("source_type" = ANY (ARRAY['company_playbook'::"text", 'rep_memory'::"text", 'call'::"text", 'manual_note'::"text"]))),
    CONSTRAINT "knowledge_embeddings_stage_check" CHECK (("stage" = ANY (ARRAY['intro'::"text", 'discovery'::"text", 'objection'::"text", 'close'::"text", 'general'::"text"])))
);


ALTER TABLE "public"."knowledge_embeddings" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."licence_pools" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "partner_id" "uuid" NOT NULL,
    "plan_type" "text" DEFAULT 'standard'::"text" NOT NULL,
    "purchased" integer DEFAULT 0 NOT NULL,
    "reserved" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "renewed_at" timestamp with time zone,
    "expires_at" timestamp with time zone,
    CONSTRAINT "licence_pools_purchased_check" CHECK (("purchased" >= 0)),
    CONSTRAINT "licence_pools_reserved_check" CHECK (("reserved" >= 0))
);


ALTER TABLE "public"."licence_pools" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."objection_evidence" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "objection_id" "uuid" NOT NULL,
    "call_id" "uuid",
    "rep_id" "uuid",
    "phrase" "text",
    "source" "text" DEFAULT 'manual'::"text" NOT NULL,
    "confidence" numeric,
    "occurred_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "objection_evidence_source_chk" CHECK (("source" = ANY (ARRAY['manual'::"text", 'suggestion'::"text", 'moment_match'::"text"])))
);


ALTER TABLE "public"."objection_evidence" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."objection_library_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "label" "text" NOT NULL,
    "category" "text" DEFAULT 'other'::"text" NOT NULL,
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "buyer_phrases" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "why_it_matters" "text",
    "approved_response" "text",
    "weak_response_patterns" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "no_go_language" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "coaching_note" "text",
    "linked_scorecard_criterion_id" "uuid",
    "linked_trigger_key" "text",
    "created_by" "uuid",
    "updated_by" "uuid",
    "approved_by" "uuid",
    "approved_at" timestamp with time zone,
    "archived_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "objection_items_category_chk" CHECK (("category" = ANY (ARRAY['price'::"text", 'timing'::"text", 'authority'::"text", 'trust'::"text", 'competitor'::"text", 'fit'::"text", 'logistics'::"text", 'other'::"text"]))),
    CONSTRAINT "objection_items_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'approved'::"text", 'archived'::"text"])))
);


ALTER TABLE "public"."objection_library_items" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."objection_suggestion_decisions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "suggestion_key" "text" NOT NULL,
    "decision" "text" NOT NULL,
    "objection_id" "uuid",
    "decided_by" "uuid",
    "decided_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "reason" "text",
    CONSTRAINT "objection_suggestion_decisions_chk" CHECK (("decision" = ANY (ARRAY['approved'::"text", 'merged'::"text", 'dismissed'::"text"])))
);


ALTER TABLE "public"."objection_suggestion_decisions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."offices" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "slug" "text" NOT NULL,
    "settings_json" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."offices" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."opportunities" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid" NOT NULL,
    "account_id" "uuid",
    "name" "text" NOT NULL,
    "stage" "text" DEFAULT 'prospect'::"text" NOT NULL,
    "amount" numeric,
    "currency" "text" DEFAULT 'GBP'::"text",
    "close_date" "date",
    "owner_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "opportunities_stage_check" CHECK (("stage" = ANY (ARRAY['prospect'::"text", 'qualified'::"text", 'proposal'::"text", 'negotiation'::"text", 'closed_won'::"text", 'closed_lost'::"text"])))
);


ALTER TABLE "public"."opportunities" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."org_limits" (
    "org_id" "uuid" NOT NULL,
    "max_users" integer,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."org_limits" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."orgs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."orgs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."partners" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "slug" "text" NOT NULL,
    "status" "text" DEFAULT 'active'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."partners" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."pins" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "call_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "t" integer NOT NULL,
    "note" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."pins" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."profiles" (
    "user_id" "uuid" NOT NULL,
    "role" "text" DEFAULT 'rep'::"text",
    "team_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "email" "text",
    "full_name" "text",
    "manager_id" "uuid",
    CONSTRAINT "profiles_role_check" CHECK (("role" = ANY (ARRAY['rep'::"text", 'manager'::"text"])))
);


ALTER TABLE "public"."profiles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."rep_memory" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "avg_score" numeric,
    "intro_score" numeric,
    "discovery_score" numeric,
    "objection_score" numeric,
    "close_score" numeric,
    "trend_overall" numeric,
    "trend_objection" numeric,
    "trend_close" numeric,
    "filler_word_rate" numeric,
    "talk_ratio" numeric,
    "strengths" "text"[],
    "weaknesses" "text"[],
    "coaching_focus" "text"[],
    "call_count" integer DEFAULT 0,
    "last_updated" timestamp with time zone DEFAULT "now"(),
    "company_id" "uuid",
    "last_call_id" "uuid"
);


ALTER TABLE "public"."rep_memory" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."rep_xp_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "rep_id" "uuid" NOT NULL,
    "xp" integer NOT NULL,
    "source" "text" NOT NULL,
    "assignment_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."rep_xp_events" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."reps" (
    "id" "uuid" DEFAULT "extensions"."uuid_generate_v4"() NOT NULL,
    "name" "text",
    "xp" integer DEFAULT 0,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "tier" "text" DEFAULT 'Bronze'::"text",
    "org_id" "uuid" NOT NULL,
    "company_id" "uuid",
    "office_id" "uuid",
    "display_name" "text",
    "first_name" "text",
    "last_name" "text",
    "email" "text",
    "avatar_url" "text",
    "phone_number" "text",
    "job_title" "text",
    "department" "text",
    "manager_id" "uuid",
    "timezone" "text" DEFAULT 'UTC'::"text" NOT NULL,
    "is_active" boolean DEFAULT true NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"(),
    CONSTRAINT "reps_tier_check" CHECK (("tier" = ANY (ARRAY['SalesRep'::"text", 'TeamLead'::"text", 'Manager'::"text", 'Owner'::"text", 'PartnerAdmin'::"text", 'SuperAdmin'::"text"])))
);


ALTER TABLE "public"."reps" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."score_cache" (
    "cache_key" "text" NOT NULL,
    "call_sha256" "text",
    "transcript_hash" "text",
    "rubric_version" "text" NOT NULL,
    "prompt_version" "text" NOT NULL,
    "model_version" "text" NOT NULL,
    "result_json" "jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."score_cache" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."scorecard_criteria" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scorecard_version_id" "uuid" NOT NULL,
    "stage" "text" NOT NULL,
    "label" "text" NOT NULL,
    "description" "text",
    "scoring_guidance" "text",
    "good_example" "text",
    "weak_example" "text",
    "coaching_prompt" "text",
    "pass_fail" boolean DEFAULT false NOT NULL,
    "critical" boolean DEFAULT false NOT NULL,
    "emphasis" "text" DEFAULT 'standard'::"text" NOT NULL,
    "sort_order" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "scorecard_criteria_critical_chk" CHECK (((NOT "critical") OR "pass_fail")),
    CONSTRAINT "scorecard_criteria_emphasis_chk" CHECK (("emphasis" = ANY (ARRAY['minor'::"text", 'standard'::"text", 'major'::"text"]))),
    CONSTRAINT "scorecard_criteria_stage_chk" CHECK (("stage" = ANY (ARRAY['intro'::"text", 'discovery'::"text", 'objection'::"text", 'close'::"text"])))
);


ALTER TABLE "public"."scorecard_criteria" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."scorecard_stage_weights" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scorecard_version_id" "uuid" NOT NULL,
    "stage" "text" NOT NULL,
    "weight" integer NOT NULL,
    "guidance" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "scorecard_stage_weights_range_chk" CHECK ((("weight" >= 0) AND ("weight" <= 100))),
    CONSTRAINT "scorecard_stage_weights_stage_chk" CHECK (("stage" = ANY (ARRAY['intro'::"text", 'discovery'::"text", 'objection'::"text", 'close'::"text"])))
);


ALTER TABLE "public"."scorecard_stage_weights" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."scorecard_versions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scorecard_id" "uuid" NOT NULL,
    "company_id" "uuid" NOT NULL,
    "version" integer NOT NULL,
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "call_types" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "origin" "text" DEFAULT 'manual'::"text" NOT NULL,
    "activation_note" "text",
    "activated_by" "uuid",
    "activated_at" timestamp with time zone,
    "snapshot" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "scorecard_versions_origin_chk" CHECK (("origin" = ANY (ARRAY['manual'::"text", 'ai_draft'::"text", 'import'::"text", 'duplicate'::"text", 'default'::"text"]))),
    CONSTRAINT "scorecard_versions_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'active'::"text", 'superseded'::"text"])))
);


ALTER TABLE "public"."scorecard_versions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."scorecards" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "company_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "description" "text",
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "is_company_default" boolean DEFAULT false NOT NULL,
    "created_by" "uuid",
    "updated_by" "uuid",
    "archived_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "scorecards_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'active'::"text", 'archived'::"text"])))
);


ALTER TABLE "public"."scorecards" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."sparring_personas" (
    "id" "text" NOT NULL,
    "label" "text" NOT NULL,
    "description" "text",
    "default_difficulty" "text" DEFAULT 'normal'::"text" NOT NULL,
    "is_global" boolean DEFAULT true NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."sparring_personas" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."sparring_sessions" (
    "id" "uuid" DEFAULT "extensions"."uuid_generate_v4"() NOT NULL,
    "rep_id" "uuid",
    "persona_id" "text",
    "score" "jsonb",
    "xp_awarded" integer,
    "turns" integer,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "meta" "jsonb" DEFAULT '{}'::"jsonb",
    "total_score" numeric,
    "duration_ms" integer,
    "summary" "text",
    "flags" "text"[],
    "difficulty" "text",
    "failed_moments" "jsonb" DEFAULT '[]'::"jsonb",
    "assignment_id" "uuid",
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "status" "text",
    "completed_at" timestamp with time zone,
    "state" "jsonb"
);


ALTER TABLE "public"."sparring_sessions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."sparring_turns" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "session_id" "uuid" NOT NULL,
    "role" "text" NOT NULL,
    "text" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "turn_score" "jsonb",
    "state_snapshot" "jsonb",
    "meta" "jsonb",
    CONSTRAINT "sparring_turns_role_check" CHECK (("role" = ANY (ARRAY['user'::"text", 'assistant'::"text"])))
);


ALTER TABLE "public"."sparring_turns" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."team_settings" (
    "org_id" "uuid" NOT NULL,
    "streak_threshold" integer DEFAULT 3 NOT NULL,
    "xp_multiplier" numeric DEFAULT 1 NOT NULL,
    "comeback_bonus" integer DEFAULT 0 NOT NULL,
    "xp_cap_daily" integer DEFAULT 500 NOT NULL,
    "voice_score_threshold" integer DEFAULT 60 NOT NULL,
    "weak_close_threshold" integer DEFAULT 60 NOT NULL,
    "filler_density_threshold" numeric DEFAULT 0.08 NOT NULL,
    "coaching_trigger_thresholds" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "updated_at" timestamp with time zone,
    "updated_by" "uuid"
);


ALTER TABLE "public"."team_settings" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."titles" (
    "id" "text" NOT NULL,
    "label" "text" NOT NULL,
    "bg_class" "text" DEFAULT 'from-violet-600/30 to-fuchsia-600/30'::"text",
    "icon" "text" DEFAULT '🎯'::"text",
    "description" "text"
);


ALTER TABLE "public"."titles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."tmcs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "slug" "text" NOT NULL,
    "settings_json" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."tmcs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_badges" (
    "user_id" "uuid" NOT NULL,
    "badge_id" "text" NOT NULL,
    "earned_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."user_badges" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_selected_title" (
    "user_id" "uuid" NOT NULL,
    "title_id" "text" NOT NULL,
    "selected_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."user_selected_title" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_titles" (
    "user_id" "uuid" NOT NULL,
    "title_id" "text" NOT NULL,
    "unlocked_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."user_titles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."users" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "email" "text" NOT NULL,
    "role" "text" DEFAULT 'rep'::"text" NOT NULL,
    "manager_id" "uuid",
    "org_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "visibility_scope" "text" DEFAULT 'team'::"text",
    "office_id" "uuid",
    "company_id" "uuid",
    "is_admin" boolean DEFAULT false,
    CONSTRAINT "users_role_check" CHECK (("role" = ANY (ARRAY['rep'::"text", 'office_manager'::"text", 'company_manager'::"text"])))
);


ALTER TABLE "public"."users" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."whisperer_segments" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "session_id" "uuid" NOT NULL,
    "call_id" "uuid",
    "user_id" "uuid",
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "speaker" "text" DEFAULT 'unknown'::"text" NOT NULL,
    "speaker_original" "text",
    "speaker_role" "text",
    "text" "text" NOT NULL,
    "text_normalised" "text",
    "confidence" numeric,
    "is_final" boolean DEFAULT true NOT NULL,
    "source" "text" DEFAULT 'live'::"text" NOT NULL,
    "started_at_ms" integer,
    "ended_at_ms" integer,
    "client_sent_at" timestamp with time zone,
    "received_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "processed_at" timestamp with time zone,
    "triggers_count" integer DEFAULT 0 NOT NULL,
    "trigger_types" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "meta" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "whisperer_segments_source_chk" CHECK (("source" = ANY (ARRAY['live'::"text", 'manual'::"text", 'simulator'::"text"]))),
    CONSTRAINT "whisperer_segments_text_chk" CHECK (("length"("btrim"("text")) > 0))
);


ALTER TABLE "public"."whisperer_segments" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."whisperer_sessions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "rep_id" "uuid",
    "user_id" "uuid",
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "call_id" "uuid",
    "status" "text" DEFAULT 'active'::"text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "ended_at" timestamp with time zone,
    "latency_p50_ms" integer,
    "latency_p95_ms" integer,
    "meta" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."whisperer_sessions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."whisperer_trigger_candidate_decisions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "candidate_id" "text" NOT NULL,
    "candidate_type" "text",
    "decision" "text" NOT NULL,
    "decided_by" "uuid",
    "note" "text",
    "source" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "whisperer_tcd_decision_chk" CHECK (("decision" = ANY (ARRAY['approved'::"text", 'dismissed'::"text", 'rejected'::"text"])))
);


ALTER TABLE "public"."whisperer_trigger_candidate_decisions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."whisperer_trigger_library" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "org_id" "uuid",
    "company_id" "uuid",
    "office_id" "uuid",
    "created_by" "uuid",
    "name" "text" NOT NULL,
    "description" "text",
    "type" "text" DEFAULT 'custom'::"text" NOT NULL,
    "match_phrases" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "match_keywords" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "suggestion_title" "text" NOT NULL,
    "suggestion_response" "text" NOT NULL,
    "urgency" "text" DEFAULT 'medium'::"text" NOT NULL,
    "emoji" "text",
    "enabled" boolean DEFAULT true NOT NULL,
    "priority" integer DEFAULT 50 NOT NULL,
    "meta" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "source_candidate_id" "text",
    "source_meta" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    CONSTRAINT "whisperer_trigger_library_urgency_chk" CHECK (("urgency" = ANY (ARRAY['low'::"text", 'medium'::"text", 'high'::"text"])))
);


ALTER TABLE "public"."whisperer_trigger_library" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."whisperer_triggers" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "session_id" "uuid" NOT NULL,
    "segment_text" "text" NOT NULL,
    "type" "text" NOT NULL,
    "phrase" "text",
    "confidence" integer DEFAULT 80 NOT NULL,
    "suggestion" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "latency_ms" integer,
    "detected_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "meta" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "suggestion_outcome" "text",
    "suggestion_outcome_at" timestamp with time zone,
    "suggestion_outcome_by" "uuid",
    "suggestion_feedback" "text",
    CONSTRAINT "whisperer_triggers_suggestion_outcome_chk" CHECK ((("suggestion_outcome" IS NULL) OR ("suggestion_outcome" = ANY (ARRAY['used'::"text", 'ignored'::"text", 'not_relevant'::"text"]))))
);


ALTER TABLE "public"."whisperer_triggers" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."xp_events" (
    "id" "uuid" DEFAULT "extensions"."uuid_generate_v4"() NOT NULL,
    "rep_id" "uuid",
    "source" "text",
    "delta" integer,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "amount" integer DEFAULT 0,
    "session_id" "uuid"
);


ALTER TABLE "public"."xp_events" OWNER TO "postgres";


ALTER TABLE ONLY "public"."call_chunks" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."call_chunks_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."account_ai_summaries"
    ADD CONSTRAINT "account_ai_summaries_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."account_ai_tasks"
    ADD CONSTRAINT "account_ai_tasks_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."account_coaching_actions"
    ADD CONSTRAINT "account_coaching_actions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."account_escalations"
    ADD CONSTRAINT "account_escalations_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."accounts"
    ADD CONSTRAINT "accounts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."activities"
    ADD CONSTRAINT "activities_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."admin_config"
    ADD CONSTRAINT "admin_config_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."assignments"
    ADD CONSTRAINT "assignments_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."audit_events"
    ADD CONSTRAINT "audit_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."audit_logs"
    ADD CONSTRAINT "audit_logs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."badges"
    ADD CONSTRAINT "badges_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."bounties"
    ADD CONSTRAINT "bounties_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."call_chunks"
    ADD CONSTRAINT "call_chunks_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."call_manager_reviews"
    ADD CONSTRAINT "call_manager_reviews_call_id_unique" UNIQUE ("call_id");



ALTER TABLE ONLY "public"."call_manager_reviews"
    ADD CONSTRAINT "call_manager_reviews_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."call_pins"
    ADD CONSTRAINT "call_pins_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."call_scores"
    ADD CONSTRAINT "call_scores_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."coach_assignments"
    ADD CONSTRAINT "coach_assignments_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."coach_notes"
    ADD CONSTRAINT "coach_notes_call_id_key" UNIQUE ("call_id");



ALTER TABLE ONLY "public"."coach_notes"
    ADD CONSTRAINT "coach_notes_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."companies"
    ADD CONSTRAINT "companies_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."company_context"
    ADD CONSTRAINT "company_context_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."company_licences"
    ADD CONSTRAINT "company_licences_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."company_playbook"
    ADD CONSTRAINT "company_playbook_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_accounts"
    ADD CONSTRAINT "crm_accounts_domain_key" UNIQUE ("domain");



ALTER TABLE ONLY "public"."crm_accounts"
    ADD CONSTRAINT "crm_accounts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_actions"
    ADD CONSTRAINT "crm_actions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_activities"
    ADD CONSTRAINT "crm_activities_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_auto_assign_runs"
    ADD CONSTRAINT "crm_auto_assign_runs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_auto_assign_runs"
    ADD CONSTRAINT "crm_auto_assign_runs_run_id_key" UNIQUE ("run_id");



ALTER TABLE ONLY "public"."crm_call_links"
    ADD CONSTRAINT "crm_call_links_call_id_contact_id_key" UNIQUE ("call_id", "contact_id");



ALTER TABLE ONLY "public"."crm_call_links"
    ADD CONSTRAINT "crm_call_links_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_contact_notes"
    ADD CONSTRAINT "crm_contact_notes_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_contacts"
    ADD CONSTRAINT "crm_contacts_email_key" UNIQUE ("email");



ALTER TABLE ONLY "public"."crm_contacts"
    ADD CONSTRAINT "crm_contacts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."crm_demo_call_links"
    ADD CONSTRAINT "crm_demo_call_links_pkey" PRIMARY KEY ("user_id", "call_id");



ALTER TABLE ONLY "public"."crm_opportunities"
    ADD CONSTRAINT "crm_opportunities_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."internal_users"
    ADD CONSTRAINT "internal_users_email_key" UNIQUE ("email");



ALTER TABLE ONLY "public"."internal_users"
    ADD CONSTRAINT "internal_users_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."jobs"
    ADD CONSTRAINT "jobs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."knowledge_embeddings"
    ADD CONSTRAINT "knowledge_embeddings_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."licence_pools"
    ADD CONSTRAINT "licence_pools_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."objection_evidence"
    ADD CONSTRAINT "objection_evidence_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."objection_library_items"
    ADD CONSTRAINT "objection_library_items_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."objection_suggestion_decisions"
    ADD CONSTRAINT "objection_suggestion_decisions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."offices"
    ADD CONSTRAINT "offices_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."opportunities"
    ADD CONSTRAINT "opportunities_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."org_limits"
    ADD CONSTRAINT "org_limits_pkey" PRIMARY KEY ("org_id");



ALTER TABLE ONLY "public"."orgs"
    ADD CONSTRAINT "orgs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."partners"
    ADD CONSTRAINT "partners_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."pins"
    ADD CONSTRAINT "pins_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_pkey" PRIMARY KEY ("user_id");



ALTER TABLE ONLY "public"."rep_memory"
    ADD CONSTRAINT "rep_memory_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."rep_xp_events"
    ADD CONSTRAINT "rep_xp_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."reps"
    ADD CONSTRAINT "reps_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."score_cache"
    ADD CONSTRAINT "score_cache_pkey" PRIMARY KEY ("cache_key");



ALTER TABLE ONLY "public"."scorecard_criteria"
    ADD CONSTRAINT "scorecard_criteria_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."scorecard_stage_weights"
    ADD CONSTRAINT "scorecard_stage_weights_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."scorecard_versions"
    ADD CONSTRAINT "scorecard_versions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."scorecards"
    ADD CONSTRAINT "scorecards_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."sparring_personas"
    ADD CONSTRAINT "sparring_personas_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."sparring_sessions"
    ADD CONSTRAINT "sparring_sessions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."sparring_turns"
    ADD CONSTRAINT "sparring_turns_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."team_settings"
    ADD CONSTRAINT "team_settings_pkey" PRIMARY KEY ("org_id");



ALTER TABLE ONLY "public"."titles"
    ADD CONSTRAINT "titles_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."tmcs"
    ADD CONSTRAINT "tmcs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."tmcs"
    ADD CONSTRAINT "tmcs_slug_key" UNIQUE ("slug");



ALTER TABLE ONLY "public"."user_badges"
    ADD CONSTRAINT "user_badges_pkey" PRIMARY KEY ("user_id", "badge_id");



ALTER TABLE ONLY "public"."user_selected_title"
    ADD CONSTRAINT "user_selected_title_pkey" PRIMARY KEY ("user_id");



ALTER TABLE ONLY "public"."user_titles"
    ADD CONSTRAINT "user_titles_pkey" PRIMARY KEY ("user_id", "title_id");



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_email_key" UNIQUE ("email");



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."whisperer_segments"
    ADD CONSTRAINT "whisperer_segments_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."whisperer_sessions"
    ADD CONSTRAINT "whisperer_sessions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."whisperer_trigger_candidate_decisions"
    ADD CONSTRAINT "whisperer_trigger_candidate_decisions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."whisperer_trigger_library"
    ADD CONSTRAINT "whisperer_trigger_library_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."whisperer_triggers"
    ADD CONSTRAINT "whisperer_triggers_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."xp_events"
    ADD CONSTRAINT "xp_events_pkey" PRIMARY KEY ("id");



CREATE UNIQUE INDEX "accounts_org_domain_uniq" ON "public"."accounts" USING "btree" ("org_id", "lower"("domain")) WHERE ("domain" IS NOT NULL);



CREATE INDEX "accounts_org_idx" ON "public"."accounts" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "activities_account_idx" ON "public"."activities" USING "btree" ("account_id", "created_at" DESC);



CREATE INDEX "activities_contact_idx" ON "public"."activities" USING "btree" ("contact_id", "created_at" DESC);



CREATE INDEX "activities_oppty_idx" ON "public"."activities" USING "btree" ("opportunity_id", "created_at" DESC);



CREATE INDEX "activities_org_created_idx" ON "public"."activities" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "call_chunks_embedding_idx" ON "public"."call_chunks" USING "ivfflat" ("embedding" "public"."vector_cosine_ops");



CREATE INDEX "call_pins_call_idx" ON "public"."call_pins" USING "btree" ("call_id");



CREATE INDEX "call_pins_user_idx" ON "public"."call_pins" USING "btree" ("user_id");



CREATE INDEX "call_scores_call_id_created_at_idx" ON "public"."call_scores" USING "btree" ("call_id", "created_at" DESC);



CREATE INDEX "calls_org_created_idx" ON "public"."calls" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "calls_org_idx" ON "public"."calls" USING "btree" ("org_id");



CREATE INDEX "calls_status_idx" ON "public"."calls" USING "btree" ("status");



CREATE INDEX "calls_user_created_at_idx" ON "public"."calls" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "calls_user_created_idx" ON "public"."calls" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "coach_assignments_org_id_idx" ON "public"."coach_assignments" USING "btree" ("org_id");



CREATE UNIQUE INDEX "contacts_org_email_uniq" ON "public"."contacts" USING "btree" ("org_id", "lower"("email")) WHERE ("email" IS NOT NULL);



CREATE INDEX "contacts_org_idx" ON "public"."contacts" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "crm_actions_user_contact_created_idx" ON "public"."crm_actions" USING "btree" ("user_id", "contact_id", "created_at" DESC);



CREATE INDEX "crm_actions_user_status_due_idx" ON "public"."crm_actions" USING "btree" ("user_id", "status", "due_at");



CREATE INDEX "crm_activities_call_id_idx" ON "public"."crm_activities" USING "btree" ("call_id");



CREATE INDEX "crm_activities_created_at_idx" ON "public"."crm_activities" USING "btree" ("created_at");



CREATE INDEX "crm_activities_org_id_idx" ON "public"."crm_activities" USING "btree" ("org_id");



CREATE INDEX "crm_activities_rep_id_idx" ON "public"."crm_activities" USING "btree" ("rep_id");



CREATE INDEX "crm_activities_source_idx" ON "public"."crm_activities" USING "btree" ("source");



CREATE INDEX "crm_auto_assign_runs_created_at_idx" ON "public"."crm_auto_assign_runs" USING "btree" ("created_at" DESC);



CREATE INDEX "crm_auto_assign_runs_requester_started_idx" ON "public"."crm_auto_assign_runs" USING "btree" ("requester_id", "started_at" DESC);



CREATE INDEX "crm_auto_assign_runs_started_idx" ON "public"."crm_auto_assign_runs" USING "btree" ("started_at" DESC);



CREATE INDEX "crm_auto_assign_runs_user_started_idx" ON "public"."crm_auto_assign_runs" USING "btree" ("user_id", "started_at" DESC);



CREATE INDEX "crm_contact_notes_author_id_idx" ON "public"."crm_contact_notes" USING "btree" ("author_id");



CREATE INDEX "crm_contact_notes_contact_created_idx" ON "public"."crm_contact_notes" USING "btree" ("contact_id", "created_at" DESC);



CREATE INDEX "crm_contact_notes_user_contact_created_idx" ON "public"."crm_contact_notes" USING "btree" ("user_id", "contact_id", "created_at" DESC);



CREATE INDEX "crm_contact_notes_user_created_idx" ON "public"."crm_contact_notes" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "crm_contacts_last_contacted_at_idx" ON "public"."crm_contacts" USING "btree" ("last_contacted_at" DESC NULLS LAST);



CREATE INDEX "crm_contacts_org_id_idx" ON "public"."crm_contacts" USING "btree" ("org_id");



CREATE INDEX "crm_contacts_user_id_idx" ON "public"."crm_contacts" USING "btree" ("user_id");



CREATE INDEX "crm_contacts_user_last_contacted_idx" ON "public"."crm_contacts" USING "btree" ("user_id", "last_contacted_at" DESC);



CREATE INDEX "crm_demo_call_links_contact_id_idx" ON "public"."crm_demo_call_links" USING "btree" ("contact_id");



CREATE INDEX "crm_demo_call_links_user_id_idx" ON "public"."crm_demo_call_links" USING "btree" ("user_id");



CREATE INDEX "crm_opportunities_stage_idx" ON "public"."crm_opportunities" USING "btree" ("stage");



CREATE INDEX "crm_opportunities_user_id_idx" ON "public"."crm_opportunities" USING "btree" ("user_id");



CREATE INDEX "idx_account_ai_summaries_account" ON "public"."account_ai_summaries" USING "btree" ("account_id");



CREATE INDEX "idx_account_ai_summaries_updated" ON "public"."account_ai_summaries" USING "btree" ("updated_at" DESC);



CREATE INDEX "idx_account_ai_tasks_account" ON "public"."account_ai_tasks" USING "btree" ("account_id");



CREATE INDEX "idx_account_ai_tasks_status" ON "public"."account_ai_tasks" USING "btree" ("status");



CREATE INDEX "idx_account_ai_tasks_urgency" ON "public"."account_ai_tasks" USING "btree" ("urgency");



CREATE INDEX "idx_account_coaching_actions_account" ON "public"."account_coaching_actions" USING "btree" ("account_id");



CREATE INDEX "idx_account_coaching_actions_status" ON "public"."account_coaching_actions" USING "btree" ("status");



CREATE INDEX "idx_account_coaching_actions_urgency" ON "public"."account_coaching_actions" USING "btree" ("urgency");



CREATE INDEX "idx_account_escalations_account" ON "public"."account_escalations" USING "btree" ("account_id");



CREATE INDEX "idx_account_escalations_company" ON "public"."account_escalations" USING "btree" ("company_id");



CREATE INDEX "idx_account_escalations_severity" ON "public"."account_escalations" USING "btree" ("severity");



CREATE INDEX "idx_account_escalations_status" ON "public"."account_escalations" USING "btree" ("status");



CREATE INDEX "idx_accounts_owner_id" ON "public"."accounts" USING "btree" ("owner_id");



CREATE INDEX "idx_activities_account" ON "public"."activities" USING "btree" ("account_id", "created_at" DESC);



CREATE INDEX "idx_activities_contact" ON "public"."activities" USING "btree" ("contact_id", "created_at" DESC);



CREATE INDEX "idx_assignments_assignee" ON "public"."coach_assignments" USING "btree" ("assignee_user_id", "created_at" DESC);



CREATE INDEX "idx_assignments_call" ON "public"."coach_assignments" USING "btree" ("call_id", "created_at" DESC);



CREATE INDEX "idx_assignments_due_at" ON "public"."assignments" USING "btree" ("due_at");



CREATE INDEX "idx_assignments_manager_id" ON "public"."assignments" USING "btree" ("manager_id");



CREATE INDEX "idx_assignments_rep_id" ON "public"."assignments" USING "btree" ("rep_id");



CREATE INDEX "idx_assignments_status" ON "public"."coach_assignments" USING "btree" ("status", "created_at" DESC);



CREATE INDEX "idx_assignments_user" ON "public"."coach_assignments" USING "btree" ("assignee_user_id", "created_at" DESC);



CREATE INDEX "idx_audit_events_action" ON "public"."audit_events" USING "btree" ("action");



CREATE INDEX "idx_audit_events_actor_user_id" ON "public"."audit_events" USING "btree" ("actor_user_id");



CREATE INDEX "idx_audit_events_created_at" ON "public"."audit_events" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_audit_events_target_user_id" ON "public"."audit_events" USING "btree" ("target_user_id") WHERE ("target_user_id" IS NOT NULL);



CREATE INDEX "idx_call_pins_call_id" ON "public"."call_pins" USING "btree" ("call_id");



CREATE INDEX "idx_call_pins_created_at" ON "public"."call_pins" USING "btree" ("created_at" DESC);



CREATE INDEX "idx_call_pins_user_id" ON "public"."call_pins" USING "btree" ("user_id");



CREATE INDEX "idx_call_scores_call_time" ON "public"."call_scores" USING "btree" ("call_id", "created_at");



CREATE INDEX "idx_call_scores_user_time" ON "public"."call_scores" USING "btree" ("user_id", "created_at");



CREATE INDEX "idx_calls_storage_path" ON "public"."calls" USING "btree" ("storage_path");



CREATE INDEX "idx_calls_user_created" ON "public"."calls" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "idx_cmr_call_id" ON "public"."call_manager_reviews" USING "btree" ("call_id");



CREATE INDEX "idx_cmr_company_created" ON "public"."call_manager_reviews" USING "btree" ("company_id", "created_at" DESC);



CREATE INDEX "idx_cmr_manager_id" ON "public"."call_manager_reviews" USING "btree" ("manager_id");



CREATE INDEX "idx_cmr_office_created" ON "public"."call_manager_reviews" USING "btree" ("office_id", "created_at" DESC);



CREATE INDEX "idx_cmr_org_created" ON "public"."call_manager_reviews" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "idx_companies_is_active" ON "public"."companies" USING "btree" ("is_active");



CREATE INDEX "idx_companies_partner_id" ON "public"."companies" USING "btree" ("partner_id");



CREATE INDEX "idx_companies_tmc_id" ON "public"."companies" USING "btree" ("tmc_id");



CREATE INDEX "idx_company_context_company_version" ON "public"."company_context" USING "btree" ("company_id", "version" DESC);



CREATE INDEX "idx_company_context_status" ON "public"."company_context" USING "btree" ("status", "updated_at" DESC);



CREATE UNIQUE INDEX "idx_company_licences_company" ON "public"."company_licences" USING "btree" ("company_id");



CREATE INDEX "idx_company_licences_partner" ON "public"."company_licences" USING "btree" ("partner_id");



CREATE INDEX "idx_company_playbook_active" ON "public"."company_playbook" USING "btree" ("company_id", "is_active");



CREATE INDEX "idx_company_playbook_company_id" ON "public"."company_playbook" USING "btree" ("company_id");



CREATE INDEX "idx_company_playbook_priority" ON "public"."company_playbook" USING "btree" ("company_id", "priority");



CREATE INDEX "idx_company_playbook_stage" ON "public"."company_playbook" USING "btree" ("company_id", "stage");



CREATE INDEX "idx_crm_accounts_company" ON "public"."crm_accounts" USING "btree" ("company_id");



CREATE INDEX "idx_crm_accounts_domain" ON "public"."crm_accounts" USING "btree" ("domain");



CREATE INDEX "idx_crm_auto_assign_runs_org_created" ON "public"."crm_auto_assign_runs" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "idx_crm_auto_assign_runs_requested_by" ON "public"."crm_auto_assign_runs" USING "btree" ("requested_by");



CREATE INDEX "idx_crm_call_links_call" ON "public"."crm_call_links" USING "btree" ("call_id");



CREATE INDEX "idx_crm_contacts_user" ON "public"."crm_contacts" USING "btree" ("user_id", "email");



CREATE INDEX "idx_knowledge_embeddings_company_id" ON "public"."knowledge_embeddings" USING "btree" ("company_id");



CREATE INDEX "idx_knowledge_embeddings_embedding" ON "public"."knowledge_embeddings" USING "ivfflat" ("embedding" "public"."vector_cosine_ops") WITH ("lists"='100');



CREATE INDEX "idx_knowledge_embeddings_source_id" ON "public"."knowledge_embeddings" USING "btree" ("source_id");



CREATE INDEX "idx_knowledge_embeddings_source_type" ON "public"."knowledge_embeddings" USING "btree" ("source_type");



CREATE INDEX "idx_knowledge_embeddings_stage" ON "public"."knowledge_embeddings" USING "btree" ("stage");



CREATE INDEX "idx_knowledge_embeddings_user_id" ON "public"."knowledge_embeddings" USING "btree" ("user_id");



CREATE INDEX "idx_licence_pools_expires" ON "public"."licence_pools" USING "btree" ("expires_at");



CREATE UNIQUE INDEX "idx_licence_pools_partner" ON "public"."licence_pools" USING "btree" ("partner_id");



CREATE INDEX "idx_objection_decisions_company_key" ON "public"."objection_suggestion_decisions" USING "btree" ("company_id", "suggestion_key");



CREATE INDEX "idx_objection_evidence_company_item" ON "public"."objection_evidence" USING "btree" ("company_id", "objection_id");



CREATE INDEX "idx_objection_items_company_status" ON "public"."objection_library_items" USING "btree" ("company_id", "status");



CREATE INDEX "idx_offices_company_id" ON "public"."offices" USING "btree" ("company_id");



CREATE UNIQUE INDEX "idx_partners_slug" ON "public"."partners" USING "btree" ("slug");



CREATE INDEX "idx_partners_status" ON "public"."partners" USING "btree" ("status");



CREATE INDEX "idx_pins_call_t" ON "public"."pins" USING "btree" ("call_id", "t");



CREATE INDEX "idx_pins_user" ON "public"."pins" USING "btree" ("user_id");



CREATE INDEX "idx_rep_memory_company_id" ON "public"."rep_memory" USING "btree" ("company_id");



CREATE UNIQUE INDEX "idx_rep_memory_user_id" ON "public"."rep_memory" USING "btree" ("user_id");



CREATE INDEX "idx_reps_company_active" ON "public"."reps" USING "btree" ("company_id", "is_active");



CREATE INDEX "idx_reps_is_active" ON "public"."reps" USING "btree" ("is_active");



CREATE INDEX "idx_reps_manager_id" ON "public"."reps" USING "btree" ("manager_id");



CREATE INDEX "idx_score_cache_call_sha256" ON "public"."score_cache" USING "btree" ("call_sha256");



CREATE INDEX "idx_score_cache_transcript_hash" ON "public"."score_cache" USING "btree" ("transcript_hash");



CREATE INDEX "idx_scorecard_criteria_version_stage" ON "public"."scorecard_criteria" USING "btree" ("scorecard_version_id", "stage", "sort_order");



CREATE INDEX "idx_scorecard_versions_company_status" ON "public"."scorecard_versions" USING "btree" ("company_id", "status");



CREATE INDEX "idx_scorecards_company_status" ON "public"."scorecards" USING "btree" ("company_id", "status");



CREATE INDEX "idx_sparring_turns_session_id_created_at" ON "public"."sparring_turns" USING "btree" ("session_id", "created_at");



CREATE INDEX "idx_ss_assignment_id" ON "public"."sparring_sessions" USING "btree" ("assignment_id");



CREATE INDEX "idx_ss_company_completed" ON "public"."sparring_sessions" USING "btree" ("company_id", "completed_at" DESC);



CREATE INDEX "idx_ss_office_completed" ON "public"."sparring_sessions" USING "btree" ("office_id", "completed_at" DESC);



CREATE INDEX "idx_ss_org_completed" ON "public"."sparring_sessions" USING "btree" ("org_id", "completed_at" DESC);



CREATE INDEX "idx_ss_status_completed" ON "public"."sparring_sessions" USING "btree" ("status", "completed_at" DESC);



CREATE INDEX "idx_st_session_created" ON "public"."sparring_turns" USING "btree" ("session_id", "created_at");



CREATE INDEX "idx_st_session_id" ON "public"."sparring_turns" USING "btree" ("session_id");



CREATE INDEX "idx_users_company_id" ON "public"."users" USING "btree" ("company_id");



CREATE INDEX "idx_users_office_id" ON "public"."users" USING "btree" ("office_id");



CREATE INDEX "idx_users_role" ON "public"."users" USING "btree" ("role");



CREATE INDEX "idx_ws_call_id" ON "public"."whisperer_sessions" USING "btree" ("call_id");



CREATE INDEX "idx_ws_company_created" ON "public"."whisperer_sessions" USING "btree" ("company_id", "created_at" DESC);



CREATE INDEX "idx_ws_office_created" ON "public"."whisperer_sessions" USING "btree" ("office_id", "created_at" DESC);



CREATE INDEX "idx_ws_org_created" ON "public"."whisperer_sessions" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "idx_ws_rep_created" ON "public"."whisperer_sessions" USING "btree" ("rep_id", "created_at" DESC);



CREATE INDEX "idx_ws_status_created" ON "public"."whisperer_sessions" USING "btree" ("status", "created_at" DESC);



CREATE INDEX "idx_ws_user_created" ON "public"."whisperer_sessions" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "idx_wseg_call_created" ON "public"."whisperer_segments" USING "btree" ("call_id", "created_at" DESC);



CREATE INDEX "idx_wseg_company_created" ON "public"."whisperer_segments" USING "btree" ("company_id", "created_at" DESC);



CREATE INDEX "idx_wseg_office_created" ON "public"."whisperer_segments" USING "btree" ("office_id", "created_at" DESC);



CREATE INDEX "idx_wseg_org_created" ON "public"."whisperer_segments" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "idx_wseg_session_created" ON "public"."whisperer_segments" USING "btree" ("session_id", "created_at" DESC);



CREATE INDEX "idx_wseg_speaker_created" ON "public"."whisperer_segments" USING "btree" ("speaker", "created_at" DESC);



CREATE INDEX "idx_wseg_trigger_types" ON "public"."whisperer_segments" USING "gin" ("trigger_types");



CREATE INDEX "idx_wseg_user_created" ON "public"."whisperer_segments" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "idx_wt_outcome_created" ON "public"."whisperer_triggers" USING "btree" ("suggestion_outcome", "created_at" DESC);



CREATE INDEX "idx_wt_session_created" ON "public"."whisperer_triggers" USING "btree" ("session_id", "created_at" DESC);



CREATE INDEX "idx_wt_type_created" ON "public"."whisperer_triggers" USING "btree" ("type", "created_at" DESC);



CREATE INDEX "idx_wtcd_company_candidate" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("company_id", "candidate_id");



CREATE INDEX "idx_wtcd_decided_by" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("decided_by", "created_at" DESC);



CREATE INDEX "idx_wtcd_decision_created" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("decision", "created_at" DESC);



CREATE INDEX "idx_wtcd_office_candidate" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("office_id", "candidate_id");



CREATE INDEX "idx_wtcd_org_candidate" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("org_id", "candidate_id");



CREATE INDEX "idx_wtl_company_enabled" ON "public"."whisperer_trigger_library" USING "btree" ("company_id", "enabled", "created_at" DESC);



CREATE INDEX "idx_wtl_creator" ON "public"."whisperer_trigger_library" USING "btree" ("created_by", "created_at" DESC);



CREATE INDEX "idx_wtl_office_enabled" ON "public"."whisperer_trigger_library" USING "btree" ("office_id", "enabled", "created_at" DESC);



CREATE INDEX "idx_wtl_org_enabled" ON "public"."whisperer_trigger_library" USING "btree" ("org_id", "enabled", "created_at" DESC);



CREATE INDEX "idx_wtl_source_candidate" ON "public"."whisperer_trigger_library" USING "btree" ("source_candidate_id");



CREATE INDEX "idx_wtl_type_enabled" ON "public"."whisperer_trigger_library" USING "btree" ("type", "enabled");



CREATE INDEX "idx_xp_events_rep_created" ON "public"."xp_events" USING "btree" ("rep_id", "created_at" DESC);



CREATE INDEX "jobs_call_idx" ON "public"."jobs" USING "btree" ("call_id");



CREATE INDEX "jobs_kind_status_idx" ON "public"."jobs" USING "btree" ("kind", "status");



CREATE INDEX "jobs_score_queued_idx" ON "public"."jobs" USING "btree" ("created_at" DESC) WHERE (("kind" = 'score'::"text") AND ("status" = 'queued'::"text"));



CREATE INDEX "jobs_status_created_idx" ON "public"."jobs" USING "btree" ("status", "created_at");



CREATE INDEX "jobs_user_created_idx" ON "public"."jobs" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "opportunities_org_idx" ON "public"."opportunities" USING "btree" ("org_id", "created_at" DESC);



CREATE INDEX "opportunities_stage_idx" ON "public"."opportunities" USING "btree" ("stage");



CREATE INDEX "pins_call_id_idx" ON "public"."pins" USING "btree" ("call_id");



CREATE INDEX "pins_user_id_idx" ON "public"."pins" USING "btree" ("user_id");



CREATE UNIQUE INDEX "profiles_email_unique_idx" ON "public"."profiles" USING "btree" ("email") WHERE ("email" IS NOT NULL);



CREATE UNIQUE INDEX "rep_xp_events_unique_assignment" ON "public"."rep_xp_events" USING "btree" ("rep_id", "assignment_id", "source") WHERE ("assignment_id" IS NOT NULL);



CREATE INDEX "reps_org_id_idx" ON "public"."reps" USING "btree" ("org_id");



CREATE UNIQUE INDEX "uq_company_context_one_draft" ON "public"."company_context" USING "btree" ("company_id") WHERE ("status" = 'draft'::"text");



CREATE UNIQUE INDEX "uq_company_context_one_published" ON "public"."company_context" USING "btree" ("company_id") WHERE ("status" = 'published'::"text");



CREATE UNIQUE INDEX "uq_crm_accounts_company_domain" ON "public"."crm_accounts" USING "btree" ("company_id", "lower"("domain")) WHERE (("company_id" IS NOT NULL) AND ("domain" IS NOT NULL));



CREATE UNIQUE INDEX "uq_crm_accounts_company_name" ON "public"."crm_accounts" USING "btree" ("company_id", "lower"("name")) WHERE ("company_id" IS NOT NULL);



CREATE UNIQUE INDEX "uq_objection_items_company_label_live" ON "public"."objection_library_items" USING "btree" ("company_id", "lower"("label")) WHERE ("status" <> 'archived'::"text");



CREATE UNIQUE INDEX "uq_scorecard_stage_weights_stage" ON "public"."scorecard_stage_weights" USING "btree" ("scorecard_version_id", "stage");



CREATE UNIQUE INDEX "uq_scorecard_versions_number" ON "public"."scorecard_versions" USING "btree" ("scorecard_id", "version");



CREATE UNIQUE INDEX "uq_scorecard_versions_one_active" ON "public"."scorecard_versions" USING "btree" ("scorecard_id") WHERE ("status" = 'active'::"text");



CREATE UNIQUE INDEX "uq_scorecard_versions_one_draft" ON "public"."scorecard_versions" USING "btree" ("scorecard_id") WHERE ("status" = 'draft'::"text");



CREATE UNIQUE INDEX "uq_scorecards_company_default" ON "public"."scorecards" USING "btree" ("company_id") WHERE ("is_company_default" AND ("status" = 'active'::"text"));



CREATE UNIQUE INDEX "uq_scorecards_company_name" ON "public"."scorecards" USING "btree" ("company_id", "lower"("name"));



CREATE UNIQUE INDEX "uq_wtcd_scope_candidate" ON "public"."whisperer_trigger_candidate_decisions" USING "btree" ("org_id", "company_id", "office_id", "candidate_id") NULLS NOT DISTINCT;



CREATE OR REPLACE TRIGGER "set_updated_at" BEFORE UPDATE ON "public"."calls" FOR EACH ROW EXECUTE FUNCTION "public"."tg_set_updated_at"();



CREATE OR REPLACE TRIGGER "set_updated_at_accounts" BEFORE UPDATE ON "public"."accounts" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "set_updated_at_contacts" BEFORE UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "set_updated_at_opportunities" BEFORE UPDATE ON "public"."opportunities" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "trg_admin_config_updated_at" BEFORE UPDATE ON "public"."admin_config" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "trg_calls_set_defaults" BEFORE INSERT OR UPDATE ON "public"."calls" FOR EACH ROW EXECUTE FUNCTION "public"."calls_set_defaults"();



CREATE OR REPLACE TRIGGER "trg_calls_set_filename" BEFORE INSERT ON "public"."calls" FOR EACH ROW EXECUTE FUNCTION "public"."calls_set_filename"();



CREATE OR REPLACE TRIGGER "trg_calls_set_storage_path" BEFORE INSERT OR UPDATE ON "public"."calls" FOR EACH ROW EXECUTE FUNCTION "public"."calls_set_storage_path"();



CREATE OR REPLACE TRIGGER "trg_calls_set_updated_at" BEFORE UPDATE ON "public"."calls" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "trg_company_playbook_updated_at" BEFORE UPDATE ON "public"."company_playbook" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "trg_knowledge_embeddings_updated_at" BEFORE UPDATE ON "public"."knowledge_embeddings" FOR EACH ROW EXECUTE FUNCTION "public"."set_updated_at"();



CREATE OR REPLACE TRIGGER "trg_sparring_personas_updated_at" BEFORE UPDATE ON "public"."sparring_personas" FOR EACH ROW EXECUTE FUNCTION "public"."set_sparring_personas_updated_at"();



ALTER TABLE ONLY "public"."account_ai_summaries"
    ADD CONSTRAINT "account_ai_summaries_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."account_ai_summaries"
    ADD CONSTRAINT "account_ai_summaries_generated_by_fkey" FOREIGN KEY ("generated_by") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_ai_tasks"
    ADD CONSTRAINT "account_ai_tasks_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."account_ai_tasks"
    ADD CONSTRAINT "account_ai_tasks_assigned_to_fkey" FOREIGN KEY ("assigned_to") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_ai_tasks"
    ADD CONSTRAINT "account_ai_tasks_generated_by_fkey" FOREIGN KEY ("generated_by") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_coaching_actions"
    ADD CONSTRAINT "account_coaching_actions_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."account_coaching_actions"
    ADD CONSTRAINT "account_coaching_actions_assigned_to_fkey" FOREIGN KEY ("assigned_to") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_coaching_actions"
    ADD CONSTRAINT "account_coaching_actions_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_escalations"
    ADD CONSTRAINT "account_escalations_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."account_escalations"
    ADD CONSTRAINT "account_escalations_assigned_manager_id_fkey" FOREIGN KEY ("assigned_manager_id") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."account_escalations"
    ADD CONSTRAINT "account_escalations_triggered_by_fkey" FOREIGN KEY ("triggered_by") REFERENCES "public"."users"("id");



ALTER TABLE ONLY "public"."activities"
    ADD CONSTRAINT "activities_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."activities"
    ADD CONSTRAINT "activities_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."activities"
    ADD CONSTRAINT "activities_contact_id_fkey" FOREIGN KEY ("contact_id") REFERENCES "public"."contacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."activities"
    ADD CONSTRAINT "activities_opportunity_id_fkey" FOREIGN KEY ("opportunity_id") REFERENCES "public"."opportunities"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."assignments"
    ADD CONSTRAINT "assignments_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."assignments"
    ADD CONSTRAINT "assignments_rep_id_fkey" FOREIGN KEY ("rep_id") REFERENCES "public"."reps"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."bounties"
    ADD CONSTRAINT "bounties_title_id_fkey" FOREIGN KEY ("title_id") REFERENCES "public"."titles"("id");



ALTER TABLE ONLY "public"."call_manager_reviews"
    ADD CONSTRAINT "call_manager_reviews_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."call_pins"
    ADD CONSTRAINT "call_pins_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."call_scores"
    ADD CONSTRAINT "call_scores_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_contact_id_fkey" FOREIGN KEY ("contact_id") REFERENCES "public"."contacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_office_id_fkey" FOREIGN KEY ("office_id") REFERENCES "public"."offices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."calls"
    ADD CONSTRAINT "calls_opportunity_id_fkey" FOREIGN KEY ("opportunity_id") REFERENCES "public"."opportunities"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."coach_assignments"
    ADD CONSTRAINT "coach_assignments_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."companies"
    ADD CONSTRAINT "companies_partner_id_fkey" FOREIGN KEY ("partner_id") REFERENCES "public"."partners"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."companies"
    ADD CONSTRAINT "companies_tmc_id_fkey" FOREIGN KEY ("tmc_id") REFERENCES "public"."tmcs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."company_licences"
    ADD CONSTRAINT "company_licences_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."company_licences"
    ADD CONSTRAINT "company_licences_partner_id_fkey" FOREIGN KEY ("partner_id") REFERENCES "public"."partners"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."crm_call_links"
    ADD CONSTRAINT "crm_call_links_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."crm_call_links"
    ADD CONSTRAINT "crm_call_links_contact_id_fkey" FOREIGN KEY ("contact_id") REFERENCES "public"."crm_contacts"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."jobs"
    ADD CONSTRAINT "jobs_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."licence_pools"
    ADD CONSTRAINT "licence_pools_partner_id_fkey" FOREIGN KEY ("partner_id") REFERENCES "public"."partners"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."objection_evidence"
    ADD CONSTRAINT "objection_evidence_objection_id_fkey" FOREIGN KEY ("objection_id") REFERENCES "public"."objection_library_items"("id");



ALTER TABLE ONLY "public"."objection_suggestion_decisions"
    ADD CONSTRAINT "objection_suggestion_decisions_objection_id_fkey" FOREIGN KEY ("objection_id") REFERENCES "public"."objection_library_items"("id");



ALTER TABLE ONLY "public"."offices"
    ADD CONSTRAINT "offices_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."opportunities"
    ADD CONSTRAINT "opportunities_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "public"."accounts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."pins"
    ADD CONSTRAINT "pins_call_id_fkey" FOREIGN KEY ("call_id") REFERENCES "public"."calls"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_manager_id_fkey" FOREIGN KEY ("manager_id") REFERENCES "public"."profiles"("user_id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."reps"
    ADD CONSTRAINT "reps_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."reps"
    ADD CONSTRAINT "reps_office_id_fkey" FOREIGN KEY ("office_id") REFERENCES "public"."offices"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."reps"
    ADD CONSTRAINT "reps_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."orgs"("id") ON DELETE RESTRICT;



ALTER TABLE ONLY "public"."scorecard_criteria"
    ADD CONSTRAINT "scorecard_criteria_scorecard_version_id_fkey" FOREIGN KEY ("scorecard_version_id") REFERENCES "public"."scorecard_versions"("id");



ALTER TABLE ONLY "public"."scorecard_stage_weights"
    ADD CONSTRAINT "scorecard_stage_weights_scorecard_version_id_fkey" FOREIGN KEY ("scorecard_version_id") REFERENCES "public"."scorecard_versions"("id");



ALTER TABLE ONLY "public"."scorecard_versions"
    ADD CONSTRAINT "scorecard_versions_scorecard_id_fkey" FOREIGN KEY ("scorecard_id") REFERENCES "public"."scorecards"("id");



ALTER TABLE ONLY "public"."sparring_sessions"
    ADD CONSTRAINT "sparring_sessions_rep_id_fkey" FOREIGN KEY ("rep_id") REFERENCES "public"."reps"("id");



ALTER TABLE ONLY "public"."sparring_turns"
    ADD CONSTRAINT "sparring_turns_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."sparring_sessions"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."user_badges"
    ADD CONSTRAINT "user_badges_badge_id_fkey" FOREIGN KEY ("badge_id") REFERENCES "public"."badges"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."user_selected_title"
    ADD CONSTRAINT "user_selected_title_title_id_fkey" FOREIGN KEY ("title_id") REFERENCES "public"."titles"("id") ON DELETE RESTRICT;



ALTER TABLE ONLY "public"."user_titles"
    ADD CONSTRAINT "user_titles_title_id_fkey" FOREIGN KEY ("title_id") REFERENCES "public"."titles"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_company_id_fkey" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_manager_id_fkey" FOREIGN KEY ("manager_id") REFERENCES "public"."users"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."whisperer_segments"
    ADD CONSTRAINT "whisperer_segments_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."whisperer_sessions"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."whisperer_triggers"
    ADD CONSTRAINT "whisperer_triggers_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."whisperer_sessions"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."xp_events"
    ADD CONSTRAINT "xp_events_rep_id_fkey" FOREIGN KEY ("rep_id") REFERENCES "public"."reps"("id");



ALTER TABLE ONLY "public"."xp_events"
    ADD CONSTRAINT "xp_events_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "public"."sparring_sessions"("id") ON DELETE SET NULL;



CREATE POLICY "Users can delete their own call pins" ON "public"."call_pins" FOR DELETE USING ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can insert own calls" ON "public"."calls" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));



CREATE POLICY "Users can insert their own call pins" ON "public"."call_pins" FOR INSERT WITH CHECK ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can select own calls" ON "public"."calls" FOR SELECT USING (("auth"."uid"() = "user_id"));



CREATE POLICY "Users can update their own call pins" ON "public"."call_pins" FOR UPDATE USING ((("auth"."uid"())::"text" = ("user_id")::"text")) WITH CHECK ((("auth"."uid"())::"text" = ("user_id")::"text"));



CREATE POLICY "Users can view their own call pins" ON "public"."call_pins" FOR SELECT USING ((("auth"."uid"())::"text" = ("user_id")::"text"));



ALTER TABLE "public"."call_chunks" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."call_manager_reviews" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."call_pins" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."calls" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."company_context" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."company_licences" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."crm_auto_assign_runs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "crm_auto_assign_runs_select_own" ON "public"."crm_auto_assign_runs" FOR SELECT USING (("auth"."uid"() = "user_id"));



ALTER TABLE "public"."jobs" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."licence_pools" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."objection_evidence" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."objection_library_items" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."objection_suggestion_decisions" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "owners can read own jobs" ON "public"."jobs" FOR SELECT USING (("auth"."uid"() = "user_id"));



CREATE POLICY "owners read own calls" ON "public"."calls" FOR SELECT USING (("auth"."uid"() = "user_id"));



ALTER TABLE "public"."profiles" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "read own profile" ON "public"."profiles" FOR SELECT USING (("auth"."uid"() = "user_id"));



ALTER TABLE "public"."scorecard_criteria" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."scorecard_stage_weights" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."scorecard_versions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."scorecards" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "update own profile" ON "public"."profiles" FOR UPDATE USING (("auth"."uid"() = "user_id"));



CREATE POLICY "upsert own profile" ON "public"."profiles" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));



ALTER TABLE "public"."whisperer_segments" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."whisperer_sessions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."whisperer_trigger_candidate_decisions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."whisperer_trigger_library" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."whisperer_triggers" ENABLE ROW LEVEL SECURITY;




ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";






GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "service_role";






















































































































































GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."calls_set_defaults"() TO "anon";
GRANT ALL ON FUNCTION "public"."calls_set_defaults"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."calls_set_defaults"() TO "service_role";



GRANT ALL ON FUNCTION "public"."calls_set_filename"() TO "anon";
GRANT ALL ON FUNCTION "public"."calls_set_filename"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."calls_set_filename"() TO "service_role";



GRANT ALL ON FUNCTION "public"."calls_set_storage_path"() TO "anon";
GRANT ALL ON FUNCTION "public"."calls_set_storage_path"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."calls_set_storage_path"() TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."fn_activity_on_assignment"() TO "anon";
GRANT ALL ON FUNCTION "public"."fn_activity_on_assignment"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."fn_activity_on_assignment"() TO "service_role";



GRANT ALL ON FUNCTION "public"."gravix_basename"("p" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."gravix_basename"("p" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."gravix_basename"("p" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "service_role";



GRANT ALL ON FUNCTION "public"."handle_new_user"() TO "anon";
GRANT ALL ON FUNCTION "public"."handle_new_user"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."handle_new_user"() TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."increment_rep_xp"("p_delta" integer, "p_rep_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."increment_rep_xp"("p_delta" integer, "p_rep_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."increment_rep_xp"("p_delta" integer, "p_rep_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "service_role";



GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."match_knowledge_embeddings"("query_embedding" "public"."vector", "match_count" integer, "filter_company_id" "uuid", "filter_user_id" "uuid", "filter_source_types" "text"[], "filter_stage" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."match_knowledge_embeddings"("query_embedding" "public"."vector", "match_count" integer, "filter_company_id" "uuid", "filter_user_id" "uuid", "filter_source_types" "text"[], "filter_stage" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_knowledge_embeddings"("query_embedding" "public"."vector", "match_count" integer, "filter_company_id" "uuid", "filter_user_id" "uuid", "filter_source_types" "text"[], "filter_stage" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."set_sparring_personas_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_sparring_personas_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_sparring_personas_updated_at"() TO "service_role";



GRANT ALL ON FUNCTION "public"."set_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."set_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."set_updated_at"() TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";



GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."tg_set_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."tg_set_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."tg_set_updated_at"() TO "service_role";



GRANT ALL ON FUNCTION "public"."trg_assign_created"() TO "anon";
GRANT ALL ON FUNCTION "public"."trg_assign_created"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."trg_assign_created"() TO "service_role";



GRANT ALL ON FUNCTION "public"."trg_assign_status"() TO "anon";
GRANT ALL ON FUNCTION "public"."trg_assign_status"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."trg_assign_status"() TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "service_role";












GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "service_role";



GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "service_role";



GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "service_role";









GRANT ALL ON TABLE "public"."account_ai_summaries" TO "anon";
GRANT ALL ON TABLE "public"."account_ai_summaries" TO "authenticated";
GRANT ALL ON TABLE "public"."account_ai_summaries" TO "service_role";



GRANT ALL ON TABLE "public"."account_ai_tasks" TO "anon";
GRANT ALL ON TABLE "public"."account_ai_tasks" TO "authenticated";
GRANT ALL ON TABLE "public"."account_ai_tasks" TO "service_role";



GRANT ALL ON TABLE "public"."account_coaching_actions" TO "anon";
GRANT ALL ON TABLE "public"."account_coaching_actions" TO "authenticated";
GRANT ALL ON TABLE "public"."account_coaching_actions" TO "service_role";



GRANT ALL ON TABLE "public"."account_escalations" TO "anon";
GRANT ALL ON TABLE "public"."account_escalations" TO "authenticated";
GRANT ALL ON TABLE "public"."account_escalations" TO "service_role";



GRANT ALL ON TABLE "public"."accounts" TO "anon";
GRANT ALL ON TABLE "public"."accounts" TO "authenticated";
GRANT ALL ON TABLE "public"."accounts" TO "service_role";



GRANT ALL ON TABLE "public"."activities" TO "anon";
GRANT ALL ON TABLE "public"."activities" TO "authenticated";
GRANT ALL ON TABLE "public"."activities" TO "service_role";



GRANT ALL ON TABLE "public"."admin_config" TO "anon";
GRANT ALL ON TABLE "public"."admin_config" TO "authenticated";
GRANT ALL ON TABLE "public"."admin_config" TO "service_role";



GRANT ALL ON TABLE "public"."assignments" TO "anon";
GRANT ALL ON TABLE "public"."assignments" TO "authenticated";
GRANT ALL ON TABLE "public"."assignments" TO "service_role";



GRANT ALL ON TABLE "public"."audit_events" TO "anon";
GRANT ALL ON TABLE "public"."audit_events" TO "authenticated";
GRANT ALL ON TABLE "public"."audit_events" TO "service_role";



GRANT ALL ON TABLE "public"."audit_logs" TO "anon";
GRANT ALL ON TABLE "public"."audit_logs" TO "authenticated";
GRANT ALL ON TABLE "public"."audit_logs" TO "service_role";



GRANT ALL ON TABLE "public"."badges" TO "anon";
GRANT ALL ON TABLE "public"."badges" TO "authenticated";
GRANT ALL ON TABLE "public"."badges" TO "service_role";



GRANT ALL ON TABLE "public"."bounties" TO "anon";
GRANT ALL ON TABLE "public"."bounties" TO "authenticated";
GRANT ALL ON TABLE "public"."bounties" TO "service_role";



GRANT ALL ON TABLE "public"."call_chunks" TO "anon";
GRANT ALL ON TABLE "public"."call_chunks" TO "authenticated";
GRANT ALL ON TABLE "public"."call_chunks" TO "service_role";



GRANT ALL ON SEQUENCE "public"."call_chunks_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."call_chunks_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."call_chunks_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."call_manager_reviews" TO "anon";
GRANT ALL ON TABLE "public"."call_manager_reviews" TO "authenticated";
GRANT ALL ON TABLE "public"."call_manager_reviews" TO "service_role";



GRANT ALL ON TABLE "public"."call_pins" TO "anon";
GRANT ALL ON TABLE "public"."call_pins" TO "authenticated";
GRANT ALL ON TABLE "public"."call_pins" TO "service_role";



GRANT ALL ON TABLE "public"."call_scores" TO "anon";
GRANT ALL ON TABLE "public"."call_scores" TO "authenticated";
GRANT ALL ON TABLE "public"."call_scores" TO "service_role";



GRANT ALL ON TABLE "public"."calls" TO "anon";
GRANT ALL ON TABLE "public"."calls" TO "authenticated";
GRANT ALL ON TABLE "public"."calls" TO "service_role";



GRANT ALL ON TABLE "public"."coach_assignments" TO "anon";
GRANT ALL ON TABLE "public"."coach_assignments" TO "authenticated";
GRANT ALL ON TABLE "public"."coach_assignments" TO "service_role";



GRANT ALL ON TABLE "public"."coach_notes" TO "anon";
GRANT ALL ON TABLE "public"."coach_notes" TO "authenticated";
GRANT ALL ON TABLE "public"."coach_notes" TO "service_role";



GRANT ALL ON TABLE "public"."companies" TO "anon";
GRANT ALL ON TABLE "public"."companies" TO "authenticated";
GRANT ALL ON TABLE "public"."companies" TO "service_role";



GRANT ALL ON TABLE "public"."company_context" TO "anon";
GRANT ALL ON TABLE "public"."company_context" TO "authenticated";
GRANT ALL ON TABLE "public"."company_context" TO "service_role";



GRANT ALL ON TABLE "public"."company_licences" TO "anon";
GRANT ALL ON TABLE "public"."company_licences" TO "authenticated";
GRANT ALL ON TABLE "public"."company_licences" TO "service_role";



GRANT ALL ON TABLE "public"."company_playbook" TO "anon";
GRANT ALL ON TABLE "public"."company_playbook" TO "authenticated";
GRANT ALL ON TABLE "public"."company_playbook" TO "service_role";



GRANT ALL ON TABLE "public"."contacts" TO "anon";
GRANT ALL ON TABLE "public"."contacts" TO "authenticated";
GRANT ALL ON TABLE "public"."contacts" TO "service_role";



GRANT ALL ON TABLE "public"."crm_accounts" TO "anon";
GRANT ALL ON TABLE "public"."crm_accounts" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_accounts" TO "service_role";



GRANT ALL ON TABLE "public"."crm_actions" TO "anon";
GRANT ALL ON TABLE "public"."crm_actions" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_actions" TO "service_role";



GRANT ALL ON TABLE "public"."crm_activities" TO "anon";
GRANT ALL ON TABLE "public"."crm_activities" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_activities" TO "service_role";



GRANT ALL ON TABLE "public"."crm_auto_assign_runs" TO "anon";
GRANT ALL ON TABLE "public"."crm_auto_assign_runs" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_auto_assign_runs" TO "service_role";



GRANT ALL ON TABLE "public"."crm_call_links" TO "anon";
GRANT ALL ON TABLE "public"."crm_call_links" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_call_links" TO "service_role";



GRANT ALL ON TABLE "public"."crm_contact_notes" TO "anon";
GRANT ALL ON TABLE "public"."crm_contact_notes" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_contact_notes" TO "service_role";



GRANT ALL ON TABLE "public"."crm_contacts" TO "anon";
GRANT ALL ON TABLE "public"."crm_contacts" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_contacts" TO "service_role";



GRANT ALL ON TABLE "public"."crm_demo_call_links" TO "anon";
GRANT ALL ON TABLE "public"."crm_demo_call_links" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_demo_call_links" TO "service_role";



GRANT ALL ON TABLE "public"."crm_opportunities" TO "anon";
GRANT ALL ON TABLE "public"."crm_opportunities" TO "authenticated";
GRANT ALL ON TABLE "public"."crm_opportunities" TO "service_role";



GRANT ALL ON TABLE "public"."internal_users" TO "anon";
GRANT ALL ON TABLE "public"."internal_users" TO "authenticated";
GRANT ALL ON TABLE "public"."internal_users" TO "service_role";



GRANT ALL ON TABLE "public"."jobs" TO "anon";
GRANT ALL ON TABLE "public"."jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."jobs" TO "service_role";



GRANT ALL ON TABLE "public"."knowledge_embeddings" TO "anon";
GRANT ALL ON TABLE "public"."knowledge_embeddings" TO "authenticated";
GRANT ALL ON TABLE "public"."knowledge_embeddings" TO "service_role";



GRANT ALL ON TABLE "public"."licence_pools" TO "anon";
GRANT ALL ON TABLE "public"."licence_pools" TO "authenticated";
GRANT ALL ON TABLE "public"."licence_pools" TO "service_role";



GRANT ALL ON TABLE "public"."objection_evidence" TO "anon";
GRANT ALL ON TABLE "public"."objection_evidence" TO "authenticated";
GRANT ALL ON TABLE "public"."objection_evidence" TO "service_role";



GRANT ALL ON TABLE "public"."objection_library_items" TO "anon";
GRANT ALL ON TABLE "public"."objection_library_items" TO "authenticated";
GRANT ALL ON TABLE "public"."objection_library_items" TO "service_role";



GRANT ALL ON TABLE "public"."objection_suggestion_decisions" TO "anon";
GRANT ALL ON TABLE "public"."objection_suggestion_decisions" TO "authenticated";
GRANT ALL ON TABLE "public"."objection_suggestion_decisions" TO "service_role";



GRANT ALL ON TABLE "public"."offices" TO "anon";
GRANT ALL ON TABLE "public"."offices" TO "authenticated";
GRANT ALL ON TABLE "public"."offices" TO "service_role";



GRANT ALL ON TABLE "public"."opportunities" TO "anon";
GRANT ALL ON TABLE "public"."opportunities" TO "authenticated";
GRANT ALL ON TABLE "public"."opportunities" TO "service_role";



GRANT ALL ON TABLE "public"."org_limits" TO "anon";
GRANT ALL ON TABLE "public"."org_limits" TO "authenticated";
GRANT ALL ON TABLE "public"."org_limits" TO "service_role";



GRANT ALL ON TABLE "public"."orgs" TO "anon";
GRANT ALL ON TABLE "public"."orgs" TO "authenticated";
GRANT ALL ON TABLE "public"."orgs" TO "service_role";



GRANT ALL ON TABLE "public"."partners" TO "anon";
GRANT ALL ON TABLE "public"."partners" TO "authenticated";
GRANT ALL ON TABLE "public"."partners" TO "service_role";



GRANT ALL ON TABLE "public"."pins" TO "anon";
GRANT ALL ON TABLE "public"."pins" TO "authenticated";
GRANT ALL ON TABLE "public"."pins" TO "service_role";



GRANT ALL ON TABLE "public"."profiles" TO "anon";
GRANT ALL ON TABLE "public"."profiles" TO "authenticated";
GRANT ALL ON TABLE "public"."profiles" TO "service_role";



GRANT ALL ON TABLE "public"."rep_memory" TO "anon";
GRANT ALL ON TABLE "public"."rep_memory" TO "authenticated";
GRANT ALL ON TABLE "public"."rep_memory" TO "service_role";



GRANT ALL ON TABLE "public"."rep_xp_events" TO "anon";
GRANT ALL ON TABLE "public"."rep_xp_events" TO "authenticated";
GRANT ALL ON TABLE "public"."rep_xp_events" TO "service_role";



GRANT ALL ON TABLE "public"."reps" TO "anon";
GRANT ALL ON TABLE "public"."reps" TO "authenticated";
GRANT ALL ON TABLE "public"."reps" TO "service_role";



GRANT ALL ON TABLE "public"."score_cache" TO "anon";
GRANT ALL ON TABLE "public"."score_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."score_cache" TO "service_role";



GRANT ALL ON TABLE "public"."scorecard_criteria" TO "anon";
GRANT ALL ON TABLE "public"."scorecard_criteria" TO "authenticated";
GRANT ALL ON TABLE "public"."scorecard_criteria" TO "service_role";



GRANT ALL ON TABLE "public"."scorecard_stage_weights" TO "anon";
GRANT ALL ON TABLE "public"."scorecard_stage_weights" TO "authenticated";
GRANT ALL ON TABLE "public"."scorecard_stage_weights" TO "service_role";



GRANT ALL ON TABLE "public"."scorecard_versions" TO "anon";
GRANT ALL ON TABLE "public"."scorecard_versions" TO "authenticated";
GRANT ALL ON TABLE "public"."scorecard_versions" TO "service_role";



GRANT ALL ON TABLE "public"."scorecards" TO "anon";
GRANT ALL ON TABLE "public"."scorecards" TO "authenticated";
GRANT ALL ON TABLE "public"."scorecards" TO "service_role";



GRANT ALL ON TABLE "public"."sparring_personas" TO "anon";
GRANT ALL ON TABLE "public"."sparring_personas" TO "authenticated";
GRANT ALL ON TABLE "public"."sparring_personas" TO "service_role";



GRANT ALL ON TABLE "public"."sparring_sessions" TO "anon";
GRANT ALL ON TABLE "public"."sparring_sessions" TO "authenticated";
GRANT ALL ON TABLE "public"."sparring_sessions" TO "service_role";



GRANT ALL ON TABLE "public"."sparring_turns" TO "anon";
GRANT ALL ON TABLE "public"."sparring_turns" TO "authenticated";
GRANT ALL ON TABLE "public"."sparring_turns" TO "service_role";



GRANT ALL ON TABLE "public"."team_settings" TO "anon";
GRANT ALL ON TABLE "public"."team_settings" TO "authenticated";
GRANT ALL ON TABLE "public"."team_settings" TO "service_role";



GRANT ALL ON TABLE "public"."titles" TO "anon";
GRANT ALL ON TABLE "public"."titles" TO "authenticated";
GRANT ALL ON TABLE "public"."titles" TO "service_role";



GRANT ALL ON TABLE "public"."tmcs" TO "anon";
GRANT ALL ON TABLE "public"."tmcs" TO "authenticated";
GRANT ALL ON TABLE "public"."tmcs" TO "service_role";



GRANT ALL ON TABLE "public"."user_badges" TO "anon";
GRANT ALL ON TABLE "public"."user_badges" TO "authenticated";
GRANT ALL ON TABLE "public"."user_badges" TO "service_role";



GRANT ALL ON TABLE "public"."user_selected_title" TO "anon";
GRANT ALL ON TABLE "public"."user_selected_title" TO "authenticated";
GRANT ALL ON TABLE "public"."user_selected_title" TO "service_role";



GRANT ALL ON TABLE "public"."user_titles" TO "anon";
GRANT ALL ON TABLE "public"."user_titles" TO "authenticated";
GRANT ALL ON TABLE "public"."user_titles" TO "service_role";



GRANT ALL ON TABLE "public"."users" TO "anon";
GRANT ALL ON TABLE "public"."users" TO "authenticated";
GRANT ALL ON TABLE "public"."users" TO "service_role";



GRANT ALL ON TABLE "public"."whisperer_segments" TO "anon";
GRANT ALL ON TABLE "public"."whisperer_segments" TO "authenticated";
GRANT ALL ON TABLE "public"."whisperer_segments" TO "service_role";



GRANT ALL ON TABLE "public"."whisperer_sessions" TO "anon";
GRANT ALL ON TABLE "public"."whisperer_sessions" TO "authenticated";
GRANT ALL ON TABLE "public"."whisperer_sessions" TO "service_role";



GRANT ALL ON TABLE "public"."whisperer_trigger_candidate_decisions" TO "anon";
GRANT ALL ON TABLE "public"."whisperer_trigger_candidate_decisions" TO "authenticated";
GRANT ALL ON TABLE "public"."whisperer_trigger_candidate_decisions" TO "service_role";



GRANT ALL ON TABLE "public"."whisperer_trigger_library" TO "anon";
GRANT ALL ON TABLE "public"."whisperer_trigger_library" TO "authenticated";
GRANT ALL ON TABLE "public"."whisperer_trigger_library" TO "service_role";



GRANT ALL ON TABLE "public"."whisperer_triggers" TO "anon";
GRANT ALL ON TABLE "public"."whisperer_triggers" TO "authenticated";
GRANT ALL ON TABLE "public"."whisperer_triggers" TO "service_role";



GRANT ALL ON TABLE "public"."xp_events" TO "anon";
GRANT ALL ON TABLE "public"."xp_events" TO "authenticated";
GRANT ALL ON TABLE "public"."xp_events" TO "service_role";









ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";































