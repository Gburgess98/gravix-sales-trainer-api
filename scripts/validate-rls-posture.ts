/** Non-vacuous self-test for check-rls-posture: planted violations MUST be caught, clean SQL MUST pass. */
import { analyseSql, isPostLockdown, checkRepo } from "./check-rls-posture";
let fail = 0;
const t = (name: string, ok: boolean) => { console.log(`${ok ? "✅" : "❌"} ${name}`); if (!ok) fail++; };
t("flags create table without RLS", analyseSql("x.sql", "create table public.foo (id uuid);").some((f) => f.rule === "public_table_without_rls"));
t("passes create table with RLS", analyseSql("x.sql", "create table public.foo (id uuid);\nalter table public.foo enable row level security;").length === 0);
t("flags grant to anon", analyseSql("x.sql", "grant select on public.foo to anon;").some((f) => f.rule === "grant_to_public_role"));
t("flags grant to authenticated", analyseSql("x.sql", "grant all on table public.foo to authenticated;").length > 0);
t("flags grant to PUBLIC", analyseSql("x.sql", "grant execute on function public.f() to public;").length > 0);
t("allows grant to service_role", analyseSql("x.sql", "grant all on public.foo to service_role;").length === 0);
t("ignores commented-out violations", analyseSql("x.sql", "-- create table public.foo (id int);").length === 0);
t("ignores temp tables", analyseSql("x.sql", "create temp table foo (id int);").length === 0);
t("pre-lockdown migrations out of scope", !isPostLockdown("sql/20260818_x.sql") && isPostLockdown("sql/20261106_x.sql"));
t("repo currently clean", checkRepo().length === 0);
console.log(fail ? `\n❌ ${fail} failed` : "\n✅ all self-tests passed");
process.exit(fail ? 1 : 0);
