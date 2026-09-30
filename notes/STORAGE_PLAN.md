# Storage plan

Updated 2026-09-30. Priority: keep new flight updates flowing; accept less history when necessary.

## Operational baseline

| Item | Evidence / implication |
| --- | --- |
| Active job | cron-job.org dispatches `scrape.yml` → `scraper.py` |
| Observed cadence | Five GitHub runs about ten minutes apart; this is evidence of recent cadence, not verification of cron-job.org settings |
| Active retention | Two months for `flights` and `flight_snapshots`; latest log confirms cleanup completed |
| Snapshot volume | New/status/time/city changes only, not a snapshot every ten-minute poll |
| Other pipeline | `origin_scraper.py` inactive per owner; its seven-day retention does not run while inactive |
| Reported usage | Owner reports 4% disk used; denominator, database quota and growth unknown |

Disk is storage, not RAM. Supabase distinguishes database size from total disk usage, which also includes WAL and system space. A low disk percentage is not sufficient to establish database-quota headroom. Confirm the exact dashboard metric, numerator/denominator and plan allowance before choosing longer history.

If the project is on Supabase Free, the current documented read-only threshold is **500 MB database size**, rather than the full disk allocation. Track that database allowance separately from total disk percentage; verify the actual plan before applying this limit.

After dormancy, current history may be sparse; measure several days of resumed activity rather than treating today's percentage as the eventual two-month steady state. Inactive `origin_*` tables may retain old rows indefinitely unless another verified process cleans them. Do not delete them or reactivate their writer just to run cleanup; first establish contents, dependencies and any needed historical value.

## Policy now

1. Keep the active collector's existing two-month retention provisionally. There is no measured reason yet to shorten or lengthen it.
2. Record the same dashboard metric and database/table sizes daily for 7–14 days; no automatic monitoring has been installed by this delivery.
3. Inspect largest tables, indexes and dead tuples; unchanged-flight upserts still create write activity even though snapshots are change-only.
4. Before enabling the newer collector, estimate its separate growth and choose its retention/filter scope. `REQUIRE_ISB_LEG` is currently False, so it would collect unrelated flights too.
5. If space becomes constrained, propose a shorter oldest-history window and verify cleanup, preserving current/upcoming flights. Do not reduce collection frequency merely to avoid saving unchanged snapshots: those are already skipped. Any deletion/window change needs an explicit reviewed task.

**Proposed planning thresholds, not Supabase limits or deployed behavior:** review at 60% of the relevant allowance; prioritize a capacity/retention task at 75% or when observed growth leaves less than 14 days of headroom. Tune these after identifying the metric and measuring growth. Do not automatically delete data at either threshold.

## Read-only diagnostics

Run in the Supabase SQL editor against the intended project. These queries only read metadata or flight-table aggregates. The review did not execute them. Counts on very large tables can be expensive; run when appropriate.

```sql
SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;

SELECT schemaname, relname,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
       pg_size_pretty(pg_relation_size(relid)) AS table_size,
       pg_size_pretty(pg_indexes_size(relid)) AS index_size,
       n_live_tup AS estimated_live_rows,
       n_dead_tup AS estimated_dead_rows,
       last_autovacuum
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

SELECT MIN(scheduled_date) AS oldest_date,
       MAX(scheduled_date) AS newest_date,
       COUNT(*) AS retained_flights
FROM public.flights;

SELECT (scraped_at AT TIME ZONE 'Asia/Karachi')::date AS observation_day,
       COUNT(*) AS changes
FROM public.flight_snapshots
WHERE scraped_at >= NOW() - INTERVAL '14 days'
GROUP BY 1
ORDER BY 1;
```

The daily snapshot query assumes `scraped_at` is timestamptz; verify its actual type before interpreting the dates. These SQL sizes measure PostgreSQL relations/database, not the entire provisioned disk. First query lists any existing origin tables without assuming they exist. Confirm their schema before querying their contents.

Deleting rows does not guarantee the dashboard's allocated disk size shrinks. PostgreSQL vacuum makes dead-row space reusable; allocated disk and logical data volume are different. Do not use `VACUUM FULL`, truncate tables or drop indexes as routine first steps.

Sources checked 2026-09-30: [Supabase database and disk size](https://supabase.com/docs/guides/platform/database-size), [safe data deletion](https://supabase.com/docs/guides/database/postgres/data-deletion).
