# Database and operations reference

Evidence date: 2026-09-30. Based on supplied catalog report, view SQL and repository; not a complete executable schema dump. Keep raw catalog/grant dumps and vulnerability remediation notes private. A prior attempt to publish detailed security metadata was blocked; no disclosure approval has been given.

## Application relations

| Relation | Role / key |
| --- | --- |
| flights | Original current state; PK number/date/type |
| flight_snapshots | Original change history; bigint id; lookup number/date |
| scraper_status | Original health; id=1 |
| origin_flights | New current state; PK number/date/type/source_airport/data_source |
| origin_snapshots | New change history; bigint id; source-aware lookup plus scraped_at index |
| origin_scraper_status | New health; scraper_id=paa_origin |
| source_priority | Registry; data_source PK, priority integer; origin_flights has source FK |
| flight_canonical_view | Actual view ranks sources then full-joins departure/arrival by number/route/date |

Both collectors' inserted columns and conflict keys match the supplied schema. Original fields include city; new fields have origin_city/destination_city. No custom public triggers listed. Statistics row counts are estimates, so zero does not prove empty source_priority or status.

## Time/schema limitations

Original flights.last_checked/last_updated and flight_snapshots.scraped_at are timestamp without time zone. New observations and status tables are timestamptz; origin_flights.last_updated is raw text. ST/ET are text in both systems. Confirm stored historical UTC convention and raw DateUpdated before any migration.

Canonical view currently selects highest priority DESC per number/date/type/source airport, then full-joins legs on identical number, origin/destination city and scheduled date. It does not blend legacy tables, require that reporting airport matches the leg's city, deterministically resolve tied priorities, or match cross-midnight dates. These are limitations to validate; do not replace the join with an unrestricted +/-1-day rule, which can pair daily flights incorrectly.

## Scheduling and tests

- Files exist: .github/workflows/scrape.yml and origin_scraper.yml. Python 3.11; DB_HOST/DB_NAME/DB_USER/DB_PASSWORD/DB_PORT secrets. Same secrets for both jobs.
- GitHub schedule blocks are commented; cron-job.org sends workflow_dispatch POST with ref main. Owner reports both now active. Original cadence ~ten minutes observed; newer interval/runtime needs checking.
- No collector dry-run or single-airport CLI is provided. Main functions upsert, snapshot and delete old history. Use an isolated DB/pure-function fixtures for tests; a production manual workflow run is a real write/delete operation.
- New collector catches batch write failures and continues; inspect logs, not just green status. Both can advance health despite failed/empty fetches.

## Storage

Free plan confirmed. SQL current database: 23 MB. Public relation totals including indexes: ~11.23 MiB. Largest: flight_snapshots ~5.94 MiB, ~26,013 estimated rows. Other reported metrics: RAM commitment 1.28 GB/use 411 MB, disk use 302 MB, space 0.04 GB; dashboard metric scopes/labels differ and are not fully reconciled.

Keep current two-month original/seven-day newer retention provisionally. Measure daily database/table growth over 7–14 days after resumed collection. Snapshot-only-on-change already controls event growth; unchanged upserts still create write activity/dead tuples. Autovacuum timestamps exist. Don't drop indexes or use VACUUM FULL without a measured problem and reviewed task.

## Evidence requests (read-only)

Only request what's relevant. Current values matter more than re-exporting everything each session.

```sql
SELECT * FROM public.source_priority ORDER BY priority DESC, data_source;
SELECT * FROM public.origin_scraper_status;
SHOW timezone;
SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
```

Public privileges require a dedicated access review (BL-017). Preserve public reads and collector access while denying unnecessary public writes; do not treat RLS alone as protecting every operation. The detailed security evidence stays in the private owner-supplied report. No permission fix has been applied or tested by this kit.
