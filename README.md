# Islamabad Flight Tracker

A static website for checking Islamabad arrivals/departures and reviewing changes to airport-reported flight information. The agreed next direction is Islamabad flights with information from both ends of the route.

## Start here

- [SPEC.md](SPEC.md): short product and technical reference, including unknowns.
- [BACKLOG.md](BACKLOG.md): prioritized fixes and improvements with stable task IDs.
- [CONTRIBUTING.md](CONTRIBUTING.md): how people and LLMs deliver small, reviewable changes.
- [ARCHITECTURE_CHANGELOG.md](ARCHITECTURE_CHANGELOG.md): decisions, reasons, and migration history.
- [AGENTS.md](AGENTS.md): repository instructions for coding assistants.
- [notes/STORAGE_PLAN.md](notes/STORAGE_PLAN.md): active-pipeline storage policy and read-only diagnostics.

## What is implemented

| Component | Current behavior |
| --- | --- |
| Public website | HTML/CSS/JavaScript in `docs/`; browser reads Supabase directly |
| Search | `flights`, filtered by flight-number substring, date, direction, nature; city filtering after retrieval |
| Detail/history | `flight_snapshots` and `scraper_status`; refresh every five minutes |
| Saved searches | Browser localStorage; ten entries; labels, reordering and deletion |
| Sharing | Copy text or open a WhatsApp draft; airline/FlightStats verification links |
| Original collector | `scraper.py`: Islamabad, yesterday/today/tomorrow; writes original tables |
| New collector | `origin_scraper.py`: six airports, today/tomorrow; writes separate `origin_*` tables |
| Automation | Two manual GitHub Actions workflows; in-file schedules are commented out. Owner confirms cron-job.org triggers `scrape.yml`; newer collector inactive. Five recent GitHub dispatches succeeded at roughly ten-minute intervals; exact cron-job.org settings unverified. |

The newer collector is **not connected to the public frontend** in this snapshot. `notes/REFERENCE_QUERIES.txt` describes a canonical view/source priorities, but its DDL is absent and some examples use fields inconsistent with the newer writer. Do not execute its migration snippets as-is.

## Running locally

Serve the site with `python3 -m http.server 8000 --directory docs`, then open `http://localhost:8000`. It uses the configured remote Supabase project; this is not an isolated test environment. Supabase browser publishable keys are public by design; access depends on database grants and RLS policies, which are not supplied here.

Collectors use Python 3.11 and `requests`, `urllib3`, `psycopg2-binary`. Configure `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, and optional `DB_PORT` (default 5432). Database tables, unique constraints, policies and views must already exist. No reproducible database setup is included yet. Running either collector writes to the configured database and performs retention deletion; use an isolated test database for development.

## Evidence boundary

Reviewed 2026-09-30. GitHub `main` is `8e8b03f71e0d23e26a0612a34a9667128a12241f`, matching the uploaded May 23 ZIP. The latest inspected [scraper run](https://github.com/abnaeem0/islamabad-airport-flight-tracking/actions/runs/36672187772) fetched all six original-pipeline batches, recorded five changes, and completed two-month cleanup. Owner confirms the newer collector is inactive. No database queries or production writes were performed by this review; deployed website and database policies remain unverified.

The former `docs/readme` wishlist is preserved in `notes/LEGACY_README.txt`. Several wishlist items are already implemented; use the backlog for current work.
