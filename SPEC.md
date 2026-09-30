# Reference spec

Status: working baseline, 2026-09-30. Source: supplied May 23 code snapshot, verified matching GitHub main, recent workflow logs and owner answers. Update this file when accepted behavior changes.

## Goal and scope

Help people checking Islamabad flights understand the airport-reported status, how it changed, and information from both the departure and arrival ends. Islamabad remains the public focus. Multi-airport collection supplies supporting evidence; expansion to a general six-airport public tracker is not agreed.

## Present architecture

PAA flight API → Python collectors → Supabase PostgreSQL → static browser website in `docs/`.

| Pipeline | Data coverage | State / history / health | Retention in code |
| --- | --- | --- | --- |
| Original | Islamabad; yesterday, today, tomorrow; arrival and departure | `flights` / `flight_snapshots` / `scraper_status` | Two months |
| Multi-airport | Islamabad, Karachi, Lahore, Faisalabad, Multan, Peshawar; today and tomorrow | `origin_flights` / `origin_snapshots` / `origin_scraper_status` | Seven days |

Both collectors upsert current state and batch-insert snapshots on new/status/time/city changes. Current-state upserts are per record. A missing flight in a nonempty response can be marked Dropped; empty responses are skipped. Terminal statuses exclude rows from drop detection, but do not stop ordinary upserts or change snapshots. Raw API payloads are not retained.

The newer collector fetches concurrently with six workers and writes serially; this does not guarantee one active request per airport. Public pages still query only the original pipeline. A canonical view and source-priority model appear in reference notes; their implementation and deployed schema are unknown.

## Current user behavior

- Search by date, flight-number/airline-code substring, arrivals/departures, domestic/international, and city dropdown. Airline-name and free-text city search are not implemented despite site wording.
- Show scheduled time (ST), estimated time (ET), and airport remarks. This is periodically collected airport data, not aircraft-position tracking.
- Inspect change history; detail page polls every five minutes. Search results do not auto-refresh.
- Keep up to ten local recent searches with custom names, order and deletion. No accounts or cross-device sync.
- Share a copyable flight update or open a WhatsApp draft. No automatic alerts.

## Agreed direction and operating constraints

1. Add information from both ends for Islamabad-connected flights; international counterpart airports outside the six collectors may have no supporting data.
2. Only the original Islamabad collector is active. Owner confirms cron-job.org dispatches `scrape.yml`; the newer collector is inactive pending investigation. Five recent runs are approximately ten minutes apart. Leave the newer collector inactive until its health, storage cost and integration plan are checked.
3. Storage management must protect continued ingestion. Keep the active collector's existing two-month retention provisionally; the latest run completed this cleanup. Owner reports 4% Supabase disk usage, but its denominator, database allowance, table sizes and growth are unverified. Inactive origin tables do not receive this cleanup and could contain old data. Measure active-table growth after restart and size inactive tables before extending retention or reactivating the newer collector. See notes/STORAGE_PLAN.md.
4. Keep source identity and observation time visible when presenting combined evidence. Source precedence, cross-midnight leg matching and conflict handling need explicit acceptance before canonical integration.
5. Prefer small changes to the existing static architecture. Notifications, accounts, pricing analysis and aircraft tracking remain proposals.

## Required contracts for reliability work (proposed)

- Flight identity includes normalized number, scheduled date and arrival/departure; source-aware rows also include airport and source. Confirm cross-midnight matching rules separately.
- Use Asia/Karachi for airport dates/display, aware UTC for observations. Confirm PAA ST/ET/DateUpdated semantics before changing source timestamps.
- Distinguish failed fetch, valid empty feed and successful nonempty feed; report success per airport/date/direction rather than presenting a global run timestamp as proof every flight is fresh.
- Define Dropped as missing from a verified feed, not a confirmed cancellation. Agree repeated-miss/partial-feed safeguards before altering the rule.

## Still needed from the owner / deployment

- Deployed website state; repository main has been reconciled to the ZIP commit.
- Exact cron-job.org interval/timezone and retry settings; original dispatch target and recent successful outcomes are known. Owner will investigate the inactive newer job; omit credentials/tokens.
- Supabase schema-only export including views, indexes, constraints, grants, RLS; no guest/user data or secrets.
- Storage allowance, largest tables and daily growth; expected update frequency and acceptable staleness.
- Priority between a reliable existing site and exposing both-end information; detailed alert requirements if alerts are wanted later.
