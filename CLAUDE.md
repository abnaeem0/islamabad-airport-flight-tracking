# CLAUDE.md: Islamabad Flight Tracker

Instructions for any LLM working on this repo (Claude or not). Read this, then `PLAN.md` (spec, decisions, backlog, journal, current handoff).
Only these two doc files exist (D-013). Do not create others. This repo is **public**: never write passwords, secret keys, the owner's email or phone, raw database permission dumps, or vulnerability details into it.

## 1. What this is

A public flight-status site for **Islamabad International Airport** (Islamabad only, D-007).
Audience: people in Islamabad, travellers coming in, the people picking them up, and hotel/transport staff.
It polls the Pakistan Airports Authority (PAA) API, keeps current state per flight, records a snapshot only when something changes, and shows status plus change history.
For domestic flights landing in Islamabad, the details page also shows what the departure airport reports, with its own source and freshness, in one card under Islamabad's line (BL-010C, BL-036).

## 2. Architecture (verified against the repo on 2026-11-01)

| Layer | What | Where |
|---|---|---|
| Website | Static vanilla JS/CSS, no build step. Pages live in `docs/` (GitHub Pages). The contact page posts to a form service (Web3Forms, key placeholder in `contact.html` until set) | `docs/index.html`, `script.js`, `airlines.js` (shared airline names and FlightStats codes), `flight_detail.html`, `flight_detail.js`, `about.html`, `contact.html`, `style.css`, `sitemap.xml`, `logos/` |
| Database | Supabase Postgres, free plan (23 MB on 2026-09-30). Browser reads directly with a publishable key | tables in section 4 |
| Original collector | Islamabad only; yesterday, today, tomorrow; keeps 2 months | `scraper.py` -> `flights`, `flight_snapshots`, `scraper_status` |
| New collector | 6 airports (Islamabad, Karachi, Lahore, Faisalabad, Multan, Peshawar); today and tomorrow; keeps 7 days; concurrent fetch, serial writes | `origin_scraper.py` -> `origin_flights`, `origin_snapshots`, `origin_scraper_status` |
| Scheduler | Workflows `.github/workflows/scrape.yml` and `origin_scraper.yml` are **manual-trigger only** (cron blocks commented out). An external cron-job.org job dispatches them, reported by owner as every 15 minutes. Python 3.11, secrets `DB_HOST DB_NAME DB_USER DB_PASSWORD DB_PORT` | |
| Probes | `.github/workflows/aerodatabox_probe.yml` (manual, read-only) runs `probes/aerodatabox_probe.py` with secret `AERODATABOX_KEY` (BL-040). Writes nothing to the database | `probes/` |
| Source | `https://paaconnectapi.paa.gov.pk/api/flights/{date}/{Arrival|Departure}/{city}`, unofficial, TLS verification disabled in both collectors (BL-027) | |

**Website reads the origin tables** through `isb_flights_view`, `isb_snapshots_view` and `isb_arrival_origin_view` (plus `origin_snapshots` for the "first seen departed" line), and falls back to the old `flights` / `flight_snapshots` tables if a view errors (BL-032). `flight_canonical_view` is not used by any page.
Data flow: PAA API -> collector (flatten, normalise flight number, compare with stored row) -> upsert current row, insert snapshot on change -> a non-terminal flight missing from a non-empty feed becomes `Dropped` -> browser queries Supabase.

## 3. Conventions

- **Flight numbers:** no spaces (`TK571`). The new collector also strips hyphens; the original does not (BL-006).
- **Terminal statuses:** `Dropped, Cancelled, Landed, Departed`. They are excluded from drop detection only; ordinary upserts and change snapshots still process them. Do not repeat the old code comment that says otherwise.
- **Change types:** `new, status_change, time_change, city_change, dropped`.
- **Time:** the owner says displayed PAA times are PKT. Airport dates should be computed in `Asia/Karachi`; observations we generate (`scraped_at`, `last_checked`, `last_run`) are UTC (verified for `scraped_at` on 2026-11-02). Original tables store some of those without a timezone. Do not cast or migrate historical timestamps blindly (BL-009).
- **`data_source`** (today `paa`) names a feed; every source must exist in `source_priority`. The views pick the **highest** `priority` number (`priority DESC`), so a new source written into `origin_flights` would outrank PAA on the site. Keep new sources (airline pages, ADS-B) in their own tables until a display rule is agreed (D-016, BL-051).
- **Dropped** means "vanished from the feed", not confirmed cancellation. Never present stale or partial data as fresh.
- **Airport board data is not aircraft position.** Do not invent estimated arrival times unless a rule is agreed.
- **Secrets:** DB credentials are environment variables only. The Supabase publishable key in the JS is fine; a service or secret key must never be committed.
- Postgres folds unquoted `ST`/`ET` to lowercase, so JS and `RealDictCursor` use `st`/`et`. In SQL write them as plain `st` and `et`; quoting them as `"ST"` fails (the columns are lowercase).

## 4. Database (public-safe summary)

| Relation | Role / key |
|---|---|
| `flights` | original current state; PK flight number, date, type |
| `flight_snapshots` | original change history; bigint id; lookup by number and date |
| `scraper_status` | original health; single row `id=1` |
| `origin_flights` | new current state; PK number, date, type, source_airport, data_source; has `origin_city` and `destination_city` |
| `origin_snapshots` | new change history; bigint id; indexed by source-aware lookup and `scraped_at` |
| `origin_scraper_status` | new health; row `scraper_id=paa_origin` |
| `source_priority` | source registry; `data_source`, `priority`. Rows (2026-11-02): `paa` 10 (in use), `airline_web` 20 and `adsb` 30 (registered, no collector yet) |
| `isb_arrival_origin_view` | applied 2026-11-04: pairs each Islamabad arrival with the departure leg from the origin airport (rules in PLAN.md section 5); not in the repo as a file, SQL is in PLAN.md |
| `flight_canonical_view` | ranks sources, splits departure and arrival legs, `FULL JOIN` on number, origin city, destination city and date |

Limits of the view: overnight flights (legs on different dates) do not pair; wrong-airport rows can multiply results; ties in priority are not resolved deterministically; international flights have no origin side. Do not "fix" it with an unrestricted +/-1 day join, which pairs daily flights wrongly (BL-019).
Time types: original `last_checked`, `last_updated`, `scraped_at` are `timestamp without time zone`; new observation columns are `timestamptz`; `origin_flights.last_updated` is raw text; ST/ET are text everywhere.
Permissions and security findings are **not** recorded here (public repo). See BL-017 and keep that evidence private.

## 5. Working mode: manual first

The owner works in a chat window and applies edits by hand. Assume no repo access and no ability to run code unless the tool proves otherwise. Tool access changes who performs a step, not what counts as evidence.

1. **One backlog task per chat.** Agree acceptance checks first. New ideas become backlog entries, not part of the current change.
2. **Get current files.** Ask for the smallest missing current file or query result. Say which supplied files are current. Never invent file contents, columns, runtime status or test results.
3. **Edit blocks.** One change at a time, anchors unique, no line numbers or ellipses:
   ```
   TASK: BL-___
   FILE: docs/script.js
   FIND EXACTLY ONCE:
   <current lines>
   REPLACE WITH:
   <new lines>
   WHY: one sentence
   CHECK: steps and expected result
   ROLLBACK: reverse this exact edit
   ```
   If the owner says the FIND text was not found exactly once, stop and ask for the surrounding current text. Give a whole file when it is small or mostly changing. SQL goes in a separate block with target, prerequisites, checks and a realistic rollback (a deletion has no rollback without a prior export).
4. **Frontend checks without a live site:** serve `docs/` locally and stub the Supabase script (`window.supabase.createClient`) in a headless browser; check at 320 to 1280 px wide for sideways scroll. Tests against hostile data (HTML in city or status text) are worth keeping. A chat-only LLM should instead give the owner click-by-click checks.
5. **Never run the collectors as casual tests.** They write and delete retained history, and have no dry-run or single-airport option. Use pure-function fixtures or an isolated database; a live run must be explicit.
6. **Report four states separately:** PROPOSED, APPLIED, CHECKED, SAVED. Generated code is only Proposed. Done needs evidence of application, passed checks, and a GitHub commit or documented backup.
7. **Two LLMs must not patch the same file from the same base.** Apply one change, then re-share the current file.
8. **Paste kit** (what the owner pastes with this file and the task):

| Task | Paste |
|---|---|
| Any | this file, the backlog entry, the Current handoff |
| Search / history | `docs/script.js`, `docs/index.html` (+ `style.css` for layout) |
| Details / sharing | `docs/flight_detail.js`, `docs/flight_detail.html` |
| Collector | the Python file (+ the workflow file when the environment matters) |
| Database | the output of the read-only query you asked for; never passwords |
| Scheduling | both workflow files, sanitised cron-job.org settings, recent run logs |
| Content | the current HTML page |

**Start prompt**
```
I work by pasting edits by hand. Here are CLAUDE.md, task BL-___, the Current handoff and the
current files. Base commit/checkpoint: ___. Unsaved edits: none / ___.
Restate the goal and acceptance checks, then propose one small change as FILE/FIND/REPLACE blocks
with manual checks. Do not mark anything applied or done until I report results.
```
**Capture prompt**
```
Capture this as a PLAN.md backlog entry; do not implement it: ___. Use the next unused BL ID,
with actual vs expected or user need, confidence, priority, size, acceptance checks, blockers.
Ask only for details that change scope.
```
**Review prompt (second LLM)**
```
Review BL-___ against the supplied current files and proposed edit: correctness, scope,
compatibility, whether the checks prove the acceptance criteria. Do not rewrite unrelated code.
Separate proof from assumption.
```
**End prompt**
```
Give me a copy-paste wrap-up for PLAN.md: (1) replacement backlog line/entry for this task,
(2) replacement Current handoff plus one journal entry, (3) decision/spec edit only if needed,
(4) checks run and still to run, (5) what was APPLIED vs only PROPOSED, (6) the exact next action.
My reported results: ___. Saved commit/backup: ___ / not saved.
```

## 6. Keeping the docs current

- Claude updates both files at the end of a working chat and hands back the full file; the owner replaces it in GitHub. Other LLMs give wrap-up text to paste into `PLAN.md`.
- Each session: update one task, replace the Current handoff, add one short journal entry.
- Task IDs are never reused. When `PLAN.md` gets long, move finished entries and old journal sessions to `ARCHIVE.md` (created only then) and keep the IDs.
- A file's recorded status is authoritative; old chats are history. Flag conflicts and prefer the newer owner decision or evidence.
