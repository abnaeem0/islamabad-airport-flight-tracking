# PLAN.md: Islamabad Flight Tracker

Spec, decisions, backlog, journal and the current handoff, in one place. Stable rules and architecture are in `CLAUDE.md`.
This merges the repo's earlier doc set (`AGENTS.md`, `docs/SPEC|BACKLOG|DECISIONS|JOURNAL|DB|WORKFLOW.md`, dated 2026-09-30) and keeps its task IDs. Last updated: 2026-11-10.
Public repo: no email, phone, credentials or security details in this file.

## 1. Current handoff

- **2026-11-10 (later):** airline status pages planned as a second source for local airlines (D-016): BL-051 collector for PIA and Fly Jinnah, stored in its own tables and not shown yet; BL-052 shows it on the site later; BL-053 Airblue, waiting for Airblue's permission. Tested 2026-11-07/10: PIA and Fly Jinnah answer plain scripts from the owner's home connection; Airblue answers scripts with a Cloudflare check page; AirSial and South Air publish no status; free ADS-B feeds do not see Islamabad approaches. Night: a GitHub runner gets the same answers, so BL-051 will be a GitHub workflow started after every Origin Scraper run, success or failure (no new cron job, no home machine), with conservative caps and a slow fallback when PAA is down. Nothing built. Next: owner approves BL-051's design (then checkpoint 1); owner writes to Airblue (BL-053).
- **2026-11-10:** BL-049 ("Check it yourself" chips on the flight page: FlightStats, airline status, call the airline) merged to `main` (PR #20) by the owner's instruction, checked in a stubbed browser only. Owner: on a phone open a PIA arrival and a Dubai night arrival and tap each chip.
- **2026-11-09 (later):** BL-041 (tap a search result to open it; History button replaced by an arrow) and BL-042 (search kept in the address, so Back/reload/shared links show the same results and scroll) merged to `main` by the owner's instruction, checked in a stubbed browser. Owner: on a phone, search, open a flight, press Back; rollback is reverting the two merge commits.
- **2026-11-09:** BL-039 (FlightStats link date, share box asks the sender when unsure, editable message, any WhatsApp country code) is merged to main, checked in a stubbed browser only. Waiting on the owner's phone check and city list (query in BL-039). BL-040 captured: real origin times from a cheap flight-data service, later. 
- **Goal now:** live-check the BL-036 details card, switch on the contact form (needs the owner's key), then pick one small task.
- **Owner reports:** everything is working; both collectors are running; an external cron-job.org job dispatches both workflows about every 15 minutes.
- **Site fixes live since 2026-11-01:** 7 files under `docs/` merged to `main` (PR #5, commit `9d4e72f`). Checked in a local headless browser against a stubbed database (18 checks pass; the same checks fail on the old files) and, per the owner's report on 2026-11-01, **checked on the live site**. The contact form is applied but **not switched on** (no access key yet).
- **Checked in the repo on 2026-11-01 (code evidence, not runtime):**
  - Workflows exist and are manual-trigger only (cron commented out).
  - Both collectors compute dates with naive `datetime.now()` (`scraper.py:245`, `origin_scraper.py:473`). On a GitHub runner (UTC by default, `[UNVERIFIED]` from a log) that is one day behind PKT between 00:00 and 05:00. Impact is small: today is still covered, tomorrow's flights simply appear after 05:00 PKT (BL-002).
  - `REQUIRE_ISB_LEG = False` (`origin_scraper.py:54`); retention is still 2 months and 7 days.
  - `docs/contact.html` still has the placeholder address (line 40) and **the file is cut off mid-footer** (ends at `<a href="index.html">Ho`), so the live contact page is broken (BL-028).
  - `docs/script.js`: the Supabase-load guard (line 3) returns before `renderHistory()` (line 24), which supports the BL-003 hypothesis.
  - `docs/flight_detail.js:242` labels arrivals "Departure" (BL-004).
- **Owner decisions:** Islamabad only; origin context for domestic arrivals; free Supabase; ingestion matters more than long history; no public email or phone (use a contact form); two doc files only.
- **Not seen yet:** recent run logs; the cron-job.org settings; whether the BL-017 fix has been applied (the 2026-11-02 check shows it was still needed).
- **Verified 2026-11-02 (owner ran read-only queries):** `source_priority` holds `paa` (priority 10, in use), `airline_web` (20) and `adsb` (30), the last two registered with no collector yet. `scraped_at` is stored as UTC (latest value 3 minutes behind UTC now), so the details page's UTC-to-PKT display is right and BL-009's open question is closed. `origin_scraper_status.last_run` was 7 minutes old, consistent with the ~15-minute dispatch. BL-017 is still needed (details kept private).
- **Owner setup, left for later:** (1) create a free Web3Forms access key and paste it into `docs/contact.html` (BL-028), then send a test message; (2) run the private BL-017 fix and re-run the permission check; (3) cron-job.org settings for BL-001.
- **Docs migration done 2026-11-04:** old doc files deleted (history stays in git), README is a pointer, draft PR #4 closed.
- **Prepared 2026-11-02, not merged:** (1) branch `origin-box-2026-11-02`: details-page departure-airport box plus this documentation; (2) a separate scraper branch for PKT dates and the early-morning yesterday window (BL-002, BL-031). Order for the owner: apply the BL-010B view SQL first, check it, then merge the website branch; merge the scraper branch separately and watch one run's log.
- **Live 2026-11-04:** BL-010B view applied and checked (133 matched). The two BL-032 views were run ("no rows returned", which is normal for a CREATE); their existence is not yet confirmed by a query. PR #10 is still not merged, so the site still reads the old tables.
- **BL-032 switch (merged 2026-11-04, owner away, by the owner's instruction):** search, history and details read `isb_flights_view` / `isb_snapshots_view` and **fall back to the old tables** if a view errors, so the site works even if the view SQL was not applied. Rollback: revert the merge commit (the old tables and collector were never touched). Turn off the old Islamabad collector only after several days of side-by-side checks. Checked in a stubbed browser with views present and missing; not checked on the live site.
- **Merged 2026-11-04 (late), by the owner's instruction:** BL-036 details-page journey card (PR #11) (needs no SQL; reads `origin_snapshots` for the "first seen departed" line and hides it if that read fails).
- **BL-038 live 2026-11-06:** Islamabad-first details page with the one-line departure notice and the "Airport board" history switch. Owner checks a few domestic arrivals (one where Lahore/Karachi is late, one departed) and an international one (no switch).
- **Simplicity review 2026-11-06 (Claude, code reading only; merged 2026-11-09 from PR #16 with IDs renumbered):** the biggest duplication is two collectors writing two sets of Islamabad tables plus fallback reads in the site (BL-045); `flight_canonical_view` is unused (BL-046); logos are already simple (BL-016); the share text's FlightStats link broke for airline codes with a digit (fixed as BL-043); South Air needs adding (BL-047); Serene Air is not in the code and is still grounded. A monthly owner check is BL-044.
- **Suggested next sequence:** BL-017 (private evidence) -> switch on the contact form (BL-028 key) -> BL-002 -> BL-010A (validate origin data) -> BL-021 / BL-022 -> BL-010B/C. A newly observed outage goes first.

## 2. Spec

Labels: **Confirmed** = owner decision. **Observed** = code or catalog evidence. **Open** = undecided. Proposals are not commitments.

**Goal (Confirmed):** a public Islamabad-only flight status website for people in Islamabad, people travelling to Islamabad, the people picking them up, and hotel/transport staff. For domestic flights landing in Islamabad, the details page shows what the departure airport reports, so users can judge whether it has left or is delayed.

**Observed features:** search by date and flight number or airline-code substring; direction and nature filters; city dropdown; current status and ST/ET; change history; ten browser-local recent searches with names; copy and WhatsApp sharing. No accounts, alerts or aircraft location. Airline-name and free-text city search are not implemented despite some page wording.

**Confirmed constraints:** manual copy-paste development across LLMs; no paid agent required; free Supabase; protect continued updates before expanding history; the owner's email and phone stay off the site, and visitors reach the owner through a contact form with an optional callback request (D-014).

**Next feature boundary:** keep existing search and history readers. Add an origin box for domestic arrivals showing origin status, reported times, source and its own last-checked time, and handle unavailable, stale or conflicting data visibly. A broader migration is a separate proposal.

**Data:** source is the PAA API; the owner says displayed times are PKT. Verify raw `DateUpdated` meaning before converting history. Dropped is not confirmed cancellation. Airport status is not aircraft position.

**Open questions (answer when they matter):**
1. Which audience should the design favour: family pickups, hotel/taxi staff, or general travellers? Should hotels get a way to watch several flights at once?
2. Origin box: show ST and ET with status, or a simple "departure reported / not yet reported"? Show unavailable evidence?
3. At what age is data "stale", and what should the user do then?
4. How much history is useful, given the ingestion-first storage policy?
5. Mobile and English priorities, traffic and SEO goals, analytics and privacy notice, any later revenue (ads change privacy and legal needs)?
6. What counts as success: successful pickups, fewer repeat checks, public repeat users?

## 3. Decisions

Accepted owner choices, proposals and observed implementation are different things. Add new dated decisions; supersede old ones explicitly.

| ID | Decision | Status |
|---|---|---|
| D-001 | Static vanilla website reads Supabase directly; safety depends on grants, RLS, view permissions | Observed |
| D-002 | Current state plus snapshots on meaningful change | Observed |
| D-003 | New collector uses separate `origin_*` tables | Observed |
| D-004 | `data_source` and `source_priority` model; sources must be registered | Observed |
| D-005 | Concurrent fetch, serial writes (new collector); partial batches can succeed, green run can hide failures | Observed |
| D-006 | Drop missing flights from a non-empty feed; skip an empty feed; partial responses can cause false drops | Observed |
| D-007 | Public site Islamabad-only; domestic arrivals get departure-airport guidance on the details page. Does **not** by itself accept filtering stored rows, using the view unchanged, moving all readers, or retiring the original collector | Accepted 2026-09-30 |
| D-008 | Manual-first, shared files, small exact edit blocks, checks, journal handoff | Accepted 2026-09-30 |
| D-009 | Airport dates and display in `Asia/Karachi`; generated observations UTC; ST/ET are PKT per owner; verify history before migrating | Accepted direction |
| D-010 | Add origin context before considering full migration; decide cutover only after measured coverage and a rollback plan | Proposed |
| D-011 | Storage protects ingestion: no automatic retention extension; keep 2 months / 7 days pending 7-14 days of growth data | Accepted priority |
| D-012 | One canonical doc set of AGENTS, README and six docs files | **Superseded by D-013** |
| D-013 | **Two doc files only:** `CLAUDE.md` (rules, architecture) and `PLAN.md` (everything else). Claude hands back full updated files; other LLMs give paste-in wrap-ups. 2026-11-01 | Accepted, owner |
| D-015 | **Site reads the origin tables for Islamabad** (through the views `isb_flights_view` and `isb_snapshots_view`); 7-day retention is accepted; the origin collector always fetches yesterday for Islamabad. Old tables and the old collector stay untouched until the owner says to stop it. 2026-11-03 | Accepted, owner |
| D-014 | **No public email or phone.** Contact goes through a form that emails the owner, with an optional callback request. Supersedes the plain-email fix in BL-005. 2026-11-01 | Accepted, owner |
| D-016 | **Airline status pages as a second source for local airlines.** Use the airlines' own pages where they answer plain requests (PIA, Fly Jinnah). Pull the bulk page where one exists, else one page per route, and only around the times flights change; per-flight pulls only when the airport data leaves a gap; keep total load small (target under about 100 requests a day); review the sources monthly (BL-044). Airblue only with Airblue's permission, by browser automation if that is what they allow, even at a lower rate (BL-053). Airline data stays out of `origin_flights` until BL-052 agrees how to show it. The structure in BL-051 is Proposed. 2026-11-10 | Accepted, owner (direction) |

Entry template: `D-### title; date; status (Proposed/Accepted/Implemented/Superseded); task ID; problem; decision; alternatives; affected contracts; rollback; evidence; unknowns.` Accepted does not mean shipped; record the commit separately.

## 4. Backlog

One queue. IDs are never reused. Next unused ID: **BL-054**. BL-010A..D are children of BL-010.
States: Inbox -> Ready -> Doing -> **Proposed** (edit prepared and checked in a local mock, not yet applied or saved) -> Testing (applied, checks incomplete) -> Done; Blocked at any stage; Deferred/Cancelled keep a reason. P0 active outage or data loss; P1 correctness, security or reliability; P2 useful next capability; P3 later. Size S one small checkable change; M split into checkpoints; L parent only. Confidence: owner report / code evidence / catalog evidence / reproduced. Done needs applied + checks passed + saved commit or backup. All items below are **unimplemented** unless a note says otherwise.

| ID | Type | Pri | Size | State | Task |
|---|---|---|---|---|---|
| BL-001 | chore | P2 | S | Ready | Document actual scheduler setup (cron-job.org target, interval, retries, recent outcomes) |
| BL-002 | bug | P1 | S | Ready | Use PKT airport dates in both collectors |
| BL-003 | bug | P2 | S | Done | Saved history when the Supabase script fails to load |
| BL-004 | bug | P2 | S | Done | Share text: correct Arrival/Departure label |
| BL-005 | bug | P2 | S | Cancelled | Plain contact email (superseded by BL-028 / D-014) |
| BL-006 | chore | P2 | M | Ready | Unify flight-number normalisation |
| BL-007 | bug | P1 | M | Done | Render external values safely (no raw `innerHTML`) |
| BL-008 | chore | P3 | S | Done | Put analytics scripts inside `<head>` |
| BL-009 | chore | P2 | M | Ready | Timestamp storage and display contract (label PKT) |
| BL-010 | feature | P2 | L | Blocked | Origin-airport context on domestic Islamabad arrivals (parent) |
| BL-010A | research | P1 | S | Ready | Validate current origin collection |
| BL-010B | bug | P1 | M | Testing | Leg-pairing view `isb_arrival_origin_view` (SQL in section 5). Applied by the owner 2026-11-04; first result 133 matched, 531 unmatched (see BL-010A progress) |
| BL-010C | feature | P2 | M | Proposed | Departure-airport box on the details page (branch ready; merge only after the BL-010B view exists) |
| BL-010D | research | P3 | M | Inbox | Evaluate full reader migration later |
| BL-031 | bug | P1 | S | Proposed | Origin collector: fetch yesterday's departures in the early morning so overnight legs keep updating (with BL-002 PKT dates) |
| BL-033 | bug | P1 | S | Inbox | Multan returns HTTP 500 from the PAA API on every run, so Multan has no rows at all; the Multan collection is wasted calls and Multan-origin flights never pair |
| BL-034 | feature | P2 | M | Inbox | Add more origin airports? About 76% of Islamabad arrivals come from airports we do not watch (international and other Pakistani cities); decide which to add once the city list is known |
| BL-035 | research | P2 | S | Inbox | Explain 9 unmatched arrivals from watched airports that do have departure rows (missing rows for today's flights; PA204 numbering; PK368 8 h gap) |
| BL-036 | feature | P1 | S | Testing | Details page as one journey card: flight number, then Islamabad's line (large), then the departure airport's line (smaller), with early/late wording, "first seen departed" time and a note when the two boards disagree. Merged 2026-11-04 (PR #11); checked in a stubbed browser (6 scenarios x 4 widths), live check pending |
| BL-038 | feature | P1 | S | Testing | Islamabad-first details page (owner decisions 2026-11-06). Islamabad's line stays on top. A one-line notice appears only when the departure board is fresh (under 60 min) and shows something Islamabad's has not: cancelled; departed (with the time we first saw it) while Islamabad shows no landed/departed/airborne status; or 15+ min later than Islamabad's expected time (`NOTICE_MINUTES`). History has an "Airport board" switch (Islamabad arrivals / <city> departures) on matched domestic arrivals only; it switches the history table, not the top line. Long notes removed; the "not aircraft position" note moved to About. Table: plain headers, short dates, fixed columns on phones, Change column hidden under 360 px. Stubbed-browser checks pass (11 cases x 320/375/768/1280 px). Live check pending |
| BL-037 | research | P2 | S | Inbox | Departure boards rarely show when a flight actually left: owner saw Islamabad say "Advanced" (30 min early) while Karachi showed only the scheduled time. Check one raw PAA departure response for fields the collector drops (it keeps only ST, ET, remarks), and how ET/remarks change after take-off |
| BL-032 | feature | P1 | M | Testing | Switch search, history and details to the origin tables via two Islamabad-only views, with fallback to the old tables; always fetch yesterday for Islamabad (D-015). Merged 2026-11-04 (PR #10); live checks pending |
| BL-011 | feature | P3 | L | Inbox | Flight-change notifications |
| BL-012 | feature | P3 | M | Inbox | Adaptive scrape frequency |
| BL-013 | chore | P3 | M | Inbox | Batch database work where measured |
| BL-014 | idea | P3 | L | Inbox | Actual aircraft tracking |
| BL-015 | idea | P3 | L | Inbox | Fare and booking-window research |
| BL-016 | chore | P3 | M | Inbox | README, sitemap, logo housekeeping |
| BL-017 | chore | P1 | M | Ready | Restrict public database privileges (evidence kept private) |
| BL-018 | research | P2 | M | Inbox | Evaluate Islamabad relevance filter safely |
| BL-019 | bug | P1 | M | Cancelled | Overnight and duplicate canonical matching. Cancelled 2026-11-06: the view it fixes is unused (BL-046); the site's pairing view `isb_arrival_origin_view` already handles overnight legs |
| BL-020 | research | P2 | S | Inbox | Measure retention and storage before extending |
| BL-021 | bug | P1 | M | Ready | Report honest freshness and batch outcomes |
| BL-022 | bug | P1 | M | Testing | Keep direction in flight history identity |
| BL-023 | bug | P2 | S | Ready | Preserve labels and saved filter state (label part done; city and stale filters still open) |
| BL-024 | bug | P2 | M | Ready | Share actual current state and retained route |
| BL-025 | bug | P1 | M | Ready | Guard against false drops from partial feeds |
| BL-026 | chore | P2 | M | Inbox | Avoid overlapping writers; pin environment |
| BL-027 | research | P2 | M | Inbox | Diagnose disabled TLS verification |
| BL-028 | feature | P1 | M | Testing | Contact form with callback option; footer repaired and email removed (done). Not live until the owner's form key is pasted and a test message arrives |
| BL-029 | bug | P2 | S | Done | Search page default date used the UTC date, not Pakistan's |
| BL-030 | bug | P2 | S | Done | Flight details page layout: unstyled header, table overflow, unreadable share links |
| BL-039 | bug | P2 | S | Proposed | Share text: FlightStats link used Islamabad's arrival date, so overnight arrivals linked to the wrong day's flight. Now the share box shows which day it took off: departure board for matched domestic arrivals, else an estimate (origin time zone + typical flight time, per-flight correction list); any doubt -> the sender picks the departure date. Message editable; Copy, Share, WhatsApp (any country code, default 92) |
| BL-040 | research | P2 | M | Inbox | Real origin departure times for international arrivals from a flight-data service (AeroDataBox airport board), replacing BL-039's estimate where available |
| BL-041 | feature | P2 | S | Testing | Search results: tapping anywhere on a flight card opens its details page; the "History" button is replaced by a `›` arrow (owner 2026-11-09: the button was not intuitive). Merged 2026-11-09 (PR #18); live check pending |
| BL-042 | feature | P2 | S | Testing | Search kept in the address (`?date=&q=&type=&route=&city=`): Back from a flight, a reload or a shared link shows the same results, filters and place in the list. Merged 2026-11-09; live check pending |
| BL-043 | bug | P1 | S | Testing | FlightStats share link broke for airline codes with a digit (J2143 became J/2143) and for airlines FlightStats files under another code. Ported the checked code table from branch `airline-links-logos` |
| BL-044 | chore | P2 | S | Ready | Monthly 10-minute owner check that the site's flows and outside links still work (checklist in task records); reminder lives on the owner's phone, not in the app |
| BL-045 | chore | P1 | M | Blocked | Retire the old Islamabad collector: stop dispatching `scrape.yml`, then remove `scraper.py`, its workflow and the site's fallback reads; export, then drop `flights`, `flight_snapshots`, `scraper_status`. Blocked on BL-032 live checks |
| BL-046 | chore | P3 | S | Ready | Drop the unused `flight_canonical_view` and `notes/REFERENCE_QUERIES.txt` (which only documents it); BL-019 is cancelled with it |
| BL-047 | feature | P2 | S | Doing | Add South Air (IATA Z8, ICAO SQB): helpline added 2026-11-09; FlightStats has no South Air data (owner checked Z8 and SQB), so no FlightStats link; logo still open. Its code Z8 previously belonged to Amaszonas (Bolivia), so generic logo and tracker sites may show the wrong airline |
| BL-049 | feature | P2 | S | Testing | Flight page shows the share message's outside links as chips under Islamabad's line ("Check it yourself"): FlightStats, the airline's status page, call the airline. Share button unchanged. Merged 2026-11-10 (PR #20); live check pending |
| BL-048 | feature | P2 | M | Testing | Search page: one box for flight, airline or city with suggestions; the day's list loads at once and narrows as you type; day arrows; Arrivals/Departures and route as full-width toggles; cards with 48 px logo, airline name and coloured status; Recent saves opened flights and settled searches |
| BL-051 | feature | P2 | M | Ready | Airline status collector, phase 1: PIA and Fly Jinnah status pages for Islamabad flights, polled only in change windows, stored in `airline_legs` with a fetch log; one config file for timing and switches; not shown on the site |
| BL-052 | feature | P2 | S | Inbox | Show airline-reported times on the site (e.g. "PIA reports it left Jeddah 03:57"), with source and age; needs a display rule; blocked on BL-051 |
| BL-053 | research | P3 | S | Blocked | Airblue: its status pages answer scripts with a Cloudflare check; ask Airblue for permission (a fixed IP to allow, or browser automation at a low rate). Adapter stays off until they agree |

### Task records

**BL-001** Record sanitised cron-job.org settings (target, ref, interval, retries) and the latest outcomes of both workflows. Do not recreate the workflow files. Accept: settings and latest coverage/failure evidence recorded, owner reports kept separate from log checks. Note: the origin job has a 22-minute timeout but is dispatched every ~15 minutes, so overlap is possible (BL-026).

**BL-002** Both `main()` functions use naive `datetime.now()`. Fix: `from zoneinfo import ZoneInfo` and `datetime.datetime.now(ZoneInfo("Asia/Karachi"))` for the date list only; leave the UTC `fetched_at` lines. Accept: fixtures around 00:00 and 05:00 PKT pick the right dates, both files pass a syntax check, an applied run's log shows the right dates. Impact is small (see section 1), so it should not displace BL-017.

**BL-003** The Supabase guard (script.js line 3) returns before `renderHistory()` (line 24); the reported symptom is not reproduced. Check: block the CDN or break storage, confirm what renders; look for "Supabase failed to load." in the console and `localStorage.getItem('flightHistory')`. Accept: saved history renders and search failure is explained, without deleting saved data. Fix idea: render history and attach handlers before the guard; add a fallback CDN or self-host. Distinct from BL-023.

**BL-004** Share text uses "Departure" for arrivals (`flight_detail.js:242`). Use the selected flight's direction, not an assumed route. Accept: one arrival and one departure share with right labels and unchanged date/time.

**BL-005** Cancelled 2026-11-01: the owner does not want any email on the site. See BL-028.

**BL-006** Original strips spaces, newer strips spaces and hyphens, frontend also uppercases and strips underscores; legacy rows may hold spaces. Accept: one shared rule with examples, a collision and legacy migration plan before any update, both paths find equivalent forms.

**BL-007** Search and details insert city/status/flight strings through `innerHTML`; city names also go into `<option value>`. Use DOM/`textContent` or an escape helper. Accept: an HTML-like fixture renders as plain text in options, cards and history; search and sharing still work.

**BL-008** Move the gtag blocks inside `<head>` where they sit outside it. Tidy-up only.

**BL-009** Original observations are timezone-less, newer are aware, raw update timestamps differ. Accept: generated vs source semantics documented, fixtures display correct PKT, no blind database cast, any migration proposed separately from display changes. Includes labelling ST/ET as PKT in the UI.

**BL-010 / A / B / C / D** Parent initiative, done in children. **A:** read source registry and status, and the newest origin-workflow logs; confirm valid origin airports and freshness; spot batch failures behind a green run. **B** (blocked on A): rank ties, wrong-airport rows, duplicate matches, missing routes and same-date joins; deterministic leg pairs on sanitised fixtures; no ambiguous +/-1 day matches. **C** (blocked on B and the Open question 2): for a domestic arrival show departure-board status, ST/ET, source and its own time; test valid, missing, stale and conflicting data; header and Islamabad history unchanged; never claim actual aircraft location. **D:** optional, compare coverage, history window, performance, rollback before any cutover or retirement. *Progress 2026-11-02 on B and C:* view written and run against 11 synthetic cases on a local Postgres 16 (same day, overnight, daily flight, no departure row, international, empty time, source priority, gap over 6 h, departure after arrival, wrong route); details box tested with a stubbed database (15 checks: matched, stale, unmatched, missing view, hostile text, 320 to 1280 px). Neither is applied to the live database or merged to `main`. *Progress 2026-11-04 (live results, owner ran the queries):* column types confirmed (`scheduled_date` date; `st`, `et`, `last_updated` text; `last_checked` and `scraped_at` timestamptz; the columns are lowercase `st`/`et`, so SQL must not quote them). Of 665 Islamabad arrival flights (28 Oct to 5 Nov): **133 matched**; **508** come from airports we do not watch (international and other Pakistani cities, so they cannot pair by design); **15** have no departure row anywhere (flight codes such as APESL, APFFL, APHKD, 9P9960, Z8921, which look like positioning or charter flights, not scheduled service); **9** have departure rows but did not pair. Of the arrivals from the five watched airports, about 85% paired (133 of 157). Of the 9: PF127, 9P678 and PA204 lack a departure row for 4 Nov at Karachi while other days exist; PA204's earlier arrivals (28 to 31 Oct) have no Karachi departure rows at all; PK368 on 4 Nov has a Karachi departure at 13:30 but arrives 21:55 (gap 8 h 25 min, outside the 6 h rule). Cause unknown (BL-035). Coverage in `origin_flights` 28 Oct to 5 Nov for Islamabad, Karachi, Lahore, Peshawar and Faisalabad, all checked within about 3 minutes of each other; **Multan has no rows**: the latest origin run log shows HTTP 500 from the PAA API for every Multan call (BL-033). The run log also shows the PKT date line working (`Fetching ['2026-11-04', '2026-11-05']`). *Progress 2026-11-02 on A:* source registry read (only `paa` in use); origin status was fresh. Still to do: confirm which origin airports report valid data and look for batch failures in the logs.

**BL-011** Need channel, recipients, opt-in, triggers, identity and cost decisions; do not assume Telegram or accounts. Split before implementing.

**BL-012** Near-flight polling is a proposal; measure useful freshness and run duration first; avoid duplicate scheduling (the external scheduler already dispatches).

**BL-013** Per-record SELECT and upsert remain; only snapshots batch. Optimise from a measured bottleneck with equivalent change/drop behaviour.

**BL-014 / BL-015** Aircraft position and fare research need new sources, cost and agreement first.

**BL-016** Sitemap lists `flight_detail.html` (useless without query parameters); README to become a short pointer to these two files (done 2026-11-04). Logos (reviewed 2026-11-06): the four local airlines' logos are in `docs/logos/`; all others still come from `pics.avs.io`, and a failed logo is simply hidden. Recommendation: keep this (downloading 30+ foreign logos adds upkeep for little gain); only change `REPO_LOGOS` in `script.js` to the relative path `logos/`. The PAA `Logo` field is stored as `airline_logo` in every row and snapshot but no page uses it. Open PR #13 (`airline-links-logos`, 2026-11-04) downloads 32 foreign logos, against this recommendation; its FlightStats code table was ported as BL-043 on 2026-11-09. Owner to decide whether to close it.


**BL-044** Monthly owner check, about 10 minutes, on a phone. Record the result as one journal line; anything broken becomes a new BL entry.
1. Search page, today: arrivals and departures appear; the freshness line says minutes, not hours.
2. A domestic arrival's details page shows the departure-airport line; an international one shows history and no departure line.
3. Share on one local and one foreign flight: each link (FlightStats, airline status page, our page) opens the right flight or page.
4. Logos: a local and a foreign one show (a failed logo is hidden, so look for gaps).
5. cron-job.org and GitHub Actions: the latest runs of both jobs succeeded.
6. Contact form (once switched on): a test message arrives.
7. Airlines: any new code in today's results with no logo, or a known airline that stopped appearing (Serene Air `ER` grounded since Oct 2025, South Air `Z8` new).
8. Airline sources (once BL-051 runs): the health query shows no source paused or empty for days. News: Air Karachi or Air Punjab flying, Serene Air back, AirSial or South Air with a status page, PIA's site changed, a reply from Airblue (BL-053).

**BL-045** The site reads the origin views and falls back to the old tables only if a view errors (BL-032). Once the views are confirmed and several days of side-by-side counts agree: (1) stop the cron-job.org job for `scrape.yml`; (2) watch a few days; (3) remove `scraper.py`, `.github/workflows/scrape.yml` and the fallback branches in `script.js` and `flight_detail.js`; (4) export the three old tables, then drop them. Gains: one collector, one workflow, half the scheduled runs, three fewer tables, no fallback code. Loss: history older than 7 days (accepted in D-015). Also settles BL-010D.

**BL-046** No page reads `flight_canonical_view` (checked in the repo 2026-11-06). Before dropping, check nothing in the database depends on it and save its definition privately (`SELECT pg_get_viewdef('public.flight_canonical_view', true);`). Rollback: re-create from that saved definition.

**BL-047** South Air: began operations November 2025 (inaugurated in Multan), scheduled domestic flights from Karachi since 16 Jul 2026 with ATR 72s; Karachi, Islamabad, Bahawalpur, Sukkur, Gwadar, Rahim Yar Khan and others. PAA already lists it as `Z8` (e.g. `Z8921` in BL-010A, `Z8922` Islamabad-Multan on 2026-11-08); booking sites and trackers show `SQB`. No public flight-status page (website `southair.com.pk`, booking engine `z8.ibe.ejmiral.com`, no manage-booking link). 2026-11-09: owner chose the call-centre number for the share text (`051 111 222 999`, from the website footer); added to `AIRLINE_INFO`. Owner checked FlightStats the same day: South Air is not found under `Z8` or `SQB`, so its share text has no FlightStats line and no departure-day question, only the phone number. Still open: logo (`docs/logos/z8.png` and `LOCAL_AIRLINES` in `script.js`).


**BL-048** *New 2026-11-09 (owner: find flights by airline name; logos unreadable on phones; dropdowns take too much space).* Owner chose one search box over a separate airline dropdown, and card option C (logo plus airline name) from chat mockups; asked for the Arrivals/Departures toggle not to be cramped. Built in `docs/index.html`, `docs/script.js`, `docs/style.css`, new shared `docs/airlines.js` (airline names, aliases, FlightStats codes; also used by `flight_detail.js`):
- The day's flights load once (`isb_flights_view`, fallback `flights`) as soon as the page opens; the box, the toggles and the day arrows only filter that list, with no Search button. The box matches flight number (`PK301`, `301`, `pk 301`), airline code, name or alias (`PIA`, `Emirates`, `airblue`, `AZAL`) and city. Suggestions under the box list matching airlines and cities in the day's list with counts.
- Controls: search box; day pill with ‹ › and the native date picker; Arrivals/Departures and All routes/International/Domestic as full-width toggles (side by side above 700 px). City dropdown, Search button and hint line removed. A line shows "N flights · updated X ago" (collector's last run) and the Recent link.
- Cards: 48 px logo on white (local logos now load from the relative `logos/` path, foreign from `pics.avs.io` at 96 px; a failed logo shows the airline code), airline name, flight, type and city, route badge, times and a coloured status label (words always shown).
- Recent: saves a flight when it is opened (flight number, day, direction) and a search once it has been left alone for 4 seconds (text and toggles), never the plain day list. Custom names kept; up to 10; older saved entries (before this change) still show and open.
- Today's list shows a "Now · HH:MM" line before the first flight scheduled at or after the current time (PKT). The page always opens at the top so visitors see the search area first (owner, 2026-11-09, after a first version jumped straight to now); a floating "Now ↓" / "Now ↑" button (today only, while the line is off-screen, above "↑ Top" when both show) scrolls to the line with about one earlier flight above.
- Today is not pinned in the address: `date` is written only for other days, so a reload tomorrow opens the new today while text and toggles carry over. Back, Forward and reload return to the same place in the list; a fresh visit starts at the top.
- Site header on phones (all pages): title and links on one line (about 48 px instead of about 100 px).
- Address keeps `date`, `q`, `type`, `route` as before (BL-042); old links with `city=` put the city in the box. Back returns to the same place in the list.
- Checks (stubbed browser, 2026-11-09): 20 frozen-clock checks for the Now line and button (midday, after midnight, late evening, other day, button up/down/stacked, address without today's date, reload after midnight, header one line at 320-600 px) and 40 search-page checks (matching for 11 queries, suggestions, toggles, address, day arrows, empty day, no-match, HTML in city/status, failed logo, view fallback, old and new Recent entries, settled-search saving, opened-flight saving, Back position, 320-1280 px without sideways scroll and with toggle labels fitting); details-page share checks (28) and airline-link checks (44) rerun after moving the airline list. Not yet seen on a real phone or with live data.


**BL-017** Confirmed by the private owner-supplied report: excess public write privileges and missing protection on one status table. Need an explicit relation/role plan, a view security review, and preserved collector access. Accept: public reads work, unnecessary public writes denied, collector writes preserved, inherited privileges considered, safe non-destructive verification. **Keep the evidence and the SQL private** (not in this repo, not in public chats). Owner's read-only check on 2026-11-02 shows the issue is still present.

**BL-018** Setting `REQUIRE_ISB_LEG = True` excludes unrelated rows from the "seen" set, so drop detection would mark existing unrelated rows Dropped. Deleting data is not a prerequisite and has no rollback without an export. Accept: filter and drop scope defined together, retained unrelated rows not falsely dropped, owner approves before the flag flips, no hidden cleanup. (This replaces my earlier suggestion to delete rows first.)

**BL-019** The same-day join splits overnight legs; wrong-airport rows can multiply results. Need real affected examples. Accept: daily-flight and midnight fixtures give correct pairs or an explicit "ambiguous/unavailable", ties resolved deterministically.

**BL-020** 23 MB is a point-in-time size, not a forecast. Collect 7-14 days of comparable growth, learn quota headroom, then choose a window. No extension just for the origin box.

**BL-021** Both collectors advance run health after failed or empty feeds; the new one catches failed batches. A green workflow is not enough. Accept: failure, empty and success separated, coverage recorded, stale/partial UI honest, total failure cannot look fully fresh.

**BL-022** *Proposed with BL-032 (history links now carry `&type=`; old links keep the direction of their first record).* In plain words: an arrival and a departure can share a flight number on the same date (the plane arrives, then leaves as the same number). The database keeps them apart, but the details link only had number and date, so the page could mix the two lists into one history. The database key includes direction (`type`) but detail URLs use only number and date, so an arrival and departure with the same number can mix. Accept: URLs, header, history and share isolate direction; old links handled; same number/date fixture tested.

**BL-023** Saving again resets the custom label, city is not saved, replay can keep stale filters. Accept: label survives repeat and replay, full filters restored, malformed storage recovers.

**BL-024** Share uses the latest event snapshot; a dropped snapshot lacks route and time; unknown airline prefixes may build invalid verification links. Accept: right current state and route with dropped/no-history fixtures, supported links accurate, absent links omitted.

**BL-025** One partial non-empty response can mark missing flights Dropped. Agree a repeated-miss or coverage rule. Accept: outage, partial and reappearance fixtures pass, route context retained.

**BL-026** No workflow concurrency guard or dependency manifest. Measure both collectors first; do not add GitHub schedules on top of the external one. Accept: overlap policy and dependencies defined, run duration observed, safe scheduling handoff documented.

**BL-027** Both collectors use `verify=False`. Diagnose the certificate chain before choosing a verified alternative. Accept: verified TLS works, or a narrow justified exception.

**BL-039** *New 2026-11-08 (owner report).* FlightStats files a flight under the date it leaves its origin (confirmed on FlightStats: PK246 Dammam 23:30 on 10 Oct, Islamabad 05:35 on 11 Oct, listed under 10 Oct). The share link used the page date, which for arrivals is Islamabad's arrival date, so overnight arrivals opened the next day's flight. Owner decisions 2026-11-08/09: estimate with a way to correct; a wrong link costs too much trust, so **if there is any doubt, ask the sender**; keep Copy, Share and WhatsApp-to-a-number. 2026-11-09 wording change: plain dates instead of "Night before / Same day" (relative wording was hard to follow and "took off" was wrong for flights not yet flown); WhatsApp country code editable. Built in `docs/flight_detail.js` (share modal) and `docs/style.css`:
- Departure day: departures use Islamabad's date (no question). Arrivals: departure board's `departure_date` when matched (sure); else `estimateDeparture`: Islamabad scheduled arrival minus the typical flight time from `ORIGIN_TIMES` (city -> time zone, minutes), read in the origin's time zone. Sure only if the date holds with the flight time off by 120 min (10 min for a `FLIGHT_MINUTES` correction).
- Sure: one line "FlightStats: flight leaving X on Sat 7 Nov" with **Change**. Doubt (near midnight, unknown city, missing time): "Which day does it leave X?" with the two dates (day before landing, landing day), our guess preselected and marked "best guess", a **Check** link to FlightStats for each, and "FlightStats lists flights by the day they leave. The right one lands in Islamabad on <date>." With no guess nothing is preselected and the link has no date until the sender picks.
- Message is an editable text box; picking a day swaps only the FlightStats link, keeping edits. Buttons: Copy, Share (phone share sheet, shown only where the browser has one), WhatsApp to a number with an editable country code (default 92; leading 0 of the number dropped). The message still carries our page link and the airline status link (AirSial: manage-reservation page).
- Corrections: add a flight to `FLIGHT_MINUTES` with its real scheduled minutes when its guess is wrong. Flight times in `ORIGIN_TIMES` are estimates; checked so far: PK246 (Dammam 245), TK710 (Istanbul, real 5 h 20 vs 5 h 30), PK782 (Toronto, real 14 h 15, table set to 855). City spellings unchecked: run `SELECT city, count(*) FROM public.isb_flights_view WHERE type = 'Arrival' GROUP BY 1 ORDER BY 2 DESC;` and add missing cities. Two read-only check queries (domestic truth test; list of flights to check by hand) were given to the owner in chat, not in the repo.
- Checks (2026-11-09, stubbed browser): 20 date cases (domestic matched/unmatched/view missing, Dubai, Kuwait, correction list, Ras Al-Khaimah, Istanbul, London winter/summer, Toronto, unknown city, missing time, HTML in city text, departure) plus picking a day keeps edits, highlight follows, Check links per day, Change, reopening starts fresh, phone share sheet sends the edited text; no sideways scroll at 320/360/390/768/1280 px. Live: share one overnight international arrival and one sure arrival; confirm the FlightStats pages.

**BL-040** *New 2026-11-08 (owner asked for the cheapest real source).* User need: the real departure date and time at the origin for international arrivals (share link, and possibly a "left Dubai at ..." line), instead of BL-039's estimate. FlightStats itself is not usable (no free API for a public site; scraping is against its terms).
- Candidate (researched 2026-11-08, not tried): AeroDataBox airport board (FIDS) for ISB arrivals with `withLeg=true`, which adds "times of departure from the origin for arriving flights, if available". Tier 2 = 2 units per call. Two calls a day (12-hour windows; window limit not confirmed) is about 120 units a month. Plans: free Basic 400 units/month (RapidAPI "free forever", API.Market 7-day trial), non-commercial only and attribution required; Pro $7.50-8/month for 5,000 units, commercial use allowed, no attribution. Data may be stored 7 days only, which matches the origin tables' retention. Flight-status-by-number also takes `dateLocalRole=Arrival`, but that is one call per flight.
- Free partial alternative already researched (project doc on airline sites): Airblue, PIA and Fly Jinnah status pages give both ends' scheduled times for their own international legs. Foreign carriers not covered.
- Shape: small collector step (GitHub Action, key as a secret) writing one row per flight number + ISB arrival date with the origin's scheduled departure (local, with time zone), `data_source` registered in `source_priority`; the share link prefers it over the estimate.
- Accept: for a day of ISB international arrivals, the stored origin dates match FlightStats for a sample of 10 flights including 3 overnight ones; units used per month recorded; terms re-checked against how the site is run (ads or not).

**BL-041** *New 2026-11-09 (owner request).* Actual: a search result opened only through its small "History" button. Expected: tap the flight itself. Built in `docs/script.js` (each card is now an `<a href>` to the details page, same URL as before including `&type=`; `window.viewHistory` replaced by a local `detailUrl`, nothing else used it) and `docs/style.css` (link card without underline, visible keyboard focus, `›` arrow pinned to the right edge even when a logo fails; the phone rule that made the button full width removed). Being a real link, long-press / middle-click / open in new tab also work.
- Checks (2026-11-09, stubbed browser): two cards render as links with no button; a click in the middle of the card text opens `flight_detail.html?flight=…&date=…&type=…`; HTML in city text stays text; arrow at the right edge with and without a logo; no sideways scroll at 320/375/768/1280 px. Live: tap a card on a phone; check the back button returns to the results.

**BL-043** *New 2026-11-09 (owner report: J2143 opened "Flight Status Not Available" on FlightStats).* Actual: the share box knew only PK, PA, 9P and PF; for any other airline it took the letters as the code and the rest as the number, so Azerbaijan Airlines J2143 became `J/2143` (also broke G9, F3, J9, 3L, W5, B9, Z8, and airlines FlightStats files under an ICAO code such as flyadeal FAD and SalamAir OMS). Fix in `docs/flight_detail.js`: `AIRLINE_INFO` replaced by the 35-airline table from unmerged branch `airline-links-logos` (commit 0a09a74, FlightStats codes checked against live tracker URLs on 2026-11-04 by that chat, not re-checked here); `splitFlightNumber` always takes the first two characters as the IATA code; an airline missing from the table falls back to its IATA code; a number that is not an airline flight (registration such as APESL) gets no FlightStats line and no departure-day question. AirSial keeps its manage-reservation page (owner, 2026-11-08). The header now names foreign airlines too. Rest of that branch (logos, search-page changes) is still unmerged.
- Checks (stubbed browser, 2026-11-09): every table airline builds `/<fsCode>/123`, plus J2143 -> `/J2/143`, 9P734 -> `/FJL/734`, PK0301 -> `/PK/301`, PK301A -> `/PK/301`, unknown ZZ123 -> `/ZZ/123`, APESL and 12345 -> no link; 42 pass. BL-039 share-box checks rerun: 28 pass.
- 2026-11-09 later: owner saw "Flight Status Not Available" for flyadeal `/FAD/651`. F3651 (Riyadh-Islamabad) flew Sat 7 Nov (22:20 Riyadh -> 04:20 PKT); October schedule was 00:05 -> 06:30, so the right FlightStats day changed with the schedule. Either the `FAD` code or the chosen day was wrong; owner tested `/F3/651` and `/FAD/651` for 7 Nov: flyadeal fine, so the earlier failure was most likely the day (links used the arrival date before BL-039 merged). The other ICAO-style codes from PR #13 (`ABQ`, `FJL`, `OMS`, `SEP`, `ANK`, `SMR`, `3L*`, `PF*`) carry the same doubt until checked. Added Riyadh Air (`RX`, flights RX659/RX661 Riyadh-Islamabad, RX660/RX662 back; FlightStats code unchecked against `RXI`: RX659 found no flight on Mon 9 Nov because Riyadh Air moved to Gassim 25 Oct-8 Nov with Islamabad flights renumbered RX9661/9662, per Aeroroutes; Islamabad board on 9 Nov shows RX9661 due 14:45) and South Air (`Z8`, see BL-047).
- From PR #16's draft entry (2026-11-06): owner's intent then was FlightStats for international flights and the airline's own status page for local ones (dropping the special local codes `ABQ`, `FJL`, `PF*`); AirSial's link should be its flight-status page (`airsial.com/flight-status`). Superseded for AirSial on 2026-11-08 (owner: keep the manage-reservation page, its status page is a static picture). The local/foreign split is still open: ask the owner.

**BL-042** *New 2026-11-09 (follow-up to BL-041, owner approved).* Actual: results exist only after pressing Search, so Back from a flight could land on an empty search page when the browser had not kept the page in memory (and a reload lost the search). Expected: the same results, filters and scroll position. Built in `docs/script.js` only: after each search the address becomes `index.html?date=…&q=…&type=…&route=…&city=…` (`replaceState`, so no extra Back steps); on load, a valid `date` fills the form and re-runs the search; the city is applied during that search because its options only exist afterwards; the scroll position is kept per search in `sessionStorage` when a card is tapped and restored after the re-run. Bad values are ignored (date must be `YYYY-MM-DD`, type/route must be existing options, query normalised and capped at 12 characters); a plain visit is unchanged (no auto-search, clean address). Side effect: a search can be shared as a link; opening one adds it to Recent Searches like any search.
- Checks (2026-11-09, stubbed browser over a local server, 30 flights): plain visit unchanged; type + city search writes the address; tapping the 7th card and pressing Back shows the same 10 results at the same scroll position; reload restores search, filters and scroll; a shared link with `q=pk-304` searches `PK304`; a bad date does nothing; a bad type is ignored and an HTML city value stays harmless; no sideways scroll at 320/768/1280 px. Live: on a phone, search, open a flight, press Back (also after switching apps for a minute); open a copied search link in another browser.
- Blockers: owner signs up and picks a plan; confirm FIDS window limit and that `withLeg` gives scheduled (not only actual) departure times.

**BL-028** *New 2026-11-01.* User need: visitors can reach the owner without the owner publishing an email address or phone number (D-014).
- Approach: a plain HTML form posts to a hosted form service, which emails the owner. The service stores the destination address in its dashboard or behind a form key, so the page source contains neither. Candidates: Web3Forms (no account) or Formspree; check current free limits `[UNVERIFIED]`.
- Fields: name, message, "Call me back" checkbox that reveals phone and preferred time. Hidden honeypot field against spam; add the service's captcha if spam appears. Thank-you message after submit. One line saying phone numbers are used only to reply.
- Page: remove the `mailto:` link and the visible address; **complete the truncated footer** so the page matches the others.
- Needs: current `docs/contact.html`; the owner creates the form key privately.
- Accept: test message arrives; view-source of the page contains no personal address or number; empty submit rejected; footer and navigation complete on mobile.

**BL-051** *New 2026-11-10 (owner: pull local airlines' statuses in bulk, as little load on them as possible; easy to retune, extend and monitor; D-016).* User need: the PAA boards often lack actual times and delays for local airlines, and international arrivals have no origin side (BL-034, BL-037, BL-040). The airlines' own pages have both ends.
- **Tested** 2026-11-07 (built-in browser) and 2026-11-10 (plain Python from the owner's home connection, about 30 requests, no blocks for these two):

  | Source | Airline | Kind | Request | Gives |
  |---|---|---|---|---|
  | `pia_departures` | PK | board | GET `https://book-pia.crane.aero/ibe/flightStatus/routeSearch/?tab=departure&depPort=ISB&startDate=10%20Nov%202026` | every PK departure from ISB that day (10 on 10 Nov): scheduled, estimated and actual at both ends |
  | `pia_route` | PK | route | same URL with `tab=route&depPort=KHI&arrPort=ISB` | every PK flight on that route that day. PIA has no arrivals-by-airport page |
  | `flyjinnah_route` | 9P | route | POST `https://www.flyjinnah.com/api/FlightStatus/Flightstatus` with `{"dataModel":{"deparureDate":"10/11/2026","origin":"KHI","destination":"ISB"}}` (sic) | every 9P flight on the route: UTC and local schedule, estimate, off/on-block, takeoff and touchdown |
  | `airblue_board` | PA | board | GET `https://www.airblue.com/m/mobile-page.asp?type=Arrivals&city=ISB&date=10-Nov-2026` | off: BL-053 |

  Quirks: PIA times are station-local on a 12-hour clock; PIA keeps search state per session and once returned the previous search for a malformed query, so keep only rows matching the ports asked for. Fly Jinnah needs a route or a flight number (origin only is rejected); unset times come back as `1970-01-01T00:00:00`; its arrival side can lag (10 Nov: 9P670 still "DEPARTED", no touchdown, an hour after it was due). Both serve yesterday's date. PIA's status words are not reliable (10 Nov: PK301 landed 5 min early but is labelled "Delayed"): go by its times, not its labels. Fly Jinnah's late arrival times do fill in later (9P670 showed its 09:19 arrival by the evening), which the morning sweep relies on.
- **Files** (one script, one config, one test file):
  - `airline_collector.py` in four parts. **Adapters:** one small block per source that builds the request and a pure `parse(page) -> legs`. **Planner:** decides what to fetch (below). **Fetcher:** one request at a time, 2-5 s apart, an honest User-Agent naming the site; `http`, or `browser` (Playwright) only for a source that allowed it. **Writer:** upserts `airline_legs`, appends `airline_fetch_log`. Flags `--dry-run` (print the plan; no requests, no writes) and `--no-write` (fetch and parse only), so it can be tested without touching data (CLAUDE.md section 5 rule 5).
  - `airline_sources.toml`: every switch and timing, so retuning needs no code change. Adding an airline = one adapter + one config block; changing approach (e.g. PIA by flight number instead of by route) = change `kind` where the adapter supports it.
  - `tests/test_airline_parsers.py`: saved real pages per source (delayed, cancelled, no actual yet, unset times, PIA stale-session rows) plus planner cases with a fixed clock.

  ```toml
  [defaults]
  min_refetch_minutes = 15                  # never the same page twice within this
  gap_seconds = [2, 5]                      # pause between requests
  pause_on = ["403", "429", "challenge"]    # source paused until the next PKT day
  daily_cap = 60                            # per source, safety net; total target under ~100 a day
  [windows]                                 # minutes from estimated (else scheduled) time
  before_departure = [-120]
  after_departure = [10, 40]
  after_arrival = [10, 40, 70, 100, 130, 160]   # until an actual appears, then give up
  morning_sweep = "04:30"                   # PKT: yesterday once, for late actuals
  [fallback]                                # airport data stale: PAA down or Origin Scraper failed
  when_airport_data_older_than_minutes = 45
  route_every_minutes = 60                  # per route, only for PK/9P flights inside a window
  [source.pia_departures]
  enabled = true
  kind = "board"
  every_minutes = 60                        # while a PK departure is 2 h ahead to 1 h behind
  daily_cap = 24
  [source.pia_route]
  enabled = true
  kind = "route"
  trigger = "gap"
  [source.flyjinnah_route]
  enabled = true
  kind = "route"
  trigger = "gap"
  [source.airblue_board]
  enabled = false                           # BL-053
  fetch = "browser"
  every_minutes = 120
  ```
- **Planner** (each run, every 15 min): read today's Islamabad rows for the enabled airlines from `isb_flights_view`. A flight needs help when it is in a window **and** the airport data has a gap: a delay or ET 15+ min off ST, no ET, overdue (10 min past ET/ST without Landed/Departed), or an international arrival with no origin time yet (once per flight per day). Then pick the fewest pages that cover those flights (board before route before single flight; two flights on one route = one request), and drop pages fetched within `min_refetch_minutes` or belonging to a paused source. Most runs send 0-3 requests. Expected load: about 20 PIA board pulls, 30-60 route pulls and about 10 sweep pulls a day, under about 100 in total (estimate; measure on the live day). **Fallback** (owner 2026-11-10: it should still run when PAA fails): if the airport data is older than `when_airport_data_older_than_minutes`, every PK/9P flight inside a window counts as needing help (its schedule is known from earlier rows), but each route is fetched at most every `route_every_minutes` and the daily caps still apply. That is when this source helps most.
- **Tables** (proposed; SQL written in the build chat with checks and rollback):
  - `airline_legs`: one current row per airline, flight number and origin-local departure date (key): origin, destination, scheduled/estimated/actual departure and arrival as `timestamptz` (station-local times converted with an airport time-zone table), the airline's raw status text, `source`, `first_seen`, `fetched_at`. No history in phase 1; keep 7 days like the origin tables.
  - `airline_fetch_log`: one row per request, or per planned request that was skipped: `at` (UTC), `source`, `target` (`KHI-ISB 2026-11-10`), `reason` (window, gap, sweep, board), HTTP status, `outcome` (`ok`, `empty`, `blocked`, `error`, `parse_fail`, `skipped_paused`, `skipped_recent`), flights parsed, `missing` (flight numbers the planner wanted that the page did not contain), milliseconds. Keep 30 days. This is the record of where data is not coming through.
  - Rows carry `data_source = 'airline_web'` (already in `source_priority`). **Do not write airline rows into `origin_flights`:** the site's views rank sources with `priority DESC`, so `airline_web` (20) would replace PAA's own rows (10) on the site. Correct the header comment in `origin_scraper.py` that says future scrapers write to the same tables. BL-052 decides how airline data is shown.
- **Health** (queries saved with the SQL): per source over the last 24 h, counts by outcome; flights wanted but never found, per day; sources paused now. A run exits with an error the first time a source is paused in a day, so a failed GitHub run emails the owner; BL-044 item 8 reads the health query monthly.
- **Where it runs:** GitHub Actions. Checked 2026-11-10 with the probe from a GitHub runner (US, Microsoft network; run 42874268835, temporary test branch since deleted): PIA 13 flights, Fly Jinnah 10, no blocks, the same as from the owner's home connection. No home machine and no new cron-job.org job (owner 2026-11-10): a separate workflow `airline_collector.yml` starts when Origin Scraper finishes, **success or failure** (`on: workflow_run: workflows: ["Origin Scraper"], types: [completed]`), plus `workflow_dispatch` for manual and `--dry-run` runs. Separate so an airline-site failure never marks the airport collector red, and the airline side can be switched off alone. `workflow_run` only fires for workflows on `main`, so test with manual runs before merging. Limit: if cron-job.org stops, nothing runs; add GitHub's own hourly `schedule` as a backup only if that becomes a problem.
- **Checkpoints:** (1) parsers and planner with fixtures, `--dry-run`; (2) runner check: passed 2026-11-10 with the probe (above), repeat once with the collector's `--no-write`; (3) tables SQL applied; (4) one supervised live day.
- **Accept:** parser and planner fixtures pass; `--dry-run` sends nothing; the live day stays under the request target with no `blocked` outcome; `airline_legs` has a row for at least 90% of the PK and 9P Islamabad flights that hit a gap; the site and its views are unchanged.

**BL-052** *New 2026-11-10.* Show what the airline reports once BL-051 has about a week of data, e.g. under Islamabad's line: "PIA reports: left Jeddah 03:57, due 11:23 (PIA website, 6 min ago)". Needs owner decisions: where it appears, when it counts as stale, what to show when it disagrees with the airport board. Never present it as the airport's status; always label the source and its age. Blocked on BL-051.

**BL-053** *New 2026-11-10 (owner: try Airblue by browser automation, even if it means polling less, if Airblue allows).* On 2026-11-10 Airblue's status pages answered a plain script with a Cloudflare check page ("Just a moment... Enable JavaScript and cookies") on the first request; they load normally in a browser. Getting past that check without permission is out (D-016). Step 1: the owner writes to Airblue's web or IT team asking to read the mobile status pages automatically at a low rate (a few dozen requests a day for Islamabad), offering a fixed IP for them to allow and a way to reach the site. Whatever they allow goes into `airline_sources.toml`: an allowed IP means plain `http`; otherwise `fetch = "browser"` with Playwright at `every_minutes = 120` or whatever they ask. Board and per-flight parsers already exist (tested on pages captured 2026-11-07). Accept: permission received (record the date only; keep the correspondence private); one supervised day within their limits. Blocked on the reply.

### Applied edits (merged 2026-11-01, commit `9d4e72f`, PR #5)

Files: `docs/index.html`, `flight_detail.html`, `about.html`, `contact.html`, `flight_detail.js`, `script.js`, `style.css`. Saved as one squashed commit, so one revert (the Revert button on PR #5) undoes all of it. Owner reported the live checks done on 2026-11-01. Still open: BL-028 (needs the form key and a real test message), the BL-023 city/filter part, BL-024, and the `scraped_at` check under BL-009.

- **BL-003:** `script.js` no longer stops when Supabase fails to load: saved searches still show and search explains the problem; the details page gets the same guard. Damaged or blocked browser storage no longer breaks the page. Live check: open the site, confirm recent searches show; in DevTools block `unpkg.com` and reload, confirm saved searches still show.
- **BL-004:** share text says Arrival or Departure from the flight's own direction. Live check: share one arrival and one departure.
- **BL-007:** city options, result cards, the details header and the history table now escape feed values. Test data containing HTML showed as plain text and did not run (on the current files it ran). Live check: normal flights look unchanged.
- **BL-008:** analytics block moved inside `<head>` on the search and details pages. Live check: page loads, no console errors.
- **BL-023 (label part only):** repeating a search keeps the custom name (e.g. "Mr Haseeb"). Still open: city is not saved, replay can keep stale filters.
- **BL-028:** contact form (name, optional email, message, "Call me back" with phone and best time). The email and phone are gone from the page and the truncated footer is repaired. The form stays disabled with a "not switched on yet" notice until the owner pastes a Web3Forms access key at the top of the script in `contact.html`. Checked with a stubbed service: empty message rejected, callback needs a phone number, spam-trap field sends nothing, success message shown. The service's real request format is `[UNVERIFIED]` until a real test message is sent. Live check after the key is set: send a test, confirm it arrives, view-source shows no personal address or number.
- **BL-029 (new):** the search page's default date used the UTC date, so between 00:00 and 05:00 PKT it showed yesterday. Now uses `Asia/Karachi`. Same family as BL-002.
- **BL-030 (new):** details page: the header block sat flush against the screen edge, the table ran edge to edge on desktop and scrolled sideways on phones, "Last checked" was misaligned, and links in the share box were dark blue on dark. Now a centred header card, a table that fits from 320 to 1280 px wide, a back link, readable links.
- **Copy (part of BL-016):** the search page and About text no longer promise airline-name or free-text city search; About names the real audience.
- **Not changed:** BL-024 (share link parsing for unusual airline codes, dropped-flight route), BL-022, BL-021.
- **BL-009 evidence (not a change):** `scraper.py` writes `scraped_at` from `datetime.now(timezone.utc).isoformat()` into a `timestamp without time zone` column. Postgres ignores the offset in that case and stores the UTC clock value. The details page reads zone-less values as UTC and shows PKT, so the display is consistent provided the scrapers produce UTC. Not yet verified on live data. Read-only check for later: `SELECT max(scraped_at) AS latest_scraped_at, now() AT TIME ZONE 'UTC' AS utc_now FROM public.flight_snapshots;` the two should differ by minutes at most (about 5 hours apart would mean local time). **Verified 2026-11-02:** latest `scraped_at` was 13:21 UTC against 13:24 UTC now, so values are stored in UTC.

### Done and history

2026-11-01 (PR #5, commit `9d4e72f`, owner-checked live): BL-003, BL-004, BL-007, BL-008, BL-029, BL-030; label part of BL-023; copy part of BL-016; contact page repaired (BL-028 applied, not yet live).

Original wishlist items already in the product: named saved searches; full API field capture; terminal-status handling for drop detection; both collectors activated (owner report). Earlier chat claims of missing workflows, guaranteed terminal skipping, automatic cutoff or deletion, and an immediate history extension are superseded.
Earlier labels `FT-001..018` from the ChatGPT review map to: FT-001 BL-001, FT-002 BL-021, FT-003 BL-022, FT-004 BL-002/BL-009, FT-005 BL-025, FT-006 BL-001/BL-017, FT-007 BL-007, FT-008 BL-023, FT-009 BL-004/BL-024, FT-010 BL-010/BL-019, FT-011 BL-020, FT-012 BL-006, FT-013 BL-013/BL-026, FT-014 BL-027, FT-015 BL-005, FT-016 BL-011, FT-017 BL-014/BL-015, FT-018 BL-017.

## 5. Database and operations notes (public-safe)

Evidence date 2026-09-30, from a catalog report, the view SQL and the repo; not a full executable dump. Relations and keys are in `CLAUDE.md` section 4.

- **Scheduling:** Python 3.11; same five secrets for both jobs; cron-job.org sends `workflow_dispatch` with ref `main`; owner reports both active about every 15 minutes. New collector catches batch write failures and continues, so inspect logs, not just green status. Both can advance health despite failed or empty fetches (BL-021).
- **Tests:** no dry-run or single-airport mode. Each run upserts, snapshots and deletes old history. Use pure-function fixtures or an isolated database; a manual production run is a real write.
- **Storage (2026-09-30):** database 23 MB; largest table `flight_snapshots` about 5.9 MiB (about 26k rows). Dashboard figures for RAM and disk use different scopes and are not reconciled. Do not drop indexes or run `VACUUM FULL` without a measured problem.
- **Read-only evidence queries** (run in the Supabase SQL editor, paste results into a chat):
  ```sql
  SELECT * FROM public.source_priority ORDER BY priority DESC, data_source;
  SELECT * FROM public.origin_scraper_status;
  SHOW timezone;
  SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
  ```
- **BL-010B view: pairing rules.** For each Islamabad arrival, the departure leg is the row of the same flight number, listed by the airport the arrival says it came from, whose destination is Islamabad, with a scheduled departure on the arrival date or the day before, at or before the scheduled arrival and no more than 6 hours earlier. Among candidates the smallest gap wins, and a pair is kept only if each side is the other's best match. If either scheduled time is missing or unreadable, nothing is guessed: the row is `unmatched`. Within one leg, the highest-priority source wins, then the newest `last_checked`, then the source name (deterministic). International arrivals and flights with no departure row stay `unmatched` and the page shows no box. Dates of both legs are exposed (`arrival_date`, `departure_date`) so overnight pairs are visible. Limit: a flight longer than 6 hours would need the window widened (domestic legs are well under that).
- **BL-010B view SQL** (target: Supabase SQL editor; prerequisites: `origin_flights` and `source_priority` exist as in section 4; creates one view and one grant, changes no data).
  ```sql
-- BL-010B: pairs each Islamabad ARRIVAL with the departure leg reported by the origin airport.
CREATE OR REPLACE VIEW public.isb_arrival_origin_view
WITH (security_invoker = true) AS
WITH base AS (
  SELECT f.*, sp.priority,
         CASE WHEN f.st ~ '^\d{1,2}:\d{2}' THEN
           (f.scheduled_date + substring(f.st from '^\d{1,2}:\d{2}')::time) END AS st_ts
  FROM public.origin_flights f
  JOIN public.source_priority sp USING (data_source)
),
arr AS (   -- best-ranked Islamabad arrival row per flight/date
  SELECT DISTINCT ON (flight_number, scheduled_date)
         flight_number, scheduled_date, origin_city, status, st, et, st_ts,
         data_source, last_checked
  FROM base
  WHERE type = 'Arrival' AND source_airport = 'Islamabad'
  ORDER BY flight_number, scheduled_date, priority DESC, last_checked DESC, data_source
),
dep AS (   -- best-ranked departure row per flight/date/airport reported by the origin airport
  SELECT DISTINCT ON (flight_number, scheduled_date, source_airport)
         flight_number, scheduled_date, source_airport, destination_city, status, st, et,
         st_ts, data_source, last_checked, last_updated
  FROM base
  WHERE type = 'Departure' AND source_airport <> 'Islamabad' AND destination_city = 'Islamabad'
  ORDER BY flight_number, scheduled_date, source_airport, priority DESC, last_checked DESC, data_source
),
cand AS (  -- departure at or before arrival, within 6 hours; gap known only if both times parse
  SELECT a.flight_number, a.scheduled_date AS arr_date, d.scheduled_date AS dep_date,
         d.source_airport, a.st_ts AS a_ts, d.st_ts AS d_ts,
         extract(epoch FROM a.st_ts - d.st_ts) AS gap_s
  FROM arr a
  JOIN dep d ON d.flight_number = a.flight_number
            AND d.source_airport = a.origin_city
            AND d.scheduled_date BETWEEN a.scheduled_date - 1 AND a.scheduled_date
  WHERE a.st_ts IS NOT NULL AND d.st_ts IS NOT NULL
    AND a.st_ts - d.st_ts BETWEEN interval '0' AND interval '6 hours'
),
ranked AS (
  SELECT c.*,
         row_number() OVER (PARTITION BY flight_number, arr_date ORDER BY gap_s, dep_date DESC) AS rn_arr,
         row_number() OVER (PARTITION BY flight_number, dep_date, source_airport ORDER BY gap_s, arr_date) AS rn_dep
  FROM cand c
),
pairs AS (  -- mutual best only
  SELECT * FROM ranked WHERE rn_arr = 1 AND rn_dep = 1
)
SELECT a.flight_number,
       a.scheduled_date AS arrival_date,
       a.origin_city,
       a.status   AS arrival_status, a.st AS arrival_st, a.et AS arrival_et,
       a.last_checked AS arrival_checked,
       p.dep_date AS departure_date,
       d.source_airport AS departure_airport,
       d.status   AS departure_status, d.st AS departure_st, d.et AS departure_et,
       d.data_source AS departure_source,
       d.last_checked AS departure_checked,
       d.last_updated AS departure_last_updated,
       CASE WHEN p.flight_number IS NULL THEN 'unmatched' ELSE 'matched' END AS match_state
FROM arr a
LEFT JOIN pairs p ON p.flight_number = a.flight_number AND p.arr_date = a.scheduled_date
LEFT JOIN dep d ON d.flight_number = p.flight_number AND d.scheduled_date = p.dep_date
               AND d.source_airport = p.source_airport;
GRANT SELECT ON public.isb_arrival_origin_view TO anon, authenticated;
  ```
  Checks after applying: `SELECT match_state, count(*) FROM public.isb_arrival_origin_view GROUP BY 1;` returns both `matched` and `unmatched` once domestic data exists; `SELECT * FROM public.isb_arrival_origin_view WHERE departure_date <> arrival_date LIMIT 5;` shows any overnight pairs; the same flight number and date never appears twice. Rollback: `DROP VIEW IF EXISTS public.isb_arrival_origin_view;` (nothing else depends on it; the details page hides its box when the view is missing).
- **BL-032 views** (target: Supabase SQL editor; prerequisites: `origin_flights`, `origin_snapshots`, `source_priority`; creates two views and grants, no data changes). They show only Islamabad rows, in the same column shape as the old tables (`city` is the origin for arrivals and the destination for departures), one row per flight, date and direction, highest-priority source first.
  ```sql
-- BL-032: Islamabad-only views over the origin tables, shaped like the old flights / flight_snapshots tables.
CREATE OR REPLACE VIEW public.isb_flights_view
WITH (security_invoker = true) AS
SELECT DISTINCT ON (f.flight_number, f.scheduled_date, f.type)
       f.flight_number, f.scheduled_date, f.type,
       CASE WHEN f.type = 'Arrival' THEN f.origin_city ELSE f.destination_city END AS city,
       f.status, f.st AS st, f.et AS et, f.nature, f.airline_logo,
       f.data_source, f.last_checked, f.last_updated
FROM public.origin_flights f
JOIN public.source_priority sp USING (data_source)
WHERE f.source_airport = 'Islamabad'
ORDER BY f.flight_number, f.scheduled_date, f.type, sp.priority DESC, f.last_checked DESC, f.data_source;

CREATE OR REPLACE VIEW public.isb_snapshots_view
WITH (security_invoker = true) AS
SELECT s.id, s.flight_number, s.scheduled_date, s.type,
       CASE WHEN s.type = 'Arrival' THEN s.origin_city ELSE s.destination_city END AS city,
       s.status, s.st AS st, s.et AS et, s.nature,
       s.data_source, s.scraped_at, s.change_type
FROM public.origin_snapshots s
WHERE s.source_airport = 'Islamabad';

GRANT SELECT ON public.isb_flights_view, public.isb_snapshots_view TO anon, authenticated;
  ```
  Checks: `SELECT type, count(*) FROM public.isb_flights_view WHERE scheduled_date = (now() AT TIME ZONE 'Asia/Karachi')::date GROUP BY 1;` returns arrivals and departures; compare with `SELECT type, count(*) FROM public.flights WHERE scheduled_date = (now() AT TIME ZONE 'Asia/Karachi')::date GROUP BY 1;` (close, not necessarily equal until yesterday is filled). Rollback: `DROP VIEW IF EXISTS public.isb_flights_view, public.isb_snapshots_view;`.
- More saved SELECT examples for the canonical view are in `notes/REFERENCE_QUERIES.txt`.

## 6. Repo tidy-up for the two-file switch

**Done 2026-11-04.** Kept for reference. After saving `CLAUDE.md` and `PLAN.md` at the repo root, delete (git history keeps them): root `AGENTS.md`; `docs/SPEC.md`, `docs/BACKLOG.md`, `docs/DECISIONS.md`, `docs/JOURNAL.md`, `docs/DB.md`, `docs/WORKFLOW.md`, `docs/readme`. Replace `README.md` with a three-line pointer to the two files. Keep `notes/REFERENCE_QUERIES.txt`. Do not put the new files in `docs/` (that folder is the published website). The old draft PR #4 uses a competing layout: close it rather than merging.
**BL-049** *New 2026-11-10 (owner: the links are only in the share box; visitors checking for themselves should see them too).* Owner chose chips inside the card (option A) over a separate list, and kept Share at the top right (A1), from chat mockups. Built in `docs/flight_detail.js` (`checkLinksHtml`, drawn after the departure-board match so it uses the same day as the share box) and `docs/style.css` (`.check-links`, `.check-chip`):
- FlightStats chip opens the same link the share message carries. For arrivals where the share box would ask which day it took off, both days get a chip ("FlightStats · left Mon 9 Nov", "· left Tue 10 Nov") instead of guessing. No chip when there is no FlightStats link (South Air, registrations).
- Airline status chip only where `statusUrl` is set; call chip only where `contact` is set, dialling the first (local) number, full text on hover. Outside links open in a new tab. No chips at all: the row is not shown.
- Checks (stubbed browser, 2026-11-10): PIA arrival (one FlightStats chip, status, call), Emirates night arrival (two day chips), PIA departure, South Air (call only), registration (no row), HTML in city text; FlightStats chip matches the share message link; no sideways scroll at 320/390/1280 px; no script errors. Not yet seen on a real phone.

## 7. Journal (newest first)

### 2026-11-10 | Claude | airline status sources (BL-051..053)
- Done: checked the local airlines' status pages (built-in browser 2026-11-07; plain Python from the owner's PC 2026-11-10): PIA and Fly Jinnah answer scripts; Airblue shows scripts a Cloudflare check; AirSial (static picture) and South Air publish nothing. Free ADS-B feeds (adsb.lol, adsb.fi, OpenSky) do not see Islamabad approaches; airplanes.live now needs registration. Planned the collector (BL-051), the site display (BL-052) and the Airblue request (BL-053); added D-016. Found that the views' `priority DESC` would let `airline_web` rows replace PAA rows.
- Night: ran the probe on a GitHub runner from a temporary test branch (deleted after): PIA and Fly Jinnah answered. Owner chose to link the collector to Origin Scraper, keep it running when PAA fails, and keep runs conservative (caps lowered, fallback mode).
- IDs: BL-050 is taken by open PR #21 (flights moved past midnight), so this uses BL-051..053; next unused is BL-054 once both are merged.
- Next: owner approves BL-051's design, then checkpoint 1; owner writes to Airblue.

### 2026-11-10 | Claude | BL-049 links on the flight page
- Done: owner picked chips in the card from mockups, Share stays top right. Built and checked in a stubbed browser; merged to main (PR #20) by the owner's instruction.
- Next: owner taps each chip on a phone.

### 2026-11-09 | Claude | BL-048 one-box search
- Done: search page rebuilt as one box (flight, airline or city) with suggestions, day arrows, full-width toggles, airline-name cards with 48 px logos and coloured status; Recent saves opened flights and settled searches; today's list opens at a "Now" line. Owner approved from screenshots; merged to main. Airline list moved to shared `airlines.js`. Closed PR #13 (logos not taken). 40 + 28 + 44 stubbed-browser checks pass. 
- Then: owner asked for the page to open at the top (search area visible) with a "Now" button instead of the jump, today not pinned in the address, and a one-line header on phones; built and merged.
- Next: live check on a phone; owner may want a fuller layout on large screens later.

### 2026-11-09 | Claude | BL-043/047 owner link checks
- Done: owner checked FlightStats: flyadeal fine; South Air not found (Z8, SQB) so its FlightStats link is removed and only the call-centre number stays; Riyadh Air RX659 not flying (temporary Gassim operation, RX9661/9662). Merged PR #16's plan with IDs renumbered to BL-044..047 and closed it.
- Next: owner checks Riyadh Air with RX9661; decides on PR #13 (logos) and on FlightStats for local flights.

### 2026-11-09 | Claude | BL-043 FlightStats airline codes
- Done: share link uses the 35-airline FlightStats code table from branch `airline-links-logos` and splits codes like J2/9P/3L correctly; registrations get no link. 42 link cases + 28 share-box checks pass in a stubbed browser. Pushed to main.
- Next: owner opens J2143's share link live; check South Air (Z8) on FlightStats.

### 2026-11-09 | Claude | BL-039 wording and country code
- Done: merged to main (d7fcbda), then reworded the departure-day block to plain dates and "leave" (owner found "night before" hard to follow) and made the WhatsApp country code editable. 28 stubbed-browser checks pass incl. country codes 92/971 and empty code. Pushed to main.
- Next: owner checks on a phone; city query. (No ID clash: the other chat continued at BL-041.)

### 2026-11-09 (later) | Claude | BL-041 tap card to open, BL-042 search in the address
- Done: whole search-result card is a link to the details page ("History" button replaced by a `›` arrow); the search is kept in the address and re-run on Back/reload/shared link, with the list's scroll position. Stubbed-browser checks pass. Both merged to `main` (owner's instruction).
- Next: owner phone check on the live site.

### 2026-11-09 | Claude | BL-039 share box: departure day
- Done: share box asks which day an arrival took off whenever there is doubt (Night before / Same day, guess preselected, Check links), else one line with Change; editable message; Copy, Share, WhatsApp. 20 date cases and interaction/width checks pass in a stubbed browser. Branch `fix/flightstats-departure-date`, not merged. BL-040 captured 2026-11-08 (AeroDataBox for later).
- Next: owner reviews on a phone, runs the city query, merges; renumber BL-039/040 if the other chat's PLAN.md already uses them.

### 2026-11-06 | Claude | BL-038 Islamabad-first page
- Done: owner set the rule: show Islamabad only, unless the departure board has an update Islamabad's doesn't reflect; departure-board history behind a labelled "Airport board" switch; Islamabad's top line fixed. Agreed from chat mockups first (no test switches in code). Built the notice (cancelled / departed / 15+ min later; owner said "16", taken as the 15 discussed), the switch, shorter table dates and plain headers, About-page note. 11 stubbed scenarios x 4 widths pass, including a deliberately long status word; the older test that looked for the removed departure box is retired.
- Next: owner checks on live flights; tune `NOTICE_MINUTES` if the notice shows too often or too rarely.

### 2026-11-06 | Claude | simplicity review (merged 2026-11-09 from PR #16, IDs renumbered)
- Done: owner asked whether the project is more complicated than it needs to be. Read the repo (code only, no live checks). Added the monthly owner check (now BL-044), retiring the old collector (BL-045), dropping the unused canonical view (BL-046), the share-link bug and local/foreign split (folded into BL-043) and South Air (BL-047); cancelled BL-019; logo note in BL-016. Serene Air: not in the code; still grounded (relaunch hoped by end of 2026); nothing to delete.
- Next: owner sets a monthly phone reminder for BL-044; then BL-045 once the views are confirmed.

### 2026-11-05 | Claude | BL-038 simplified
- Done: owner found the `&layout=` test link awkward and asked for the choice on the page, without "Side by side". Two buttons now sit above the history (Both airports by default, Islamabad only), switch without reloading and are remembered per browser; the combined list is newest first with larger text. Stubbed-browser checks pass (3 flights x 320/375/1280 px: buttons, switching, remembered choice, no sideways scroll, hostile text; no switcher for international, departure or missing-view cases; old links).
- Then: owner said no switchers in the code; wants mockups in chat and only the chosen design built. Restored the three details-page files to the BL-036 state (stubbed-browser checks pass).
- Next: owner picks a mockup.

### 2026-11-04 (night) | Claude | BL-038 history layout test
- Done: owner asked whether the history would be easier with both boards in one place. Built two combined layouts behind `&layout=` so the owner can compare on real flights; the departure board's history now comes from one `origin_snapshots` read that also gives the "first seen departed" time. 45 stubbed-browser checks pass (4 flights x 3 layouts x 320/375/1280 px, plus the earlier set); the public page is unchanged without the parameter.
- Not done: branch deletion is not allowed from this session; the owner deletes the 11 merged or abandoned branches on GitHub (all checked: nothing unmerged is lost).
- Next: owner compares the three layouts on a few domestic arrivals and picks one.

### 2026-11-04 (late) | Claude | BL-036 journey card
- Done: owner said the text sizes on the details page made it hard to tell what matters, and asked to fold the departure-airport box into the main flight view. Rebuilt the header as one card with a clear size ladder; Islamabad's current status and time now sit in the card (before, they were only in the history table). Added a note when Islamabad has moved the time but the departure board has not, and a "first seen departed" line from our own snapshots. 6 scenarios x 4 widths pass in a stubbed browser (no sideways scroll, hostile text escaped, view missing hides the line).
- Not done: live check; BL-037 (needs a raw PAA response, which this environment cannot reach).
- Next: merged on the owner's instruction (PR #11); owner opens one domestic and one international arrival on the live site. Then, on the owner's instruction, did the section 6 tidy-up: deleted the old doc files, README now a pointer, closed draft PR #4, brought CLAUDE.md up to date on what the site reads.

### 2026-11-04 (evening) | Claude | BL-032 merged with fallback
- Done: owner left and asked for the website to go live if safe. Could not confirm the views exist (no database access from here), so added a fallback to the old tables for search, history and freshness, tested both cases in a stubbed browser (10 checks plus the earlier 11 and 15), tested the new fetch plan with a stubbed fetcher, and merged PR #10.
- Not done: live checks (owner tomorrow); the follow-up queries on missing cities and the 9 unpaired flights; BL-017.
- Next: owner opens a search page and a details page, runs the view-exists query and the two follow-up queries.

### 2026-11-04 | Claude | live results recorded
- Done: recorded the owner's query results (column types, match counts and reasons, coverage); read the latest origin run log (Multan HTTP 500); added BL-033, BL-034, BL-035.
- Not done: BL-032 merge (waiting on confirmation that the views exist); causes for the 9 unpaired flights; BL-017 still the owner's.
- Next: owner pastes the follow-up query results; then decide on merging PR #10 and on the Multan call.

### 2026-11-04 | Claude | SQL column-name fix
- Found: the owner's database has lowercase `st`/`et` columns (unquoted `ST` folds to lowercase); my first SQL quoted `"ST"` and failed with 42703. Fixed both view scripts and re-tested on a lowercase schema. Lesson: do not quote `ST`/`ET` in SQL (see CLAUDE.md section 3).

### 2026-11-03 | Claude | origin switch prepared
- Done: BL-032 and BL-022 prepared on a branch: two Islamabad-only views (tested on a local Postgres), site readers switched, history links carry direction, freshness read from the origin status row, origin collector always fetches yesterday for Islamabad; 11 browser checks and 6 plan checks pass. Removed accidentally committed `__pycache__` and ignored it.
- Not done: nothing applied or merged; the collectors were not run.
- Next: owner applies the view SQL and checks counts; then merge; watch for several days before turning off the old collector.

### 2026-11-02 (night) | Claude | origin box prepared
- Done: pairing view designed and run on a throwaway Postgres 16 with 11 fixtures (all as intended); details-page box written and tested at 320 to 1280 px with a stubbed database; backlog BL-010B/C set to Proposed, BL-031 and BL-032 added.
- Not done: nothing applied to the live database; nothing merged; BL-017 left for the owner as agreed.
- Next: owner applies the view SQL and runs its checks; merge the website branch; then BL-010A (which origin airports report valid data) and the scraper branch.

### 2026-11-02 | Claude | database checks recorded
- Done: recorded the owner's read-only query results (source registry, `scraped_at` timezone, origin status freshness, permission check). `scraped_at` confirmed UTC; BL-017 confirmed still needed.
- Not done: the BL-017 fix itself (owner runs it, private notes); BL-028 form key.
- Next: owner runs the BL-017 fix and re-runs the permission check; then BL-010A (origin airports and logs) and BL-002.

### 2026-11-01 | Claude | live checks recorded
- Done: merged PR #5 to `main` at the owner's request (squash commit `9d4e72f`); the owner then reported the live checks done. Marked BL-003, BL-004, BL-007, BL-008, BL-029, BL-030 Done and BL-028 Testing; updated the handoff.
- Not verified by me: the live site (this environment cannot reach it); the owner's report is the evidence.
- Next: owner creates the Web3Forms key, pastes it into `docs/contact.html` and sends a test message; then BL-017 and the `source_priority` result.

### 2026-11-01 | Claude | site fixes (second session)
- Goal: fix obvious bugs in the live pages (contact, layout and similar); record the `scraped_at` timezone finding.
- Done: reviewed all pages and scripts in the repo; rendered them in a headless browser with stubbed data at 320 to 1280 px; prepared fixes for 7 `docs/` files (see "Proposed edits awaiting upload"); found and fixed two bugs beyond the backlog (default date on the search page; details page layout); added BL-029 and BL-030; added a Proposed state.
- Checked: 18 automated browser checks pass on the new files, and the same checks fail on the current files (HTML injection ran, default date was yesterday at 02:00 PKT, arrivals labelled Departure). No horizontal scroll at 320, 360, 390, 768, 1280 px.
- Not checked: the live site, the real Supabase data, the real form service (needs the owner's key).
- Unverified: Web3Forms request format; `scraped_at` stored values on the live database.
- Next: owner uploads the 7 files and does the live checks; then the owner setup list in section 1.

### 2026-11-01 | Claude | repo review and docs merge
- Goal: use the real repo, reconcile with the earlier doc set, apply the two-file decision and the no-public-contact decision.
- Done: cloned the repo read-only and checked the claims in section 1; merged the repo's kit into these two files, keeping its task IDs and its corrections (BL-018 no longer says delete data; no retention extension; terminal-status wording); added D-013, D-014, BL-028; cancelled BL-005; wrote a private notes file kept out of the repo.
- Withdrawn: my earlier recommendations to delete non-Islamabad rows first and to raise retention now.
- Unverified: runtime state of both collectors and cron-job.org settings; the runner's timezone; whether BL-017 was applied; form-service free limits. Nothing was run or changed in the repo.
- Next: owner saves the two files and tidies the repo (section 6), runs the private BL-017 steps, pastes `docs/contact.html` for BL-028, and the result of `SELECT * FROM source_priority`.

### 2026-09-30 | ChatGPT | DOC-SETUP
- Consolidated earlier Claude documents and a review into one layout; mapped FT IDs; added the staged origin feature and evidence-based statuses. Corrected: workflow files do exist; terminal processing continues; filtering does not justify deleting data; migration and retention increase are not prerequisites for the origin box. Document and ID checks only; no application edits, SQL or scheduler changes.

### 2026-09-30 | Claude | setup (two sessions)
- Produced BL-001..020 and D-001..009, proposed email, access, time and history changes and the manual paste format. Some claims (missing workflows, terminal skipping, deletion-first filtering) were wrong or superseded as noted above.
