# PLAN.md: Islamabad Flight Tracker

Spec, decisions, backlog, journal and the current handoff, in one place. Stable rules and architecture are in `CLAUDE.md`.
This merges the repo's earlier doc set (`AGENTS.md`, `docs/SPEC|BACKLOG|DECISIONS|JOURNAL|DB|WORKFLOW.md`, dated 2026-09-30) and keeps its task IDs. Last updated: 2026-11-01.
Public repo: no email, phone, credentials or security details in this file.

## 1. Current handoff

- **Goal now:** switch on the contact form (needs the owner's key), delete the old doc files, then pick one small task.
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
- **Owner setup, left for later:** (1) create a free Web3Forms access key and paste it into `docs/contact.html` (BL-028), then send a test message; (2) run the private BL-017 fix and re-run the permission check; (3) cron-job.org settings for BL-001; (4) delete the old doc files and close draft PR #4 (section 6).
- **Docs migration:** delete the old doc files listed in section 6 after saving these two (history stays in git). `docs/` is the published website folder, so the old `.md` files there are publicly served.
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
| D-014 | **No public email or phone.** Contact goes through a form that emails the owner, with an optional callback request. Supersedes the plain-email fix in BL-005. 2026-11-01 | Accepted, owner |

Entry template: `D-### title; date; status (Proposed/Accepted/Implemented/Superseded); task ID; problem; decision; alternatives; affected contracts; rollback; evidence; unknowns.` Accepted does not mean shipped; record the commit separately.

## 4. Backlog

One queue. IDs are never reused. Next unused ID: **BL-031**. BL-010A..D are children of BL-010.
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
| BL-010B | bug | P1 | M | Blocked | Validate canonical matching contract |
| BL-010C | feature | P2 | M | Blocked | Add origin box without switching existing readers |
| BL-010D | research | P3 | M | Inbox | Evaluate full reader migration later |
| BL-011 | feature | P3 | L | Inbox | Flight-change notifications |
| BL-012 | feature | P3 | M | Inbox | Adaptive scrape frequency |
| BL-013 | chore | P3 | M | Inbox | Batch database work where measured |
| BL-014 | idea | P3 | L | Inbox | Actual aircraft tracking |
| BL-015 | idea | P3 | L | Inbox | Fare and booking-window research |
| BL-016 | chore | P3 | M | Inbox | README, sitemap, logo housekeeping |
| BL-017 | chore | P1 | M | Ready | Restrict public database privileges (evidence kept private) |
| BL-018 | research | P2 | M | Inbox | Evaluate Islamabad relevance filter safely |
| BL-019 | bug | P1 | M | Inbox | Overnight and duplicate canonical matching |
| BL-020 | research | P2 | S | Inbox | Measure retention and storage before extending |
| BL-021 | bug | P1 | M | Ready | Report honest freshness and batch outcomes |
| BL-022 | bug | P1 | M | Ready | Keep direction in flight history identity |
| BL-023 | bug | P2 | S | Ready | Preserve labels and saved filter state (label part done; city and stale filters still open) |
| BL-024 | bug | P2 | M | Ready | Share actual current state and retained route |
| BL-025 | bug | P1 | M | Ready | Guard against false drops from partial feeds |
| BL-026 | chore | P2 | M | Inbox | Avoid overlapping writers; pin environment |
| BL-027 | research | P2 | M | Inbox | Diagnose disabled TLS verification |
| BL-028 | feature | P1 | M | Testing | Contact form with callback option; footer repaired and email removed (done). Not live until the owner's form key is pasted and a test message arrives |
| BL-029 | bug | P2 | S | Done | Search page default date used the UTC date, not Pakistan's |
| BL-030 | bug | P2 | S | Done | Flight details page layout: unstyled header, table overflow, unreadable share links |

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

**BL-010 / A / B / C / D** Parent initiative, done in children. **A:** read source registry and status, and the newest origin-workflow logs; confirm valid origin airports and freshness; spot batch failures behind a green run. **B** (blocked on A): rank ties, wrong-airport rows, duplicate matches, missing routes and same-date joins; deterministic leg pairs on sanitised fixtures; no ambiguous +/-1 day matches. **C** (blocked on B and the Open question 2): for a domestic arrival show departure-board status, ST/ET, source and its own time; test valid, missing, stale and conflicting data; header and Islamabad history unchanged; never claim actual aircraft location. **D:** optional, compare coverage, history window, performance, rollback before any cutover or retirement. *Progress 2026-11-02 on A:* source registry read (only `paa` in use); origin status was fresh. Still to do: confirm which origin airports report valid data and look for batch failures in the logs.

**BL-011** Need channel, recipients, opt-in, triggers, identity and cost decisions; do not assume Telegram or accounts. Split before implementing.

**BL-012** Near-flight polling is a proposal; measure useful freshness and run duration first; avoid duplicate scheduling (the external scheduler already dispatches).

**BL-013** Per-record SELECT and upsert remain; only snapshots batch. Optimise from a measured bottleneck with equivalent change/drop behaviour.

**BL-014 / BL-015** Aircraft position and fare research need new sources, cost and agreement first.

**BL-016** Sitemap lists `flight_detail.html` (useless without query parameters); non-local airline logos hotlink `pics.avs.io`; README to become a short pointer to these two files.

**BL-017** Confirmed by the private owner-supplied report: excess public write privileges and missing protection on one status table. Need an explicit relation/role plan, a view security review, and preserved collector access. Accept: public reads work, unnecessary public writes denied, collector writes preserved, inherited privileges considered, safe non-destructive verification. **Keep the evidence and the SQL private** (not in this repo, not in public chats). Owner's read-only check on 2026-11-02 shows the issue is still present.

**BL-018** Setting `REQUIRE_ISB_LEG = True` excludes unrelated rows from the "seen" set, so drop detection would mark existing unrelated rows Dropped. Deleting data is not a prerequisite and has no rollback without an export. Accept: filter and drop scope defined together, retained unrelated rows not falsely dropped, owner approves before the flag flips, no hidden cleanup. (This replaces my earlier suggestion to delete rows first.)

**BL-019** The same-day join splits overnight legs; wrong-airport rows can multiply results. Need real affected examples. Accept: daily-flight and midnight fixtures give correct pairs or an explicit "ambiguous/unavailable", ties resolved deterministically.

**BL-020** 23 MB is a point-in-time size, not a forecast. Collect 7-14 days of comparable growth, learn quota headroom, then choose a window. No extension just for the origin box.

**BL-021** Both collectors advance run health after failed or empty feeds; the new one catches failed batches. A green workflow is not enough. Accept: failure, empty and success separated, coverage recorded, stale/partial UI honest, total failure cannot look fully fresh.

**BL-022** The database key includes direction (`type`) but detail URLs use only number and date, so an arrival and departure with the same number can mix. Accept: URLs, header, history and share isolate direction; old links handled; same number/date fixture tested.

**BL-023** Saving again resets the custom label, city is not saved, replay can keep stale filters. Accept: label survives repeat and replay, full filters restored, malformed storage recovers.

**BL-024** Share uses the latest event snapshot; a dropped snapshot lacks route and time; unknown airline prefixes may build invalid verification links. Accept: right current state and route with dropped/no-history fixtures, supported links accurate, absent links omitted.

**BL-025** One partial non-empty response can mark missing flights Dropped. Agree a repeated-miss or coverage rule. Accept: outage, partial and reappearance fixtures pass, route context retained.

**BL-026** No workflow concurrency guard or dependency manifest. Measure both collectors first; do not add GitHub schedules on top of the external one. Accept: overlap policy and dependencies defined, run duration observed, safe scheduling handoff documented.

**BL-027** Both collectors use `verify=False`. Diagnose the certificate chain before choosing a verified alternative. Accept: verified TLS works, or a narrow justified exception.

**BL-028** *New 2026-11-01.* User need: visitors can reach the owner without the owner publishing an email address or phone number (D-014).
- Approach: a plain HTML form posts to a hosted form service, which emails the owner. The service stores the destination address in its dashboard or behind a form key, so the page source contains neither. Candidates: Web3Forms (no account) or Formspree; check current free limits `[UNVERIFIED]`.
- Fields: name, message, "Call me back" checkbox that reveals phone and preferred time. Hidden honeypot field against spam; add the service's captcha if spam appears. Thank-you message after submit. One line saying phone numbers are used only to reply.
- Page: remove the `mailto:` link and the visible address; **complete the truncated footer** so the page matches the others.
- Needs: current `docs/contact.html`; the owner creates the form key privately.
- Accept: test message arrives; view-source of the page contains no personal address or number; empty submit rejected; footer and navigation complete on mobile.

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
- More saved SELECT examples for the canonical view are in `notes/REFERENCE_QUERIES.txt`.

## 6. Repo tidy-up for the two-file switch

After saving `CLAUDE.md` and `PLAN.md` at the repo root, delete (git history keeps them): root `AGENTS.md`; `docs/SPEC.md`, `docs/BACKLOG.md`, `docs/DECISIONS.md`, `docs/JOURNAL.md`, `docs/DB.md`, `docs/WORKFLOW.md`, `docs/readme`. Replace `README.md` with a three-line pointer to the two files. Keep `notes/REFERENCE_QUERIES.txt`. Do not put the new files in `docs/` (that folder is the published website). The old draft PR #4 uses a competing layout: close it rather than merging.

## 7. Journal (newest first)

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
