# Product spec

Last reconciled: 2026-09-30. Labels: **Confirmed** = owner decision; **Observed** = code/catalog evidence; **Open** = undecided. Proposals are not commitments.

## Goal and scope

**Confirmed:** a public Islamabad-only flight status website. Help users checking domestic flights landing in Islamabad by showing what the departure airport reports on the details page, so they can judge whether the flight has left or is delayed.

**Observed current features:** date/flight-number or airline-code substring search, direction/nature filters, city dropdown, current status/ST/ET, change history, ten browser-local recent searches with names, and copy/WhatsApp sharing. No user accounts, automatic alerts or aircraft location tracking. Airline-name/free-text city search is not implemented despite some page wording.

**Confirmed constraints:** manual copy-paste development across LLMs; no paid agent required. Free Supabase. Protect continued updates before expanding history. Contact email should be elodge50@gmail.com; application edit is still unverified.

**Next feature boundary:** enrich details for domestic arrivals into Islamabad. Keep existing search/history readers initially. Show origin status, reported times, source and its own last-checked time; handle unavailable/stale/conflicting evidence explicitly. A broader rewrite/migration is a separate proposal, not a prerequisite.

## Architecture and live-state evidence

Static vanilla JS/CSS website in docs/, hosted using GitHub Pages URLs in source; direct Supabase reads. Original collector writes legacy tables; new six-airport collector writes origin_* tables. Actual canonical view exists. Two GitHub workflow_dispatch files exist; in-file schedules are commented, external cron-job.org dispatches them.

Owner reports both scrapers active (latest correction). Original recent ten-minute runs and one complete successful log were inspected earlier; newer recent success/cadence still needs verification. Last inspected main commit matched ZIP 8e8b03f71e0d23e26a0612a34a9667128a12241f; subsequent application edits/merges are not established by this kit.

## Data and constraints

- Source is PAA API. Owner reports its displayed flight times are PKT. Verify raw source DateUpdated/time-format semantics before converting historical data.
- Use Asia/Karachi for airport dates; observations generated in UTC. Original schema stores some observations without timezone; newer schema uses timestamptz. See DB.md.
- Keep current windows provisionally: original two months, newer seven days. Supplied database size 23 MB is a point-in-time observation, not a growth forecast.
- Dropped means disappeared from a feed; it is not confirmed cancellation. Failed/partial data must not imply fresh or cancelled flights.
- Airport board data does not establish actual aircraft location. Do not present estimated arrival derived from flight duration unless a separate validated rule is agreed.

## Questions to answer when they matter

1. **Audience/action:** primarily family pickups, hotel staff handling pickups, or general travellers? What should they do after checking?
2. **Origin box:** show ST and ET with airport status, or prioritize a simple "departure reported / not yet reported"? Should unavailable evidence be visible rather than hidden?
3. **Freshness:** at what age should we flag data as stale, and what action should the user take?
4. **History:** how much history is useful, given ingestion-first storage policy? New collection coverage and history windows need not match immediately.
5. **Public product:** mobile/English priorities, traffic/SEO goals, analytics/privacy and any later revenue plans?
6. **Success:** what concrete result counts—successful personal pickups, fewer repeated checks, or public repeat users?

Small fixes can proceed without answering all questions. Record new accepted answers here and significant tradeoffs in DECISIONS.md.
