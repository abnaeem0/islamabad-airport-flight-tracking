# Shared backlog

Single work queue until the owner elects to move execution into GitHub Issues. Priority order below is provisional. No application fix has been implemented in this delivery.

**States:** Inbox → Ready → Doing → Review → Done; Blocked requires a reason. **Priorities:** P0 active outage/data loss, P1 correctness/reliability, P2 usability or next capability, P3 later. No verified active P0 outage is established by this snapshot.

| ID | Priority / state | Task and evidence | Done when |
| --- | --- | --- | --- |
| FT-001 | P1 / Blocked | Partially reconciled: ZIP matches main `8e8b03f`; owner confirms only scrape.yml active; five successful dispatches ~10 minutes apart, latest logs show all six feeds and cleanup | Remaining: exact cron-job.org settings, deployed website and inactive newer-job explanation; original run updates flights/flight_snapshots/scraper_status |
| FT-002 | P1 / Ready | Honest freshness: both collectors advance last_run despite empty/failed fetches; newer collector catches DB batch failures and continues | Failed/empty/successful fetches differentiated; successful coverage tracked by batch; UI shows stale/partial data honestly; total failure cannot appear healthy |
| FT-003 | P1 / Ready | Preserve direction in detail identity: search links and history query only number/date although original DB key includes type | Links, header, history and sharing isolate arrival/departure; old links handled explicitly; test same number/date with both types |
| FT-004 | P1 / Ready | Fix airport-date/time semantics: collectors use host-local dates; dateInput.valueAsDate uses UTC date; formatter assumes timezone-less values are UTC | Airport date uses Asia/Karachi near midnight; source timestamp semantics documented; UTC observations display correctly; source-local times are not shifted incorrectly |
| FT-005 | P1 / Ready | Safer disappearance detection: a single incomplete nonempty response can mark many flights Dropped | Agreed repeated-miss/coverage safeguards, retained route/time context and recovery behavior; fixtures cover outage, partial feed and reappearance |
| FT-006 | P1 / Blocked | Capture actual DB schema and access policy: no DDL/migrations/RLS in archive; reference notes use city where new writer uses origin_city/destination_city | Schema-only baseline and migrations reproduce tables/views/constraints; anonymous reads allowed only as intended; anonymous writes denied; query notes match schema |
| FT-007 | P1 / Ready | Render external data safely: flight/city/status strings enter innerHTML in search and detail pages | External values rendered as text or context-escaped; fixtures containing HTML do not execute or create markup |
| FT-008 | P2 / Ready | Saved-search correctness: saveToLocalHistory resets userLabel; city is not saved; flight searches replay with leftover filters; malformed localStorage can abort initialization | Labels survive repeat/replay; complete filter state restored; invalid storage recovers without disabling search |
| FT-009 | P2 / Ready | Correct sharing/current state: latest snapshot used as current state; dropped snapshot loses city/time; arrival ST labeled Departure; unknown two-character airline prefix misparsed | Share uses selected flight's current state, retains route, labels arrival/departure accurately and constructs supported external links correctly |
| FT-010 | P2 / Blocked | Define both-end canonical contract before UI integration | Agree provenance, missing counterpart display, source preference, conflicts, date rollover and route matching using real sanitized examples; then implement reviewed view/migration/UI with rollback |
| FT-011 | P2 / Blocked | Storage baseline revised: only original collector active, two-month cleanup confirmed in logs; origin seven-day cleanup inactive; reported disk usage 4% | Verify disk metric/allowance; measure table/index sizes and post-restart daily growth; inspect inactive origin tables; keep two months provisionally; measure before expansion/reactivation; see notes/STORAGE_PLAN.md |
| FT-012 | P2 / Ready | Shared normalization: frontend strips whitespace/hyphens/underscores and uppercases; collectors normalize differently | One documented contract used in readers/writers; legacy-data migration and collision checks planned before updates |
| FT-013 | P2 / Ready | Improve operations/performance: no workflow concurrency guard; per-row DB queries/upserts; no pinned dependency manifest | Run durations and overlap measured; choose concurrency behavior and batch writes if warranted; add reproducible dependencies and focused checks |
| FT-014 | P2 / Blocked | Replace verify=False with verified TLS after diagnosing PAA certificate behavior | Certificate issue documented; verified connection works or narrowly scoped justified alternative accepted; no blanket warning suppression |
| FT-015 | P3 / Ready | Clean user-facing claims/placeholders: airline-name/city text search overstated; contact email is placeholder; supplied contact page ends mid-link | Copy reflects supported search and collection freshness; owner supplies contact address; page markup complete |
| FT-016 | P3 / Inbox | Notifications for named flights / groups (legacy wishlist) | Owner agrees channel, triggers, frequency, cost, recipients, privacy and opt-out before implementation |
| FT-017 | P3 / Inbox | Aircraft-position tracking / fare analysis (legacy wishlist) | Separate scope/data sources/cost established; do not infer that existing airport status data supports either |

## Task detail template

**ID/title:** FT-XXX ...
**Priority/state/owner:** ...
**Observed behavior and reproduction:** ...
**Expected behavior:** ...
**Scope / files / dependencies:** ...
**Acceptance criteria:** ...
**Evidence / tests:** ...
**Branch / PR / base commit:** ...
**Handoff / blockers:** ...

Append details for a task before starting it. Keep IDs stable. Mark Done only with a merged commit and validation evidence, not an LLM assertion. Reprioritize after FT-001: if live collection has stopped or data is being corrupted, promote that incident to P0.
