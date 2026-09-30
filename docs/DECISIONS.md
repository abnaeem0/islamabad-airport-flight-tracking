# Decisions and architecture history

Accepted owner choices, proposals and observed implementation are distinct. Add new dated decisions; supersede old ones explicitly rather than silently rewriting rationale. Small fixes need a backlog result/commit, not a new architecture entry.

## D-001..D-006 — inherited implementation, original dates unknown

| ID | Observed choice | Consequence |
| --- | --- | --- |
| D-001 | Static vanilla website reads Supabase directly | Public safety depends on grants, RLS, view/function permissions and API exposure |
| D-002 | Current state plus snapshots on meaningful change | Timeline records changes, not every poll |
| D-003 | New collector uses separate origin tables | Allows independent evaluation; duplicated Islamabad collection |
| D-004 | data_source and source_priority model | Sources must be registered; selection/conflict rules matter |
| D-005 | Concurrent fetch, serial writes in newer collector | Partial batches can succeed; overall green run can conceal failures |
| D-006 | Drop missing flights from nonempty feed; skip empty feed | Partial responses can cause false drops |

Status: **Observed**, not retrospectively owner-approved or guaranteed permanent. Source timing/performance claims in comments are not benchmark results.

## D-007 — Islamabad-only with origin context

- Date/status: 2026-09-30 / Accepted, owner.
- Decision: public search remains Islamabad-only; domestic arrivals get departure-airport guidance on details page.
- Consequences: use other-airport evidence where available. Filtering stored rows, using canonical view unchanged, moving all readers or retiring legacy collector are NOT automatically accepted by this choice.
- Supersedes: broader "both ends" wording and Claude D-007's implied automatic filter/cutover. Missing origin evidence may also affect domestic flights, not only international ones.

## D-008 — Manual-first, shared files

- Date/status: 2026-09-30 / Accepted, owner.
- Decision: AGENTS entry point, one BL backlog, small exact edit blocks, checks and journal handoff. Same process in agent mode with evidence of actual actions.
- Reason: different LLM platforms and paid tools are not always available.
- Tradeoff: owner performs application/checkpoint steps; avoid parallel conflicting patches and large sessions.

## D-009 — Time contract

- Date/status: 2026-09-30 / Accepted direction; historical/raw-source interpretation still to verify.
- Decision: airport-date/display in Asia/Karachi; generated observations UTC. Source displayed ST/ET are PKT per owner.
- Consequence: fix collector date calculation; don't blindly cast legacy timezone-less timestamps or assume every source string's timezone is confirmed.

## D-010 — Add origin context before considering full migration

- Date/status: 2026-09-30 / Proposed implementation order matching accepted scope.
- Decision proposed: retain existing search/history pipeline; validate canonical matching and add the origin box as a small feature. Decide reader cutover only after measured reliability/coverage and rollback design.
- Reason: limits simultaneous UI/schema/history/scheduler changes. Overnight matching and valid-source-airport filtering need validation first.

## D-011 — Storage protects ingestion

- Date/status: 2026-09-30 / Accepted priority; current-window recommendation provisional.
- Decision: no automatic retention extension. Keep current two-month/seven-day windows pending resumed-growth measurement; less historical data is preferable to blocked updates.
- Evidence: owner reports Free and 23 MB SQL database size. No present measured storage emergency.

## D-012 — One canonical documentation set

- Date/status: 2026-09-30 / Proposed installation, produced in this kit.
- Decision: root AGENTS/README; docs/SPEC, BACKLOG, DECISIONS, JOURNAL, WORKFLOW and DB. BL IDs retained; older FT IDs map to them. Archive other active document sets when installing.
- Reason: avoid contradictory instructions/status across chats and draft PRs. No GitHub merge or installation was performed here.

## Entry template

D-### / title; Date; Status (Proposed/Accepted/Implemented/Superseded); task ID; problem; decision; why/alternatives; affected contracts; migration/rollback; evidence; remaining unknowns. Accepted does not mean shipped: record implementation commit separately.
