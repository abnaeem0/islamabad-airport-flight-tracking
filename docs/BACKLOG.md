# Shared backlog

Last reconciled: 2026-09-30. **One queue.** IDs never change/reuse; duplicates get an alias, not a second task. Priorities are proposed and can be changed by the owner.

## States and evidence

Inbox → Ready → Doing → Testing → Done. Blocked can occur at any active stage; record what unlocks it. Deferred/Cancelled preserve the reason. A reopened Done task gets new evidence/reason, not erased history.

- P0: observed active outage/data loss; P1: correctness/security/reliability; P2: useful usability/next capability; P3: later idea/tidy-up. No confirmed active P0 incident established by this kit.
- Size S: one small, independently checkable change; M: split into checkpoints; L: parent only, create child steps before implementation. SQL/access changes are not automatically S just because short.
- Confidence: Owner report / Code evidence / Catalog evidence / Reproduced. Label hypotheses clearly.
- Done: applied, acceptance checks passed, saved commit/checkpoint recorded. Testing means applied but incomplete verification. Generated code alone stays Ready/Doing with Proposed noted.

## Index

| ID | Type | Priority | Size | Status | Task |
| --- | --- | --- | --- | --- | --- |
| BL-001 | chore | P2 | S | Ready | Document actual scheduler setup |
| BL-002 | bug | P1 | S | Ready | Use PKT airport dates |
| BL-003 | bug | P2 | S | Inbox | History when Supabase fails to load |
| BL-004 | bug | P2 | S | Ready | Share arrival/departure label correctly |
| BL-005 | bug | P2 | S | Ready | Use confirmed contact email |
| BL-006 | chore | P2 | M | Ready | Unify flight-number normalization |
| BL-007 | bug | P1 | M | Ready | Render external values safely |
| BL-008 | chore | P3 | S | Inbox | Place analytics scripts inside page structure |
| BL-009 | chore | P2 | M | Ready | Establish timestamp storage/display contract |
| BL-010 | feature | P2 | L | Blocked | Origin-airport context on domestic ISB arrivals |
| BL-010A | research | P1 | S | Ready | Validate current origin collection |
| BL-010B | bug | P1 | M | Blocked | Validate canonical matching contract |
| BL-010C | feature | P2 | M | Blocked | Add origin box without switching existing readers |
| BL-010D | research | P3 | M | Inbox | Evaluate full reader migration later |
| BL-011 | feature | P3 | L | Inbox | Flight-change notifications |
| BL-012 | feature | P3 | M | Inbox | Adaptive scrape frequency |
| BL-013 | chore | P3 | M | Inbox | Batch database work where warranted |
| BL-014 | idea | P3 | L | Inbox | Actual aircraft tracking |
| BL-015 | idea | P3 | L | Inbox | Fare and booking-window research |
| BL-016 | chore | P3 | M | Inbox | README, sitemap and logos housekeeping |
| BL-017 | chore | P1 | M | Ready | Restrict public database privileges |
| BL-018 | research | P2 | M | Inbox | Evaluate Islamabad relevance filter safely |
| BL-019 | bug | P1 | M | Inbox | Overnight and duplicate canonical matching |
| BL-020 | research | P2 | S | Inbox | Measure retention and storage before extending |
| BL-021 | bug | P1 | M | Ready | Report honest freshness and batch outcomes |
| BL-022 | bug | P1 | M | Ready | Keep direction in flight history identity |
| BL-023 | bug | P2 | S | Ready | Preserve labels and saved filter state |
| BL-024 | bug | P2 | M | Ready | Share actual current state and retained route |
| BL-025 | bug | P1 | M | Ready | Guard against false drops from partial feeds |
| BL-026 | chore | P2 | M | Inbox | Avoid overlapping writers and pin environment |
| BL-027 | research | P2 | M | Inbox | Diagnose disabled TLS verification |

## Task records

Expand the selected task before work: add owner/session, exact base files/commit, reproduction, proposed edit, checks/results and save reference. All tasks below are unimplemented unless evidence is added.

### BL-001: Document actual scheduler setup

- Type / priority / size / state: chore / P2 / S / Ready.
- Context / evidence: Workflow files already exist; do not recreate them. Record sanitized cron-job.org target/ref/interval/retry settings and newest original/newer outcomes.
- Acceptance: Both job settings and latest coverage/failure evidence recorded; separate owner reports from log checks.
- Applied / checks / save reference: none established; fill during work.

### BL-002: Use PKT airport dates

- Type / priority / size / state: bug / P1 / S / Ready.
- Context / evidence: Both main functions use naive datetime.now on host; UTC midnight can select the wrong airport day. Observation generation stays UTC.
- Acceptance: Boundary fixtures around 00:00/05:00 PKT choose correct dates; both collectors syntax checked; applied run dates verified.
- Applied / checks / save reference: none established; fill during work.

### BL-003: History when Supabase fails to load

- Type / priority / size / state: bug / P2 / S / Inbox.
- Context / evidence: Supabase-load guard returns before renderHistory. This supports a failure hypothesis; the reported symptom has not been reproduced. Also consider malformed storage.
- Acceptance: Reproduce blocked CDN or malformed storage, then verify saved history can render and search failure is explained without deleting saved data.
- Applied / checks / save reference: none established; fill during work.

### BL-004: Share arrival/departure label correctly

- Type / priority / size / state: bug / P2 / S / Ready.
- Context / evidence: Arrival ST is currently called Departure in share text. Use the selected flight direction, not assumed route.
- Acceptance: One arrival and one departure share with correct labels and unchanged date/time.
- Applied / checks / save reference: none established; fill during work.

### BL-005: Use confirmed contact email

- Type / priority / size / state: bug / P2 / S / Ready.
- Context / evidence: Change both placeholder address occurrences in docs/contact.html to elodge50@gmail.com. Supplied file ends mid-footer: complete markup as a separate defined check.
- Acceptance: Correct visible address/mailto; page opens and footer/navigation are complete. Applied change and saved checkpoint recorded.
- Applied / checks / save reference: none established; fill during work.

### BL-006: Unify flight-number normalization

- Type / priority / size / state: chore / P2 / M / Ready.
- Context / evidence: Original strips spaces; newer strips spaces/hyphens; frontend also uppercases/strips underscores. Readers and stored legacy data can disagree.
- Acceptance: Agreed shared rule with examples; collision/legacy migration plan before updates; both paths find equivalent flight forms.
- Applied / checks / save reference: none established; fill during work.

### BL-007: Render external values safely

- Type / priority / size / state: bug / P1 / M / Ready.
- Context / evidence: Search and details insert source city/status/flight strings through innerHTML. Prefer DOM/textContent for untrusted fields.
- Acceptance: HTML-like fixture renders as plain text in options/cards/history; search and sharing continue to work.
- Applied / checks / save reference: none established; fill during work.

### BL-008: Place analytics scripts inside page structure

- Type / priority / size / state: chore / P3 / S / Inbox.
- Context / evidence: Some gtag script blocks sit outside head/body. This is tidy-up, not the first reliability fix.
- Acceptance: Valid placement, no duplicated analytics call or page error.
- Applied / checks / save reference: none established; fill during work.

### BL-009: Establish timestamp storage/display contract

- Type / priority / size / state: chore / P2 / M / Ready.
- Context / evidence: Original observations timezone-less; newer aware. Source raw update timestamps differ. PKT label and historical convention need verification.
- Acceptance: Document generated/source semantics; fixtures display correct PKT; no blind database cast; proposed migration separate from display changes.
- Applied / checks / save reference: none established; fill during work.

### BL-010: Origin-airport context on domestic ISB arrivals

- Type / priority / size / state: feature / P2 / L / Blocked.
- Context / evidence: Parent initiative. Add context first; keep existing list/history readers. Do child steps separately.
- Acceptance: BL-010A/B/C accepted checks pass with unavailable/stale/conflicting data; no unapproved legacy retirement.
- Applied / checks / save reference: none established; fill during work.

### BL-010A: Validate current origin collection

- Type / priority / size / state: research / P1 / S / Ready.
- Context / evidence: Read source registry/status and latest newer workflow logs; check valid origin airport reporting and feed freshness.
- Acceptance: Confirmed current registry/health/coverage; identify batch failures even if run green.
- Applied / checks / save reference: none established; fill during work.

### BL-010B: Validate canonical matching contract

- Type / priority / size / state: bug / P1 / M / Blocked.
- Context / evidence: Blocked on 010A examples. Rank ties, wrong-airport rows, duplicate matches, missing routes and same-date join need review; see BL-019.
- Acceptance: Sanitized fixtures produce deterministic correct leg pairs; no ambiguous +/-1-day daily-flight matches; provenance preserved.
- Applied / checks / save reference: none established; fill during work.

### BL-010C: Add origin box without switching existing readers

- Type / priority / size / state: feature / P2 / M / Blocked.
- Context / evidence: Blocked on 010B and owner display choice in SPEC. For a domestic arrival show departure-board status/ST/ET and separate source/time.
- Acceptance: Valid/missing/stale/conflicting origin data tested; header and ISB history intact; no claim of actual aircraft location.
- Applied / checks / save reference: none established; fill during work.

### BL-010D: Evaluate full reader migration later

- Type / priority / size / state: research / P3 / M / Inbox.
- Context / evidence: Separate optional proposal. Compare coverage, history window, performance and rollback before any retirement.
- Acceptance: Owner accepts explicit cutover; archival/deletion separately authorized; old links/identities work; measured parallel checks.
- Applied / checks / save reference: none established; fill during work.

### BL-011: Flight-change notifications

- Type / priority / size / state: feature / P3 / L / Inbox.
- Context / evidence: Need channel, recipients, opt-in, trigger/frequency, identity and cost choices. Do not choose Telegram or add accounts by default.
- Acceptance: Agreed minimum scope and cost; split into smaller tasks before implementation.
- Applied / checks / save reference: none established; fill during work.

### BL-012: Adaptive scrape frequency

- Type / priority / size / state: feature / P3 / M / Inbox.
- Context / evidence: Near-flight polling is a proposal; external scheduler already dispatches jobs. Measure useful freshness and run duration first.
- Acceptance: Required age defined; no duplicate scheduling/overlap; runtime/source/host budget measured.
- Applied / checks / save reference: none established; fill during work.

### BL-013: Batch database work where warranted

- Type / priority / size / state: chore / P3 / M / Inbox.
- Context / evidence: Per-record SELECT/upsert remains; only snapshots batch. Optimize from measured bottleneck.
- Acceptance: Measured improvement with equivalent state/change/drop behavior; no duplicate snapshot regression.
- Applied / checks / save reference: none established; fill during work.

### BL-014: Actual aircraft tracking

- Type / priority / size / state: idea / P3 / L / Inbox.
- Context / evidence: Airport status is not positional tracking. New source/cost/coverage/contracts needed.
- Acceptance: Owner agrees product need and source plan before implementation.
- Applied / checks / save reference: none established; fill during work.

### BL-015: Fare and booking-window research

- Type / priority / size / state: idea / P3 / L / Inbox.
- Context / evidence: Separate price source and methodology required; existing data cannot answer fares.
- Acceptance: Scope/data/cost agreed; independent research task.
- Applied / checks / save reference: none established; fill during work.

### BL-016: README, sitemap and logos housekeeping

- Type / priority / size / state: chore / P3 / M / Inbox.
- Context / evidence: Documentation kit supplies proposed README. Sitemap/logo suggestions are separate implementation decisions.
- Acceptance: Install/reconcile docs; review URL/logo behavior with measured checks; do not claim unrelated edits applied.
- Applied / checks / save reference: none established; fill during work.

### BL-017: Restrict public database privileges

- Type / priority / size / state: chore / P1 / M / Ready.
- Context / evidence: Confirmed private report shows excess write privileges and missing status protection. Need explicit relation/role plan, view security review and collector access preservation.
- Acceptance: Public reads work, unnecessary public writes denied, collector writes preserved; inherited privileges considered; safe non-destructive verification. Detailed raw metadata stays private.
- Applied / checks / save reference: none established; fill during work.

### BL-018: Evaluate Islamabad relevance filter safely

- Type / priority / size / state: research / P2 / M / Inbox.
- Context / evidence: REQUIRE_ISB_LEG currently False. Simply setting True excludes unrelated rows from seen set; drop detection can then mark existing unrelated rows Dropped. Data deletion is not a prerequisite.
- Acceptance: Define filter/drop scope compatibility; retained unrelated rows not falsely dropped; owner approves scope before flipping flag; no hidden cleanup.
- Applied / checks / save reference: none established; fill during work.

### BL-019: Overnight and duplicate canonical matching

- Type / priority / size / state: bug / P1 / M / Inbox.
- Context / evidence: Same-day number/route join can split overnight legs; wrong-airport multiple rows can multiply results. Actual affected flight examples needed.
- Acceptance: Daily-flight and midnight fixtures yield correct unambiguous pairs or explicit unavailable/ambiguous results; ties resolved deterministically.
- Applied / checks / save reference: none established; fill during work.

### BL-020: Measure retention and storage before extending

- Type / priority / size / state: research / P2 / S / Inbox.
- Context / evidence: Current 23 MB is small, not long-term forecast. Original two months/new seven days remain. No extension just to add origin box.
- Acceptance: 7–14 days comparable growth collected; quota/headroom known; window choice accepted; no premature deletion.
- Applied / checks / save reference: none established; fill during work.

### BL-021: Report honest freshness and batch outcomes

- Type / priority / size / state: bug / P1 / M / Ready.
- Context / evidence: Both collectors advance run health after failed/empty feeds; new catches failed DB batches. Overall green is insufficient.
- Acceptance: Failure/empty/success separated; coverage recorded; stale/partial UI honest; total failure cannot appear fully fresh.
- Applied / checks / save reference: none established; fill during work.

### BL-022: Keep direction in flight history identity

- Type / priority / size / state: bug / P1 / M / Ready.
- Context / evidence: DB key includes type; detail URL/query only use number/date. Same-number arrival/departure can mix.
- Acceptance: URLs/header/history/share isolate direction; old links handled explicitly; same number/date fixture tested.
- Applied / checks / save reference: none established; fill during work.

### BL-023: Preserve labels and saved filter state

- Type / priority / size / state: bug / P2 / S / Ready.
- Context / evidence: Repeated save resets userLabel; city not saved; replay can keep stale filters. Distinct from BL-003 CDN hypothesis.
- Acceptance: Label survives repeat/replay; saved full filters restored; malformed storage recovers.
- Applied / checks / save reference: none established; fill during work.

### BL-024: Share actual current state and retained route

- Type / priority / size / state: bug / P2 / M / Ready.
- Context / evidence: Share uses latest event snapshot; dropped snapshot lacks route/time. Unknown airline prefix parsing may build invalid verification URL.
- Acceptance: Correct selected current state/route with dropped/no-history fixture; supported links accurate; absent links omitted.
- Applied / checks / save reference: none established; fill during work.

### BL-025: Guard against false drops from partial feeds

- Type / priority / size / state: bug / P1 / M / Ready.
- Context / evidence: A single partial nonempty response can mark missing nonterminal flights Dropped. Distinguish this from cancellation.
- Acceptance: Agreed repeated-miss/coverage rule; outage/partial/reappearance fixtures; retained route context.
- Applied / checks / save reference: none established; fill during work.

### BL-026: Avoid overlapping writers and pin environment

- Type / priority / size / state: chore / P2 / M / Inbox.
- Context / evidence: Workflow concurrency guards/manifest absent. Measure both collectors first; no duplicate external and GitHub schedules.
- Acceptance: Defined overlap policy and dependencies; run duration observed; safe scheduling handoff documented.
- Applied / checks / save reference: none established; fill during work.

### BL-027: Diagnose disabled TLS verification

- Type / priority / size / state: research / P2 / M / Inbox.
- Context / evidence: Both collectors use verify=False. Diagnose certificate/chain before selecting a verified alternative.
- Acceptance: Verified TLS works or narrow justified exception accepted; no blanket suppression without evidence.
- Applied / checks / save reference: none established; fill during work.

## Earlier ID aliases (do not create a second queue)

| Previous ChatGPT ID | Canonical BL record |
| --- | --- |
| FT-001 | BL-001 |
| FT-002 | BL-021 |
| FT-003 | BL-022 |
| FT-004 | BL-002 and BL-009 |
| FT-005 | BL-025 |
| FT-006 | BL-001 / DB reference and BL-017 |
| FT-007 | BL-007 |
| FT-008 | BL-023 |
| FT-009 | BL-004 and BL-024 |
| FT-010 | BL-010 / BL-019 |
| FT-011 | BL-020 |
| FT-012 | BL-006 |
| FT-013 | BL-013 / BL-026 |
| FT-014 | BL-027 |
| FT-015 | BL-005; remaining inaccurate search copy may become a new task |
| FT-016 | BL-011 |
| FT-017 | BL-014 / BL-015 |
| FT-018 | BL-017 |

Legacy BL-000/000b/000c/000d in the uploaded draft are historical summaries, not new task IDs. Named history, field extraction and source capture are observed; full raw API storage and terminal skipping were not implemented. Both collector activation is owner-reported, not independently verified here.

## Suggested next sequence

Install/save kit → choose BL-017 access review (highest impact) or BL-005 small manual warm-up → BL-002 date correctness → BL-021 freshness / BL-022 identity → BL-010A/B/C origin feature. Prioritize a newly observed outage ahead of this sequence. Capture other work without widening the selected task.

## Copy-paste task template

BL-### title; type/priority/size/state; reporter/date/confidence; actual/expected or user need; current files/base; acceptance checks; dependencies; proposed edits; applied edits; checks/results; commit/checkpoint; next action. Next unused integer ID is BL-028; BL-010A..D are children of BL-010.
