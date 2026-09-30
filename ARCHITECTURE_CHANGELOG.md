# Architecture change log

Record why system boundaries, data contracts and operating behavior change. Git records exact code; this file records reasoning. Add newest implemented decisions first. Proposed decisions must say Proposed and must not masquerade as shipped changes.

## 2026-09-30 — Active scheduler and storage baseline (documented; runtime unchanged)

**Correction:** Owner clarified that only cron-job.org dispatches to `scrape.yml` are active; the newer job is inactive. This supersedes the initial statement that both collectors were running. GitHub main matches the archive commit. Five recent original-workflow runs succeeded approximately ten minutes apart. Run 36672187772 fetched all six date/direction feeds, recorded five changes, and completed two-month cleanup.

**Decision / rationale:** Use the original collector as the active operational baseline. Keep its current two-month history provisionally. Evaluate storage growth after restart rather than inferring long-term capacity from the reported 4% disk reading. Size inactive origin tables separately: their seven-day cleanup does not run while their collector is inactive. Reassess storage before multi-airport reactivation or longer retention.

**Impact:** Documentation and backlog only. No job activation, retention code change, database deletion or schema change. Actual database size, allowance and growth remain unknown. Read-only diagnostic queries are in notes/STORAGE_PLAN.md; proposed thresholds are not deployed automation.

## 2026-09-30 — Documentation baseline (implemented in this delivery)

**Problem:** The existing readme is an undated wishlist and does not explain the second pipeline or guide multiple LLMs.

**Decision:** Add a root README, concise SPEC, prioritized BACKLOG, contributor workflow and AGENTS instructions. Preserve the original wishlist as `notes/LEGACY_README.txt` and replace `docs/readme` with a pointer.

**Why:** Give all contributors the same durable reference, stable work IDs and evidence boundaries without changing collectors or the public website.

**Impact / validation:** Documentation only. Python syntax, JavaScript syntax and focused pure-function assertions checked. Production scheduling, schemas, credentials and access policies remain unverified. Documentation is a proposed repository update until merged by the owner.

**Rollback:** Remove the new documentation and restore `docs/readme` from `notes/LEGACY_README.txt`.

## Inherited architecture — observed, original decision dates unknown

| Choice visible in the snapshot | Reason stated in source or inferred | Consequence |
| --- | --- | --- |
| Static site reads Supabase directly | Inferred: simple hosting with no app server | Database read policies are the public security boundary |
| Original state + change snapshots | Source: track meaningful changes while reducing writes | History is an event log, not every poll; it is not a substitute for current state |
| New pipeline uses separate tables | Source: avoid interfering with original system | Low-risk experimentation but duplicated collection; website gets no benefit until integration |
| Concurrent fetch / serial DB writes | Source: reduce request latency while keeping DB writes single-threaded | Partial batches can succeed independently; run health needs explicit coverage reporting |
| Seven-day vs two-month cleanup | Source: control storage | Different pipelines expose different historical windows |
| Source priorities / canonical view | Reference notes describe future multi-source selection | SQL definitions absent; implemented state cannot be established |

These are observations, not a reconstructed release history. The ZIP contains no Git log.

## Future entry template

### YYYY-MM-DD — Title [Proposed / Accepted / Implemented / Superseded]

- Task / PR / commit:
- Problem and concrete trigger:
- Decision and why:
- Alternatives considered and tradeoff:
- Data/API/UI contracts affected:
- Migration, compatibility and rollback:
- Validation and operational evidence:
- Remaining limitations:

Use this log for retention rules, canonical matching, source precedence, scheduling/health design, schema changes, access policies and notifications. Ordinary styling or isolated bug fixes belong in their task/PR unless they change a contract.
