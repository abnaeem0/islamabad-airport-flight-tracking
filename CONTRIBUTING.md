# Working on this project

## Recommended management

Keep GitHub as the source of truth for code and these reference documents. Start with BACKLOG.md as the only work queue: it travels with a ZIP and is readable by every LLM. Use a small branch and PR per task. The owner accepts product decisions and merges reviewed work.

If task volume grows, move active execution to GitHub Issues with labels `bug`, `improvement`, `operations`, `architecture`, priorities P0–P3 and a board with Ready/Doing/Review/Done. Make that move explicit: BACKLOG becomes a roadmap/index linking issues, not a duplicate status tracker. Do not maintain two independent queues.

Work first on live-state verification, freshness and data identity; then integrate both ends. Limit work in progress to one application change per contributor. Different LLMs should work on separate branches/tasks and avoid editing the same schema or file at once. No agent delegation is implied by this document.

## Each change

1. Read README, SPEC, BACKLOG, ARCHITECTURE_CHANGELOG and AGENTS. Check current branch/base commit and working changes. An uploaded ZIP can be stale; reconcile it before preparing a production merge.
2. Select/claim a stable task ID. Write reproduction, expected outcome and acceptance criteria. Ask only about choices the code/evidence cannot settle; log unresolved decisions.
3. Make the smallest coherent change. Do not quietly combine migration, scheduler changes and UI redesign. Update the contract/spec if behavior changes.
4. Validate appropriately: syntax checks for touched code; focused fixtures for correctness bugs; isolated DB integration for SQL/retention changes; browser checks for UI changes. Failed requests, partial data, midnight rollover and arrival/departure identity are important boundaries.
5. Open a PR naming the task. Explain the problem, resulting behavior, verification and material limits. Record architectural reasons in ARCHITECTURE_CHANGELOG when relevant.
6. Before merging, review diff, schema compatibility and rollback. After deployment, verify collection and public reads at the agreed freshness interval. Mark Done with commit and evidence.

## LLM handoff format

- Task ID and goal:
- Branch/base commit and latest commit:
- Changed files and reason:
- Checks run and results:
- Current blockers / decisions still needed:
- Exact next action:

Put handoffs under the task's BACKLOG detail (or its GitHub Issue after migration). Chat history is supporting context; it must not be the only place holding a decision.

## Development boundaries

No production secrets in files, screenshots, logs or prompts. Existing browser publishable keys are not privileged database credentials. Never run scraper.py/origin_scraper.py against production as a casual test: both write and delete data. Never apply notes/REFERENCE_QUERIES.txt migration examples without reconciling the schema.

The archive has no test suite or schema setup. Begin by adding regression coverage to the first correctness fix and a schema baseline to FT-006; avoid building a large test framework before a real use case.

## Review baseline (2026-09-30)

Python files parse; both JavaScript files pass `node --check`. Focused pure-function checks confirm normalization differences and status/time detection behavior. GitHub main matches the ZIP. Five recent original-workflow dispatches succeeded at roughly ten-minute intervals; the latest inspected logs show all six feeds fetched, five changes and retention cleanup completed. Owner confirms the newer collector inactive. No live DB queries, RLS/canonical-view tests, cron-job.org settings inspection or deployed-browser testing were performed. Documentation delivery modifies no Python, JavaScript, HTML or CSS. GitHub branch/PR is the proposed shared update; merge remains with the owner.
