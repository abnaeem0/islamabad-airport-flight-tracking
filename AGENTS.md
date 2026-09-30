# Shared LLM instructions

Default: the owner uses a chat window and applies edits manually. Follow these rules even if you have no tools. Tool access changes who performs a step, not what counts as evidence.

## Read and establish context

Read docs/SPEC.md, the selected task in docs/BACKLOG.md, and the Current handoff in docs/JOURNAL.md. Read docs/DB.md for database/scraper tasks and relevant docs/DECISIONS.md before changing contracts. Use docs/WORKFLOW.md for delivery format.

If chat-only, ask for the smallest missing current file/result, not the entire repo. State which supplied files are current and which are old evidence. Never invent file contents, database columns, runtime status or performed tests. Do not require every spec question to be answered before a small independent fix.

## Ground truth

- Public site stays Islamabad-only. Add origin departure information for domestic arrivals into Islamabad on the details page, with its own source/freshness. No full reader migration or collector retirement has been accepted.
- Owner reports BOTH collectors active as of 2026-09-30; only the original's recent successful runs were independently inspected. Confirm runtime after any change.
- Original scraper writes flights/flight_snapshots/scraper_status; website reads these. New collector writes separate origin_* tables; canonical view exists but is not used by the current website.
- .github/workflows/scrape.yml and origin_scraper.yml ARE included. GitHub in-file schedules are commented out; cron-job.org dispatches workflows externally. Do not enable an additional schedule unintentionally.
- Owner says displayed PAA times are PKT. Derive airport dates in Asia/Karachi; generated observations use UTC. Historical timestamp storage and raw DateUpdated semantics need verification before migrations.
- Terminal statuses exclude drop detection; ordinary upserts/change snapshots still process terminal records. Do not copy the old comment as implemented behavior.
- Free Supabase reports 23 MB database as of 2026-09-30. Keep existing retention provisionally; ingestion matters more than longer history. Low current usage does not prove a safe long-term growth rate.

## Working rules

1. Pick one stable BL task and agree acceptance criteria. Split large work into individually verifiable steps. Capture other ideas without adding them to the current patch.
2. Check current file versions and pending edits before proposing changes. Two LLMs must not produce competing patches against the same file/base; apply one and re-share current files before the next.
3. Default to small FILE / FIND / REPLACE blocks. Exact anchors must be unique; if not found exactly once, tell the owner to stop and paste the surrounding current text. Offer full files for small files or when requested; preserve unrelated content.
4. Separate SQL from code. Explain scope, prerequisites, application steps, checks and realistic rollback. A data deletion has no genuine rollback without a prior export/backup. Do not recommend deletion as a casual filtering fix.
5. Do not execute collectors as casual tests: they write and delete retained history. Current collectors have no dry-run CLI or supported single-airport flag. Use pure-function fixtures or an isolated DB for tests; any controlled live run must be explicit.
6. Report PROPOSED / APPLIED / CHECKED / SAVED separately. Never mark Done because you generated code. Done requires owner/agent evidence of application, acceptance checks and a GitHub commit or documented backup checkpoint.
7. Update backlog for task state, journal for handoff, decisions only for accepted choices, spec only for accepted scope/contracts. Do not force every tiny fix into the decision log.
8. Retain existing IDs; alias previous FT IDs using BACKLOG's mapping. One queue only. Finished work stays in the record.
9. No credentials in chats or commits. Public publishable keys are different from secret/service-role keys. Keep raw database access/security dumps private; ask before posting them publicly. The earlier public schema update was blocked and has not been published.
10. Preserve originals and other contributors' work. Agent mode may prepare a branch/PR; do not merge or change production merely because a draft exists. Finish with exact next action and a copy-paste handoff.

For tools that support a platform instruction file, its content can point here: "Read AGENTS.md and docs/WORKFLOW.md." Do not maintain separate rule sets for each model.
