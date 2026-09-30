# Instructions for coding assistants

Read README.md, SPEC.md, BACKLOG.md and ARCHITECTURE_CHANGELOG.md before changing behavior. Follow CONTRIBUTING.md. These files and the current repository are the shared project memory; do not rely solely on your chat history.

- State the task ID, base commit (if Git exists), intended scope and acceptance criteria. Check uncommitted changes before edits; preserve other contributors' work.
- Treat SPEC's current behavior, agreed direction, proposals and unknowns as distinct. Do not report an inherited proposal as implemented.
- Keep the original Islamabad collector and public functionality working. The newer separate pipeline is currently inactive; do not reactivate it, switch readers, retire the original collector or migrate databases without an agreed compatibility/rollback plan.
- Protect ingestion over longer history. Keep two-month active retention provisionally; verify disk metric/allowance, growth and inactive-table sizes before lengthening retention. Follow notes/STORAGE_PLAN.md; documentation of a cleanup proposal is not authorization to delete data.
- Keep credentials out of the repo and handoffs. Use an isolated DB for collector/schema tests; collector entry points write data and delete retained history.
- Address timezone, flight identity, source provenance and failed/partial fetch behavior explicitly when touched.
- Use focused regression checks for correctness fixes. Report what was actually tested and what remains unverified. Do not claim live success from syntax checks.
- Update SPEC for accepted contract changes, the architecture log for significant decisions, and the task entry with status/evidence/handoff. Do not erase prior rationale or fabricate historical dates.
- Keep tasks small; avoid broad refactors during incident fixes. Ask the owner to settle material product ambiguities and record the answer.
- Do not mark a task Done until merged and validated. If no Git/remote is available, provide a proposed patch/delivery and say so.
- Never execute reference migration snippets blindly; actual schema definitions are currently missing from the supplied archive.
