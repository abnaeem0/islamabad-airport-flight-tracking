# Islamabad Flight Tracker — working kit

A manual-first system for improving this project with any LLM. Start with **docs/WORKFLOW.md**; AGENTS.md tells the LLM how to help. This kit consolidates the supplied Claude drafts, prior ChatGPT review, repository evidence and owner decisions. It does not apply application or database fixes.

## Install once

1. Download/keep a backup of the current repository before editing.
2. Add AGENTS.md and this README.md at the repository root. Add the supplied Markdown files under docs/. Existing website files in docs/ stay in place.
3. If README.md or AGENTS.md already has custom content, reconcile it before replacing. This is a proposed documentation update, not permission to discard unrelated work.
4. Use **docs/BACKLOG.md as the only task queue**. Archive older root SPEC/BACKLOG/architecture documents under docs/archive/ with their dates; replace their old locations with a short pointer to these canonical documents. Preserve old logs rather than keeping two active queues. Existing GitHub draft PR #4 uses a different layout; reconcile or supersede it before merging, rather than installing both layouts.
5. Change docs/readme into a pointer to ../README.md after preserving its old wishlist. Its work items are captured in the backlog.
6. Save the documentation in GitHub in one documentation-only commit. This kit has not been pushed or merged.

## File map

| File | Job | When to update |
| --- | --- | --- |
| AGENTS.md | Shared rules every LLM follows | When the working process changes |
| docs/WORKFLOW.md | Your steps, copy-paste prompts and editing format | When your routine changes |
| docs/SPEC.md | Accepted goal, scope, constraints and unanswered questions | When product behavior/scope is agreed |
| docs/BACKLOG.md | One queue, stable task IDs, status and checks | When work is captured, applied or verified |
| docs/JOURNAL.md | Current handoff and recent session history | At the end of a work session |
| docs/DECISIONS.md | Why significant choices were made | When an architectural/product choice is accepted |
| docs/DB.md | Schema/operations reference and evidence limits | After confirmed schema, source or scheduling changes |

Actual application paths remain docs/index.html, docs/script.js, docs/flight_detail.html, docs/flight_detail.js, docs/style.css and docs/contact.html. Python collectors remain at the root; workflows are under .github/workflows/.

Do not add a project-management app yet. The Markdown queue works with free chats and ZIP uploads. If later moving to GitHub Issues, explicitly make Issues the sole status queue and turn BACKLOG into an index; do not duplicate statuses.
