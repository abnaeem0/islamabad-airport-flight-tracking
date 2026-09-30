# Manual-first working routine

You only need GitHub, these Markdown files and a chat. The LLM diagnoses, proposes small edits and prepares the record; you apply, check and save. Use one working chat for a task. A second LLM can review the same proposed edit before application, but should not independently patch the same old files.

## The six steps

1. **Capture:** write a rough symptom/idea. Ask an LLM to turn it into an Inbox task. No need to perfect the wording.
2. **Choose:** pick the highest-impact small Ready task. Fix an active outage first, then correctness/security, then usability/features. Agree the expected outcome and how to verify it.
3. **Share current context:** paste AGENTS, the task, Current handoff and relevant source files. Include a commit/link when known. If you have unsaved local edits, say so and paste those versions; a GitHub link alone does not represent them.
4. **Get and apply a patch:** review one edit at a time. Save an original copy, find the exact unique block and replace it. If it does not match exactly, stop and ask the LLM to adapt to your current file. Never guess where it goes.
5. **Check:** follow specific click/query/log steps. Paste the result or error back. A generated patch stays Proposed; applied-but-unchecked work stays Testing. If it fails, record the result and fix/restore from the checkpoint.
6. **Save and hand off:** save the verified change plus backlog/journal edits to GitHub. If temporarily unable, keep a dated backup and label it local-only. Record the commit/backup reference and next action. Start the next chat from that handoff, not an old transcript.

For a small website change, GitHub's browser editor is enough. Several dependent code/SQL changes need an ordered plan: define compatible changes first and avoid publishing a half-applied state. Prepare an agent branch/PR when available; otherwise use manual edits with backup checkpoints. Database SQL is applied separately and gets its own verification record.

## What to paste

| Task | Small context bundle |
| --- | --- |
| Every task | AGENTS.md + one backlog entry + Current handoff; relevant SPEC/decision excerpts |
| Search/history | docs/script.js + docs/index.html; style.css only for layout |
| Detail/sharing | docs/flight_detail.js + docs/flight_detail.html |
| Collector | The relevant Python file + relevant DB section + workflow when environment matters |
| Database/view/access | DB section + current private query results + relevant query/writer code |
| Scheduling | Both workflow files + sanitized cron-job.org settings + recent run logs |
| Content | Current HTML page |

Ask the LLM which excerpt it needs if a file is large. Do not paste passwords or full logs containing credentials.

## Start prompt

```text
I work mostly by manually pasting edits. Here are AGENTS.md, task BL-___,
the Current handoff and current source files. Base commit/checkpoint: ___.
Uncommitted edits: none / ___.
Restate the goal, check the evidence and acceptance criteria, then propose
one small change. Ask only for missing context needed for that change.
Use exact FILE/FIND/REPLACE blocks and give concrete manual checks.
Do not mark the change applied or done until I report results.
```

## Edit format the LLM must use

```text
TASK: BL-___
BASE: supplied file / commit / checkpoint
FILE: docs/script.js
EDIT 1 of 2
FIND EXACTLY ONCE:
<current lines>
REPLACE WITH:
<replacement lines>
WHY: one sentence
CHECK: steps + expected result
ROLLBACK: restore <named backup> / reverse this exact edit
```

Put FIND and REPLACE contents in separate code blocks, without line numbers, ellipses or added escaping. Say NEW FILE for additions and supply its full content. File names are paths from repo root. For multiple edits, anchors must match the state after earlier edits. SQL gets a separate block, target project, prerequisites and outcome; no hidden destructive cleanup in a UI task.

## Capture prompt

```text
Capture this as a backlog task; do not implement it yet: ___.
Use the next unused BL ID. Include actual/expected behavior or user need,
evidence confidence, proposed priority, acceptance checks and blockers.
Ask only for details that change diagnosis or scope.
```

## Review with another LLM

```text
Review task BL-___ against the supplied current files and proposed edit.
Check correctness, scope, compatibility and whether the checks prove the
acceptance criteria. Do not rewrite unrelated code. Return concrete
issues or say no issue found; distinguish proof from assumptions.
```

## End prompt

```text
Give me a copy-paste wrap-up: (1) exact backlog replacement for this task,
(2) replacement Current handoff plus one journal entry, (3) a decision/spec
edit only if needed, (4) checks actually run and remaining checks,
(5) files/SQL applied versus only proposed, and (6) exact next action.
My reported results: ___. Saved commit/backup reference: ___ / not saved.
```

## Keep it manageable over time

- Each session: update one task and Current handoff; add one short journal entry.
- Weekly or after a few sessions: review Ready priorities, close duplicates and choose the next small task.
- Keep at most one task Doing by default. Research/review may run alongside it if it does not edit the same base.
- When BACKLOG/JOURNAL become cumbersome, move completed entries/old sessions to docs/archive/YYYY-MM.md with IDs and evidence preserved. Keep Current handoff, active tasks and archive links; do not keep a separate active queue in the archive.
- File statuses are authoritative; pasted old chats are historical context. An LLM should flag a conflict and use the newer recorded owner decision/evidence.
