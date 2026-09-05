---
name: pick-issue
description: Survey the repo's open GitHub issues and propose one to work on, recapping each candidate in plain terms. Use when the user asks "pick an issue", "what should we work on next", "what's outstanding", or asks to be reminded what an open issue is about.
---

# Pick an issue to work on

The user does not remember the issue bodies, and neither do you. A number and a title are not a recap — read the
issue, then say what it is about in the terms an operator would notice.

## 1. List

```
gh issue list --repo Sootopolis/ccas --state open --limit 200 --json number,title,labels \
  --template '{{range .}}#{{.number}} | {{.title}} | {{range .labels}}{{.name}}, {{end}}{{"\n"}}{{end}}'
```

Do not add `comments` to the `--json` set: the payload is ~85KB of comment bodies and buys nothing at this stage.

## 2. Shortlist

Four to six, spanning different shapes of work — a small self-contained bug, something operator-facing the user has
hit themselves, and at most one decision-only issue (labelled `pending-decision` or `decision:*`; those are near-zero
code and need the user's judgement, so they are cheap to close but are not really work).

Skip anything labelled `blocked`, epics, and issues gated on another open issue (#60 waits on #110 + #111).

## 3. Read the bodies

`gh issue view <n> --repo Sootopolis/ccas --json title,body` for every shortlisted issue. Never recap from the title.

## 4. Recap

Two to four sentences each:

- what breaks or is missing, described as a symptom rather than a component
- what it costs today — the reason it is worth doing at all
- the fix in one sentence, **plus its hidden cost** where there is one: a schema change needs a hand-written `ALTER`
  against every existing database, a CLI tree change needs `completions/ccas.bash` regenerated, some areas need an ADR
  read first

Mention a label only where it changes the work: `pending-decision` means the deliverable is the user's call, not code.

## 5. Recommend, then ask

Recommend one with the reason in a clause (self-contained, no migration, they hit it last week). Then put the
shortlist through `AskUserQuestion` so the pick is one keystroke, and wait — do not start work on the recommendation
before the answer comes back.

## After the pick

Read whatever the issue names — the ADRs in particular — before editing `ccas.utils.client`, the cache tables, or the
CLI command tree. Design thinking that comes out of the work belongs back in the issue, not only in the chat.
