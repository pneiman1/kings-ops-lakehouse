# Autonomy contract

Read this before asking a question. Most stops are unnecessary, and every
unnecessary stop costs a full round trip through a human who is not watching.

## PROCEED — do it, don't ask

- A thing you need does not exist. **Absence is never a stop.** Create the
  schema, the table, the notebook, the job, the config file, and continue.
- A tooling failure: a CLI version mismatch, an unexpected response shape, a
  missing folder, a bad import, a lint error. Fix it and keep going.
- Anything in a dev catalog or dev target.
- Rewriting, refactoring, or regenerating your own prior work.
- Installing a package with `pip install --user`.
- Committing and pushing to a branch.
- Adding a board item, a doc, or a troubleshooting entry.
- Starting a cluster or warehouse you will stop again in the same run.

## DEFER — note it, work around it, keep moving

- An item blocks on a permission or an external answer. Write it to
  `docs/BLOCKERS.md` with who owns it, drop the item, take the next one.
- A design question with more than one defensible answer and no urgency.
  Pick the reversible one, record the choice and the alternative, continue.
- Work you can do 80% of now and finish when the blocker clears. Do the 80%.

**Never end a run because one item blocked.** Items are independent unless
stated otherwise.

## STOP — only these three

1. **It destroys existing data.** Dropping a table, a schema, a catalog, or
   a volume that already holds rows. Overwriting a table someone else owns.
2. **It spends money that wasn't approved.** Creating compute that outlives
   the run. Resizing a warehouse up. Anything with no auto-termination.
3. **It writes to production.** Any `-t prod` target, any prod catalog, any
   job that prod reads.

That's the list. Uncertainty is not on it. Novelty is not on it. "This seems
important enough to check" is not on it.

## Compute discipline

Compute is the cost. Every run:

- Prefer serverless. It has nothing to leave running.
- If you start a cluster or warehouse, **stop it before the run ends**, and
  say in the report that you did.
- Never create a cluster without `autotermination_minutes`.
- Run `python scripts/dbxctl.py cost` as the last step and include the output
  in the report. If something is still up, that's a finding, not a footnote.

## Reporting

One report at the end, not progress narration in between. It covers:

- what closed
- what you decided and why — especially where you picked between options
- what's left and who owns it
- anything you found that nobody asked about

Never claim a state you haven't read back. Re-read the table, the job, the
run output after writing. A notebook that "should" have worked is not a
notebook that worked.

## Long runs

Launch with `claude --dangerously-skip-permissions` for unattended work.
That bypasses the deny list too, so the destructive commands in
`.claude/settings.json` become live. The STOP list above is then the only
thing standing between you and a dropped schema. Treat it accordingly.
