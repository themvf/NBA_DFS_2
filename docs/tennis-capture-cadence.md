# Tennis pre-match capture cadence

Version: tennis-dense-v1 (September 4, 2026).

The shared closing-line worker is scheduled every five minutes. Tennis now has
30-minute targets from six hours to 90 minutes before scheduled start,
15-minute targets from 90 to 30 minutes, and five-minute targets in the final
30 minutes. Existing checkpoint records are retained for audit continuity.
Overlapping due jobs in the same tournament are grouped into one paid request;
one capture can satisfy overlapping checkpoints. Tennis has 21 checkpoint names
per schedule version versus the previous four.

Provider schedule changes supersede pending/attempted/failed checkpoints for
the old start, without rewriting accepted captures or frozen closes. New slots
are seeded for updated schedules on the next worker pass. Match starts depend
on preceding matches and provider estimates: this is scheduled-start coverage,
not proof of the exact first serve or guaranteed five-minute delivery. GitHub
queue delays and quota exhaustion can still cause explicitly missed windows.

Spreads, totals, and moneylines retain existing book selection, tournament
batching, append-only capture and quality-graded closing evidence. No new
historical fetch or in-play capture is enabled. Existing shared repository
variables remain 2,000 closing-worker credits/day and 5,000 credits reserved;
these are shared across sports, not an additional tennis allocation. The final
30-minute targets cost at most six bulk requests (18 credits) per isolated
tournament/match window, before any retries, with overlapping matches sharing
requests. Other scheduled tennis ingestion remains unchanged.

The shared scheduled odds capture/grading job uses `--existing-schema`: it does
not rerun global database migrations, which can wait on unrelated long-running
imports and exhaust the capture windows. Migrations must be applied separately
before deploying a new schema-dependent worker version. Normal CLI behavior
still initializes schema unless this flag is explicitly supplied; a missing
required table/column remains an error, never a silent fallback.
