# NHL market audit v2

Acceptance contract, registered October 12, 2026 before implementation:

1. Probability and execution-price CLV use the same eligible immutable NHL close. The execution book must have the exact full-game moneyline pair in that row; missing coverage produces null, never an older quote. Close and trigger identities and pregame boundaries are checked.
2. Quote timing includes trigger capture, selected book update, alert creation and scheduled start. A quote is fresh only when capture and book update precede the decision, are at most five minutes old, and the game has not started. Freshness is an observation, not a fill. Preserve stale research observations.
3. Settlement uses only completed canonical scores and the original frozen entry. Regrading appends grade history atomically with the current grade, never edits entries or deletes prior grades, and is idempotent. Dry run and evidence backup precede repair.
4. NHL reports show unique games, duplicate/opposing observations, priced settlement denominators, missing prices, fresh/stale/unknown timing, separate price/probability CLV, and steam intervals (<=30m, >30–180m, >180m, unknown). Game-cluster bootstrap uncertainty is descriptive and does not imply independent alerts.
5. A separately frozen forward protocol starts at registration, retains the four existing thresholds, separates historical development results, includes all eligible games and null grades, and compares signals with same-game market baselines. Promotion requires independent game coverage, positive prospective CLV, uncertainty assessment, and review; no automatic recommendation follows a sample count.
6. Goalie/injury evidence remains explicitly unavailable in this market-only study. Integrating it requires a separately registered experiment with timestamped pregame evidence; retrospective starters cannot be substituted. No threshold tuning from the observed winning subsets.

Validation: adversarial unit tests for absent/mismatched/future/older quotes, trigger timing, final versus live scores, regrade history and idempotency; scorecard denominator, clustering and interval tests; NHL market freshness tests; live dry-run and before/after reconciliation. No schema-wide initialization is needed for the repair.
