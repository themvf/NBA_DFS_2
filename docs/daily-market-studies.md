# Daily Market Studies

The top of `/sports/market-studies` lists games with at least one matched
study cohort at or above 65% historical wins. It shows the original study ID,
the matched side and outcome, the unrounded-threshold result, and the decided
sample. Exact pushes are shown separately and excluded from the rate.
Small samples are explicitly labelled and remain visible. Multiple rows on a
game may be related or point to opposing sides. Rates are descriptive.

The comparison uses completed games on strictly earlier Eastern dates and
requires observed final evidence before the earlier of the current time and
the matched game's scheduled start. Missing timestamps are excluded. NBA uses
the completed ESPN box observation time. NFL requires agreed canonical/provider
finals and the archived final observation time. Historical views replay the
available pregame archive and current outcome evidence. They do not establish
the time a user saw a match or a calibrated win probability.

NBA and NFL use their existing first or last archived pregame observations,
according to the study. NBA-05 paper matches keep the separate original paper
cutoff and history. CFB uses matching original, pregame frozen board entries
for both the target and historical sample, with the corrected T5 Eastern window.
This population is separate from the closing-line ledger and its verdict.
Daily Studies and CFB Pattern Watch are publicly readable. A source outage is shown separately from a date with no results.

## Adding a sport study

In the same change as the sport-specific study, register its ID and title in
`web/src/lib/study-registry.ts`. Give it a matching, diagnostic, or
awaiting-evidence policy. For a matching study, add its cohort and outcome to
the corresponding adapter in `web/src/lib/daily-study-matches.ts`. Preserve its
market, timing, and selection rules and add a fixture covering the adapter.
Use the shared heading on the sport page so the public ID stays aligned.
Diagnostics such as line error or price shopping do not produce a game-win
match. Run `node --import tsx scripts/test-daily-study-matches.ts` from `web`.
CI checks that displayed NBA/NFL IDs and Python CFB triggers are registered,
and every matching policy has an adapter.
