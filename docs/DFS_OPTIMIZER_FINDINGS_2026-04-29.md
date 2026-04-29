# DFS Optimizer Findings

Date: April 29, 2026

## Scope

This note captures the current findings from the NBA and MLB GPP optimizer review, plus the related NBA playoff-stats refresh change.

## NBA Findings

### Cheap-player problem is real

NBA GPP lineups were too willing to use sub-$4k players, especially near-minimum-salary punts. The issue was not mainly ownership underestimation. It was projection edge inflation.

Key pattern:

- cheap players were often getting a large positive `our_proj` versus field baseline
- that projection gap created positive leverage
- the optimizer then treated them as attractive tournament values

Historical review showed:

- cheap NBA players were systematically overprojected versus actual results
- the worst misses concentrated in the high-leverage cheap subset
- perfect lineups rarely used multiple sub-$4k players
- when a cheap player did appear in a perfect lineup, he was usually a true smash role event, not a filler punt

### NBA optimizer changes already shipped

The NBA tournament optimizer was tightened to reflect those findings.

Current behavior:

- maximum `1` sub-$4k player in NBA GPP lineups
- stricter admission for `$3.0k-$3.3k` players
- cheap-player projection and leverage dampening
- interactive `gpp_ls` now uses the same calibrated ownership path as job runs
- cheap-player debug reasons are visible in the lineup generator

### Starter signal interpretation

For NBA, the DraftKings `inStartingLineup` flag is used as role evidence only.

It should mean:

- auto-admit a cheap starter into the candidate pool
- do not force him into the final lineup

That preserves cheap starter access without turning every announced starter into an automatic lineup lock.

## MLB Findings

### MLB does not have the same cheap-player problem as NBA

The broad MLB pool behaves differently.

Perfect-lineup review showed:

- leaving salary on the table is normal in MLB
- cheap hitters are extremely common in perfect lineups
- multiple sub-$4k hitters in one winning lineup are common

That means NBA-style cheap-player caps should not be copied into MLB.

### The real MLB risk is narrower

The bad MLB subgroup was:

- hitter
- salary under `$4,000`
- high leverage
- no confirmed batting-order signal

That group missed badly on average, and our projection was materially too high on it.

### Important workflow distinction

`No batting order yet` is not the same as `bad lineup signal`.

The user often runs projections before MLB lineups post. Because of that:

- missing order should not be treated as a hard blocker by itself
- pending lineups should continue to use the existing pending-lineup policy
- confirmed lineup information should still matter once available

Current recommendation:

- keep MLB as-is for now
- do not hard-punish missing batting order by itself
- revisit only if we want a mild dampener for pending cheap high-leverage hitters later

## NBA Stats Refresh

The daily NBA stats refresh previously updated only `Regular Season` data. That was not sufficient on April 29, 2026 because playoff games were active.

The refresh script was updated so it now:

- fetches both `Regular Season` and `Playoffs` by default
- accepts `--season-type All`
- computes rolling/player stats from the combined season-log pool instead of one season type at a time

Result:

- playoff team and player game logs now refresh correctly
- the latest completed playoff data was ingested through April 28, 2026 at the time of verification

## Follow-up

Open follow-up items:

1. Backtest whether the NBA cheap-player guardrails improve actual GPP lineup quality over a larger historical sample.
2. Re-check MLB cheap high-leverage hitters after more slates accumulate.
3. If MLB needs a change later, prefer a mild pending-lineup dampener over a hard exclusion.
