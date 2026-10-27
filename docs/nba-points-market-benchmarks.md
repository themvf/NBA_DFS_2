# NBA market-line evidence

Every `research.nba_points_daily` run now preserves three separate market observations for the original frozen player pool:

1. Original line recorded in the pregame ghost snapshot, unchanged.
2. Last archived main points quote strictly before the common first-game contest lock.
3. Last archived main points quote strictly before the player's own verified scheduled game start.

The latter two are **archived pre-tip proxies, not certified closing lines**. A quote within fifteen minutes of tip receives a descriptive freshness flag only. The existing archive is intermittent; this implementation does not add paid odds requests or establish that the final available sportsbook quote was captured. Missing close coverage remains a material collection gap.

For each observation preserve the original source row ID/capture key, capture timestamp, provider update timestamps, per-book lines/prices, official scheduled start, market commence time, age before tip and rejection reasons. The consensus line uses the existing points-market reader. Identity requires the frozen market event ID, canonical unique player name, same home/away teams and slate date. Multiple games for a team pair or conflicting rows at the latest capture fail closed. Neither alternate markets nor another provider's event ID can substitute for a main quote.

Reject captures at or after either the official scheduled start or the source row's market commence time, whichever comes first. Later advertised market starts never admit live quotes. Provider updates after capture are rejected; missing provider timestamps stay explicit. If a game's scheduled start was delayed, the earlier documented cutoff is conservative and may omit valid later pregame quotes.

The report writes `market-history.json` (raw input archive), `market-benchmarks.json` (digest-protected evidence and comparisons), and an additional section in `review.md`. The enriched grade is append-only when the existing v2 `--apply` option is used. Original snapshot rows/files and model coefficients remain unchanged. Detailed closing evidence is kept out of the compact inline chart payload.

Over/under/push counts require a verified played outcome; DNP and unresolved results are excluded rather than assigned zero. Publish denominators, push counts, unpaired coverage and exactly paired three-baseline rates separately. Ghost-versus-late-market MAEs use identical model-specific players; the high-scoring cohort remains defined by the original frozen market line >=20. Later-game quotes may contain information unavailable at first-game contest lock, so they are a distinct benchmark, not a replacement for the original decision-time comparison. No profitability or line-value edge is inferred.

The existing daily local automation extracts this evidence from already-collected quotes when grading original snapshots. No new scheduler, paid capture, deployed workflow or website change is introduced. Genuine closing-line collection would require an additional suitable source or separately authorized near-tip capture coverage.
