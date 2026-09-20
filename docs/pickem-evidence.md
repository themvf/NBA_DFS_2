# Pick'em evidence and decision review

The pick'em board uses the latest captured pregame moneylines for upcoming picks, alongside availability reports, prior-game performance, a hypothetical scenario comparison, and an immutable pregame evidence snapshot.

## Data and freshness

- `game_odds_history` supplies the first and latest captured pregame consensus; `nfl_season_games.market_*` supplies an additional season quote. Home spread conventions are normalized before display. A first captured quote is not claimed to be the actual sportsbook opener.
- Two-sided moneylines are normalized without vig. Missing moneylines stay missing; spread-only quotes can still detect a favorite change. No quote at or after kickoff is used.
- Upcoming picks and the evidence panel share one capture snapshot. Valid two-sided moneylines replace the persisted probability directly, including its spread and capture timestamp; missing/invalid moneylines retain the model fallback. Started/completed games retain their stored forecast. Saving a card reads a fresh shared snapshot and rejects a stale submitted probability. This removes the dependency on the separately scheduled probability refresh for upcoming picks.
- The latest persisted model row is selected per game, preventing duplicate games when multiple model versions exist. Frozen cards are never rewritten.
- Quote age is checked independently of `nfl_game_win_probs.computed_at`. The operational freshness threshold is two hours inside 24 hours of kickoff, otherwise 24 hours. These thresholds are not probability adjustments.
- The board warns when a quote is stale, the favored team changed, the stored forecast favors the opposite team, or a report is newer than the captured quote. Page reload reads stored data; the existing odds/survivor jobs own refreshing it.
- The open board automatically re-reads stored data every minute while visible, and on return/focus or reconnection. Next router refresh preserves local picks and form state. The banner separates the latest odds capture from the last successful page data check; checking the page does not trigger a new sportsbook capture.

## News

- The latest stored injury observation per player is considered before filtering healthy players, preventing older OUT statuses from resurfacing. Observations older than 48 hours are omitted; old transient provider designations are also excluded. This feed mainly covers fantasy positions, not complete offensive-line or defensive depth charts.
- The board's “Add sourced report” form supports quarterback, offensive-line, playmaker, defense, weather, coaching and other news. It requires an HTTPS source and separates reported/confirmed/uncertain claims. Publication time may remain unknown; capture time is always recorded. Reports are restricted to a participating team and saved before kickoff.
- Reviewed reports appear ahead of feed observations. Reports can disagree, and none automatically adjusts the win probability. A quote captured after a report is not proof that the market incorporated it.
- `pickem_news` stores reviewed reports. Frozen cards store the report text as well as the source link, so later changes to a rolling source page do not rewrite the audit.

## Performance and decisions

- Stored nflverse play/drive data supplies prior-game offensive and defensive EPA, success rate, rushing yards, giveaways/takeaways, field goals and opponent defensive/punt/field-goal return touchdowns. Kickoff-return scores and garbage-time splits are not available. Missing coverage remains explicit.
- Recent form compares up to three covered games strictly before the selected week. It does not infer a trend from a single result or adjust the forecast.
- Each row explains the selected side, confidence weight and exact expected-points price of switching. Availability scenarios use a user-entered assumption and home win probability, showing expected points and probability rank among existing picks. They never silently alter the card, field shares, optimizer or forecast.

## Freezing and grading

- The freeze action re-reads server probabilities, schedule and evidence, validates the full card, verifies baseline weights and recalculates expected points. It rejects changed probabilities, duplicate/missing games, invalid weights, blank scenario explanations and post-kickoff cards.
- One PostgreSQL statement inserts the recommendation, all game evidence and the supersede pointer atomically. Kickoff is checked again at insertion time.
- `pickem_recommendation_games.evidence_json` contains the quote history endpoints, news text/timestamps, performance, narrative verdict, scenario assumptions, coverage warnings and probability calculation timestamp.
- A separate market baseline is ranked and frozen only when every game has a captured two-sided moneyline. Brier scores and confidence points compare the same settled game sample. Ties remain ungraded under the existing policy.
- The evidence audit is scoped to the selected pool and live cards. It compares narrative labels with entered observed pick shares; modeled shares do not validate their own assumptions. Older cards without evidence remain unmeasured.

## Verification

Run from `web`:

```text
npm run test:pickem-evidence
npm run test:pickem
npm run test:pickem-grading
npm run test:archetypes
npm run verify:pickem-evidence
npm run test:pickem-freeze-evidence
npx tsc --noEmit
```

The two database checks load `.env.local`. The verification command reads real feeds and applies idempotent schema additions. The freeze integration test creates an isolated test pool, exercises the real action with only Next cache invalidation stubbed, and removes its own test pool in `finally`. It does not modify user cards.

To repair stored probabilities for other consumers using already captured moneylines, run `node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx ./scripts/refresh-pickem-market.ts 2026 2` from `web`. It defaults to a dry run; `--apply` updates only older rows for upcoming games in the requested week, preserving tie mass and using the quote's capture time. It makes no external odds purchases and sends no alerts.
