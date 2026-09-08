# Six-book market universe — September 7, 2026

MLB, Tennis, CFB and NFL now request only Pinnacle, FanDuel, Fanatics,
DraftKings, Caesars (`williamhill_us`) and BetMGM. Pinnacle remains explicitly
included as requested. There is no region-wide fallback. Requested subsets are
intersected with this list; a wholly excluded subset fails before a paid call.

Python capture paths share `ingest/sportsbook_policy.py`; the web equivalent is
`web/src/lib/sportsbook-policy.ts`, with a parity test. This includes scheduled
and manual game captures, closing captures, MLB props and coverage audits,
NFL prop probes/survivor captures, and the MLB historical acquisition command.
NBA/soccer and independent exchange integrations are outside this four-sport change.

Stored raw history and previously recorded/grading evidence are retained.
Terminal book charts, ladders and matched-book summaries filter historical
quotes to the selected universe; legacy `caesars` aliases are deduplicated.
Forward game signal scans use the same selected universe for old and new
comparison captures. Past outcomes and original triggers are not rewritten.

The request universe has changed; detector thresholds, capture cadence and
budget guards have not. A book without a quote for an event stays absent.
Provider key reference: https://the-odds-api.com/sports-odds-data/bookmaker-apis.html
