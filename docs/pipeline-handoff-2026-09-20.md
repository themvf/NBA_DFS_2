# Pipeline handoff verification — September 20, 2026

## Implemented in this checkout

- All three tennis-data consumers use `ingest/tennis_data_source.py`. It discovers
  exact ATP/WTA year-workbook links from the provider index, caches discovery
  per process, rejects ambiguous/missing links, and propagates HTTP failures.
  No rotating prefix is hardcoded.
- NFL disclosure test scopes its assertions to the render loop and obtains the
  actual callback variable. Tennis walking test distinguishes capture SQL from
  observation SQL and rejects unknown queries.
- Pipeline health independently registers shadow forecasts (240-hour weekly
  pregame budget) and tennis result observations (72-hour budget). These are
  evidence freshness checks; tennis reconciliation still gates provider health
  and unresolved matches.
- Specials documentation reflects 10 ranked families, seven propositions and
  four scopes, with live publication evidence.

## Verification

120 targeted tests passed: tennis source discovery, foundation, walking study,
shared disclosure floor, pipeline health, and slate specials. `git diff --check`
passed. The new monitor queries were also executed against the live database.

The specials board was rebuilt for 2026 week 2 across every scope: 301, 337,
283 and 331 published rows for sunday_1pm, sunday_all, sunday_late and
sunday_main, respectively. All runs contain all 17 families; zero blocked rows.
See `nfl-slate-specials-handoff.md` for run IDs and projection provenance.

## Remaining external blocker

Live settlement was attempted using the shared resolver. The provider index
`https://www.tennis-data.co.uk/alldata.php` returned HTTP 403, including with
its normal browser User-Agent. The provider failure remains visible; no static
prefix fallback or failure suppression was added. Reconciliation still reports
three stale unresolved matches and no fresh healthy provider runs, with all
other listed defect counters zero. Recovery cannot be claimed until an
accessible index and successful settlement/reconciliation run are verified.

## Concurrent NFL work

Production/research job separation was already present. A research refresh
already running when this handoff started completed with 18,615 samples and
updated the study pin to v3 study
`4b726852851f17b4b8b01158386061e8a95afdd37e4f9abf79a6c338665daec1`.
Old study rows remain separate by study_run_id; the drift guard is intact.

A live shadow run against the replacement study completed successfully and
reported 609 frozen player-weeks, with zero scored and 609 pending. These are
new prospective forecasts, not reconstructed evidence for the missed runs.
The verification output is `artifacts/handoff-shadow-verification.log`.

The tennis resolver, test repairs, monitoring entries and documentation changes
remain local; they have not been deployed by this task. Live board publication
and shadow freezing wrote their normal append-only database records.
