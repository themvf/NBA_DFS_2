# CFB forecast refresh operations

The Vercel dispatcher requests `refresh_cfb_plays.yml` daily at 10:07 UTC
(6:07 a.m. EDT / 5:07 a.m. EST), August–February, and on the single
quarter-hour tick 3–3.25 hours before each known kickoff group. This covers
Tuesday, Wednesday and Thursday games as well as weekends and bowls. A daily
10:17 UTC GitHub schedule is a fallback; it may run late. Workflow concurrency
serializes overlapping requests without cancelling publication.

Each run refreshes the canonical schedule/scores, imports the latest two completed
weeks of plays/drives, and checks all current-season prior FBS games for teams
playing in the next 24 hours. Missing scores or inadequate PPA/drive coverage
fails the run before forecasts publish. Teams with fewer than two completed
FBS games are explicitly excluded from publication expectations. All three
models append new forecast versions; existing forecasts and their frozen market
comparisons remain intact. A post-publication check requires matching kickoff,
publication after this run started, and the expected completed-game counts for
all three models. Health JSON is retained with workflow artifacts; failures make
the workflow red and visible to the existing workflow health/failure tooling.

The analytics overview and game page display generation time, eligible FBS-game
counts, and odds capture time separately. Forecasts older than 24 hours are stale;
inside three hours of kickoff the threshold is four hours. Historical age is
measured at kickoff. A fresh timestamp does not establish complete inputs or
predictive quality. Injuries and weather are explicitly not modeled.

Schedule-read failure is exposed in the dispatch heartbeat and does not suppress
the daily refresh. The pregame slot is not a guaranteed completion time: Actions
queueing, provider outages or input failures can delay or block it. Investigate
failed runs and dispatch the same workflow manually after repair; never relabel
an old forecast as fresh. No new paid odds capture is introduced.
