"use client";

import type { PickemSlateGame } from "@/db/queries";
import { EMPTY_EVIDENCE, marketReview, timestamp, type PickemEvidence } from "@/lib/nfl/pickem-evidence";

const easternTime = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", month: "short", day: "numeric", year: "numeric",
  hour: "numeric", minute: "2-digit", timeZoneName: "short",
});

export function OddsFreshnessBanner({ games, evidence, now, week }: {
  games: PickemSlateGame[]; evidence: PickemEvidence; now: string; week: number;
}) {
  const at = Math.max(timestamp(now), timestamp(evidence.loadedAt));
  const captures = games.map(g => timestamp(evidence.games[g.gameId]?.latest?.capturedAt))
    .filter(t => Number.isFinite(t) && t <= at);
  const latest = captures.length ? Math.max(...captures) : null;
  const upcoming = games.filter(g => !g.completed && !(timestamp(g.kickoff) <= at));
  const needsRefresh = upcoming.filter(g => marketReview(
    evidence.games[g.gameId] ?? EMPTY_EVIDENCE, g.pHome, g.kickoff, new Date(at).toISOString(),
  ).stale).length;
  const incomplete = evidence.warnings.some(w => w.startsWith("Odds history"));
  const warning = latest == null || needsRefresh > 0 || incomplete;
  const minutes = latest == null ? null : Math.floor((at - latest) / 60_000);
  const age = minutes == null ? "" : minutes < 1 ? "less than a minute ago"
    : minutes < 60 ? `${minutes} min ago`
    : minutes < 1440 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m ago`
    : `${Math.floor(minutes / 1440)}d ${Math.floor(minutes % 1440 / 60)}h ago`;

  return (
    <section aria-label="Odds freshness" className={`flex flex-wrap items-center justify-between gap-3 rounded-lg border p-3 text-sm ${
      warning ? "border-amber-500/40 bg-amber-500/10" : "border-emerald-500/30 bg-emerald-500/10"
    }`}>
      <div className="space-y-1">
        <p className="font-semibold">
          Week {week} · Last odds captured: {latest == null ? "Unavailable" : <>
            <time dateTime={new Date(latest).toISOString()}>{easternTime.format(latest)}</time>
            <span className="ml-2 font-normal">({age})</span>
          </>}
        </p>
        <p className="text-xs text-muted-foreground">
          {upcoming.length === 0 ? "No upcoming games in this week. Showing the last pregame capture."
            : `${upcoming.length - needsRefresh} of ${upcoming.length} upcoming games have fresh odds.${needsRefresh ? ` ${needsRefresh} have stale or missing odds.` : ""}`}
          {incomplete && " Odds history is unavailable; coverage is incomplete."}
          {" "}Odds are flagged after 2 hours within a day of kickoff, or 24 hours otherwise.
        </p>
        <p className="text-xs text-muted-foreground">
          Scheduled captures: daily at 8:00 a.m. Eastern and 30 minutes before each kickoff.
          {" "}This page checks once after each capture window; scheduler delays can occur.
          {" "}Last loaded: <time dateTime={evidence.loadedAt}>{easternTime.format(timestamp(evidence.loadedAt))}</time>.
        </p>
      </div>
    </section>
  );
}
