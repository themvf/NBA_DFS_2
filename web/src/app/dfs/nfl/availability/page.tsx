import Link from "next/link";
import { getNflWeeklyReports } from "@/db/nfl-dfs-report-card";
import { getNflWeekBallParticipants, getNflWeekPlayContext } from "@/db/queries";
import AvailabilityBoard from "./availability-board";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL DFS · Availability Review" };

export default async function Page({ searchParams }: { searchParams: Promise<{ season?: string; week?: string }> }) {
  const { season: raw, week: rawWeek } = await searchParams;
  const now = new Date();
  const season = raw && /^20\d{2}$/.test(raw) ? Number(raw) : now.getUTCFullYear() - (now.getUTCMonth() < 3 ? 1 : 0);

  let reports;
  try {
    const week = rawWeek && /^\d+$/.test(rawWeek) && Number(rawWeek) >= 1 && Number(rawWeek) <= 18 ? Number(rawWeek) : undefined;
    reports = await getNflWeeklyReports(season, week);
  } catch (error) {
    console.error("NFL weekly report unavailable", error);
  }
  if (!reports?.reports.length) {
    return <main className="mx-auto max-w-5xl space-y-4 p-8"><Link href="/dfs/nfl">← NFL DFS workspace</Link>
      <h1 className="text-2xl font-bold">Availability Review</h1>
      <p role="alert">Saved weekly reports are unavailable, so there is nothing to check availability
        against. Check the daily NFL DFS report-card job. This is not a zero-result report.</p></main>;
  }

  // Play-by-play for the week actually rendered. Fetched here so a missing
  // refresh degrades to a stated "not available" rather than an empty board
  // that reads like "nobody left the game".
  const shown = reports.reports.at(-1)!.week;
  let participants = null;
  let playContext = null;
  try {
    [participants, playContext] = await Promise.all([
      getNflWeekBallParticipants(season, shown),
      getNflWeekPlayContext(season, shown),
    ]);
  } catch (error) {
    console.error("NFL week play-by-play unavailable", error);
    participants = null;
    playContext = null;
  }

  return <AvailabilityBoard key={`${season}:${shown}`} reports={reports.reports}
    availableWeeks={reports.weeks} season={season}
    participants={participants} playContext={playContext} />;
}
