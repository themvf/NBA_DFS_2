import Link from "next/link";
import { getNflWeeklyReports } from "@/db/nfl-dfs-report-card";
import WeeklyReview from "./weekly-review";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL DFS · Weekly Player Review" };

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
  if (!reports) return <main className="mx-auto max-w-5xl space-y-4 p-8"><Link href="/dfs/nfl">← NFL DFS workspace</Link>
      <h1 className="text-2xl font-bold">Weekly Player Review</h1>
      <p role="alert">Saved reports are unavailable. Check the daily NFL DFS report-card job. This is not a zero-result report.</p></main>;
  return <WeeklyReview key={`${season}:${reports.reports[0]?.week}`} reports={reports.reports} availableWeeks={reports.weeks} season={season} viewedAt={now.getTime()} />;
}
