import { connection } from "next/server";
import { getNflFirstTdDates, getNflFirstTdGames, getNflFirstTdGrades } from "@/db/nfl-first-td";
import { etDate, FirstTdView, pickDate } from "@/components/first-td-view";

export const dynamic = "force-dynamic";
export const metadata = { title: "NFL First Touchdown" };

async function now() { await connection(); return Date.now(); }

export default async function Page({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const nowMs = await now();
  const today = etDate(nowMs);
  const dates = await getNflFirstTdDates().catch(() => [] as string[]);
  const date = pickDate(dates, today, params.date);
  const [games, grades] = await Promise.all([date ? getNflFirstTdGames(date) : Promise.resolve([]), getNflFirstTdGrades()]);
  return <FirstTdView
    subtitle="NFL · our own model, no sportsbook player prices used."
    banner={<>Not validated against sportsbook prices. Tested on 372 games from 2025 and 2026 weeks 1-8 that the fitting never saw:
      it is about as good as splitting each team&apos;s chance by carries and targets alone, and its top pick scored first 13% of the
      time against 15-16% stated, so the biggest chances run a little high. When DraftKings lists a team&apos;s players for anytime
      touchdown (captured the day before and about an hour before kickoff), ours missing from the list are removed; prices are not used.
      Fair price is the chance as American odds with no bookmaker margin.</>}
    dates={dates} date={date} today={today} nowMs={nowMs} games={games} grades={grades}
    emptyNote="They are saved each morning before a game day and about an hour before each kickoff group."
    volumeLabel="Carries + targets"
    actualWait="The first touchdown appears here once the game's play-by-play is stored (usually within a day or two)."
    gradeWait="Each version's last pregame forecast is graded once the game's play-by-play is stored."
    enoughGames={100}
    notes={<>
      <li>About 6% of NFL first touchdowns are returns or defensive scores; that share goes to no player.</li>
      <li>Which team: implied points from the closing spread and total, as a race between the two scoring rates (fitted on 2016-2022).</li>
      <li>Which player: this season&apos;s touchdowns plus what his carries and targets would normally produce, which dominates; shares are flattened slightly because the raw ones ran overconfident. Candidates had a carry or target in the team&apos;s last two games; 5% of the team&apos;s share is left to everyone else.</li>
      <li>Who plays: only DraftKings lists captured within 30 hours of kickoff count. If a list has at least four of a team&apos;s players from ours, our players missing from it are removed. A surname alone matches when it is unique (Marquise and Hollywood Brown). Both versions are saved and graded.</li>
      <li>Chips are context, not part of the chance: goal-line role (40%+ of the team&apos;s rushing touchdowns), 35%+ of the team&apos;s carries and targets inside the 10, a touchdown in each of the last two games, a new role (10+ carries and targets a game in the last two, 1.5x his earlier average), a running quarterback (2+ rushing touchdowns), not on DraftKings&apos; list, a big favourite (7+ implied points), and how often the team has scored the first touchdown this season. None has been tested as a predictor.</li>
      <li>Model version nfl-first-td-v1. Each game shows the last forecast saved before kickoff.</li>
    </>}
  />;
}
