import { connection } from "next/server";
import { getFirstTdDates, getFirstTdGames, getFirstTdGrades } from "@/db/cfb-first-td";
import { etDate, FirstTdView, pickDate } from "@/components/first-td-view";

export const dynamic = "force-dynamic";
export const metadata = { title: "CFB First Touchdown" };

async function now() { await connection(); return Date.now(); }

export default async function Page({ searchParams }: { searchParams: Promise<{ date?: string }> }) {
  const params = await searchParams;
  const nowMs = await now();
  const today = etDate(nowMs);
  const dates = await getFirstTdDates().catch(() => [] as string[]);
  const date = pickDate(dates, today, params.date);
  const [games, grades] = await Promise.all([date ? getFirstTdGames(date) : Promise.resolve([]), getFirstTdGrades()]);
  return <FirstTdView
    subtitle="College football · our own model, no sportsbook player prices used."
    banner={<>Not validated against sportsbook prices. Tested on 514 games from 2026 weeks 1-10 that the fitting never saw:
      it beat shares from carries and catches alone, and its chances matched what happened within about three points.
      It does not know injuries directly; when DraftKings lists a team&apos;s players for anytime touchdown, ours who are missing from that list are removed (prices are not used). Fair price is the chance as American odds with no bookmaker margin.</>}
    dates={dates} date={date} today={today} nowMs={nowMs} games={games} grades={grades}
    emptyNote="They are saved each morning and about three hours before each kickoff group, for games with betting lines."
    volumeLabel="Carries + catches"
    actualWait="The first touchdown appears here once the game's play-by-play is stored (the morning after)."
    gradeWait="Each version's last pregame forecast is graded a day or two after the game, once its play-by-play and box score are stored."
    enoughGames={150}
    notes={<>
      <li>About 4% of first touchdowns are returns or defensive scores; that share goes to no player.</li>
      <li>Which team: each team&apos;s implied points from the spread and total, run as a race between the two scoring rates.</li>
      <li>Which player: his touchdowns this season, pulled strongly toward what his carries and catches would normally produce. Candidates are the players who appeared in the team&apos;s last two games; 5% of the team&apos;s share is left to everyone else.</li>
      <li>Who plays: about three hours before each kickoff group we read DraftKings&apos; anytime-touchdown list. If it lists at least four of a team&apos;s players from our list, our players missing from it are removed and their share goes to teammates. A team with fewer listed is left alone, since a missing line then says little. Both versions are saved and graded.</li>
      <li>Chips are context, not part of the chance: goal-line role (40%+ of the team&apos;s rushing touchdowns, team has 4+), a touchdown in each of the last two games, a new role (10+ touches a game in the last two, at least 1.5x his earlier average), a running quarterback (3+ rushing touchdowns), not on DraftKings&apos; list (only when the team has too few listed for the filter to remove him), a big favourite (14+ implied points ahead), and how often the team has scored the first touchdown this season. None of them has been tested as a predictor; which team scores first is mostly its implied points, so a streak there is likely luck.</li>
      <li>Recent seasons&apos; favorites scored first more often than the model says (69% against 64% in 2025-26), so favorites are likely a little underrated.</li>
      <li>Model version cfb-first-td-v1. Each game shows the last forecast saved before kickoff.</li>
    </>}
  />;
}
