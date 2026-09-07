export const dynamic = "force-dynamic";

import { getNflPickemSlate } from "@/db/queries";
import PickemClient from "./pickem-client";

export const metadata = {
  title: "NFL Pick'em Pools",
  description:
    "Confidence and straight pick'em strategy: the EV-optimal entry, the exact price of every deviation from it, and what the field simulation thinks that price buys.",
};

export default async function PickemPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string; week?: string }>;
}) {
  const { season, week } = await searchParams;
  const parsedSeason = Number(season);
  const targetSeason = Number.isFinite(parsedSeason) && parsedSeason > 2000 ? parsedSeason : 2026;

  const slate = await getNflPickemSlate(targetSeason);

  // Default to the first week that still has an unplayed game -- the week the
  // user actually has to submit. Falls back to the last week of the season.
  const parsedWeek = Number(week);
  const firstOpen =
    slate.weeks.find((w) => slate.games.some((g) => g.week === w && !g.completed)) ??
    slate.weeks[slate.weeks.length - 1] ??
    1;
  const targetWeek =
    Number.isFinite(parsedWeek) && slate.weeks.includes(parsedWeek) ? parsedWeek : firstOpen;

  return (
    <PickemClient
      slate={slate}
      initialWeek={targetWeek}
      loadedAt={new Date().toISOString()}
    />
  );
}
