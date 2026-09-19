export const dynamic = "force-dynamic";

import { getNflSpecialsBoard } from "@/db/queries";
import SpecialsClient from "./specials-client";

export const metadata = {
  title: "NFL Slate Specials",
  description:
    "What we project for each of DraftKings' slate-specials questions: our ranking of the candidates and the expected stat behind it, every week, with DK's price beside it when one can be captured.",
};

const SCOPES = new Set(["sunday_all", "sunday_1pm"]);

export default async function SpecialsPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string; week?: string; scope?: string; family?: string }>;
}) {
  const { season, week, scope, family } = await searchParams;

  const parsedSeason = Number(season);
  const targetSeason = Number.isFinite(parsedSeason) && parsedSeason > 2000 ? parsedSeason : 2026;
  const targetScope = scope && SCOPES.has(scope) ? scope : "sunday_all";

  // With no week given, probe for which weeks have a run and take the newest,
  // preferring this scope and falling back to any scope.
  //
  // It used to fall back to `?? 1`, which invented a week: with no boards at
  // all the page announced "no board for week 1" -- naming a week nobody asked
  // about and that has no special status. 0 means "no week to show", and the
  // empty state says that instead of blaming a week.
  const parsedWeek = Number(week);
  const requested = Number.isFinite(parsedWeek) && parsedWeek > 0 ? parsedWeek : null;
  const probe = await getNflSpecialsBoard(targetSeason, requested ?? -1, targetScope);
  const newestInScope = probe.weeksInScope[probe.weeksInScope.length - 1];
  const newestAnywhere = probe.weeksAnyScope[probe.weeksAnyScope.length - 1];
  const targetWeek = requested ?? newestInScope ?? newestAnywhere ?? 0;
  const board =
    targetWeek === probe.week
      ? probe
      : await getNflSpecialsBoard(targetSeason, targetWeek, targetScope);

  return <SpecialsClient board={board} initialFamily={family ?? null} loadedAt={new Date().toISOString()} />;
}
