export const dynamic = "force-dynamic";

import { getNflPickemSlate, getPickemLedger, getPickemPools } from "@/db/queries";
import PickemClient from "./pickem-client";
import { getPickemEvidence } from "@/db/pickem-evidence";
import { resolveNflSeason } from "@/lib/nfl/season";

export const metadata = {
  title: "NFL Pick'em Pools",
  description:
    "Confidence and straight pick'em strategy: the EV-optimal entry, the exact price of every deviation from it, what the field simulation thinks that price buys, and a ledger that grades all of it against real results.",
};

export default async function PickemPage({
  searchParams,
}: {
  searchParams: Promise<{ season?: string; week?: string }>;
}) {
  const { season, week } = await searchParams;
  const targetSeason = resolveNflSeason(season);

  const evidencePromise = getPickemEvidence(targetSeason);
  const [slate, pools, ledger, evidence] = await Promise.all([
    evidencePromise.then(evidence => getNflPickemSlate(targetSeason, evidence)),
    getPickemPools(targetSeason),
    getPickemLedger(targetSeason),
    evidencePromise,
  ]);

  // Default to the first week that still has an unplayed game -- the week the
  // user actually has to submit. Falls back to the last week of the season.
  const parsedWeek = Number(week);
  const firstOpen =
    slate.weeks.find((w) => slate.games.some((g) => g.week === w && !g.completed)) ??
    slate.weeks[slate.weeks.length - 1] ??
    1;
  const targetWeek =
    Number.isFinite(parsedWeek) && slate.weeks.includes(parsedWeek) ? parsedWeek : firstOpen;

  // PFR player stats are read only by the evidence panel of the week on screen,
  // and for the whole season they were most of a 28 MB page (2026-11-01). Send
  // them for this week only; choosing another week loads that week's page. Each
  // snapshot's frozen identity table (~640 players) and source-file list are
  // not shown either, so the browser copy drops them. Freezing a card reads its
  // own full evidence on the server, so the ledger keeps both.
  const shownGameIds = new Set(slate.games.filter((g) => g.week === targetWeek).map((g) => g.gameId));
  const shownEvidence = {
    ...evidence,
    games: Object.fromEntries(Object.entries(evidence.games).map(([id, game]) =>
      [id, !shownGameIds.has(Number(id)) ? { ...game, pfr: [] } : {
        ...game,
        pfr: (game.pfr ?? []).map((team) => ({
          ...team,
          games: team.games.map((g) => ({ ...g, manifest: { ...g.manifest, identity: null, sourceFiles: [] } })),
        })),
      }])),
  };

  return (
    <PickemClient
      slate={slate}
      evidence={shownEvidence}
      evidenceWeek={targetWeek}
      pools={pools}
      ledger={ledger}
      initialWeek={targetWeek}
      loadedAt={new Date().toISOString()}
    />
  );
}
