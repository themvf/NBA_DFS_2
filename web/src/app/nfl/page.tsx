export const dynamic = "force-dynamic";

import {
  getLineAlertBacktest,
  getLineAlerts,
  getMovementSignalObservations,
  getLineMovementHistory,
  getNflPipelineHealth,
  getNflVegasBoard,
  getDetectorHealth,
  getNflArchetypeGames,
  getNflArchetypePlays,
} from "@/db/queries";
import Link from "next/link";
import NflVegasClient from "./nfl-vegas-client";
import PbpArchetypeClient from "./pbp-archetype-client";
import tabs from "./pbp-archetype.module.css";

// The tab lives in the URL rather than in client state so a view is
// linkable and so the archetype query is never run for a visitor who is
// only looking at markets.
function Tabs({ active, queryDate, view }: { active: string; queryDate: string; view?: string }) {
  const markets = `/nfl?date=${queryDate}${view ? `&view=${view}` : ""}`;
  return (
    <nav className={tabs.tabs} aria-label="NFL view">
      <Link href={markets} data-active={active === "markets"}>MARKETS</Link>
      <Link href="/nfl?tab=pbp" data-active={active === "pbp"}>PBP ARCHETYPE</Link>
    </nav>
  );
}

function easternDate(value: Date): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(value);
  const get = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

export default async function NflPage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string; view?: string; tab?: string; game?: string }>;
}) {
  const { date, view, tab, game } = await searchParams;
  const evaluatedAt = new Date().toISOString();
  const queryDate = date ?? easternDate(new Date(evaluatedAt));

  if (tab === "pbp") {
    const games = await getNflArchetypeGames();
    // Default to the most recently labelled game rather than an empty table.
    const selected = game && games.some(row => row.gameId === game) ? game : games[0]?.gameId ?? null;
    const plays = selected ? await getNflArchetypePlays(selected) : [];
    return (
      <>
        <Tabs active="pbp" queryDate={queryDate} view={view} />
        <PbpArchetypeClient games={games} gameId={selected} plays={plays} />
      </>
    );
  }

  const weekView = view === "week" || (!date && view !== "day");
  const end = new Date(`${queryDate}T12:00:00Z`);
  end.setUTCDate(end.getUTCDate() + 7);
  const board = await getNflVegasBoard(queryDate, weekView ? end.toISOString().slice(0, 10) : undefined);
  const matchups = weekView ? board.filter(row => !row.completed && Date.parse(row.commenceTime ?? "") > Date.parse(evaluatedAt)) : board;
  const [lineAlerts, lineAlertBacktest, lineMovementHistory, health, detectorHealth, observations] = await Promise.all([
    getLineAlerts("nfl", 100, undefined, matchups.map(row => row.matchupId)),
    getLineAlertBacktest("nfl"),
    getLineMovementHistory("nfl", 1, 100),
    getNflPipelineHealth(queryDate, weekView ? end.toISOString().slice(0, 10) : undefined),
    getDetectorHealth("nfl"),
    getMovementSignalObservations("nfl", matchups.map(row => row.matchupId)),
  ]);

  return (
    <>
    <Tabs active="markets" queryDate={queryDate} view={view} />
    <NflVegasClient
      queryDate={queryDate}
      weekView={weekView}
      evaluatedAt={evaluatedAt}
      matchups={matchups}
      lineAlerts={lineAlerts}
      observations={observations}
      lineAlertBacktest={lineAlertBacktest}
      lineMovementHistory={lineMovementHistory}
      health={health}
      detectorHealth={detectorHealth}
    />
    </>
  );
}
