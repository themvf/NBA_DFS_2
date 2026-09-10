export const dynamic = "force-dynamic";

import {
  getLineAlertBacktest,
  getLineAlerts,
  getMovementSignalObservations,
  getLineMovementHistory,
  getNflPipelineHealth,
  getNflVegasBoard,
  getDetectorHealth,
} from "@/db/queries";
import NflVegasClient from "./nfl-vegas-client";

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
  searchParams: Promise<{ date?: string; view?: string }>;
}) {
  const { date, view } = await searchParams;
  const evaluatedAt = new Date().toISOString();
  const queryDate = date ?? easternDate(new Date(evaluatedAt));
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
  );
}
