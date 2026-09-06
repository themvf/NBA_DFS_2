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
  searchParams: Promise<{ date?: string }>;
}) {
  const { date } = await searchParams;
  const evaluatedAt = new Date().toISOString();
  const queryDate = date ?? easternDate(new Date(evaluatedAt));
  const matchups = await getNflVegasBoard(queryDate);
  const [lineAlerts, lineAlertBacktest, lineMovementHistory, health, detectorHealth, observations] = await Promise.all([
    getLineAlerts("nfl", 100, undefined, matchups.map(row => row.matchupId)),
    getLineAlertBacktest("nfl"),
    getLineMovementHistory("nfl", 1, 100),
    getNflPipelineHealth(queryDate),
    getDetectorHealth("nfl"),
    getMovementSignalObservations("nfl", matchups.map(row => row.matchupId)),
  ]);

  return (
    <NflVegasClient
      queryDate={queryDate}
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
