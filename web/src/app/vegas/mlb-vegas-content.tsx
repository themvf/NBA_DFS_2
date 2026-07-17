import {
  getLineAlertBacktest,
  getLineAlerts,
  getLineMovementHistory,
  getMlbLineMovement,
  getMlbPipelineHealth,
  getMlbVegasMatchups,
} from "@/db/queries";
import MlbVegasClient from "./mlb-vegas-client";

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

export default async function MlbVegasContent({ date }: { date?: string }) {
  const evaluatedAt = new Date().toISOString();
  const queryDate = date ?? easternDate(new Date(evaluatedAt));
  const [matchups, health, lineMovement, lineAlerts, lineAlertBacktest, lineMovementHistory] = await Promise.all([
    getMlbVegasMatchups(queryDate),
    getMlbPipelineHealth(),
    getMlbLineMovement(7),
    getLineAlerts("mlb", 100),
    getLineAlertBacktest("mlb"),
    getLineMovementHistory("mlb", 1, 100),
  ]);

  return (
    <MlbVegasClient
      queryDate={queryDate}
      evaluatedAt={evaluatedAt}
      matchups={matchups}
      health={health}
      lineMovement={lineMovement}
      lineAlerts={lineAlerts}
      lineAlertBacktest={lineAlertBacktest}
      lineMovementHistory={lineMovementHistory}
    />
  );
}
