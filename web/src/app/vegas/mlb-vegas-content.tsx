import {
  getLineAlertBacktest,
  getLineAlerts,
  getLineMovementHistory,
  getMlbActionabilityEvidence,
  getMlbBetBacktest,
  getMlbBets,
  getMlbClv,
  getMlbLineMovement,
  getMlbMoneylineModelBacktest,
  getMlbPipelineHealth,
  getMlbTotalModelBacktest,
  getMlbVegasCoverageStatus,
  getMlbVegasMatchups,
} from "@/db/queries";
import { buildMlbDecisionBoard } from "@/lib/mlb-vegas-decisions";
import { evaluateMlbActionability } from "@/lib/mlb-vegas-trust";
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
  const [
    matchups,
    coverage,
    actionabilityEvidence,
    health,
    totalBacktest,
    moneylineBacktest,
    bets,
    betBacktest,
    clv,
    lineMovement,
    lineAlerts,
    lineAlertBacktest,
    lineMovementHistory,
  ] = await Promise.all([
    getMlbVegasMatchups(queryDate),
    getMlbVegasCoverageStatus(),
    getMlbActionabilityEvidence(),
    getMlbPipelineHealth(),
    getMlbTotalModelBacktest(),
    getMlbMoneylineModelBacktest(),
    getMlbBets(1, 200),
    getMlbBetBacktest(),
    getMlbClv(),
    getMlbLineMovement(7),
    getLineAlerts("mlb"),
    getLineAlertBacktest("mlb"),
    getLineMovementHistory("mlb", 1, 100),
  ]);
  const trustDecisions = actionabilityEvidence.map(evaluateMlbActionability);
  const decisions = buildMlbDecisionBoard(matchups, { evaluatedAt, trustDecisions });

  return (
    <MlbVegasClient
      queryDate={queryDate}
      evaluatedAt={evaluatedAt}
      decisions={decisions}
      trustDecisions={trustDecisions}
      actionabilityEvidence={actionabilityEvidence}
      coverage={coverage}
      health={health}
      totalBacktest={totalBacktest}
      moneylineBacktest={moneylineBacktest}
      bets={bets}
      betBacktest={betBacktest}
      clv={clv}
      lineMovement={lineMovement}
      lineAlerts={lineAlerts}
      lineAlertBacktest={lineAlertBacktest}
      lineMovementHistory={lineMovementHistory}
    />
  );
}
