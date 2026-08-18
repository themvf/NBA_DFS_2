import {
  getVegasMatchups,
  getOuHitRate,
  getTeamTotalAccuracy,
  getSpreadCoverage,
  getLineMovement,
  getLineMovementHistory,
  getLineAlerts,
  getLineAlertBacktest,
  getDetectorHealth,
  getVegasSummaryStats,
  getBiggestMisses,
  getTeamVegasInsights,
  getMoneylineBacktest,
  getSoccerVegasMatchups,
  getSoccerBets,
  getSoccerSettledBets,
  getSoccerBetBacktestByType,
  getSoccerKnockoutAdvance,
  getSoccerKnockoutAsOf,
  getSoccerTitleOdds,
  getSoccerClv,
  getSoccerCalibrationCuts,
  getSoccerClvTrend,
  getSoccerFirstScorers,
  getSoccerMatchGoals,
  getSoccerPlayerStats,
  getSoccerFirstScorerTiers,
  getSoccerFirstScorerNearMisses,
  getSoccerTopPickAccuracy,
  getSoccerSettlementHealth,
} from "@/db/queries";
import { getTennisVegasMatchups, getTennisBets, getTennisBetBacktest, getTennisEloDashboard } from "@/db/queries";
import type { Sport } from "@/db/queries";
import VegasClient from "./vegas-client";
import SoccerVegasClient from "./soccer-vegas-client";
import TennisVegasClient from "./tennis-vegas-client";
import MlbVegasContent from "./mlb-vegas-content";

export default async function VegasContent({ date, sport = "nba" }: { date?: string; sport?: Sport }) {
  if (sport === "mlb") {
    return <MlbVegasContent date={date} />;
  }

  // Tennis (Wimbledon MVP): fixtures + our-model-vs-market + rated moneyline bets.
  if (sport === "tennis") {
    const [matchups, bets, backtest, lineMovement, lineAlerts, lineAlertBacktest, eloDashboard, detectorHealth] = await Promise.all([
      getTennisVegasMatchups(date),
      getTennisBets(300),
      getTennisBetBacktest(),
      getLineMovement("tennis"),
      getLineAlerts("tennis"),
      getLineAlertBacktest("tennis"),
      getTennisEloDashboard(),
      getDetectorHealth("tennis"),
    ]);
    return <TennisVegasClient matchups={matchups} bets={bets} backtest={backtest} lineMovement={lineMovement} lineAlerts={lineAlerts} lineAlertBacktest={lineAlertBacktest} eloDashboard={eloDashboard} detectorHealth={detectorHealth} queryDate={date ?? null} />;
  }

  // Soccer: focused fixtures view + star-rated bet ledger + backtest, rather
  // than the NBA/MLB analytics panels.
  if (sport === "soccer") {
    const [matchups, bets, settledBets, backtest, firstScorers, matchGoals, playerStats,
           fscorerTiers, fscorerNearMisses, topPickAccuracy, clv, calibCuts, clvTrend,
           settlementHealth, knockoutTies, titleOdds, knockoutAsOf, soccerLineMovement,
           soccerLineAlerts, soccerLineAlertBacktest, soccerLineMovementHistory, soccerDetectorHealth] = await Promise.all([
      getSoccerVegasMatchups(date),
      getSoccerBets(1, 150),
      getSoccerSettledBets(),
      getSoccerBetBacktestByType(),
      getSoccerFirstScorers(8),
      getSoccerMatchGoals(),
      getSoccerPlayerStats(),
      getSoccerFirstScorerTiers(),
      getSoccerFirstScorerNearMisses(),
      getSoccerTopPickAccuracy(),
      getSoccerClv(),
      getSoccerCalibrationCuts(),
      getSoccerClvTrend(),
      getSoccerSettlementHealth(),
      getSoccerKnockoutAdvance(),
      getSoccerTitleOdds(),
      getSoccerKnockoutAsOf(),
      getLineMovement("soccer"),
      getLineAlerts("soccer"),
      getLineAlertBacktest("soccer"),
      getLineMovementHistory("soccer", 1, 250),
      getDetectorHealth("soccer"),
    ]);
    return (
      <SoccerVegasClient
        matchups={matchups}
        bets={bets}
        settledBets={settledBets}
        backtest={backtest}
        clv={clv}
        calibCuts={calibCuts}
        clvTrend={clvTrend}
        firstScorers={firstScorers}
        matchGoals={matchGoals}
        playerStats={playerStats}
        fscorerTiers={fscorerTiers}
        fscorerNearMisses={fscorerNearMisses}
        topPickAccuracy={topPickAccuracy}
        settlementHealth={settlementHealth}
        knockoutTies={knockoutTies}
        titleOdds={titleOdds}
        knockoutAsOf={knockoutAsOf}
        lineMovement={soccerLineMovement}
        lineAlerts={soccerLineAlerts}
        lineAlertBacktest={soccerLineAlertBacktest}
        lineMovementHistory={soccerLineMovementHistory}
        detectorHealth={soccerDetectorHealth}
        queryDate={date ?? null}
      />
    );
  }

  const [sportData, vegasSummary, biggestMisses, teamInsights, moneylineBacktest] = await Promise.all([
    Promise.all([getVegasMatchups(date), getOuHitRate(), getTeamTotalAccuracy(), getSpreadCoverage()]),
    getVegasSummaryStats(sport),
    getBiggestMisses(sport, 20),
    getTeamVegasInsights(sport),
    getMoneylineBacktest(sport),
  ]);
  const [matchups, ouHitRate, teamTotalAccuracy, spreadCoverage] = sportData;

  return (
    <VegasClient
      matchups={matchups}
      ouHitRate={ouHitRate}
      teamTotalAccuracy={teamTotalAccuracy}
      spreadCoverage={spreadCoverage}
      mlbCoverageStatus={null}
      mlbTotalBacktest={null}
      mlbMoneylineBacktest={null}
      mlbBets={null}
      mlbBetBacktest={null}
      mlbBetBySide={null}
      mlbActionabilityEvidence={null}
      mlbClv={null}
      mlbLineMovement={null}
      mlbLineAlerts={null}
      mlbLineAlertBacktest={null}
      mlbLineMovementHistory={null}
      mlbHealth={null}
      vegasSummary={vegasSummary}
      biggestMisses={biggestMisses}
      teamInsights={teamInsights}
      moneylineBacktest={moneylineBacktest}
      queryDate={date ?? new Date().toISOString().slice(0, 10)}
      sport={sport}
    />
  );
}
