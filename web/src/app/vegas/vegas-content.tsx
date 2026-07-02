import {
  getVegasMatchups,
  getOuHitRate,
  getTeamTotalAccuracy,
  getSpreadCoverage,
  getMlbVegasMatchups,
  getMlbOuHitRate,
  getMlbTeamTotalAccuracy,
  getMlbRunLineCoverage,
  getMlbVegasCoverageStatus,
  getMlbTotalModelBacktest,
  getMlbMoneylineModelBacktest,
  getMlbBets,
  getMlbBetBacktest,
  getMlbBetBacktestBySide,
  getMlbClv,
  getMlbLineMovement,
  getLineMovement,
  getLineAlerts,
  getLineAlertBacktest,
  getMlbPipelineHealth,
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
import { getTennisVegasMatchups, getTennisBets, getTennisBetBacktest } from "@/db/queries";
import type { Sport } from "@/db/queries";
import VegasClient from "./vegas-client";
import SoccerVegasClient from "./soccer-vegas-client";
import TennisVegasClient from "./tennis-vegas-client";

export default async function VegasContent({ date, sport = "nba" }: { date?: string; sport?: Sport }) {
  // Tennis (Wimbledon MVP): fixtures + our-model-vs-market + rated moneyline bets.
  if (sport === "tennis") {
    const [matchups, bets, backtest, lineMovement, lineAlerts, lineAlertBacktest] = await Promise.all([
      getTennisVegasMatchups(date),
      getTennisBets(300),
      getTennisBetBacktest(),
      getLineMovement("tennis"),
      getLineAlerts("tennis"),
      getLineAlertBacktest("tennis"),
    ]);
    return <TennisVegasClient matchups={matchups} bets={bets} backtest={backtest} lineMovement={lineMovement} lineAlerts={lineAlerts} lineAlertBacktest={lineAlertBacktest} queryDate={date ?? null} />;
  }

  // Soccer: focused fixtures view + star-rated bet ledger + backtest, rather
  // than the NBA/MLB analytics panels.
  if (sport === "soccer") {
    const [matchups, bets, settledBets, backtest, firstScorers, matchGoals, playerStats,
           fscorerTiers, fscorerNearMisses, topPickAccuracy, clv, calibCuts, clvTrend,
           settlementHealth, knockoutTies, titleOdds, knockoutAsOf, soccerLineMovement,
           soccerLineAlerts, soccerLineAlertBacktest] = await Promise.all([
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
        queryDate={date ?? null}
      />
    );
  }

  const [sportData, mlbCoverageStatus, mlbTotalBacktest, mlbMoneylineBacktest, mlbBets, mlbBetBacktest, mlbBetBySide, mlbClv, mlbLineMovement, mlbLineAlerts, mlbLineAlertBacktest, mlbHealth, vegasSummary, biggestMisses, teamInsights, moneylineBacktest] = await Promise.all([
    Promise.all(
      sport === "mlb"
        ? [getMlbVegasMatchups(date), getMlbOuHitRate(), getMlbTeamTotalAccuracy(), getMlbRunLineCoverage()]
        : [getVegasMatchups(date), getOuHitRate(), getTeamTotalAccuracy(), getSpreadCoverage()],
    ),
    sport === "mlb" ? getMlbVegasCoverageStatus() : Promise.resolve(null),
    sport === "mlb" ? getMlbTotalModelBacktest() : Promise.resolve(null),
    sport === "mlb" ? getMlbMoneylineModelBacktest() : Promise.resolve(null),
    sport === "mlb" ? getMlbBets(1, 200) : Promise.resolve(null),
    sport === "mlb" ? getMlbBetBacktest() : Promise.resolve(null),
    sport === "mlb" ? getMlbBetBacktestBySide() : Promise.resolve(null),
    sport === "mlb" ? getMlbClv() : Promise.resolve(null),
    sport === "mlb" ? getMlbLineMovement(7) : Promise.resolve(null),
    sport === "mlb" ? getLineAlerts("mlb") : Promise.resolve(null),
    sport === "mlb" ? getLineAlertBacktest("mlb") : Promise.resolve(null),
    sport === "mlb" ? getMlbPipelineHealth() : Promise.resolve(null),
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
      mlbCoverageStatus={mlbCoverageStatus}
      mlbTotalBacktest={mlbTotalBacktest}
      mlbMoneylineBacktest={mlbMoneylineBacktest}
      mlbBets={mlbBets}
      mlbBetBacktest={mlbBetBacktest}
      mlbBetBySide={mlbBetBySide}
      mlbClv={mlbClv}
      mlbLineMovement={mlbLineMovement}
      mlbLineAlerts={mlbLineAlerts}
      mlbLineAlertBacktest={mlbLineAlertBacktest}
      mlbHealth={mlbHealth}
      vegasSummary={vegasSummary}
      biggestMisses={biggestMisses}
      teamInsights={teamInsights}
      moneylineBacktest={moneylineBacktest}
      queryDate={date ?? new Date().toISOString().slice(0, 10)}
      sport={sport}
    />
  );
}
