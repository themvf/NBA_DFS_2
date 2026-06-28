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
import type { Sport } from "@/db/queries";
import VegasClient from "./vegas-client";
import SoccerVegasClient from "./soccer-vegas-client";

export default async function VegasContent({ date, sport = "nba" }: { date?: string; sport?: Sport }) {
  // Soccer: focused fixtures view + star-rated bet ledger + backtest, rather
  // than the NBA/MLB analytics panels.
  if (sport === "soccer") {
    const [matchups, bets, settledBets, backtest, firstScorers, matchGoals, playerStats,
           fscorerTiers, fscorerNearMisses, topPickAccuracy, clv, calibCuts, clvTrend,
           settlementHealth, knockoutTies, titleOdds] = await Promise.all([
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
        queryDate={date ?? null}
      />
    );
  }

  const [sportData, mlbCoverageStatus, mlbTotalBacktest, mlbMoneylineBacktest, mlbBets, mlbBetBacktest, mlbBetBySide, mlbClv, mlbHealth, vegasSummary, biggestMisses, teamInsights, moneylineBacktest] = await Promise.all([
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
