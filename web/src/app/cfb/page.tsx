import type { Metadata } from "next";
import { getCfbResearchBoard, getCfbSignalBacktest, getCfbStudyStatus, getCfbTerminalBoard, getLineAlerts, getMovementSignalObservations, getMarketCaptureHealth, getMarketSignalScorecard, type CfbResearchBoard, type CfbSignalBacktestRow, type CfbStudyStatus, type CfbTerminalBoard, type LineAlertRow, type MarketCaptureHealth, type MarketSignalScorecardRow } from "@/db/queries";
import CfbTerminalClient from "./cfb-terminal-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "CFB Line Terminal",
  description: "College football line movement, market catalysts, news, and paper-trade tracking.",
};

export default async function CfbPage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string }>;
}) {
  const { date } = await searchParams;
  let board: CfbTerminalBoard;
  try {
    board = await getCfbTerminalBoard(date);
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Unknown CFB data error";
    board = {
      gameDate: date ?? new Date().toISOString().slice(0, 10),
      asOf: new Date().toISOString(),
      status: "unavailable",
      statusDetail: `CFB live data is unavailable: ${detail}`,
      games: [],
      unmappedEvents: 0,
    };
  }
  let signals: LineAlertRow[] = [];
  let observations: LineAlertRow[] = [];
  let backtest: CfbSignalBacktestRow[] = [];
  let research: CfbResearchBoard = {};
  let scorecard: MarketSignalScorecardRow[] = [];
  let captureHealth: MarketCaptureHealth | null = null;
  let studyStatus: CfbStudyStatus | null = null;
  const [signalsResult, backtestResult, researchResult, scorecardResult, healthResult, observationsResult] =
    await Promise.allSettled([
      getLineAlerts("cfb", 250, undefined, board.games.map((game) => game.matchupId)),
      getCfbSignalBacktest(),
      getCfbResearchBoard(board.gameDate),
      getMarketSignalScorecard("cfb"),
      getMarketCaptureHealth("cfb", board.gameDate),
      getMovementSignalObservations("cfb", board.games.map(game => game.matchupId)),
    ]);
  if (signalsResult.status === "fulfilled") signals = signalsResult.value;
  if (backtestResult.status === "fulfilled") backtest = backtestResult.value;
  if (researchResult.status === "fulfilled") research = researchResult.value;
  if (scorecardResult.status === "fulfilled") scorecard = scorecardResult.value;
  if (healthResult.status === "fulfilled") captureHealth = healthResult.value;
  if (observationsResult.status === "fulfilled") observations = observationsResult.value;
  try {
    studyStatus = await getCfbStudyStatus();
  } catch {
    // A missing study must never imply permission to act on research signals.
  }
  return <CfbTerminalClient board={board} observations={observations} signals={signals} backtest={backtest} research={research} scorecard={scorecard} captureHealth={captureHealth} studyStatus={studyStatus} />;
}
