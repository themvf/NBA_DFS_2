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
  searchParams: Promise<{ date?: string; game?: string; view?: string }>;
}) {
  const { date, game, view } = await searchParams;
  const initialView = view === "favorites" ? "favorites" : "terminal";
  const requestedGameId = Number(game);
  const initialGameId = Number.isSafeInteger(requestedGameId) && requestedGameId > 0 ? requestedGameId : undefined;
  let board: CfbTerminalBoard;
  try {
    board = await getCfbTerminalBoard(date);
  } catch (error) {
    console.error("CFB market board unavailable", error);
    board = {
      gameDate: date ?? new Date().toISOString().slice(0, 10),
      asOf: new Date().toISOString(),
      status: "unavailable",
      statusDetail: "CFB live data is unavailable. The market board could not be loaded.",
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
  const dataFailures: string[] = [];
  const [signalsResult, backtestResult, researchResult, scorecardResult, healthResult, observationsResult] =
    await Promise.allSettled([
      getLineAlerts("cfb", 250, undefined, board.games.map((game) => game.matchupId)),
      getCfbSignalBacktest(),
      getCfbResearchBoard(board.gameDate),
      getMarketSignalScorecard("cfb"),
      getMarketCaptureHealth("cfb", board.gameDate),
      getMovementSignalObservations("cfb", board.games.map(game => game.matchupId)),
    ]);
  function failed(label: string, reason: unknown) {
    console.error(`CFB ${label} unavailable`, reason);
    dataFailures.push(label);
  }
  if (signalsResult.status === "fulfilled") signals = signalsResult.value;
  else failed("signals", signalsResult.reason);
  if (backtestResult.status === "fulfilled") backtest = backtestResult.value;
  else failed("prospective signal audit", backtestResult.reason);
  if (researchResult.status === "fulfilled") research = researchResult.value;
  else failed("research context", researchResult.reason);
  if (scorecardResult.status === "fulfilled") scorecard = scorecardResult.value;
  else failed("signal scorecard", scorecardResult.reason);
  if (healthResult.status === "fulfilled") captureHealth = healthResult.value;
  else failed("capture health", healthResult.reason);
  if (observationsResult.status === "fulfilled") observations = observationsResult.value;
  else failed("movement observations", observationsResult.reason);
  try {
    studyStatus = await getCfbStudyStatus();
  } catch (error) {
    failed("study status", error);
  }
  return <CfbTerminalClient board={board} initialGameId={initialGameId} initialView={initialView} observations={observations} signals={signals} backtest={backtest} research={research} scorecard={scorecard} captureHealth={captureHealth} studyStatus={studyStatus} dataFailures={dataFailures} />;
}
