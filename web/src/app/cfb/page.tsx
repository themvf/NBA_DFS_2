import type { Metadata } from "next";
import { getCfbResearchBoard, getCfbSignalBacktest, getCfbTerminalBoard, getLineAlerts, getMarketCaptureHealth, getMarketSignalScorecard, type CfbResearchBoard, type CfbSignalBacktestRow, type CfbTerminalBoard, type LineAlertRow, type MarketCaptureHealth, type MarketSignalScorecardRow } from "@/db/queries";
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
  let backtest: CfbSignalBacktestRow[] = [];
  let research: CfbResearchBoard = {};
  let scorecard: MarketSignalScorecardRow[] = [];
  let captureHealth: MarketCaptureHealth | null = null;
  try {
    [signals, backtest, research, scorecard, captureHealth] = await Promise.all([
      getLineAlerts("cfb", 250, undefined, board.games.map((game) => game.matchupId)),
      getCfbSignalBacktest(),
      getCfbResearchBoard(board.gameDate),
      getMarketSignalScorecard("cfb"),
      getMarketCaptureHealth("cfb", board.gameDate),
    ]);
  } catch {
    // The market board remains useful during a first-deploy schema bootstrap.
  }
  return <CfbTerminalClient board={board} signals={signals} backtest={backtest} research={research} scorecard={scorecard} captureHealth={captureHealth} />;
}
