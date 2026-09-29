import type { Metadata } from "next";
import { getMarketCaptureHealth, getNhlTerminalBoard, type MarketCaptureHealth, type NhlTerminalBoard } from "@/db/queries";
import NhlTerminalClient from "./nhl-terminal-client";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "NHL Line Terminal",
  description: "NHL moneyline, puck line, and total movement across sportsbooks, with closes and paper tracking.",
};

export default async function NhlPage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string }>;
}) {
  const { date } = await searchParams;
  let board: NhlTerminalBoard;
  try {
    board = await getNhlTerminalBoard(date);
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Unknown NHL data error";
    board = {
      gameDate: date ?? new Date().toISOString().slice(0, 10),
      asOf: new Date().toISOString(),
      status: "unavailable",
      statusDetail: `NHL live data is unavailable: ${detail}`,
      games: [],
      unmappedEvents: 0,
    };
  }
  let captureHealth: MarketCaptureHealth | null = null;
  try {
    captureHealth = await getMarketCaptureHealth("nhl", board.gameDate);
  } catch {
    // The market board stays useful while the checkpoint ledger is unavailable.
  }
  return <NhlTerminalClient board={board} captureHealth={captureHealth} />;
}
