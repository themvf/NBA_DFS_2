import type { Metadata } from "next";
import { getCfbTerminalBoard, type CfbTerminalBoard } from "@/db/queries";
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
  return <CfbTerminalClient board={board} />;
}
