import { getMlbTerminalBoard } from "@/db/queries-mlb-terminal";
import { normalizeMlbDate, type MlbTerminalBoard } from "@/lib/mlb-terminal";
import MlbTerminalClient from "./mlb-terminal-client";

export default async function MlbVegasContent({ date }: { date?: string }) {
  let board: MlbTerminalBoard;
  try { board = await getMlbTerminalBoard(date); }
  catch (error) {
    console.error("MLB terminal query failed", error instanceof Error ? error.name : "Unknown error");
    const queryDate = normalizeMlbDate(date);
    board = { date: queryDate, asOf: new Date().toISOString(), auditFrom: queryDate, games: [], signals: [], issues: ["MLB data is temporarily unavailable. Refresh to retry."] };
  }
  return <MlbTerminalClient board={board} />;
}
