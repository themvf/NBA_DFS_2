// Read-only integration check: no schema initialization, capture or settlement.
import { getMlbTerminalBoard } from "../src/db/queries-mlb-terminal";
async function main() {
  for (const date of process.argv.slice(2)) {
    const board = await getMlbTerminalBoard(date);
    console.log(JSON.stringify({ date: board.date, games: board.games.length,
      captures: board.games.reduce((n, g) => n + g.history.length, 0),
      closes: board.games.filter((g) => g.close).length, signals: board.signals.length,
      issues: board.issues }));
    if (board.issues.some((issue) => issue.includes("unavailable"))) process.exitCode = 1;
  }
}
main().catch((error) => { console.error(error instanceof Error ? error.message : "Query failed"); process.exitCode = 1; });
