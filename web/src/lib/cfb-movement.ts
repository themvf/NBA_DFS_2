import type { CfbTerminalRow, LineAlertRow } from "@/db/queries";

export function movementKind(type: string): "steam" | "walk" | "reversal" | null {
  if (["steam", "spread_steam", "total_steam"].includes(type)) return "steam";
  if (["walking", "spread_walking", "total_walking"].includes(type)) return "walk";
  return type === "reversal" ? "reversal" : null;
}

export function movementSignals(signals: LineAlertRow[], matchupId: number) {
  return signals.filter((signal) => signal.matchupId === matchupId && movementKind(signal.alertType))
    .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));
}

export function movementSeries(game: Pick<CfbTerminalRow, "history" | "commenceTime">, market: "spread" | "total") {
  const key = market === "spread" ? "spread_home" : "total_line";
  const kickoff = game.commenceTime ? Date.parse(game.commenceTime) : Infinity;
  return game.history.flatMap((capture) => {
    const time = Date.parse(capture.capturedAt);
    if (!Number.isFinite(time) || time >= kickoff) return [];
    const values = Object.values(capture.books).flatMap((book) => {
      const value = book[key];
      return value != null && Number.isFinite(Number(value)) ? [Number(value)] : [];
    }).sort((a, b) => a - b);
    return values.length ? [{ time, value: values[Math.floor((values.length - 1) / 2)] }] : [];
  }).sort((a, b) => a.time - b.time);
}
