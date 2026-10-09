import { selectedSportsbooks } from "./sportsbook-policy";
import type { CfbBookQuote, CfbTerminalRow, LineAlertRow } from "@/db/queries";

const RETAIL = ["draftkings", "fanduel", "fanatics", "williamhill_us", "betmgm"];
function fairAt(book: CfbBookQuote | undefined, capturedAt: string): number | null {
  if (!book || book.ml_home == null || book.ml_away == null || !book.last_update) return null;
  const age = (Date.parse(capturedAt) - Date.parse(String(book.last_update))) / 60_000;
  const home = Number(book.ml_home), away = Number(book.ml_away);
  if (!Number.isFinite(age) || age < 0 || age > 35 || !Number.isInteger(home) || !Number.isInteger(away) || Math.abs(home) < 100 || Math.abs(away) < 100) return null;
  const implied = (value: number) => value > 0 ? 100 / (100 + value) : -value / (100 - value);
  const h = implied(home), a = implied(away);
  return h / (h + a);
}

export function movementKind(type: string): "steam" | "walk" | "reversal" | "gap" | "cumulative" | null {
  if (["steam", "spread_steam", "total_steam"].includes(type)) return "steam";
  if (["walking", "spread_walking", "total_walking"].includes(type)) return "walk";
  if (["gap_repricing", "timing_unverified"].includes(type)) return "gap";
  if (type === "cumulative_move") return "cumulative";
  return type === "reversal" ? "reversal" : null;
}

/** Keep immutable v1 alert records, but do not display an unobserved path as speed evidence. */
export function classifyCfbSignal(signal: LineAlertRow, game: Pick<CfbTerminalRow, "history">): LineAlertRow {
  if ((signal.alertType !== "steam" && signal.alertType !== "walking") ||
      signal.details?.signal_version !== "cfb-lines-v1") return signal;
  const triggerId = Number(signal.details?.trigger_history_id);
  const history = game.history;
  const triggerIndex = history.findIndex(point => point.historyId === triggerId);
  if (triggerIndex < 0) {
    return { ...signal, alertType: "timing_unverified", details: { ...signal.details, legacy_alert_type: signal.alertType } };
  }
  if (signal.alertType === "steam") {
    const previous = history[triggerIndex - 1];
    const gap = previous ? (Date.parse(history[triggerIndex].capturedAt) - Date.parse(previous.capturedAt)) / 60_000 : NaN;
    if (!Number.isFinite(gap) || gap <= 0) {
      return { ...signal, alertType: "timing_unverified", details: { ...signal.details, legacy_alert_type: "steam" } };
    }
    if (gap > 40) {
      return { ...signal, alertType: "gap_repricing", details: { ...signal.details, legacy_alert_type: "steam", capture_gap_minutes: Math.round(gap) } };
    }
    const beforeBooks = selectedSportsbooks(previous.books), afterBooks = selectedSportsbooks(history[triggerIndex].books);
    const direction = signal.side === "home" ? 1 : signal.side === "away" ? -1 : 0;
    const support = RETAIL.filter(key => {
      const before = fairAt(beforeBooks[key], previous.capturedAt);
      const after = fairAt(afterBooks[key], history[triggerIndex].capturedAt);
      return before != null && after != null && (after - before) * direction * 100 >= 1.5 - 1e-9;
    });
    if (support.length < 3) {
      return { ...signal, alertType: "timing_unverified", details: { ...signal.details, legacy_alert_type: "steam", capture_gap_minutes: Math.round(gap), retail_support_books: support } };
    }
    return signal;
  }
  const path = history.slice(0, triggerIndex + 1);
  return { ...signal, alertType: "cumulative_move", details: { ...signal.details, legacy_alert_type: "walking", observations: path.length } };
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
    const values = Object.values(selectedSportsbooks(capture.books)).flatMap((book) => {
      const value = book[key];
      return value != null && Number.isFinite(Number(value)) ? [Number(value)] : [];
    }).sort((a, b) => a - b);
    return values.length ? [{ time, value: values[Math.floor((values.length - 1) / 2)] }] : [];
  }).sort((a, b) => a.time - b.time);
}
