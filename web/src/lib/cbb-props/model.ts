export const CBB_PROP_MARKETS = {
  player_points: "Points", player_rebounds: "Rebounds", player_assists: "Assists",
} as const;
export type CbbPropMarket = keyof typeof CBB_PROP_MARKETS;
export interface CbbPropEvent { id: string; home: string; away: string; start: string }
export interface CbbPropLine {
  player: string; market: CbbPropMarket; line: number; book: string; bookName: string;
  over: number | null; under: number | null; updatedAt: string | null;
}
export interface CbbPropBoard {
  event: CbbPropEvent; lines: CbbPropLine[]; capturedAt: string; skipped: number;
  source: "The Odds API"; region: "us"; raw: unknown;
}
const record = (v: unknown): Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {};
const list = (v: unknown): unknown[] => Array.isArray(v) ? v : [];
const text = (v: unknown): string => typeof v === "string" ? v.trim() : "";
const timestamp = (v: unknown): string | null => typeof v === "string" && /^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(v) && Number.isFinite(Date.parse(v)) ? new Date(v).toISOString() : null;
export const validCbbEventId = (id: string) => /^[a-f0-9]{32}$/i.test(id);

export function parseCbbPropEvent(value: unknown): CbbPropEvent {
  const e = record(value);
  const id = text(e.id), home = text(e.home_team), away = text(e.away_team), start = timestamp(e.commence_time);
  if (!validCbbEventId(id) || !home || !away || home === away || !start || (e.sport_key !== undefined && e.sport_key !== "basketball_ncaab")) throw new Error("The college basketball event feed returned an invalid game.");
  return { id, home, away, start };
}
export function parseCbbPropEvents(value: unknown): CbbPropEvent[] {
  if (!Array.isArray(value)) throw new Error("The college basketball schedule is unavailable.");
  const events = value.map(parseCbbPropEvent);
  if (new Set(events.map(e => e.id)).size !== events.length) throw new Error("The schedule contains conflicting game IDs.");
  return events.sort((a, b) => a.start.localeCompare(b.start) || a.away.localeCompare(b.away));
}

/** Pair sides only within the SAME event, book, player, stat, and threshold. No DK name/team joins. */
export function normalizeCbbProps(value: unknown, expected: CbbPropEvent, capturedAt: string): CbbPropBoard {
  const raw = record(value), event = parseCbbPropEvent(value);
  if (event.id !== expected.id || event.home !== expected.home || event.away !== expected.away) throw new Error("The returned props do not match the selected game. Reload the schedule.");
  if (!Array.isArray(raw.bookmakers)) throw new Error("The player-prop feed returned an invalid response.");
  const paired = new Map<string, CbbPropLine>();
  const conflicts = new Set<string>();
  let skipped = 0;
  for (const b of raw.bookmakers) {
    const book = record(b), bookKey = text(book.key), bookName = text(book.title);
    if (!bookKey || !bookName) { skipped++; continue; }
    if (!Array.isArray(book.markets)) { skipped++; continue; }
    for (const m of list(book.markets)) {
      const market = record(m), marketKey = text(market.key);
      if (!Object.hasOwn(CBB_PROP_MARKETS, marketKey)) continue;
      if (!Array.isArray(market.outcomes)) { skipped++; continue; }
      const updatedAt = timestamp(market.last_update) ?? timestamp(book.last_update);
      for (const o of list(market.outcomes)) {
        const outcome = record(o), player = text(outcome.description), side = text(outcome.name).toLowerCase();
        const point = outcome.point, price = outcome.price;
        if (!player || (side !== "over" && side !== "under") || typeof point !== "number" || !Number.isFinite(point) || point < 0 || typeof price !== "number" || !Number.isInteger(price) || Math.abs(price) < 100) { skipped++; continue; }
        const key = JSON.stringify([bookKey, player, marketKey, point]);
        const row = paired.get(key) ?? { player, market: marketKey as CbbPropMarket, line: point, book: bookKey, bookName, over: null, under: null, updatedAt };
        if (row[side] !== null && row[side] !== price) { conflicts.add(key); skipped++; }
        if (row.updatedAt !== updatedAt) { conflicts.add(key); skipped++; }
        row[side] = price; paired.set(key, row);
      }
    }
  }
  const lines = [...paired].filter(([key]) => !conflicts.has(key)).map(([, row]) => row).sort((a, b) => a.player.localeCompare(b.player) || a.market.localeCompare(b.market) || (a.book === "draftkings" ? -1 : b.book === "draftkings" ? 1 : a.bookName.localeCompare(b.bookName)) || a.line - b.line);
  return { event, lines, capturedAt, skipped, source: "The Odds API", region: "us", raw: value };
}

/** Freshness uses the bookmaker timestamp, never the time a cached page was opened. */
export function cbbPropFreshness(updatedAt: string | null, now: number): "recent" | "stale" | "unknown" {
  if (!updatedAt || !Number.isFinite(Date.parse(updatedAt)) || Date.parse(updatedAt) > now + 60_000) return "unknown";
  return now - Date.parse(updatedAt) > 15 * 60_000 ? "stale" : "recent";
}
