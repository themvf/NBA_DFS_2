import { easternDateString } from "@/lib/eastern-date";

/**
 * The NFL season a date belongs to. An NFL season is named for the year it
 * kicks off in but runs into the following February, and the league year
 * (free agency, the draft, the published schedule) starts in March. So the
 * boundary is March 1 ET: January and February still belong to the season
 * that started the previous September.
 *
 * Every page that used to default to a literal `2026` goes through here, so
 * the pick'em, survivor, specials and fantasy pages stop silently showing the
 * wrong season the day after a boundary.
 */
export const NFL_SEASON_ROLLOVER_MONTH = 3;

/** First season with data in this repository (pick'em selectable range). */
export const EARLIEST_NFL_SEASON = 2020;

export function currentNflSeason(at: Date = new Date()): number {
  const [year, month] = easternDateString(at).split("-").map(Number);
  return month >= NFL_SEASON_ROLLOVER_MONTH ? year : year - 1;
}

/** Seasons a season picker may offer, newest first, down to the earliest loaded. */
export function selectableNflSeasons(at: Date = new Date()): number[] {
  const seasons: number[] = [];
  for (let season = currentNflSeason(at); season >= EARLIEST_NFL_SEASON; season -= 1) seasons.push(season);
  return seasons;
}

/** Parse a `?season=` query value, falling back to the current season. */
export function resolveNflSeason(raw: string | undefined, at: Date = new Date()): number {
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 2000 ? parsed : currentNflSeason(at);
}
