/**
 * Games the weekly Game Leaders publish left off the page, or kept on an older forecast.
 *
 * Mirrors `skip_kind` / `matchup` in research/nfl_game_leaders_auto.py: a game that
 * started before any forecast was saved is `expected`; every other reason is a
 * `data_error`, which also turns the publish run red (`--check`). Pages written
 * before the tag carry only `game_id` and `reason`, so both are read from those.
 */
export type SkippedGame = { game_id: string; reason: string; kind?: string; away?: string; home?: string; kickoff?: string };

export type SkippedNotice = {
  gameId: string;
  matchup: string;
  kickoff: string | null;
  kind: "expected" | "data_error";
  /** The game is still on the page, on a forecast saved by an earlier run. */
  olderForecast: boolean;
  reason: string;
};

const STARTED_UNFORECAST = "started before any forecast was saved";
const KEPT_PREVIOUS = "kept previous forecast: ";

export function skippedNotices(skipped: readonly SkippedGame[] | undefined): SkippedNotice[] {
  return (skipped ?? []).map((s) => {
    const parts = s.game_id.split("_");
    const matchup = s.away && s.home ? `${s.away} at ${s.home}` : parts.length === 4 ? `${parts[2]} at ${parts[3]}` : s.game_id;
    const olderForecast = s.reason.startsWith(KEPT_PREVIOUS);
    return {
      gameId: s.game_id,
      matchup,
      kickoff: s.kickoff ?? null,
      kind: s.kind === "expected" || (!s.kind && s.reason === STARTED_UNFORECAST) ? "expected" : "data_error",
      olderForecast,
      reason: olderForecast ? s.reason.slice(KEPT_PREVIOUS.length) : s.reason,
    };
  });
}
