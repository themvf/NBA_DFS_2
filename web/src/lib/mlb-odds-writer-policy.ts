/**
 * MLB game-line odds have one writer: ingest.refresh_mlb_vegas in Python.
 * Web surfaces may read and request revalidation, but may not write lines.
 */
export type MlbWebOddsSurface = "vegas_action" | "dfs_slate_fallback";

export function canWebSurfaceWriteMlbOdds(_surface: MlbWebOddsSurface): false {
  void _surface;
  return false;
}
