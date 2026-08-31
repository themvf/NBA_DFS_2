/**
 * Player pool for the mock draft rooms.
 *
 * Both rooms used to take the first N of our board and stop. That silently
 * removed players the market drafts early but our model ranks low -- Bhayshul
 * Tuten goes at ADP 52, round 6 of a 10-team draft, and sat at our #285, so he
 * could not be selected in a mock of the draft he is certain to appear in.
 * Eleven such players were missing from redraft and fourteen from Best Ball.
 *
 * The cut is our rank, but the draft being simulated runs on market order, so
 * the pool has to be the union of the two: our top N, plus anyone whose ADP
 * lands inside the picks this format actually makes. That mirrors the
 * force-include guarantee `_must_include_ids` already applies in
 * ingest/ff_independent.py when it builds the board itself.
 *
 * Order is unchanged -- players are returned in board order, so a market
 * darling our model dislikes still appears where our rank puts him, deep in
 * the list, rather than being promoted. He is merely reachable.
 */

export type DraftPoolPlayer = {
  playerId: number;
  position: string;
  adp: number | null;
};

export function buildDraftPool<T extends DraftPoolPlayer>(
  rankings: T[],
  positions: readonly string[],
  boardSize: number,
  draftablePicks: number,
): T[] {
  const eligible = rankings.filter((player) => positions.includes(player.position));
  const keep = new Set(eligible.slice(0, boardSize).map((player) => player.playerId));
  for (const player of eligible) {
    if (player.adp != null && player.adp <= draftablePicks) keep.add(player.playerId);
  }
  return eligible.filter((player) => keep.has(player.playerId));
}
