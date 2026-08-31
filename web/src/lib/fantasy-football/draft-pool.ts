/**
 * Player pool for the mock draft rooms.
 *
 * Both rooms used to take the first N of our board and stop. That silently
 * removed players the market drafts early but our model ranks low -- Bhayshul
 * Tuten goes at ADP 52, round 6 of a 10-team draft, and sat at our #285, so he
 * could not be selected in a mock of the draft he is certain to appear in.
 *
 * The cut is our rank, but the draft being simulated runs on market order, so
 * the pool has to be the union of the two: our top N, plus anyone the market
 * plausibly drafts in this format. That mirrors the force-include guarantee
 * `_must_include_ids` already applies in ingest/ff_independent.py when it
 * builds the board itself.
 *
 * Two details the first version of this got wrong, both found by a user
 * searching for a player who was still missing:
 *
 * 1. ONE MARKET IS NOT THE MARKET. Gating on the FFC ADP alone missed players
 *    Yahoo prices far earlier -- Aaron Rodgers 180 FFC against 117 Yahoo, Cam
 *    Ward 169 against 125 -- and missed Jayden Higgins entirely, who has no
 *    FFC price at all. The gate now takes the EARLIEST price any market we
 *    hold puts on the player, so a room labelled "Yahoo" is not filtered by
 *    somebody else's draft board.
 *
 * 2. A HARD CUTOFF AT THE LAST PICK IS A CLIFF. MarShawn Lloyd sat at FFC ADP
 *    150.4 against a 150-pick threshold and was excluded by four tenths of a
 *    pick, despite Yahoo pricing him at 130.8. ADP is a central tendency: a
 *    player priced at the final pick goes earlier about half the time. The
 *    gate therefore carries `headroom` -- roughly one standard deviation of
 *    ADP spread, which runs about 18% of ADP on this board.
 *
 * Being generous here is close to free. Extra names arrive deep in a list that
 * is sorted by our rank and filterable, and a mock draft only ever makes its
 * own number of picks. Being stingy is not free: it removes precisely the
 * players our model is most likely to be wrong about.
 *
 * Order is unchanged -- players are returned in board order, so a market
 * darling our model dislikes still appears where our rank puts him, deep in
 * the list, rather than being promoted. He is merely reachable.
 */

export type DraftPoolPlayer = {
  playerId: number;
  position: string;
  /** Fantasy Football Calculator 12-team ADP. */
  adp: number | null;
  /** Yahoo's own pre-draft ADP, when we hold a capture for the player. */
  yahooAdp?: number | null;
};

/** Multiplier on the format's pick count, covering ADP spread. See note 2. */
export const DRAFT_POOL_HEADROOM = 1.25;

/** The earliest pick any market we hold expects this player to go. */
export function earliestMarketPick(player: DraftPoolPlayer): number | null {
  const prices = [player.adp, player.yahooAdp].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value),
  );
  return prices.length ? Math.min(...prices) : null;
}

export function buildDraftPool<T extends DraftPoolPlayer>(
  rankings: T[],
  positions: readonly string[],
  boardSize: number,
  draftablePicks: number,
  headroom: number = DRAFT_POOL_HEADROOM,
): T[] {
  const eligible = rankings.filter((player) => positions.includes(player.position));
  const reachable = draftablePicks * headroom;
  const keep = new Set(eligible.slice(0, boardSize).map((player) => player.playerId));
  for (const player of eligible) {
    const price = earliestMarketPick(player);
    if (price != null && price <= reachable) keep.add(player.playerId);
  }
  return eligible.filter((player) => keep.has(player.playerId));
}
