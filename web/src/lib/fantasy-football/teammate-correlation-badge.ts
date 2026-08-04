import type { FantasyRankingRow, TeammateCorrelationRow } from "@/db/queries-fantasy-football";

export type RosterCorrelationBadge = {
  code: "TEAMMATE_STACK" | "TEAMMATE_OFFSET";
  class: "fact";
  label: string;
  value: number;
  evidence: { withPlayerId: number; withName: string; relationshipType: string; sampleWeeks: number };
};

// Below this shrunk-correlation magnitude, ingest/ff_teammate_correlation.py's
// shrinkage-to-prior has already pulled the pair too close to the league-wide
// relationship-type average (e.g. RB_RB ~0.03) to be worth surfacing as a signal.
export const TEAMMATE_CORRELATION_BADGE_THRESHOLD = 0.15;

// For each undrafted player, finds their single strongest real correlation
// against the user's own current roster (by |shrunkCorrelation|) and renders it
// as a small chip: teal "stacks with" (positive -- a Weeks 15-17 ceiling lever)
// or magenta "trades off with" (negative -- a Weeks 1-14 floor/diversification
// lever). Correlation only changes a lineup's variance, never its expected
// points, so this is deliberately a hint, not a rank change.
export function buildRosterCorrelationBadges(
  rankings: FantasyRankingRow[],
  myRosterIds: number[],
  correlations: TeammateCorrelationRow[],
  nameById: Map<number, string>,
): Map<number, RosterCorrelationBadge> {
  const badges = new Map<number, RosterCorrelationBadge>();
  if (myRosterIds.length === 0 || correlations.length === 0) return badges;

  const byPair = new Map<string, TeammateCorrelationRow>();
  for (const row of correlations) {
    byPair.set(`${Math.min(row.playerAId, row.playerBId)}:${Math.max(row.playerAId, row.playerBId)}`, row);
  }

  const rosterIdSet = new Set(myRosterIds);
  for (const player of rankings) {
    if (rosterIdSet.has(player.playerId)) continue;
    // Track which roster id produced the best match directly, rather than
    // reading identity back off best.playerAId/playerBId -- those come from a
    // raw sql`` query on bigint columns, which the driver can return as
    // strings, so a strict === against player.playerId (a real number) can
    // silently pick the wrong side of the pair. Iterating our own known-good
    // numeric roster ids (same pattern as ai-draft-advisor.ts's correlationsFor)
    // sidesteps that entirely.
    let best: TeammateCorrelationRow | null = null;
    let bestRosterId: number | null = null;
    for (const rosterId of myRosterIds) {
      const row = byPair.get(`${Math.min(rosterId, player.playerId)}:${Math.max(rosterId, player.playerId)}`);
      if (row && (!best || Math.abs(row.shrunkCorrelation) > Math.abs(best.shrunkCorrelation))) {
        best = row;
        bestRosterId = rosterId;
      }
    }
    if (!best || bestRosterId === null || Math.abs(best.shrunkCorrelation) < TEAMMATE_CORRELATION_BADGE_THRESHOLD) continue;

    const partnerName = nameById.get(bestRosterId) ?? `Player ${bestRosterId}`;
    const positive = best.shrunkCorrelation >= 0;
    badges.set(player.playerId, {
      code: positive ? "TEAMMATE_STACK" : "TEAMMATE_OFFSET",
      class: "fact",
      label: `${positive ? "+" : ""}${best.shrunkCorrelation.toFixed(2)} w/ ${partnerName.split(" ").at(-1)}`,
      value: best.shrunkCorrelation,
      evidence: { withPlayerId: bestRosterId, withName: partnerName, relationshipType: best.relationshipType, sampleWeeks: best.sampleWeeks },
    });
  }
  return badges;
}
