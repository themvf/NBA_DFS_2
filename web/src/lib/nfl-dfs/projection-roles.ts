import {
  applyTeamQbContext, identifyTeamQb1s, nflTeamKey, noTeamReason, offDepthChartReason, resolveGameAvailability,
  type Availability, type RosterEvidence,
} from "./availability";
import { ruledOutPlayer } from "./confirmed-starter";

export type RoleInput = { key: number; ffPlayerId: number | null; name: string; team: string; position: string; kickoff: string };
export type ProjectionRole = {
  /** "WR2", from the depth chart; null when the chart gives no rank. */
  depth: string | null;
  /** Not playing: an injury or roster status newer than the frozen projection can be. */
  unavailableReason: string | null;
  /** Healthy, but the depth chart says he does not get a starter's work, or any work. */
  backupReason: string | null;
};

/**
 * The DraftKings optimizer's roster rules applied to the scheduled projection run:
 * listed QB2+, a QB with no rank behind a listed QB1, a kicker who is not K1, the
 * practice squad, and (`offDepthChartReason`, `noTeamReason`) a player the depth chart omits.
 * Ruled-out wins over backup, so each player lands in exactly one group.
 */
export function resolveProjectionRoles(players: readonly RoleInput[], roster: ReadonlyMap<number, RosterEvidence>,
  now: number, week: number): Map<number, ProjectionRole> {
  const resolved = players.map(player => {
    const evidence = roster.get(player.ffPlayerId ?? -1);
    return { ...player, evidence, availability: resolveGameAvailability(evidence, player.team, player.position, now, week, player.kickoff) };
  });
  const teamQb1s = identifyTeamQb1s(resolved);
  return new Map(resolved.map(player => {
    const availability: Availability = applyTeamQbContext(player.availability, player.position, teamQb1s.get(nflTeamKey(player.team)));
    const out = ruledOutPlayer({ availability });
    return [player.key, {
      depth: availability.depthOrder ? `${player.position}${availability.depthOrder}` : null,
      unavailableReason: out ? availability.blockedReason ?? `Unavailable: ${availability.status}` : null,
      backupReason: out ? null : availability.blockedReason ?? offDepthChartReason(availability, player.position)
        ?? noTeamReason(player.evidence, player.position, now),
    }];
  }));
}
