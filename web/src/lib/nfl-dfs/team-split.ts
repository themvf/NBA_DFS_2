/**
 * Team split targets for Showdown (`nfl-team-split-targets-v1`, owner's request
 * 2026-11-12): the share of lineups that are 3-3, 4-2 or 5-1 by team.
 *
 * A split that is given gets that share of the run; the splits left out share
 * the rest. With a heavy team, a 4-2 or 5-1 target counts only lineups where
 * that team holds the larger side, and the mirror image stays open to the
 * other lineups. The optimizer schedules the splits lineup by lineup (the one
 * furthest behind its share goes next) and relaxes a split last, after the
 * build shape's own quotas, because the split is the owner's explicit target.
 *
 * Not an edge: over 29 prime-time 2026 Showdowns graded by prize money no
 * split paid clearly more than another (3-3 0.97, 4-2 0.90, 5-1 1.30 with a
 * range of 0.90 to 1.72, against 1.0 for an average entry). The setting lets
 * the owner express a read of one game.
 *
 * Pure, and free of the solver, so the page can import it.
 */
export type TeamSplit = "3-3" | "4-2" | "5-1";
export const TEAM_SPLITS: TeamSplit[] = ["3-3", "4-2", "5-1"];
export const NFL_TEAM_SPLIT_TARGETS_VERSION = "nfl-team-split-targets-v1";

export interface TeamSplitPlan {
  /** The slate's first team alphabetically; counts below are this team's players in a lineup. */
  firstTeam: string;
  heavyTeam: string | null;
  classes: Array<{ split: TeamSplit; share: number; counts: number[] }>;
  /** Counts open to the lineups no targeted split claims. */
  restCounts: number[];
}

/**
 * Which roster counts of the slate's first team each targeted split allows.
 * Null when no split is targeted or the slate does not have exactly two teams.
 */
export function resolveTeamSplitPlan(targets: Partial<Record<TeamSplit, number>> | undefined, heavyTeam: string | undefined, teams: readonly string[]): TeamSplitPlan | null {
  const given = TEAM_SPLITS.filter((split) => targets?.[split] != null && Number.isFinite(targets[split]));
  if (!given.length) return null;
  const sorted = [...new Set(teams)].sort();
  if (sorted.length !== 2) return null;
  const [first, second] = sorted;
  const heavy = heavyTeam ? heavyTeam : null;
  if (heavy && heavy !== first && heavy !== second) throw new Error(`The heavy side ${heavy} is not a team on this slate.`);
  const larger: Record<TeamSplit, number> = { "3-3": 3, "4-2": 4, "5-1": 5 };
  const countsFor = (split: TeamSplit) => {
    const big = larger[split];
    if (big === 3) return [3];
    return heavy === first ? [big] : heavy === second ? [6 - big] : [6 - big, big];
  };
  const classes = given.map((split) => ({ split, share: targets![split]!, counts: countsFor(split) }));
  const taken = new Set(classes.flatMap((c) => c.counts));
  return { firstTeam: first, heavyTeam: heavy, classes, restCounts: [1, 2, 3, 4, 5].filter((count) => !taken.has(count)) };
}

/** The targeted split a lineup counts toward, or null when it is one of the other lineups. */
export function teamSplitOf(plan: TeamSplitPlan, lineup: { slots: ReadonlyArray<{ player: { team: string } }> }): TeamSplit | null {
  const count = lineup.slots.filter(({ player }) => player.team === plan.firstTeam).length;
  return plan.classes.find((c) => c.counts.includes(count))?.split ?? null;
}
