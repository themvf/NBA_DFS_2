/**
 * A saved run's `input_snapshot` back into optimizer players. Shared by the
 * pool-rule study runner and the ghost-lineup builder, so both rebuild from a
 * frozen snapshot exactly the same way (the study tests prove a rebuilt arm
 * matches the original build lineup for lineup).
 */
import type { NflOptimizerPlayer } from "@/app/dfs/nfl/nfl-optimizer";

// Frozen database JSON uses aliases, and preserves fields beyond the optimizer type.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FrozenPlayer = Record<string, any>;

export function restorePlayers(snapshot: FrozenPlayer[]): NflOptimizerPlayer[] {
  return snapshot.map(p => {
    const own = typeof p.ownership === "object" && p.ownership !== null ? p.ownership : null;
    for (const key of ["dkPlayerId", "salary", "ourProj"]) {
      if (!(key in p)) throw new Error(`Incomplete frozen player ${p.name}: missing ${key}`);
    }
    if (!p.gameKey || !p.position || !p.projectionStatus || typeof p.isOut !== "boolean") {
      throw new Error(`Incomplete identity/eligibility for ${p.name}`);
    }
    return { ...p, id: p.id ?? p.dkPlayerId, captainDkPlayerId: p.captainDkPlayerId ?? null,
      captainSalary: p.captainSalary ?? null, dkStatus: p.status ?? null,
      floorFpts: p.floor ?? null, ceilingFpts: p.ceiling ?? null,
      avgFptsDk: p.dkAvg ?? null, fantasyprosProj: p.fantasypros ?? null,
      linestarProj: p.linestar ?? null, customProj: p.custom ?? null,
      linestarOwnPct: own?.linestar ?? (typeof p.ownership === "number" ? p.ownership : null),
      ownPct: own?.total ?? (typeof p.ownership === "number" ? p.ownership : null),
      ownSource: own?.source ?? null, captainOwnPct: own?.captain ?? null, flexOwnPct: own?.flex ?? null,
    } as NflOptimizerPlayer;
  });
}
