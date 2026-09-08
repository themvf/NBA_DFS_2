import type { NflGeneratedLineup, NflOptimizerPlayer } from '@/app/dfs/nfl/nfl-optimizer';

export function savedSlateLabel(format: string, gameInfo: string | null, games: string[]) {
  const match = gameInfo?.match(/(\d{2})\/(\d{2})\/(\d{4})/);
  const date = match ? new Date(`${match[3]}-${match[1]}-${match[2]}T12:00:00Z`) : null;
  const day = date ? new Intl.DateTimeFormat('en-US', { weekday: 'long', month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }).format(date) : 'Date unavailable';
  return `${day} · ${format === 'showdown' ? `Showdown · ${games.join(', ')}` : `Classic · ${games.length} games`}`;
}

type FrozenPlayer = Partial<NflOptimizerPlayer> & { dkPlayerId: number; floor?: number | null; ceiling?: number | null; dkAvg?: number | null; fantasypros?: number | null; linestar?: number | null; ownership?: number | null; custom?: number | null };
type SavedSlot = { slot: string; dkPlayerId: number; captainDkPlayerId?: number | null; name: string; team: string; salary: number; projection: number; source: NflGeneratedLineup['slots'][number]['projectionSource']; multiplier?: number };
export type SavedLineupRow = { lineupNumber: number; slots: unknown; playerIds: unknown; totalSalary: number; projectedFpts: number; floorFpts: number | null; ceilingFpts: number | null; projectedOwnership: number | null; stackSummary: unknown };

/** Reconstruct from the run's frozen snapshot, never today's player projections. */
export function restoreSavedLineups(snapshot: unknown, rows: SavedLineupRow[]): NflGeneratedLineup[] {
  if (!Array.isArray(snapshot)) throw new Error('Saved player snapshot is unavailable.');
  const players = new Map((snapshot as FrozenPlayer[]).map(p => [p.dkPlayerId, p]));
  return rows.map(row => {
    if (!Array.isArray(row.slots) || row.floorFpts == null || row.ceilingFpts == null) throw new Error('This legacy run lacks a complete lineup snapshot. Download its audit instead.');
    const slots = (row.slots as SavedSlot[]).map(entry => {
      const p = players.get(entry.dkPlayerId);
      if (!p || !p.position || p.salary == null) throw new Error('A saved lineup player is missing from its snapshot.');
      const captainId = entry.captainDkPlayerId ?? p.captainDkPlayerId ?? null;
      if (entry.slot === 'CPT' && captainId == null) throw new Error('Saved Captain ID is missing; export cannot be restored safely.');
      const player: NflOptimizerPlayer = { ...p, id: p.id ?? p.dkPlayerId, dkPlayerId: p.dkPlayerId,
        captainDkPlayerId: captainId, name: entry.name, position: p.position, team: entry.team,
        opponent: p.opponent ?? null, gameKey: p.gameKey ?? null, salary: p.salary,
        captainSalary: p.captainSalary ?? null, isOut: p.isOut ?? false,
        projectionStatus: p.projectionStatus ?? 'unavailable', ourProj: p.ourProj ?? null,
        floorFpts: p.floor ?? null, ceilingFpts: p.ceiling ?? null, boomRate: p.boomRate ?? null,
        avgFptsDk: p.dkAvg ?? null, fantasyprosProj: p.fantasypros ?? null,
        linestarProj: p.linestar ?? null, linestarOwnPct: p.ownership ?? null, customProj: p.custom ?? null };
      return { slot: entry.slot, player, salary: entry.salary, multiplier: entry.multiplier ?? (entry.slot === 'CPT' ? 1.5 : 1), projection: entry.projection, projectionSource: entry.source };
    });
    return { ...row, slots, playerIds: row.playerIds as number[], floorFpts: row.floorFpts, ceilingFpts: row.ceilingFpts, stackSummary: row.stackSummary as NflGeneratedLineup['stackSummary'] };
  });
}
