import { csvRows, numberCell, type CbbSlate } from "./salary-csv";

export interface CbbEstimate {
  projection: number | null; minutes: number | null; rate: number | null; bonus: number;
  ownership: number | null; captainOwnership: number | null;
  source: string; asOf: string; method: "import" | "history"; games: number; excluded: boolean;
}
export type CbbEstimates = Record<string, CbbEstimate>;

export function prelockTimestamp(raw: string, cutoff: string, label: string, decisionTime = Date.now()): string {
  if (!/^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:\d{2})$/.test(raw) || !Number.isFinite(Date.parse(raw)) || Date.parse(raw) >= Date.parse(cutoff) || Date.parse(raw) > decisionTime) throw new Error(`${label} must be a timestamp with timezone before slate lock and no later than the decision time.`);
  return new Date(raw).toISOString();
}

/** Blank is unknown, never zero. Imports match only explicit utility/classic DK IDs. */
export function importCbbEstimates(text: string, slate: CbbSlate, decisionTime = Date.now()): CbbEstimates {
  const result: CbbEstimates = {};
  for (const row of csvRows(text)) {
    if (!slate.players.some(p => p.id === row.ID)) throw new Error(`Unmatched DK ID ${row.ID}; use the Classic/utility ID from this salary file.`);
    if (result[row.ID]) throw new Error(`Duplicate projection ID ${row.ID}.`);
    if (!row.Source) throw new Error("Each projection needs a Source.");
    const asOf = prelockTimestamp(row.AsOf ?? "", slate.firstTipoff, "AsOf", decisionTime);
    const optional = (key: string, max = Infinity) => row[key]?.trim() ? numberCell(row[key], key, 0, max) : null;
    const minutes = optional("Minutes", 40), rate = optional("Fppm");
    const bonus = optional("Bonus", 4.5) ?? 0;
    const projection = optional("Projection") ?? (minutes !== null && rate !== null ? minutes * rate + bonus : null);
    if (projection === null) throw new Error(`Supply Projection or both Minutes and Fppm for ${row.ID}.`);
    const ownership = optional("Ownership", 100), captainOwnership = optional("CaptainOwnership", 100);
    if (slate.mode === "showdown" && ownership !== null && captainOwnership !== null && ownership + captainOwnership > 100) throw new Error(`Utility plus Captain ownership cannot exceed 100% for ${row.ID}. Use utility share rather than overall roster share.`);
    if (slate.mode === "classic" && captainOwnership !== null) throw new Error("Captain ownership belongs to Showdown, not Classic.");
    result[row.ID] = { projection, minutes, rate, bonus, ownership, captainOwnership, source: row.Source, asOf, method: "import", games: 0, excluded: false };
  }
  return result;
}

export interface CbbBox { points: number; threes: number; rebounds: number; assists: number; steals: number; blocks: number; turnovers: number }
export function cbbScore(box: CbbBox): { base: number; bonus: number; total: number } {
  const base = box.points + box.threes * .5 + box.rebounds * 1.25 + box.assists * 1.5 + box.steals * 2 + box.blocks * 2 - box.turnovers * .5;
  const doubles = [box.points, box.rebounds, box.assists, box.steals, box.blocks].filter(n => n >= 10).length;
  const bonus = (doubles >= 2 ? 1.5 : 0) + (doubles >= 3 ? 3 : 0);
  return { base, bonus, total: base + bonus };
}

/** Transparent, uncalibrated starting model: last 5 games' minutes × last 20 games' base FPPM + empirical bonus/game. */
export function projectCbbHistory(text: string, slate: CbbSlate, decisionTime = Date.now()): CbbEstimates {
  const grouped = new Map<string, Array<{ minutes: number; base: number; bonus: number; playedAt: string; capturedAt: string; source: string }>>();
  const seen = new Set<string>();
  for (const row of csvRows(text)) {
    const player = slate.players.find(p => p.id === row.ID);
    if (!player || row.Team?.toUpperCase() !== player.team) throw new Error(`Unresolved ID/team for ${row.ID}. Map source identities to this slate explicitly.`);
    if (!row.GameID || !row.Source) throw new Error("GameID and Source are required for every box score.");
    const key = `${row.ID}|${row.GameID}`;
    if (seen.has(key)) throw new Error(`Repeated player/game: ${key}.`);
    seen.add(key);
    const playedAt = prelockTimestamp(row.PlayedAt ?? "", slate.firstTipoff, "PlayedAt", decisionTime);
    const capturedAt = prelockTimestamp(row.CapturedAt ?? "", slate.firstTipoff, "CapturedAt", decisionTime);
    if (capturedAt < playedAt) throw new Error("Box scores cannot be captured before the game starts.");
    const n = (key: string) => { const value = numberCell(row[key], key); if (!Number.isInteger(value)) throw new Error(`${key} must be a whole-number count.`); return value; };
    const minutes = numberCell(row.Minutes, "Minutes", 0, 80);
    const box = { points: n("PTS"), threes: n("3PM"), rebounds: n("REB"), assists: n("AST"), steals: n("STL"), blocks: n("BLK"), turnovers: n("TO") };
    if (box.threes * 3 > box.points) throw new Error("Three-pointers cannot exceed total points.");
    const score = cbbScore(box);
    if (minutes === 0 && Object.values(box).some(n => n !== 0)) throw new Error("A zero-minute row cannot have scored stats.");
    // Keep DNPs in the evidence file, but do not equate a DNP with a rotation-minute observation.
    if (minutes === 0) continue;
    const games = grouped.get(row.ID) ?? [];
    games.push({ minutes, ...score, playedAt, capturedAt, source: row.Source }); grouped.set(row.ID, games);
  }
  const result: CbbEstimates = {};
  for (const [id, games] of grouped) {
    const sorted = games.sort((a, b) => b.playedAt.localeCompare(a.playedAt));
    // No automatic carryover from a previous roster/season. November belongs to the following ending-year season.
    const season = (iso: string) => { const d = new Date(iso); return d.getUTCFullYear() + (d.getUTCMonth() >= 6 ? 1 : 0); };
    const sample = sorted.filter(g => season(g.playedAt) === season(slate.firstTipoff)).slice(0, 20);
    if (!sample.length) continue;
    const recent = sample.slice(0, 5);
    const minutes = Math.min(40, recent.reduce((a, g) => a + g.minutes, 0) / recent.length);
    const rate = sample.reduce((a, g) => a + g.base, 0) / sample.reduce((a, g) => a + g.minutes, 0);
    const bonus = sample.reduce((a, g) => a + g.bonus, 0) / sample.length;
    result[id] = { projection: Math.max(0, minutes * rate + bonus), minutes, rate, bonus, source: [...new Set(sample.map(g => g.source))].join("; "), asOf: sample.map(g => g.capturedAt).sort().at(-1)!, method: "history", games: sample.length, ownership: null, captainOwnership: null, excluded: false };
  }
  if (!Object.keys(result).length) throw new Error("No played games from this season before lock. Import explicit projections for freshmen, transfers, or opening-night slates.");
  return result;
}
