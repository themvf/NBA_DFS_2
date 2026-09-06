import {
  DK_SALARY_CAP, NFL_FLEX_POSITIONS,
  type NflDkPlayer, type NflDkSlate, type NflSlateFormat,
} from "./dk-salary-csv";
import { nflRandom, nflShuffle } from "./random";

export type NflSlot = "QB" | "RB" | "WR" | "TE" | "FLEX" | "DST" | "CPT";
export type NflLineup = Array<{ slot: NflSlot; playerId: number }>;
const CLASSIC: NflSlot[] = ["QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST"];
const SHOWDOWN: NflSlot[] = ["CPT", "FLEX", "FLEX", "FLEX", "FLEX", "FLEX"];
export const nflSlots = (format: NflSlateFormat): readonly NflSlot[] =>
  format === "classic" ? CLASSIC : SHOWDOWN;

function eligible(player: NflDkPlayer, slot: NflSlot, format: NflSlateFormat): boolean {
  if (player.isOut) return false;
  if (format === "showdown") {
    return slot === "CPT" ? player.captain !== null : slot === "FLEX" && player.rosterPositions.includes("FLEX");
  }
  return player.rosterPositions.includes(slot) && (slot === "FLEX"
    ? NFL_FLEX_POSITIONS.includes(player.position) : player.position === slot);
}

export function nflPoolIndex(slate: NflDkSlate): Map<number, NflDkPlayer> {
  if (slate.format !== "classic" && slate.format !== "showdown") throw new Error("Unknown NFL format");
  const index = new Map<number, NflDkPlayer>();
  const purchaseIds = new Set<number>();
  for (const player of slate.players) {
    const purchases = [{ dkPlayerId: player.dkPlayerId, salary: player.salary }, ...(player.captain ? [player.captain] : [])];
    for (const purchase of purchases) {
      if (!Number.isSafeInteger(purchase.dkPlayerId) || purchase.dkPlayerId <= 0 || purchaseIds.has(purchase.dkPlayerId)) {
        throw new Error(`Invalid or duplicate DK purchase ID: ${purchase.dkPlayerId}`);
      }
      if (!Number.isSafeInteger(purchase.salary) || purchase.salary < 0) throw new Error("Invalid DK salary");
      purchaseIds.add(purchase.dkPlayerId);
    }
    index.set(player.dkPlayerId, player);
  }
  return index;
}

/** Slot order is canonical DK export order; IDs always identify underlying players. */
export function validateNflLineup(slate: NflDkSlate, lineup: NflLineup): { key: string; salary: number } {
  const index = nflPoolIndex(slate);
  const slots = nflSlots(slate.format);
  if (lineup.length !== slots.length) throw new Error(`Expected ${slots.length} NFL slots`);
  const seen = new Set<number>();
  const teams = new Set<string>();
  const games = new Set<string>();
  let salary = 0;
  lineup.forEach((entry, i) => {
    if (entry.slot !== slots[i]) throw new Error(`Expected slot ${slots[i]} at index ${i}`);
    const player = index.get(entry.playerId);
    if (!player) throw new Error(`Unknown underlying player ID: ${entry.playerId}`);
    if (seen.has(entry.playerId)) throw new Error(`Duplicate underlying player: ${entry.playerId}`);
    if (!eligible(player, entry.slot, slate.format)) throw new Error(`Ineligible player ${entry.playerId} for ${entry.slot}`);
    if (!player.gameKey || !slate.games.includes(player.gameKey)) throw new Error(`Missing or unknown game: ${entry.playerId}`);
    const gameTeams = player.gameKey.split("@");
    if (gameTeams.length !== 2 || new Set(gameTeams).size !== 2 || !gameTeams.includes(player.teamAbbrev)) {
      throw new Error(`Player/team/game mismatch: ${entry.playerId}`);
    }
    seen.add(entry.playerId);
    teams.add(player.teamAbbrev);
    games.add(player.gameKey);
    salary += entry.slot === "CPT" ? player.captain!.salary : player.salary;
  });
  if (salary > DK_SALARY_CAP) throw new Error("NFL lineup exceeds salary cap");
  if (slate.format === "classic" && games.size < 2) throw new Error("Classic requires at least two games");
  if (slate.format === "showdown" && (slate.games.length !== 1 || games.size !== 1 || teams.size !== 2)) {
    throw new Error("Showdown requires one game and both teams");
  }
  const sorted = (ids: number[]) => ids.sort((a, b) => a - b).join(",");
  const key = slate.format === "classic" ? `classic:${sorted([...seen])}`
    : `showdown:${lineup[0].playerId}:${sorted(lineup.slice(1).map((p) => p.playerId))}`;
  return { key, salary };
}

/** Bounded randomized candidate search, not an optimality or infeasibility proof. */
export function generateNflCandidates(slate: NflDkSlate, options: {
  count: number; seed: number; maxAttempts?: number; maxNodes?: number;
}): { lineups: NflLineup[]; status: "complete" | "search-limit"; attempts: number; nodes: number } {
  nflPoolIndex(slate);
  const { count, seed, maxAttempts = 5000, maxNodes = 200_000 } = options;
  for (const value of [count, maxAttempts, maxNodes]) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error("Candidate search limits must be positive integers");
  }
  const random = nflRandom(seed);
  const slots = nflSlots(slate.format);
  const pools = slots.map((slot) => slate.players.filter((p) => eligible(p, slot, slate.format))
    .sort((a, b) => a.dkPlayerId - b.dkPlayerId));
  const keys = new Set<string>();
  const lineups: NflLineup[] = [];
  let nodes = 0;
  let attempts = 0;
  while (lineups.length < count && attempts < maxAttempts && nodes < maxNodes) {
    attempts++;
    const choices = pools.map((pool) => nflShuffle(pool, random));
    const selected: NflLineup = [];
    const used = new Set<number>();
    const visit = (salary: number): NflLineup | null => {
      if (nodes >= maxNodes) return null;
      nodes++;
      if (selected.length === slots.length) {
        try { validateNflLineup(slate, selected); return [...selected]; }
        catch { return null; }
      }
      const slot = slots[selected.length];
      for (const player of choices[selected.length]) {
        if (nodes >= maxNodes) break;
        const cost = slot === "CPT" ? player.captain!.salary : player.salary;
        if (used.has(player.dkPlayerId) || salary + cost > DK_SALARY_CAP) continue;
        selected.push({ slot, playerId: player.dkPlayerId });
        used.add(player.dkPlayerId);
        const result = visit(salary + cost);
        selected.pop();
        used.delete(player.dkPlayerId);
        if (result) return result;
      }
      return null;
    };
    const lineup = visit(0);
    if (lineup) {
      const { key } = validateNflLineup(slate, lineup);
      if (!keys.has(key)) { keys.add(key); lineups.push(lineup); }
    }
  }
  return { lineups, status: lineups.length === count ? "complete" : "search-limit", attempts, nodes };
}
