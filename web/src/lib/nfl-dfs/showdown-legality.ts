/** Shared by the solver and browser export; never trust a cached salary total. */
export type ShowdownPlayer = {
  dkPlayerId: number;
  captainDkPlayerId: number | null;
  name: string;
  team: string;
  gameKey: string | null;
  salary: number;
  captainSalary: number | null;
  rosterPositions?: string[];
};

export function showdownSalary(player: ShowdownPlayer, captain: boolean): number {
  if (!Number.isSafeInteger(player.salary) || player.salary <= 0) throw new Error(`Invalid salary for ${player.name}. Re-upload DraftKings salaries.`);
  if (!captain) return player.salary;
  const expected = Math.round(player.salary * 1.5);
  if (!Number.isSafeInteger(player.captainDkPlayerId) || player.captainDkPlayerId! <= 0 || player.captainSalary !== expected) {
    throw new Error(`Invalid Captain price or ID for ${player.name}: expected $${expected}. Re-upload DraftKings salaries.`);
  }
  return expected;
}

export function showdownFlexEligible(player: ShowdownPlayer): boolean {
  // Older snapshots lack rosterPositions. A CPT-only import uses its CPT ID
  // as the canonical player ID, and must never export that ID as FLEX.
  return player.rosterPositions ? player.rosterPositions.includes("FLEX") : player.dkPlayerId !== player.captainDkPlayerId;
}

export function assertShowdownLineup(lineup: {
  slots: Array<{ slot: string; salary: number; player: ShowdownPlayer }>;
  totalSalary: number;
  playerIds: number[];
}): void {
  const { slots } = lineup;
  if (slots.length !== 6 || slots[0].slot !== "CPT" || slots.slice(1).some(s => !/^FLEX[1-5]?$/.test(s.slot))) {
    throw new Error("Showdown requires one Captain followed by five FLEX players.");
  }
  const ids = slots.map(s => s.player.dkPlayerId);
  const identities = slots.map(s => `${s.player.team}|${s.player.name.trim().toLowerCase()}`);
  if (new Set(ids).size !== 6 || new Set(identities).size !== 6 || lineup.playerIds.length !== 6 || new Set(lineup.playerIds).size !== 6 || ids.some(id => !lineup.playerIds.includes(id))) {
    throw new Error("Showdown requires six distinct players matching the lineup roster.");
  }
  const games = new Set(slots.map(s => s.player.gameKey));
  const teams = new Set(slots.map(s => s.player.team));
  const game = slots[0].player.gameKey;
  if (!game || games.size !== 1 || teams.size !== 2 || [...teams].some(team => !game.split("@").includes(team))) {
    throw new Error("Showdown requires players from both teams in one game.");
  }
  let total = 0;
  for (const entry of slots) {
    const captain = entry.slot === "CPT";
    if (!captain && !showdownFlexEligible(entry.player)) throw new Error(`${entry.player.name} has no DraftKings FLEX purchase.`);
    const salary = showdownSalary(entry.player, captain);
    if (entry.salary !== salary) throw new Error(`Incorrect ${entry.slot} salary for ${entry.player.name}. Regenerate this lineup.`);
    total += salary;
  }
  if (total > 50000 || lineup.totalSalary !== total) throw new Error("Showdown salary exceeds $50,000 or the saved total is incorrect. Regenerate this lineup.");
}
