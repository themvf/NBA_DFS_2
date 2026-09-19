// Turning a week of play-by-play into one availability proposal per player.
//
// Extracted so the page renders a prepared list rather than classifying inside
// JSX: the index build is a full scan of a week of plays, and doing it per row
// would repeat that work for every repaint.

import {
  appearances, classifyRemoval, findAppearance, injuriesFor, injuryEvents,
  NEEDS_ATTENTION, teamOffensivePlays,
  type ParticipantRow, type PlayRow, type Proposal, type Verdict,
} from "./removal";
import { delta } from "./review-insights";
import type { ReportRow } from "./report-card";

export type AvailabilityEntry = { row: ReportRow; proposal: Proposal };

// Most attention first. Verdicts that need a human lead; cleared players
// follow, so the page opens on the short list rather than the long one.
const VERDICT_ORDER: Verdict[] = [
  "INJURED_OUT", "LAST_SEEN_EARLY", "INJURED_RETURNED", "NO_OPPORTUNITY", "PLAYED_LATE",
];

export function buildAvailability(
  rows: readonly ReportRow[],
  participants: readonly ParticipantRow[] | null,
  playContext: readonly PlayRow[] | null,
): AvailabilityEntry[] | null {
  // Null, not an empty list: "we have no play-by-play" and "nobody left the
  // game" are different statements, and only one of them is a finding.
  if (!participants || !playContext) return null;

  const quarters = new Map<string, number | null>(
    playContext.map(p => [`${p.gameId}:${p.playId}`, p.quarter]));
  const injuries = injuryEvents(playContext);
  const index = appearances(participants, quarters);

  const teamPlays = new Map<string, number[]>();
  const gamesByTeam = new Map<string, string[]>();
  for (const row of participants) {
    if (row.team == null) continue;
    const games = gamesByTeam.get(row.team);
    if (!games) gamesByTeam.set(row.team, [row.gameId]);
    else if (!games.includes(row.gameId)) games.push(row.gameId);
  }
  for (const [key, plays] of teamOffensivePlays(participants)) teamPlays.set(key, plays);

  return rows.map(row => {
    // The report card's game_id is our internal numeric id, not the nflverse
    // text one, so a player is located by (team, name) across the week's games.
    let gameId: string | null = null;
    let appearance = null;
    for (const candidate of gamesByTeam.get(row.team) ?? []) {
      const found = findAppearance(index, candidate, row.name);
      if (found) { gameId = candidate; appearance = found; break; }
    }
    return {
      row,
      proposal: classifyRemoval({
        position: row.position, appearance,
        teamPlays: teamPlays.get(`${gameId ?? ""}:${appearance?.team ?? row.team}`) ?? [],
        injuries: gameId ? injuriesFor(injuries, gameId, row.name) : [],
      }),
    };
  });
}

/** Needs-a-human first, then by how badly the projection missed. */
export function sortAvailability(entries: readonly AvailabilityEntry[]): AvailabilityEntry[] {
  return [...entries].sort((a, b) => {
    const rank = VERDICT_ORDER.indexOf(a.proposal.verdict) - VERDICT_ORDER.indexOf(b.proposal.verdict);
    if (rank !== 0) return rank;
    // A bigger shortfall first; unscored rows sink rather than sorting as 0.
    const da = delta(a.row), db = delta(b.row);
    if (da == null && db == null) return a.row.name.localeCompare(b.row.name);
    if (da == null) return 1;
    if (db == null) return -1;
    return da - db;
  });
}

export function countByVerdict(entries: readonly AvailabilityEntry[]): Record<Verdict, number> {
  const counts = Object.fromEntries(VERDICT_ORDER.map(v => [v, 0])) as Record<Verdict, number>;
  for (const entry of entries) counts[entry.proposal.verdict] += 1;
  return counts;
}

export const ATTENTION_VERDICTS = VERDICT_ORDER.filter(v => NEEDS_ATTENTION.has(v));
export const ALL_VERDICTS = VERDICT_ORDER;
