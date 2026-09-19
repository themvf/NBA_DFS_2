// Was this player injured out of the game, or did he simply not produce?
//
// A port of model/nfl_dfs_removal.py, kept deliberately equivalent: the CI
// probe and this panel must never propose different verdicts about the same
// player, or a tag recorded from one becomes unexplainable by the other.
//
// Evidence, strongest first:
//  1. The play description NAMES the injured player -- nflverse writes
//     "M.Evans was injured during the play." That is an observation with a
//     timestamp, not an inference.
//  2. PRESENCE afterwards. A touch in the fourth quarter is near-proof a
//     player finished, and unlike absence it works at every position.
//  3. ABSENCE. Weak, and weak asymmetrically -- a quarterback touches nearly
//     every snap, a receiver can run a half of routes without a single row.
//
// What the source cannot tell us: whether a player was on the field without
// being thrown to. That is nflverse's `participation` dataset, published
// after the postseason -- there is no 2026 file today. No snap rate exists
// in-season, and this module must not pretend otherwise.

export const REMOVAL_VERSION = "nfl-dfs-removal-v2";

const BALL_ROLES = new Set(["passer", "rusher", "receiver"]);

// nflverse's fixed phrasing. Anchored on the phrase rather than a name
// pattern: the phrase is stable, names are not.
const INJURY_RE = /((?:[A-Z][A-Za-z'\-]*[. ])?[A-Z][A-Za-z'\-]+(?: (?:Jr|Sr|II|III|IV)\.?)?)\s+was injured during the play/g;

// Quarter 4 (and overtime) clears a player.
const LATE_QUARTER = 4;
// Below this many team plays after a last touch, silence is the game ending.
const MIN_PLAYS_AFTER = 10;

export type Verdict =
  | "INJURED_OUT" | "INJURED_RETURNED" | "PLAYED_LATE"
  | "LAST_SEEN_EARLY" | "NO_OPPORTUNITY";

export type ParticipantRow = {
  gameId: string; playId: number; team: string | null;
  side: string | null; role: string; playerName: string;
};
export type PlayRow = {
  gameId: string; playId: number;
  quarter: number | null; clock: string | null; description: string | null;
};
export type InjuryEvent = {
  gameId: string; playId: number; quarter: number | null;
  clock: string | null; playerName: string; description: string;
};
export type Appearance = {
  playerName: string; team: string | null;
  firstPlay: number; lastPlay: number; lastQuarter: number | null;
  plays: Set<number>;
  targets: number; carries: number; dropbacks: number;
};
export type Proposal = {
  verdict: Verdict; version: string; confidence: "high" | "medium" | "low";
  reason: string;
  evidence: {
    targets: number; carries: number; dropbacks: number; plays: number;
    lastPlay: number | null; lastQuarter: number | null;
    teamPlays: number; playsAfter: number | null;
    injuries: { quarter: number | null; clock: string | null; name: string }[];
  };
};

// Ordered by how much attention each deserves.
export const VERDICT_MARK: Record<Verdict, string> = {
  INJURED_OUT: "🔴", LAST_SEEN_EARLY: "🟠", INJURED_RETURNED: "🟡",
  PLAYED_LATE: "🟢", NO_OPPORTUNITY: "⚪",
};
export const VERDICT_LABEL: Record<Verdict, string> = {
  INJURED_OUT: "Injured out of the game",
  LAST_SEEN_EARLY: "Last seen early — needs a look",
  INJURED_RETURNED: "Injured, but came back",
  PLAYED_LATE: "Played to the end",
  NO_OPPORTUNITY: "No opportunity",
};
// Only these should draw the eye in a table of deltas.
export const NEEDS_ATTENTION: ReadonlySet<Verdict> = new Set<Verdict>(["INJURED_OUT", "LAST_SEEN_EARLY"]);

const normalise = (name: string) => (name || "").toLowerCase().replace(/[^a-z0-9]/g, "");

/** Every spelling one player might be written under. The report card says
 * "Colston Loveland"; play-by-play says "C.Loveland". A missed join looks
 * exactly like a player who never touched the ball -- the one distinction
 * this module exists to draw. */
export function nameKeys(name: string): string[] {
  const keys = [normalise(name)];
  const parts = (name || "").replace(/\./g, ". ").split(/\s+/).filter(Boolean);
  if (parts.length >= 2) keys.push(normalise(parts[0][0] + parts[parts.length - 1]));
  return keys.filter(Boolean);
}

/** Named injuries, read from the play text rather than inferred. The stored
 * `injury_on_play` boolean comes from this same phrase but drops the name,
 * which is the only part that lets an injury reach a projection. */
export function injuryEvents(plays: readonly PlayRow[]): InjuryEvent[] {
  const out: InjuryEvent[] = [];
  for (const play of plays) {
    const text = play.description ?? "";
    if (!text) continue;
    INJURY_RE.lastIndex = 0;
    for (let m = INJURY_RE.exec(text); m; m = INJURY_RE.exec(text)) {
      out.push({
        gameId: play.gameId, playId: play.playId, quarter: play.quarter,
        clock: play.clock, playerName: m[1].trim(), description: text,
      });
    }
  }
  return out;
}

/** Sorted, de-duplicated offensive play ids per `${gameId}:${team}`.
 *
 * De-duplication is load-bearing: one snap yields a passer row AND a receiver
 * row. Counted from ball roles only, matching the Python exactly. */
export function teamOffensivePlays(rows: readonly ParticipantRow[]): Map<string, number[]> {
  const seen = new Map<string, Set<number>>();
  for (const row of rows) {
    if (row.side !== "offense" || !BALL_ROLES.has(row.role) || row.team == null) continue;
    const key = `${row.gameId}:${row.team}`;
    let set = seen.get(key);
    if (!set) seen.set(key, (set = new Set()));
    set.add(row.playId);
  }
  return new Map([...seen].map(([k, v]) => [k, [...v].sort((a, b) => a - b)]));
}

/** One footprint per (game, player), indexed under every spelling.
 *
 * Keyed by game as well as name: two games in a week can carry the same
 * surname, and a cross-game merge would invent a late appearance that never
 * happened -- precisely the error that would clear an injured player. */
export function appearances(
  participants: readonly ParticipantRow[],
  quarters: Map<string, number | null>,
): Map<string, Appearance> {
  const canonical = new Map<string, Appearance>();
  const index = new Map<string, Appearance>();
  for (const row of participants) {
    if (!BALL_ROLES.has(row.role) || !row.playerName) continue;
    const primary = `${row.gameId}:${normalise(row.playerName)}`;
    let entry = canonical.get(primary);
    if (!entry) {
      canonical.set(primary, (entry = {
        playerName: row.playerName, team: row.team,
        firstPlay: row.playId, lastPlay: row.playId, lastQuarter: null,
        plays: new Set(), targets: 0, carries: 0, dropbacks: 0,
      }));
    }
    entry.firstPlay = Math.min(entry.firstPlay, row.playId);
    entry.plays.add(row.playId);
    if (row.role === "receiver") entry.targets += 1;
    else if (row.role === "rusher") entry.carries += 1;
    else entry.dropbacks += 1;
    if (row.playId >= entry.lastPlay) {
      entry.lastPlay = row.playId;
      const quarter = quarters.get(`${row.gameId}:${row.playId}`);
      // Only overwrite with a known quarter; an unmapped play must not erase
      // a quarter already established.
      if (quarter != null) entry.lastQuarter = quarter;
    }
    for (const key of nameKeys(row.playerName)) {
      const k = `${row.gameId}:${key}`;
      if (!index.has(k)) index.set(k, entry);
    }
  }
  return index;
}

export function findAppearance(
  index: Map<string, Appearance>, gameId: string, name: string,
): Appearance | null {
  for (const key of nameKeys(name)) {
    const found = index.get(`${gameId}:${key}`);
    if (found) return found;
  }
  return null;
}

export function injuriesFor(
  events: readonly InjuryEvent[], gameId: string, name: string,
): InjuryEvent[] {
  const keys = new Set(nameKeys(name));
  return events
    .filter(e => e.gameId === gameId && nameKeys(e.playerName).some(k => keys.has(k)))
    .sort((a, b) => a.playId - b.playId);
}

const touchCount = (a: Appearance) => a.targets + a.carries + a.dropbacks;

/** Propose one AVAILABILITY verdict, with the evidence behind it.
 *
 * Availability only. Whether a player converted the chances he got is a
 * different axis, reported as evidence (targets, carries) and never folded
 * into this label -- one verdict per question, or neither can be acted on. */
export function classifyRemoval(input: {
  position: string;
  appearance: Appearance | null;
  teamPlays?: readonly number[];
  injuries?: readonly InjuryEvent[];
}): Proposal {
  const { appearance } = input;
  const position = (input.position || "").toUpperCase();
  const teamPlays = input.teamPlays ?? [];
  const injuries = input.injuries ?? [];
  const injuryEvidence = injuries.map(e => ({ quarter: e.quarter, clock: e.clock, name: e.playerName }));

  if (!appearance) {
    return {
      verdict: "NO_OPPORTUNITY", version: REMOVAL_VERSION,
      confidence: injuries.length ? "high" : "low",
      reason: injuries.length
        ? "Named as injured and never threw, carried or was thrown to."
        : "No passer, rusher or receiver row in this game.",
      evidence: {
        targets: 0, carries: 0, dropbacks: 0, plays: 0, lastPlay: null,
        lastQuarter: null, teamPlays: teamPlays.length, playsAfter: null,
        injuries: injuryEvidence,
      },
    };
  }

  let playsAfter = 0;
  for (const play of teamPlays) if (play > appearance.lastPlay) playsAfter += 1;
  const evidence = {
    targets: appearance.targets, carries: appearance.carries,
    dropbacks: appearance.dropbacks, plays: appearance.plays.size,
    lastPlay: appearance.lastPlay, lastQuarter: appearance.lastQuarter,
    teamPlays: teamPlays.length, playsAfter: teamPlays.length ? playsAfter : null,
    injuries: injuryEvidence,
  };

  if (injuries.length) {
    const first = injuries[0];
    let after = 0;
    for (const play of appearance.plays) if (play > first.playId) after += 1;
    if (after > 0) {
      return {
        verdict: "INJURED_RETURNED", version: REMOVAL_VERSION, confidence: "high",
        reason: `Injury noted in Q${first.quarter ?? "?"}, then ${after} further touches. `
          + "He came back and played.",
        evidence,
      };
    }
    return {
      verdict: "INJURED_OUT", version: REMOVAL_VERSION, confidence: "high",
      reason: `Named as injured in Q${first.quarter ?? "?"}`
        + `${first.clock ? ` (${first.clock})` : ""} and never touched the ball again.`,
      evidence,
    };
  }

  const quarter = appearance.lastQuarter;
  if (quarter != null && quarter >= LATE_QUARTER) {
    return {
      verdict: "PLAYED_LATE", version: REMOVAL_VERSION, confidence: "high",
      reason: `Still being given the ball in Q${quarter}. Whatever the box score says, `
        + "availability was not the problem.",
      evidence,
    };
  }
  if (touchCount(appearance) === 0) {
    return {
      verdict: "NO_OPPORTUNITY", version: REMOVAL_VERSION, confidence: "medium",
      reason: "On the roster, never targeted or handed the ball.", evidence,
    };
  }
  if (teamPlays.length && playsAfter < MIN_PLAYS_AFTER) {
    return {
      verdict: "PLAYED_LATE", version: REMOVAL_VERSION, confidence: "medium",
      reason: `Last touch came with only ${playsAfter} offensive plays left. `
        + "That is the game ending, not an exit.",
      evidence,
    };
  }
  const detail = quarter != null
    ? `Last touch in Q${quarter}, with ${playsAfter} team plays after it.`
    : `${playsAfter} team plays came after his last touch.`;
  const note = position === "QB"
    ? "A quarterback touches nearly every snap, so this absence is meaningful."
    : "At this position, participation rows record touches rather than snaps, so "
      + "absence is weak evidence on its own.";
  return {
    verdict: "LAST_SEEN_EARLY", version: REMOVAL_VERSION,
    confidence: position === "QB" ? "medium" : "low",
    reason: `${detail} No injury was recorded against his name. ${note}`,
    evidence,
  };
}
