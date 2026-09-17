// Did a player leave the game, or did he just not produce?
//
// A port of model/nfl_dfs_removal.py, deliberately kept line-for-line
// equivalent: the CI probe and this panel must never propose different
// verdicts about the same player, or a tag recorded from one becomes
// unexplainable by the other.
//
// Play-by-play participation records EVENT participation, not snaps: a row
// exists because a player threw, carried or was thrown to. A quarterback
// touches nearly every offensive snap, so his silence is evidence he left. A
// receiver can run a half of routes without producing a single row, so his
// silence is evidence of almost nothing. That asymmetry is enforced below,
// not merely documented.

export const REMOVAL_VERSION = "nfl-dfs-removal-v1";

// Roles meaning "the ball came to this player". Blocking and coverage roles
// would answer the better question — was he on the field — which this source
// cannot answer, so they are excluded rather than half-used.
const BALL_ROLES = new Set(["passer", "rusher", "receiver"]);

// Share of a team's offensive plays after which silence stops looking like a
// gap between touches and starts looking like an absence.
const SILENCE_THRESHOLD = 0.25;

// Snap share at which a quarterback counts as the starter, for the purpose of
// "did the starter leave".
const QB_WORKLOAD_FLOOR = 0.5;

export type Verdict =
  | "LIKELY_REMOVED" | "OPPORTUNITY_NO_CONVERSION" | "NO_OPPORTUNITY" | "NORMAL" | "UNKNOWN";

export type ParticipantRow = {
  playId: number; team: string | null; side: string | null;
  role: string; playerName: string; playerId?: string | null;
};

export type Appearance = {
  playerId: string | null; playerName: string; team: string | null;
  firstPlay: number; lastPlay: number; plays: number;
  targets: number; carries: number; dropbacks: number;
};

export type Proposal = {
  verdict: Verdict; version: string; confidence: "high" | "medium" | "low";
  reason: string;
  evidence: {
    plays: number; targets: number; carries: number; dropbacks: number;
    lastPlay: number | null; teamPlays: number;
    silenceShare: number | null; receptions: number | null;
  };
};

// Verdicts a human is meant to act on, in the order they deserve attention.
export const VERDICT_MARK: Record<Verdict, string> = {
  LIKELY_REMOVED: "🔴", OPPORTUNITY_NO_CONVERSION: "🟡",
  NO_OPPORTUNITY: "⚪", NORMAL: "🟢", UNKNOWN: "❔",
};
export const VERDICT_LABEL: Record<Verdict, string> = {
  LIKELY_REMOVED: "Likely removed",
  OPPORTUNITY_NO_CONVERSION: "Opportunity, not availability",
  NO_OPPORTUNITY: "No opportunity",
  NORMAL: "Played normally",
  UNKNOWN: "Not determinable",
};

/** Sorted, de-duplicated offensive play ids per team.
 *
 * De-duplication is load-bearing: one snap yields a passer row AND a receiver
 * row, so counting rows would double every denominator and halve every share
 * derived from it.
 *
 * Counted from BALL_ROLES only — "snaps where somebody threw, carried or was
 * thrown to", not every credited offensive player. The Python probe uses the
 * identical definition; the two must agree exactly. */
export function teamOffensivePlays(rows: readonly ParticipantRow[]): Map<string, number[]> {
  const seen = new Map<string, Set<number>>();
  for (const row of rows) {
    if (row.side !== "offense" || !BALL_ROLES.has(row.role)
        || row.team == null || !Number.isFinite(row.playId)) continue;
    let set = seen.get(row.team);
    if (!set) seen.set(row.team, (set = new Set()));
    set.add(row.playId);
  }
  return new Map([...seen].map(([team, plays]) => [team, [...plays].sort((a, b) => a - b)]));
}

function normaliseName(name: string): string {
  return (name || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

/** Collapse participation rows into one footprint per player, indexed under
 * every spelling we might later look it up by. The two sources abbreviate
 * differently ("A.Loveland" vs "Colston Loveland"), and a missed join is
 * indistinguishable from a player who never touched the ball — which is the
 * one distinction this whole feature exists to make. */
export function appearancesByName(rows: readonly ParticipantRow[]): Map<string, Appearance> {
  const acc = new Map<string, Appearance & { playSet: Set<number> }>();
  for (const row of rows) {
    if (!BALL_ROLES.has(row.role) || !row.playerName || !Number.isFinite(row.playId)) continue;
    const key = row.playerId ? `id:${row.playerId}` : `name:${normaliseName(row.playerName)}`;
    let entry = acc.get(key);
    if (!entry) {
      acc.set(key, (entry = {
        playerId: row.playerId ?? null, playerName: row.playerName, team: row.team,
        firstPlay: row.playId, lastPlay: row.playId, plays: 0,
        targets: 0, carries: 0, dropbacks: 0, playSet: new Set(),
      }));
    }
    entry.firstPlay = Math.min(entry.firstPlay, row.playId);
    entry.lastPlay = Math.max(entry.lastPlay, row.playId);
    entry.playSet.add(row.playId);
    if (row.role === "receiver") entry.targets += 1;
    else if (row.role === "rusher") entry.carries += 1;
    else entry.dropbacks += 1;
  }

  const index = new Map<string, Appearance>();
  for (const entry of acc.values()) {
    const appearance: Appearance = { ...entry, plays: entry.playSet.size };
    delete (appearance as Partial<typeof entry>).playSet;
    const parts = entry.playerName.split(/\s+/).filter(Boolean);
    const keys = [normaliseName(entry.playerName)];
    if (parts.length >= 2) keys.push(normaliseName(parts[0][0] + parts[parts.length - 1]));
    for (const key of keys) if (!index.has(key)) index.set(key, appearance);
  }
  return index;
}

/** Look a report-card player up under either spelling. */
export function findAppearance(
  index: Map<string, Appearance>, name: string,
): Appearance | null {
  const full = normaliseName(name);
  if (index.has(full)) return index.get(full)!;
  const parts = (name || "").split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    const initial = normaliseName(parts[0][0] + parts[parts.length - 1]);
    if (index.has(initial)) return index.get(initial)!;
  }
  return null;
}

/** Share of the team's offensive plays that came after this player's last
 * appearance. `null` when the denominator is unknown — never 0, which would
 * read as "played to the whistle". */
export function silenceShare(appearance: Appearance, teamPlays: readonly number[]): number | null {
  if (!teamPlays.length) return null;
  let after = 0;
  for (const play of teamPlays) if (play > appearance.lastPlay) after += 1;
  return after / teamPlays.length;
}

const pct = (value: number) => `${Math.round(value * 100)}%`;

/** Propose one verdict, with the evidence that produced it.
 *
 * Returns no probability, on purpose: a number here would invite reading a
 * proposal as a finding. A human confirms, or it stays a proposal. */
export function classifyRemoval(input: {
  position: string;
  appearance: Appearance | null;
  teamPlays: readonly number[];
  receptions?: number | null;
}): Proposal {
  const { appearance, teamPlays } = input;
  const position = (input.position || "").toUpperCase();
  const receptions = input.receptions ?? null;

  if (!appearance) {
    return {
      verdict: "NO_OPPORTUNITY", version: REMOVAL_VERSION, confidence: "low",
      reason: "No passer, rusher or receiver row in this game.",
      evidence: {
        plays: 0, targets: 0, carries: 0, dropbacks: 0, lastPlay: null,
        teamPlays: teamPlays.length, silenceShare: null, receptions,
      },
    };
  }

  const share = silenceShare(appearance, teamPlays);
  const touches = appearance.targets + appearance.carries + appearance.dropbacks;
  const evidence = {
    plays: appearance.plays, targets: appearance.targets, carries: appearance.carries,
    dropbacks: appearance.dropbacks, lastPlay: appearance.lastPlay,
    teamPlays: teamPlays.length, silenceShare: share, receptions,
  };

  if (position === "QB") {
    const workload = teamPlays.length ? appearance.dropbacks / teamPlays.length : null;
    const started = workload !== null && workload >= QB_WORKLOAD_FLOOR;
    if (share !== null && share >= SILENCE_THRESHOLD && started) {
      return {
        verdict: "LIKELY_REMOVED", version: REMOVAL_VERSION, confidence: "high",
        reason: `Took ${pct(workload!)} of offensive snaps, then none of the final ${pct(share)}.`,
        evidence,
      };
    }
    if (share !== null && share >= SILENCE_THRESHOLD) {
      return {
        verdict: "UNKNOWN", version: REMOVAL_VERSION, confidence: "low",
        reason: "Absent late, but never carried a starter's snap share — more likely a backup than a removal.",
        evidence,
      };
    }
  } else if (share !== null && share >= SILENCE_THRESHOLD && touches > 0) {
    return {
      verdict: "UNKNOWN", version: REMOVAL_VERSION, confidence: "low",
      reason: `No touch in the final ${pct(share)} of snaps. At this position that is common `
        + "without an injury — participation rows record touches, not snaps.",
      evidence,
    };
  }

  if (touches > 0 && receptions !== null && receptions <= 0 && appearance.targets > 0) {
    return {
      verdict: "OPPORTUNITY_NO_CONVERSION", version: REMOVAL_VERSION, confidence: "high",
      reason: `Targeted ${appearance.targets}×, no catch — the ball came, it did not stick. `
        + "Not an availability problem.",
      evidence,
    };
  }
  if (touches === 0) {
    return {
      verdict: "NO_OPPORTUNITY", version: REMOVAL_VERSION, confidence: "medium",
      reason: "On the roster, never targeted or handed the ball.", evidence,
    };
  }
  return {
    verdict: "NORMAL", version: REMOVAL_VERSION, confidence: "medium",
    reason: `${touches} touches spread across the game.`, evidence,
  };
}
