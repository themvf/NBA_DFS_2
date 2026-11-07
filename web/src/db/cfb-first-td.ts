import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";

export type FirstTdChip = { code: string; label: string };

export type FirstTdPlayer = {
  name: string; team: string; prob: number; fair: number; td: number; touches: number;
  /** Descriptive context; never part of the chance. Absent on forecasts saved before chips existed. */
  chips?: FirstTdChip[];
  /** On DraftKings' anytime-TD list for this game; null when no list was captured. */
  listed?: boolean | null;
};

export type FirstTdGame = {
  /** Unique per game within a sport (the CFBD game id or the nflverse game id). */
  gameKey: string;
  kickoff: string;
  away: string;
  home: string;
  awayImplied: number;
  homeImplied: number;
  pHomeFirst: number;
  otherOffense: number;
  nonOffense: number;
  players: FirstTdPlayer[];
  savedAt: string;
  version: string;
  /** The first touchdown play once the game's play-by-play is stored; null before then. */
  actual: { playType: string; text: string } | null;
  /** The sportsbook availability filter on this forecast; null for a forecast saved before it existed. */
  teamChips: { home: FirstTdChip[]; away: FirstTdChip[] };
  availability: {
    status: "applied" | "no_lines";
    capturedAt: string | null;
    dropped: { name: string; team: string }[];
  } | null;
};

export type FirstTdGrades = {
  games: number;
  changed: number;
  baseLogLoss: number;
  linesLogLoss: number;
  baseTopPick: number;
  linesTopPick: number;
};

const LINES_VERSION = "cfb-first-td-lines-v1";

const ET = "America/New_York";

/** Eastern dates that have saved forecasts, newest first. */
export async function getFirstTdDates(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT to_char(kickoff AT TIME ZONE ${ET}, 'YYYY-MM-DD') AS d
    FROM cfb_first_td_predictions ORDER BY d DESC LIMIT 30`);
  return (result.rows as { d: string }[]).map((r) => r.d);
}

/**
 * Each game on an Eastern date with its latest forecast saved before kickoff.
 * The publisher never writes a started game, so this is the frozen pregame number.
 */
export async function getFirstTdGames(date: string): Promise<FirstTdGame[]> {
  const result = await db.execute(sql`
    WITH latest AS (
      SELECT DISTINCT ON (p.cfbd_game_id) p.run_id, p.cfbd_game_id, p.kickoff, p.away, p.home,
             p.away_implied, p.home_implied, p.p_home_first, p.other_offense, p.non_offense, p.players,
             p.availability, p.context, r.created_at, r.version
      FROM cfb_first_td_predictions p JOIN cfb_first_td_runs r ON r.id = p.run_id
      WHERE (p.kickoff AT TIME ZONE ${ET})::date = ${date}::date AND r.created_at < p.kickoff
      -- The version with the sportsbook availability filter when one was saved, else the model alone.
      ORDER BY p.cfbd_game_id, (r.version = ${LINES_VERSION}) DESC, r.created_at DESC)
    SELECT l.*, td.play_type, td.play_text
    FROM latest l
    LEFT JOIN cfb_matchups m ON m.cfbd_game_id = l.cfbd_game_id
    -- Plays are read through the indexed game_id; cfb_plays.cfbd_game_id has no index, and filtering on it
    -- scanned every play once per game (104 s for one Saturday's 51 games).
    LEFT JOIN LATERAL (
      SELECT play_type, play_text FROM cfb_plays c
      WHERE c.game_id = m.id
        AND (c.play_type ILIKE '%touchdown%'
             OR (c.scoring AND c.play_text ILIKE '%touchdown%' AND c.play_type IN ('Rush', 'Pass Reception', 'Pass Completion')))
      ORDER BY c.period, c.clock_seconds_remaining DESC, c.drive_number, c.play_number LIMIT 1) td ON TRUE
    ORDER BY l.kickoff, l.home`);
  return (result.rows as Record<string, unknown>[]).map((r) => ({
    gameKey: String(r.cfbd_game_id),
    kickoff: new Date(r.kickoff as string).toISOString(),
    away: r.away as string,
    home: r.home as string,
    awayImplied: Number(r.away_implied),
    homeImplied: Number(r.home_implied),
    pHomeFirst: Number(r.p_home_first),
    otherOffense: Number(r.other_offense),
    nonOffense: Number(r.non_offense),
    players: r.players as FirstTdPlayer[],
    savedAt: new Date(r.created_at as string).toISOString(),
    version: r.version as string,
    actual: r.play_type ? { playType: r.play_type as string, text: (r.play_text as string) ?? "" } : null,
    availability: availabilityOf(r.availability),
    teamChips: teamChipsOf(r.context),
  }));
}

export function teamChipsOf(raw: unknown): FirstTdGame["teamChips"] {
  const t = (raw as { team_chips?: { home?: FirstTdChip[]; away?: FirstTdChip[] } } | null)?.team_chips;
  return { home: t?.home ?? [], away: t?.away ?? [] };
}

export function availabilityOf(raw: unknown): FirstTdGame["availability"] {
  if (!raw || typeof raw !== "object") return null;
  const a = raw as { status?: string; captured_at?: string; dropped?: { name: string; team: string }[] };
  return {
    status: a.status === "applied" ? "applied" : "no_lines",
    capturedAt: a.captured_at ?? null,
    dropped: (a.dropped ?? []).filter((d) => d.name.trim() !== "Team").map((d) => ({ name: d.name, team: d.team })),
  };
}

/** Both versions on the games both have graded (latest pregame forecast of each, scored once). */
export async function getFirstTdGrades(): Promise<FirstTdGrades | null> {
  const result = await db.execute(sql`
    SELECT count(*)::int AS games,
           count(*) FILTER (WHERE abs(a.prob - b.prob) > 1e-12)::int AS changed,
           avg(-ln(greatest(a.prob, 1e-9))) AS base_ll, avg(-ln(greatest(b.prob, 1e-9))) AS lines_ll,
           avg(a.top_pick_hit::int) AS base_top, avg(b.top_pick_hit::int) AS lines_top
    FROM cfb_first_td_grades a
    JOIN cfb_first_td_grades b ON b.cfbd_game_id = a.cfbd_game_id AND b.version = ${LINES_VERSION}
    WHERE a.version = 'cfb-first-td-v1'`);
  const r = result.rows[0] as Record<string, unknown> | undefined;
  if (!r || !Number(r.games)) return null;
  return {
    games: Number(r.games), changed: Number(r.changed),
    baseLogLoss: Number(r.base_ll), linesLogLoss: Number(r.lines_ll),
    baseTopPick: Number(r.base_top), linesTopPick: Number(r.lines_top),
  };
}
