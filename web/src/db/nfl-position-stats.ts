import "server-only";

import { sql } from "drizzle-orm";
import { db } from "@/db";
import type { GameLeadersPage } from "@/lib/nfl/game-leaders-weeks";
import { getGameLeadersAvailability } from "@/db/nfl-game-leaders-availability";
import { ANCHOR_MARKETS, type PropQuote } from "@/lib/nfl-dfs/market-anchor";
import { buildWeek, nameKey, type GameInput, type PlayerInput, type WeekResult } from "@/lib/nfl/position-stats";

type SimPlayer = { identity: string; name: string; team: string; residual: boolean; mean: number };
type SavedGame = {
  game: { game_id: string; kickoff: string; home: string; away: string; completed: boolean };
  decision_at: string;
  metrics: Record<string, { players: SimPlayer[] }>;
};

export type PositionStatsWeek = WeekResult & {
  season: number; week: number; simulatedAt: string; projectionRun: { runId: string; asOf: string } | null;
  newestLine: string | null; availability: { checked: number; incomplete: number };
  skippedGames: { gameId: string; reason: string }[]; errors: string[];
};

const BOOKS = ["draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics", "pinnacle"];
const rows = (r: { rows: unknown[] }) => r.rows as Record<string, unknown>[];
const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString());
const optional = (v: unknown) => (v == null ? null : Number(v));

/**
 * The week the game-leaders simulation was saved for, with the DFS model's
 * positions and TD means, the latest pregame prop lines and the game odds.
 * Each extra source can fail on its own; the page then says what is missing.
 */
export async function getNflPositionStatsWeek(saved: GameLeadersPage): Promise<PositionStatsWeek> {
  const games = saved.games as unknown as SavedGame[];
  const { season, week } = saved;
  const ids = [...new Set(games.flatMap((g) => Object.values(g.metrics).flatMap((m) => m.players.filter((p) => !p.residual).map((p) => p.identity))))];
  const errors: string[] = [];
  const kickoffs = games.map((g) => Date.parse(g.game.kickoff));
  const from = new Date(Math.min(...kickoffs) - 3600_000).toISOString();
  const to = new Date(Math.max(...kickoffs) + 3600_000).toISOString();

  const [runResult, playerResult, propResult, oddsResult] = await Promise.allSettled([
    db.execute(sql`
      SELECT run_id AS "runId", as_of_at AS "asOf" FROM nfl_dfs_projection_runs
      WHERE season = ${season} AND week = ${week} AND as_of_at <= NOW()
      ORDER BY as_of_at DESC, created_at DESC LIMIT 1`),
    ids.length
      ? db.execute(sql`SELECT gsis_id AS id, position FROM ff_players
          WHERE gsis_id IN (${sql.join(ids.map((id) => sql`${id}`), sql`, `)})`)
      : Promise.resolve({ rows: [] }),
    games.length
      ? db.execute(sql`
          SELECT DISTINCT ON (p.event_id, p.player, p.market) p.player, p.market, p.commence_time, p.captured_at, p.books,
            ht.abbreviation AS home, at.abbreviation AS away
          FROM prop_odds_history p
          JOIN nfl_teams ht ON COALESCE(ht.odds_api_name, ht.name) = p.home_team_name
          JOIN nfl_teams at ON COALESCE(at.odds_api_name, at.name) = p.away_team_name
          WHERE p.sport = 'nfl' AND p.market IN (${sql.join(ANCHOR_MARKETS.map((m) => sql`${m}`), sql`, `)})
            AND p.commence_time BETWEEN ${from}::timestamptz AND ${to}::timestamptz
            AND p.captured_at < p.commence_time AND p.captured_at <= NOW()
          ORDER BY p.event_id, p.player, p.market, p.captured_at DESC`)
      : Promise.resolve({ rows: [] }),
    db.execute(sql`
      SELECT g.nflverse_game_id AS "gameId", odds.home_spread AS "homeSpread", odds.vegas_total AS total
      FROM nfl_season_games g
      LEFT JOIN LATERAL (
        SELECT h.home_spread, h.vegas_total FROM game_odds_history h
        WHERE h.sport = 'nfl' AND h.matchup_id = g.matchup_id AND h.captured_at < g.kickoff AND h.captured_at <= NOW()
        ORDER BY h.captured_at DESC, h.id DESC LIMIT 1
      ) odds ON TRUE
      WHERE g.season = ${season} AND g.week = ${week} AND g.game_type = 'REG'`),
  ]);

  // DFS model: positions and TD/yardage means from the newest run for this week.
  let projectionRun: PositionStatsWeek["projectionRun"] = null;
  const model = new Map<string, { position: string; means: PlayerInput["model"]; status: string }>();
  if (runResult.status === "fulfilled" && rows(runResult.value)[0]) {
    const r = rows(runResult.value)[0];
    projectionRun = { runId: String(r.runId), asOf: iso(r.asOf) };
    try {
      const proj = await db.execute(sql`
        SELECT player_gsis_id AS id, position, projection_status AS status,
          stat_means->'rushing_yards' AS ry, stat_means->'receiving_yards' AS recy,
          stat_means->'rushing_tds' AS rtd, stat_means->'receiving_tds' AS rectd
        FROM nfl_dfs_player_projections WHERE run_id = ${projectionRun.runId} AND player_gsis_id IS NOT NULL`);
      for (const p of rows(proj)) {
        model.set(String(p.id), { position: String(p.position), status: String(p.status), means: {
          rushing_yards: optional(p.ry), receiving_yards: optional(p.recy), rushing_tds: optional(p.rtd), receiving_tds: optional(p.rectd) } });
      }
    } catch (e) { errors.push(`DFS model rows could not be read, so touchdowns without a line are missing: ${e instanceof Error ? e.message : e}`); }
  } else {
    errors.push(runResult.status === "rejected" ? "The DFS projection run could not be read." : `No DFS projection run is saved for ${season} week ${week}; touchdowns without a line are missing.`);
  }
  const rosterPosition = new Map<string, string>();
  if (playerResult.status === "fulfilled") {
    for (const p of rows(playerResult.value)) if (p.position) rosterPosition.set(String(p.id), String(p.position));
  } else errors.push("Roster positions could not be read.");

  // Prop lines grouped by game (home|away) and player name key; a name shared by two rows is dropped.
  const props = new Map<string, Map<string, Record<string, PropQuote> | null>>();
  let newestLine: string | null = null;
  if (propResult.status === "fulfilled") {
    const perEvent = new Map<string, Record<string, PropQuote>>();
    for (const r of rows(propResult.value)) {
      const books = (r.books ?? {}) as Record<string, PropQuote | undefined>;
      const book = BOOKS.find((b) => books[b]);
      if (!book) continue;
      const gameKey = `${r.home}|${r.away}`;
      const game = games.find((g) => `${g.game.home}|${g.game.away}` === gameKey && Math.abs(Date.parse(g.game.kickoff) - Date.parse(String(r.commence_time))) < 7200_000);
      if (!game) continue;
      const id = `${gameKey}|${String(r.player)}`;
      const quote = books[book]!;
      const lines = perEvent.get(id) ?? {};
      lines[String(r.market)] = { line: quote.line ?? null, over: quote.over ?? null, under: quote.under ?? null, yes: quote.yes ?? null };
      perEvent.set(id, lines);
      const at = iso(r.captured_at);
      if (!newestLine || at > newestLine) newestLine = at;
    }
    for (const [id, lines] of perEvent) {
      const [home, away, player] = id.split("|");
      const byName = props.get(`${home}|${away}`) ?? new Map();
      const key = nameKey(player);
      byName.set(key, byName.has(key) ? null : lines);
      props.set(`${home}|${away}`, byName);
    }
  } else errors.push("Prop lines could not be read, so every player keeps the simulation's numbers.");

  const odds = new Map<string, { homeSpread: number | null; total: number | null }>();
  if (oddsResult.status === "fulfilled") for (const r of rows(oddsResult.value)) odds.set(String(r.gameId), { homeSpread: optional(r.homeSpread), total: optional(r.total) });
  else errors.push("Game odds could not be read, so implied team totals are missing.");

  const availability = await Promise.all(games.map((g) => getGameLeadersAvailability(season, week, g.game.away, g.game.home, g.game.kickoff).catch(() => null)));
  const gameInputs: GameInput[] = games.map((g, i) => {
    const out = new Set((availability[i]?.complete ? availability[i]!.confirmedOut : []).map((p) => p.identity));
    const byId = new Map<string, PlayerInput>();
    for (const [metric, family] of Object.entries(g.metrics)) {
      for (const p of family.players) {
        if (p.residual) continue;
        const m = model.get(p.identity);
        const entry = byId.get(p.identity) ?? {
          identity: p.identity, name: p.name, team: p.team,
          position: m?.position ?? rosterPosition.get(p.identity) ?? null,
          sim: { rushing_yards: null, receptions: null, receiving_yards: null },
          model: m?.means ?? null,
          lines: props.get(`${g.game.home}|${g.game.away}`)?.get(nameKey(p.name)) ?? null,
          ruledOut: out.has(p.identity) || m?.status === "out",
        };
        if (metric === "rushing_yards" || metric === "receptions" || metric === "receiving_yards") entry.sim[metric] = p.mean;
        byId.set(p.identity, entry);
      }
    }
    const o = odds.get(g.game.game_id);
    const simTeamReceivingYards: Record<string, number> = {};
    for (const p of g.metrics.receiving_yards?.players ?? []) simTeamReceivingYards[p.team] = (simTeamReceivingYards[p.team] ?? 0) + p.mean;
    return { gameId: g.game.game_id, kickoff: g.game.kickoff, home: g.game.home, away: g.game.away,
      homeSpread: o?.homeSpread ?? null, total: o?.total ?? null, players: [...byId.values()], simTeamReceivingYards };
  });

  return { ...buildWeek(gameInputs), season, week, simulatedAt: saved.decision_at, projectionRun, newestLine,
    availability: { checked: availability.filter((a) => a?.complete).length, incomplete: availability.filter((a) => !a?.complete).length },
    skippedGames: (saved.skipped as { game_id: string; reason: string }[]).map((s) => ({ gameId: s.game_id, reason: s.reason })), errors };
}
