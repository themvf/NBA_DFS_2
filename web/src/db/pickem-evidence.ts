import "server-only";
import { sql } from "drizzle-orm";
import { db } from "@/db";
import { ensurePickemTables } from "@/db/ensure-schema";
import { noVigHome, recentForm, safeSourceUrl, timestamp, type MarketQuote, type PickemEvidence,
  type PickemNews, type Performance } from "@/lib/nfl/pickem-evidence";

type Row = Record<string, unknown>;
const num = (v: unknown) => v == null ? null : Number(v);
const iso = (v: unknown) => v == null ? null : new Date(String(v)).toISOString();

/** Independent feeds fail visibly. No post-kickoff quotes or news in historical games. */
export async function getPickemEvidence(season: number): Promise<PickemEvidence> {
  await ensurePickemTables();
  const loadedAt = new Date().toISOString();
  const schedule = await db.execute(sql`
    SELECT g.id, g.week, g.kickoff, g.matchup_id, g.market_home_ml, g.market_away_ml,
      g.market_spread_line, g.market_captured_at,
      h.abbreviation home, a.abbreviation away
    FROM nfl_season_games g
    JOIN nfl_teams h ON h.team_id = g.home_team_id
    JOIN nfl_teams a ON a.team_id = g.away_team_id
    WHERE g.season = ${season} AND g.game_type = 'REG'`);
  const games: PickemEvidence["games"] = {};
  const byId = new Map<number, Row>();
  for (const row of schedule.rows as Row[]) {
    byId.set(Number(row.id), row);
    games[Number(row.id)] = { opening: null, latest: null, news: [], performance: [] };
  }
  const feeds = await Promise.allSettled([
    db.execute(sql`
      SELECT g.id game_id, q.* FROM nfl_season_games g
      JOIN LATERAL (
        (SELECT home_ml, away_ml, home_spread, captured_at FROM game_odds_history
         WHERE sport = 'nfl' AND matchup_id = g.matchup_id
           AND captured_at < g.kickoff AND captured_at <= ${loadedAt}::timestamptz
           AND (home_ml IS NOT NULL OR home_spread IS NOT NULL)
         ORDER BY captured_at, id LIMIT 1)
        UNION ALL
        (SELECT home_ml, away_ml, home_spread, captured_at FROM game_odds_history
         WHERE sport = 'nfl' AND matchup_id = g.matchup_id
           AND captured_at < g.kickoff AND captured_at <= ${loadedAt}::timestamptz
           AND (home_ml IS NOT NULL OR home_spread IS NOT NULL)
         ORDER BY captured_at DESC, id DESC LIMIT 1)
      ) q ON TRUE WHERE g.season = ${season}`),
    db.execute(sql`
      SELECT n.* FROM pickem_news n JOIN nfl_season_games g ON g.id = n.game_id
      WHERE g.season = ${season} AND COALESCE(n.published_at, n.observed_at) < g.kickoff
        AND COALESCE(n.published_at, n.observed_at) <= ${loadedAt}::timestamptz
        AND n.observed_at < g.kickoff AND n.observed_at <= ${loadedAt}::timestamptz
      ORDER BY n.published_at DESC, n.id DESC`),
    db.execute(sql`
      SELECT DISTINCT ON (g.id, p.id) g.id game_id, p.canonical_name, p.position,
        p.team_abbrev, o.id, o.normalized_status, o.body_part, o.practice_status,
        o.description, o.source, o.observed_at, o.provider_updated_at,
        COALESCE(o.raw_payload->>'url', s.request_params->>'url') source_url
      FROM nfl_season_games g
      JOIN nfl_teams h ON h.team_id = g.home_team_id
      JOIN nfl_teams a ON a.team_id = g.away_team_id
      JOIN ff_players p ON p.season = g.season AND p.team_abbrev IN (h.abbreviation, a.abbreviation)
      JOIN ff_player_injury_observations o ON o.player_id = p.id AND o.season = g.season
      LEFT JOIN ff_source_snapshots s ON s.id = o.source_snapshot_id
      WHERE g.season = ${season} AND o.observed_at < g.kickoff
        AND o.observed_at <= ${loadedAt}::timestamptz
        AND o.observed_at >= g.kickoff - INTERVAL '7 days'
        AND (o.provider_updated_at IS NULL OR
          (o.provider_updated_at < g.kickoff AND o.provider_updated_at <= ${loadedAt}::timestamptz))
      ORDER BY g.id, p.id, o.observed_at DESC, o.id DESC`),
    db.execute(sql`
      SELECT g.id game_id, g.week, p.posteam team,
        COUNT(*) FILTER (WHERE p.play_type IN ('run','pass'))::int plays,
        AVG(p.epa) FILTER (WHERE p.play_type IN ('run','pass')) epa,
        AVG(p.success::int) FILTER (WHERE p.play_type IN ('run','pass')) success,
        SUM(p.yards_gained) FILTER (WHERE p.play_type = 'run') rush_yards,
        COUNT(*) FILTER (WHERE p.turnover_type IN ('interception','fumble_lost') AND p.play_type <> 'no_play')::int turnovers,
        COUNT(*) FILTER (WHERE p.play_type = 'field_goal' AND p.st_outcome = 'made')::int field_goals,
        COUNT(DISTINCT p.drive) FILTER (WHERE p.drive_score_against_mechanism IN ('pass_return','fumble_return'))::int defensive_return_tds_allowed,
        COUNT(DISTINCT p.drive) FILTER (WHERE p.drive_score_against_mechanism IN ('punt','field_goal'))::int kick_return_tds_allowed
      FROM nfl_season_games g JOIN nfl_pbp_archetypes p ON p.game_id = g.nflverse_game_id
      WHERE g.season = ${season} AND g.completed = TRUE
      GROUP BY g.id, g.week, p.posteam`),
  ]);
  const names = ["Odds history", "Reviewed news", "Availability feed", "Prior-game play data"];
  const warnings: string[] = [];
  const rows = feeds.map((result, i): Row[] => {
    if (result.status === "fulfilled") return result.value.rows as Row[];
    console.error(`Pick'em ${names[i]} unavailable`, result.reason);
    warnings.push(`${names[i]} unavailable; coverage is incomplete.`);
    return [];
  });
  const quotes = new Map<number, MarketQuote[]>();
  for (const r of rows[0]) {
    const id = Number(r.game_id);
    if (!quotes.has(id)) quotes.set(id, []);
    quotes.get(id)!.push({ capturedAt: iso(r.captured_at)!, pHome: noVigHome(num(r.home_ml), num(r.away_ml)),
      homeSpread: num(r.home_spread), source: "Captured sportsbook consensus" });
  }
  for (const [id, r] of byId) {
    const captured = iso(r.market_captured_at);
    const list = quotes.get(id) ?? [];
    if (captured && timestamp(captured) < timestamp(iso(r.kickoff)) && timestamp(captured) <= timestamp(loadedAt)) {
      list.push({ capturedAt: captured, pHome: noVigHome(num(r.market_home_ml), num(r.market_away_ml)),
        homeSpread: r.market_spread_line == null ? null : -Number(r.market_spread_line), source: "Season market capture" });
    }
    list.sort((a, b) => timestamp(a.capturedAt) - timestamp(b.capturedAt));
    games[id].opening = list.length > 1 && list[0].capturedAt !== list.at(-1)!.capturedAt ? list[0] : null;
    games[id].latest = list.at(-1) ?? null;
  }
  for (const r of rows[1]) {
    games[Number(r.game_id)]?.news.push({ id: `review-${r.id}`, team: String(r.team),
      category: r.category as PickemNews["category"], headline: String(r.headline), detail: String(r.detail),
      status: r.status as PickemNews["status"], source: String(r.source), url: safeSourceUrl(String(r.url)),
      publishedAt: iso(r.published_at), observedAt: iso(r.observed_at)! });
  }
  for (const r of rows[2]) {
    // Latest observations are selected BEFORE filtering healthy, so old OUTs do not resurface.
    if (["HEALTHY", "UNKNOWN"].includes(String(r.normalized_status))) continue;
    const game = byId.get(Number(r.game_id))!;
    const cutoff = Math.min(timestamp(loadedAt), timestamp(iso(game.kickoff)));
    // Do not carry a prior week's inactive designation into this week's preview.
    if (cutoff - timestamp(iso(r.observed_at)) > 48 * 3600_000) continue;
    if (!["IR", "PUP", "NFI", "SUSPENDED"].includes(String(r.normalized_status)) &&
        r.provider_updated_at != null && timestamp(iso(r.provider_updated_at)) < timestamp(iso(game.kickoff)) - 6 * 86400_000) continue;
    const position = String(r.position);
    games[Number(r.game_id)]?.news.push({ id: `availability-${r.id}`, team: String(r.team_abbrev),
      category: position === "QB" ? "quarterback" : ["OT","G","OG","C","OL"].includes(position)
        ? "offensive-line" : ["RB","WR","TE","FB"].includes(position) ? "playmaker" : "defense",
      headline: `${r.canonical_name} (${position}): ${r.normalized_status}`,
      detail: [r.body_part, r.practice_status, r.description].filter(Boolean).join(" · "),
      status: ["QUESTIONABLE","DOUBTFUL"].includes(String(r.normalized_status)) ? "uncertain" : "reported",
      source: String(r.source), url: r.source_url ? safeSourceUrl(String(r.source_url)) : null,
      publishedAt: iso(r.provider_updated_at), observedAt: iso(r.observed_at)! });
  }
  const performance: Performance[] = rows[3].map(r => ({ gameId: Number(r.game_id), team: String(r.team),
    week: Number(r.week), plays: Number(r.plays), epaPerPlay: num(r.epa), successRate: num(r.success),
    rushYards: num(r.rush_yards), turnovers: Number(r.turnovers), fieldGoalsMade: Number(r.field_goals),
    defensiveReturnTdsAllowed: Number(r.defensive_return_tds_allowed), kickReturnTdsAllowed: Number(r.kick_return_tds_allowed) }));
  for (const [id, g] of byId) {
    games[id].recentForm = [String(g.home), String(g.away)].map(team => recentForm(performance, team, Number(g.week)));
    for (const team of [String(g.home), String(g.away)]) {
      const previous = [...byId.values()].filter(p => Number(p.week) < Number(g.week) && (p.home === team || p.away === team))
        .sort((a, b) => Number(b.week) - Number(a.week))[0];
      if (previous) {
        for (const p of performance.filter(p => p.gameId === Number(previous.id))) {
          if (!games[id].performance.some(x => x.gameId === p.gameId && x.team === p.team)) games[id].performance.push(p);
        }
      }
    }
    const priority = (n: PickemNews) => (n.id.startsWith("review-") ? 10 : 0) +
      (n.category === "quarterback" ? 5 : ["offensive-line", "defense"].includes(n.category) ? 3 : 0);
    games[id].news.sort((a, b) => priority(b) - priority(a) ||
      timestamp(b.publishedAt ?? b.observedAt) - timestamp(a.publishedAt ?? a.observedAt));
  }
  return { loadedAt, games, warnings };
}
