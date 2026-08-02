import "server-only";

import { sql } from "drizzle-orm";
import { db } from ".";
import { ensureFantasyFootballTables } from "./ensure-schema";
import { queryRows } from "./query-result";

export type FantasyRankingRow = {
  playerId: number;
  name: string;
  position: string;
  team: string | null;
  rookie: boolean;
  byeWeek: number | null;
  injuryStatus: string | null;
  ecr: number | null;
  positionRank: number | null;
  ourRank: number | null;
  tier: number | null;
  adp: number | null;
  projectedPoints: number | null;
  ourProjectedPoints: number | null;
  games2025: number | null;
  fantasyPoints2025: number | null;
  expectedGames: number | null;
  confidence: number | null;
  indicators: Array<{ code: string; class: string; label: string; value: number | null; evidence: Record<string, unknown> }>;
};

export type FantasyRankingSetSummary = {
  id: number;
  season: number;
  name: string;
  scoring: string;
  createdAt: string;
  playerCount: number;
};

export type FantasyDraftSummary = {
  id: string;
  name: string;
  status: string;
  teamCount: number;
  controlledSlot: number;
  currentPick: number;
  totalPicks: number;
  updatedAt: string;
};

export async function getFantasyHomeData(): Promise<{
  rankingSets: FantasyRankingSetSummary[];
  drafts: FantasyDraftSummary[];
  latestSuccess: string | null;
  dataStale: boolean;
}> {
  await ensureFantasyFootballTables();
  const [setsResult, draftsResult, healthResult] = await Promise.all([
    db.execute(sql`WITH ranked_sets AS (
      SELECT rs.id, rs.season, rs.name,
        COALESCE(rs.scoring_profile->>'preset', 'PPR') AS scoring,
        rs.created_at::text AS "createdAt", COUNT(pr.id)::int AS "playerCount",
        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(rs.scoring_profile->>'preset', 'PPR')
          ORDER BY rs.created_at DESC, rs.id DESC
        ) AS recency
      FROM ff_ranking_sets rs LEFT JOIN ff_player_rankings pr ON pr.ranking_set_id=rs.id
      GROUP BY rs.id
    ) SELECT id,season,name,scoring,"createdAt","playerCount"
      FROM ranked_sets WHERE recency=1 ORDER BY "createdAt" DESC`),
    db.execute(sql`SELECT id::text, name, status, team_count AS "teamCount",
      controlled_slot AS "controlledSlot", current_pick AS "currentPick",
      (team_count*round_count)::int AS "totalPicks", updated_at::text AS "updatedAt"
      FROM ff_draft_sessions ORDER BY updated_at DESC LIMIT 12`),
    db.execute(sql`SELECT MAX(fetched_at)::text AS latest,
      COALESCE(MAX(fetched_at) < NOW()-INTERVAL '12 hours', TRUE) AS stale
      FROM ff_source_snapshots WHERE status='success'`),
  ]);
  const health = queryRows<{ latest: string | null; stale: boolean }>(healthResult)[0];
  return {
    rankingSets: queryRows<FantasyRankingSetSummary>(setsResult),
    drafts: queryRows<FantasyDraftSummary>(draftsResult),
    latestSuccess: health?.latest ?? null,
    dataStale: health?.stale ?? true,
  };
}

export async function getLatestRankingSet(scoring = "PPR"): Promise<FantasyRankingSetSummary | null> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`SELECT rs.id,rs.season,rs.name,
    COALESCE(rs.scoring_profile->>'preset','PPR') AS scoring,
    rs.created_at::text AS "createdAt",COUNT(pr.id)::int AS "playerCount"
    FROM ff_ranking_sets rs LEFT JOIN ff_player_rankings pr ON pr.ranking_set_id=rs.id
    WHERE COALESCE(rs.scoring_profile->>'preset','PPR')=${scoring}
    GROUP BY rs.id ORDER BY rs.created_at DESC LIMIT 1`);
  return queryRows<FantasyRankingSetSummary>(result)[0] ?? null;
}

export async function getFantasyRankings(rankingSetId: number): Promise<FantasyRankingRow[]> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`SELECT p.id::int AS "playerId",p.canonical_name AS name,
    p.position,p.team_abbrev AS team,p.rookie,p.bye_week AS "byeWeek",
    p.injury_status AS "injuryStatus",r.overall_rank AS ecr,r.position_rank AS "positionRank",
    r.our_rank AS "ourRank",r.tier,r.adp,r.projected_points AS "projectedPoints",
    r.our_projected_points AS "ourProjectedPoints",f.games AS "games2025",
    CASE COALESCE(rs.scoring_profile->>'preset','PPR')
      WHEN 'STD' THEN f.fantasy_points_std
      WHEN 'HALF' THEN (f.fantasy_points_std+f.fantasy_points_ppr)/2.0
      ELSE f.fantasy_points_ppr
    END AS "fantasyPoints2025",r.expected_games AS "expectedGames",
    r.confidence,COALESCE(jsonb_agg(jsonb_build_object('code',i.indicator_code,
      'class',i.indicator_class,'label',i.label,'value',i.metric_value,'evidence',i.evidence)
      ORDER BY CASE WHEN i.indicator_code='NEW_TEAM' THEN 1
        WHEN i.indicator_code='TOP_3_POSITION_POINTS' THEN 2
        WHEN i.indicator_class='risk' THEN 3 WHEN i.indicator_class='role' THEN 4
        WHEN i.indicator_class='model' THEN 5 ELSE 6 END)
      FILTER (WHERE i.id IS NOT NULL),'[]'::jsonb) AS indicators
    FROM ff_player_rankings r JOIN ff_players p ON p.id=r.player_id
    JOIN ff_ranking_sets rs ON rs.id=r.ranking_set_id
    LEFT JOIN ff_player_season_features f ON f.player_id=p.id AND f.season=2025 AND f.source='nflverse'
    LEFT JOIN ff_player_indicators i ON i.ranking_set_id=r.ranking_set_id AND i.player_id=p.id
    WHERE r.ranking_set_id=${rankingSetId}
    GROUP BY p.id,r.id,rs.id,f.id ORDER BY COALESCE(r.our_rank,r.overall_rank,9999),p.canonical_name`);
  return queryRows<FantasyRankingRow>(result);
}

export type DraftBoardSlot = {
  overallPick: number;
  round: number;
  pickInRound: number;
  teamId: number;
  teamSlot: number;
  teamName: string;
  isControlled: boolean;
  eventId: number | null;
  playerId: number | null;
  playerName: string | null;
  position: string | null;
  nflTeam: string | null;
};

export type FantasyDraftState = {
  draft: {
    id: string; name: string; season: number; status: string; teamCount: number;
    controlledSlot: number; roundCount: number; currentPick: number; revision: number;
    rankingSetId: number; rosterConfig: Record<string, number>; scoringConfig: Record<string, number | string>;
  };
  board: DraftBoardSlot[];
  available: FantasyRankingRow[];
};

export async function getFantasyDraftState(draftId: string): Promise<FantasyDraftState | null> {
  await ensureFantasyFootballTables();
  const draftResult = await db.execute(sql`SELECT id::text,name,season,status,team_count AS "teamCount",
    controlled_slot AS "controlledSlot",round_count AS "roundCount",current_pick AS "currentPick",
    revision,ranking_set_id::int AS "rankingSetId",roster_config AS "rosterConfig",scoring_config AS "scoringConfig"
    FROM ff_draft_sessions WHERE id=${draftId}::uuid`);
  const draft = queryRows<FantasyDraftState["draft"]>(draftResult)[0];
  if (!draft) return null;
  const boardResult = await db.execute(sql`WITH active_picks AS (
      SELECT e.* FROM ff_draft_events e LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=e.id
      WHERE e.draft_id=${draftId}::uuid AND e.event_type='pick_made' AND reversal.id IS NULL
    ) SELECT s.overall_pick AS "overallPick",s.round,s.pick_in_round AS "pickInRound",
      t.id::int AS "teamId",t.slot AS "teamSlot",t.name AS "teamName",t.is_controlled AS "isControlled",
      e.id::int AS "eventId",p.id::int AS "playerId",p.canonical_name AS "playerName",p.position,
      p.team_abbrev AS "nflTeam" FROM ff_draft_slots s JOIN ff_draft_teams t ON t.id=s.draft_team_id
      LEFT JOIN active_picks e ON e.overall_pick=s.overall_pick LEFT JOIN ff_players p ON p.id=e.player_id
      WHERE s.draft_id=${draftId}::uuid ORDER BY s.overall_pick`);
  const all = await getFantasyRankings(draft.rankingSetId);
  const drafted = new Set(queryRows<DraftBoardSlot>(boardResult).flatMap((slot) => slot.playerId ? [slot.playerId] : []));
  return { draft, board: queryRows<DraftBoardSlot>(boardResult), available: all.filter((player) => !drafted.has(player.playerId)) };
}
