import "server-only";

import { sql } from "drizzle-orm";
import { db } from ".";
import { ensureFantasyFootballTables } from "./ensure-schema";
import { queryRows } from "./query-result";
import type { FantasyInjuryDetails } from "@/lib/fantasy-football/injury-display";

export type FantasyRankingRow = {
  playerId: number;
  name: string;
  position: string;
  team: string | null;
  rookie: boolean;
  byeWeek: number | null;
  injuryStatus: string | null;
  injuryDetails?: FantasyInjuryDetails | null;
  ecr: number | null;
  positionRank: number | null;
  ourRank: number | null;
  tier: number | null;
  adp: number | null;
  adpStdev: number | null;
  adpHigh: number | null;
  adpLow: number | null;
  adpSampleSize: number | null;
  // DraftKings' own Best Ball ADP -- a manual, cookie-gated capture (see
  // ingest/ff_dk_bestball_adp.py), distinct from the FFC-derived `adp` above.
  // Null for any player not captured in the most recent DK snapshot.
  dkBestBallAdp: number | null;
  dkBestBallRank: number | null;
  dkBestBallDraftPct: number | null;
  dkBestBallDraftGroupId: number | null;
  dkBestBallCapturedAt: string | null;
  yahooXRank: number | null;
  yahooAdp: number | null;
  yahooSourceOrder: number | null;
  yahooCapturedAt: string | null;
  projectionLow: number | null;
  projectionHigh: number | null;
  rankMin: number | null;
  rankMax: number | null;
  rankStd: number | null;
  projectedPoints: number | null;
  fantasyProsProjectedPoints: number | null;
  fantasyProsProjectionFetchedAt: string | null;
  fantasyProsProjectionUpdatedAt: string | null;
  ourProjectedPoints: number | null;
  games2025: number | null;
  fantasyPoints2025: number | null;
  positionFinish2025: number | null;
  positionFinishTieCount2025: number | null;
  projectionDetails: Record<string, unknown> | null;
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

export type FantasySourceDatasetHealth = {
  dataset: string;
  status: string;
  rowCount: number;
  matchedCount: number;
  unmatchedCount: number;
  fetchedAt: string;
  sourceUpdatedAt: string | null;
};

export type FantasyProsSourceHealth = {
  connected: boolean;
  stale: boolean;
  requiredDatasets: number;
  availableRequiredDatasets: number;
  latestFetchedAt: string | null;
  datasets: FantasySourceDatasetHealth[];
};

const FANTASYPROS_REQUIRED_DATASETS = [
  "players",
  "projections",
  "draft-rankings-std",
  "draft-rankings-half",
  "draft-rankings-ppr",
  "adp-std",
  "adp-half",
  "adp-ppr",
] as const;

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

export async function getFantasyProsSourceHealth(season: number): Promise<FantasyProsSourceHealth> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`WITH latest AS (
      SELECT DISTINCT ON (dataset) dataset,status,row_count::int AS "rowCount",
        matched_count::int AS "matchedCount",unmatched_count::int AS "unmatchedCount",
        fetched_at::text AS "fetchedAt",source_updated_at::text AS "sourceUpdatedAt"
      FROM ff_source_snapshots
      WHERE source='fantasypros' AND season=${season}
      ORDER BY dataset,fetched_at DESC,id DESC
    )
    SELECT dataset,status,"rowCount","matchedCount","unmatchedCount","fetchedAt","sourceUpdatedAt"
    FROM latest ORDER BY dataset`);
  const datasets = queryRows<FantasySourceDatasetHealth>(result);
  const available = new Set(
    datasets.filter((row) => row.status === "success" && row.rowCount > 0).map((row) => row.dataset),
  );
  const availableRequiredDatasets = FANTASYPROS_REQUIRED_DATASETS.filter((dataset) => available.has(dataset)).length;
  const timestamps = datasets.map((row) => Date.parse(row.fetchedAt)).filter(Number.isFinite);
  const latestMillis = timestamps.length ? Math.max(...timestamps) : null;
  return {
    connected: availableRequiredDatasets === FANTASYPROS_REQUIRED_DATASETS.length,
    stale: latestMillis === null || Date.now() - latestMillis > 12 * 60 * 60 * 1000,
    requiredDatasets: FANTASYPROS_REQUIRED_DATASETS.length,
    availableRequiredDatasets,
    latestFetchedAt: latestMillis === null ? null : new Date(latestMillis).toISOString(),
    datasets,
  };
}

export async function getFantasyRankings(rankingSetId: number): Promise<FantasyRankingRow[]> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`WITH scoring_context AS (
      SELECT COALESCE(scoring_profile->>'preset','PPR') AS scoring
      FROM ff_ranking_sets WHERE id=${rankingSetId}
    ), prior_points AS (
      SELECT sf.player_id,prior_player.position,
        CASE sc.scoring
          WHEN 'STD' THEN sf.fantasy_points_std
          WHEN 'HALF' THEN (sf.fantasy_points_std+sf.fantasy_points_ppr)/2.0
          ELSE sf.fantasy_points_ppr
        END AS fantasy_points
      FROM ff_player_season_features sf
      JOIN ff_players prior_player ON prior_player.id=sf.player_id
      CROSS JOIN scoring_context sc
      WHERE sf.season=2025 AND sf.source='nflverse'
    ), prior_position_finishes AS (
      SELECT player_id,
        RANK() OVER (PARTITION BY position ORDER BY fantasy_points DESC)::int AS position_finish,
        COUNT(*) OVER (PARTITION BY position,fantasy_points)::int AS position_finish_tie_count
      FROM prior_points WHERE fantasy_points IS NOT NULL
    )
    SELECT p.id::int AS "playerId",p.canonical_name AS name,
    p.position,p.team_abbrev AS team,p.rookie,p.bye_week AS "byeWeek",
    p.injury_status AS "injuryStatus",injury.details AS "injuryDetails",
    r.overall_rank AS ecr,r.position_rank AS "positionRank",
    r.our_rank AS "ourRank",r.tier,r.adp,
    (r.source_row->'adp'->>'stdev')::double precision AS "adpStdev",
    (r.source_row->'adp'->>'high')::double precision AS "adpHigh",
    (r.source_row->'adp'->>'low')::double precision AS "adpLow",
    (r.source_row->'adp'->>'times_drafted')::int AS "adpSampleSize",
    dk.average_draft_position AS "dkBestBallAdp",dk.rank AS "dkBestBallRank",
    dk.draft_percentage AS "dkBestBallDraftPct",dk.draft_group_id AS "dkBestBallDraftGroupId",
    dk.captured_at::text AS "dkBestBallCapturedAt",
    yahoo.xrank AS "yahooXRank",yahoo.adp AS "yahooAdp",
    yahoo.source_order AS "yahooSourceOrder",yahoo.captured_at::text AS "yahooCapturedAt",
    r.projected_points AS "projectedPoints",r.projection_low AS "projectionLow",r.projection_high AS "projectionHigh",
    r.rank_min AS "rankMin",r.rank_max AS "rankMax",r.rank_std AS "rankStd",
    fp.projected_points AS "fantasyProsProjectedPoints",
    fp.fetched_at::text AS "fantasyProsProjectionFetchedAt",
    fp.source_updated_at::text AS "fantasyProsProjectionUpdatedAt",
    r.our_projected_points AS "ourProjectedPoints",f.games AS "games2025",
    CASE COALESCE(rs.scoring_profile->>'preset','PPR')
      WHEN 'STD' THEN f.fantasy_points_std
      WHEN 'HALF' THEN (f.fantasy_points_std+f.fantasy_points_ppr)/2.0
      ELSE f.fantasy_points_ppr
    END AS "fantasyPoints2025",prior.position_finish AS "positionFinish2025",
    prior.position_finish_tie_count AS "positionFinishTieCount2025",
    r.projected_stats AS "projectionDetails",r.expected_games AS "expectedGames",
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
    LEFT JOIN prior_position_finishes prior ON prior.player_id=p.id
    LEFT JOIN ff_player_indicators i ON i.ranking_set_id=r.ranking_set_id AND i.player_id=p.id
    LEFT JOIN LATERAL (
      SELECT jsonb_build_object(
        'active',episode.active,'status',episode.status,
        'bodyPart',COALESCE(observation.body_part,episode.body_part),
        'injuryType',COALESCE(observation.injury_type,episode.injury_type),
        'description',observation.description,
        'practiceStatus',observation.practice_status,
        'expectedReturnMin',COALESCE(observation.expected_return_min,episode.expected_return_min)::text,
        'expectedReturnMax',COALESCE(observation.expected_return_max,episode.expected_return_max)::text,
        'weeksOutMin',COALESCE(observation.weeks_out_min,episode.weeks_out_min),
        'weeksOutMax',COALESCE(observation.weeks_out_max,episode.weeks_out_max),
        'availabilityProbability',COALESCE(observation.availability_probability,episode.confidence),
        'estimateBasis',episode.estimate_basis,'confidence',episode.confidence,
        'primarySource',episode.primary_source,'detailSource',observation.source,
        'sourceConflict',episode.source_conflict,
        'firstSeenAt',episode.first_seen_at::text,
        'lastConfirmedAt',episode.last_confirmed_at::text,
        'providerUpdatedAt',observation.provider_updated_at::text,
        'clearedAt',episode.cleared_at::text
      ) AS details
      FROM LATERAL (
        SELECT * FROM ff_player_injuries candidate
        WHERE candidate.player_id=p.id
          AND (candidate.active OR candidate.cleared_at >= NOW()-INTERVAL '48 hours')
        ORDER BY candidate.active DESC,
          COALESCE(candidate.cleared_at,candidate.last_confirmed_at) DESC
        LIMIT 1
      ) episode
      LEFT JOIN LATERAL (
        SELECT source,body_part,injury_type,description,practice_status,provider_updated_at,
          expected_return_min,expected_return_max,weeks_out_min,weeks_out_max,availability_probability
        FROM ff_player_injury_observations detail
        WHERE detail.player_id=p.id AND detail.observed_at>=episode.first_seen_at
          AND detail.observed_at<=COALESCE(episode.cleared_at,NOW()+INTERVAL '1 minute')
        ORDER BY (
          (detail.body_part IS NOT NULL)::int+
          (detail.description IS NOT NULL)::int+
          (detail.practice_status IS NOT NULL)::int+
          (detail.weeks_out_max IS NOT NULL)::int+
          (detail.expected_return_max IS NOT NULL)::int
        ) DESC,COALESCE(detail.provider_updated_at,detail.observed_at) DESC
        LIMIT 1
      ) observation ON TRUE
    ) injury ON TRUE
    LEFT JOIN LATERAL (
      SELECT v.projected_points,s.fetched_at,s.source_updated_at
      FROM ff_player_source_projections v
      JOIN ff_source_snapshots s ON s.id=v.source_snapshot_id
      WHERE v.player_id=p.id AND v.source='fantasypros' AND v.season=rs.season
        AND v.scoring=COALESCE(rs.scoring_profile->>'preset','PPR')
        AND s.status='success' AND s.dataset='projections'
      ORDER BY s.fetched_at DESC,s.id DESC LIMIT 1
    ) fp ON TRUE
    LEFT JOIN LATERAL (
      SELECT average_draft_position,rank,draft_percentage,draft_group_id,captured_at
      FROM ff_dk_bestball_adp
      WHERE player_id=p.id
      ORDER BY captured_at DESC,id DESC LIMIT 1
    ) dk ON TRUE
    LEFT JOIN LATERAL (
      SELECT yr.xrank,yr.adp,yr.source_order,yr.captured_at
      FROM ff_yahoo_predraft_rankings yr
      JOIN ff_source_snapshots ys ON ys.id=yr.source_snapshot_id
      WHERE yr.player_id=p.id AND yr.season=rs.season
        AND ys.source='yahoo' AND ys.dataset='predraft-rankings'
        AND ys.status IN ('success','partial')
      ORDER BY yr.captured_at DESC,yr.id DESC LIMIT 1
    ) yahoo ON TRUE
    WHERE r.ranking_set_id=${rankingSetId}
    GROUP BY p.id,r.id,rs.id,f.id,prior.position_finish,prior.position_finish_tie_count,
      fp.projected_points,fp.fetched_at,fp.source_updated_at,
      injury.details,
      dk.average_draft_position,dk.rank,dk.draft_percentage,dk.draft_group_id,dk.captured_at,
      yahoo.xrank,yahoo.adp,yahoo.source_order,yahoo.captured_at
    ORDER BY COALESCE(r.our_rank,r.overall_rank,9999),p.canonical_name`);
  return queryRows<FantasyRankingRow>(result);
}

export type FantasyAdpMover = {
  playerId: number;
  name: string;
  position: string;
  team: string | null;
  currentAdp: number;
  baselineAdp: number;
  // positive = riser (ADP went down, i.e. drafted earlier / more valued now)
  delta: number;
  latestCapturedAt: string;
  baselineCapturedAt: string;
};

export type FantasyAdpMovers = {
  risers: FantasyAdpMover[];
  fallers: FantasyAdpMover[];
  sinceHours: number;
  latestCapturedAt: string | null;
  earliestCapturedAt: string | null;
  hasEnoughHistory: boolean;
};

// Risers/fallers over `sinceHours` -- compares each player's latest captured
// ADP against the closest snapshot at or before (now - sinceHours). Captures
// come from ingest/ff_adp_snapshot.py on a 12-hour cadence
// (.github/workflows/refresh_ff_adp_snapshot.yml); ff_player_rankings.adp
// alone has no history, it's overwritten on every board rebuild.
export async function getFantasyAdpMovers(
  season: number,
  scoring: string,
  sinceHours: number,
  limit = 15,
): Promise<FantasyAdpMovers> {
  await ensureFantasyFootballTables();
  const cutoff = new Date(Date.now() - sinceHours * 60 * 60 * 1000).toISOString();
  const [moversResult, rangeResult] = await Promise.all([
    db.execute(sql`WITH latest AS (
        SELECT DISTINCT ON (player_id) player_id, adp, captured_at
        FROM ff_adp_snapshots
        WHERE season=${season} AND scoring=${scoring}
        ORDER BY player_id, captured_at DESC
      ), baseline AS (
        SELECT DISTINCT ON (player_id) player_id, adp AS baseline_adp, captured_at AS baseline_captured_at
        FROM ff_adp_snapshots
        WHERE season=${season} AND scoring=${scoring} AND captured_at <= ${cutoff}::timestamptz
        ORDER BY player_id, captured_at DESC
      )
      SELECT p.id::int AS "playerId",p.canonical_name AS name,p.position,p.team_abbrev AS team,
        l.adp AS "currentAdp",b.baseline_adp AS "baselineAdp",
        (b.baseline_adp - l.adp) AS delta,
        l.captured_at::text AS "latestCapturedAt",b.baseline_captured_at::text AS "baselineCapturedAt"
      FROM latest l
      JOIN baseline b ON b.player_id=l.player_id AND b.baseline_captured_at < l.captured_at
      JOIN ff_players p ON p.id=l.player_id
      ORDER BY delta DESC`),
    db.execute(sql`SELECT MIN(captured_at)::text AS "earliestCapturedAt",MAX(captured_at)::text AS "latestCapturedAt"
      FROM ff_adp_snapshots WHERE season=${season} AND scoring=${scoring}`),
  ]);
  const rows = queryRows<FantasyAdpMover>(moversResult);
  const range = queryRows<{ earliestCapturedAt: string | null; latestCapturedAt: string | null }>(rangeResult)[0];
  return {
    risers: rows.filter((row) => row.delta > 0).slice(0, limit),
    fallers: rows.filter((row) => row.delta < 0).slice(-limit).reverse(),
    sinceHours,
    latestCapturedAt: range?.latestCapturedAt ?? null,
    earliestCapturedAt: range?.earliestCapturedAt ?? null,
    hasEnoughHistory: rows.length > 0,
  };
}

export type FantasyAdpSnapshotHealth = {
  scoring: string;
  snapshotRows: number;
  captureRuns: number;
  earliestCapturedAt: string | null;
  latestCapturedAt: string | null;
};

export async function getFantasyAdpSnapshotHealth(season: number): Promise<FantasyAdpSnapshotHealth[]> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`SELECT scoring,COUNT(*)::int AS "snapshotRows",
      COUNT(DISTINCT captured_at)::int AS "captureRuns",
      MIN(captured_at)::text AS "earliestCapturedAt",MAX(captured_at)::text AS "latestCapturedAt"
    FROM ff_adp_snapshots WHERE season=${season} GROUP BY scoring ORDER BY scoring`);
  return queryRows<FantasyAdpSnapshotHealth>(result);
}

export type FantasyDkBestBallAdpHealth = {
  draftGroupId: number;
  playerCount: number;
  matchedCount: number;
  capturedAt: string;
};

// One row per DK draft-group capture (there is no automated cadence -- see
// ingest/ff_dk_bestball_adp.py -- so this exists purely to show the user how
// stale the most recent manual capture is, rather than implying live data.
export async function getDkBestBallAdpHealth(season: number): Promise<FantasyDkBestBallAdpHealth[]> {
  await ensureFantasyFootballTables();
  const result = await db.execute(sql`SELECT draft_group_id AS "draftGroupId",
      COUNT(*)::int AS "playerCount",COUNT(player_id)::int AS "matchedCount",
      captured_at::text AS "capturedAt"
    FROM ff_dk_bestball_adp WHERE season=${season}
    GROUP BY draft_group_id,captured_at ORDER BY captured_at DESC LIMIT 10`);
  return queryRows<FantasyDkBestBallAdpHealth>(result);
}

export type FantasyPercentileStat = {
  value: number | null;
  percentile: number | null;
};

export type FantasyPercentileProfile = {
  playerId: number;
  position: string;
  season: number;
  games: number;
  positionPoolSize: number;
  eligible: boolean;
  reason: string | null;
  stats: Record<string, FantasyPercentileStat>;
};

// Positions this profile supports. K/DST don't have a comparable stat model
// -- callers should treat a request for either as unsupported rather than
// silently returning an empty profile.
const PERCENTILE_PROFILE_POSITIONS = ["QB", "RB", "WR", "TE"] as const;
const PERCENTILE_MIN_GAMES = 4;

// Per-game/ratio percentile profile (PlayerProfiler-style), computed live
// against every RB/WR/TE with >= PERCENTILE_MIN_GAMES that season -- not
// precomputed/stored, so it's always consistent with whatever nflverse data
// is currently loaded. Advanced fields (EPA, air yards, WOPR, RACR) come from
// ff_player_season_features.source_row, which now stores the FULL raw
// nflverse row (see ingest/ff_independent.py::save_history) instead of the
// small curated subset the dedicated columns cover.
export async function getFantasyPercentileProfile(
  playerId: number,
  season: number,
  scoring: string,
): Promise<FantasyPercentileProfile | null> {
  await ensureFantasyFootballTables();
  const positionResult = await db.execute(sql`SELECT position FROM ff_players WHERE id=${playerId}`);
  const position = queryRows<{ position: string }>(positionResult)[0]?.position ?? null;
  if (!position) return null;
  if (!(PERCENTILE_PROFILE_POSITIONS as readonly string[]).includes(position)) {
    return {
      playerId, position, season, games: 0, positionPoolSize: 0, eligible: false,
      reason: `Percentile profiles aren't built for ${position} yet -- only QB/RB/WR/TE.`,
      stats: {},
    };
  }
  const points = sql`(CASE WHEN ${scoring}='STD' THEN f.fantasy_points_std WHEN ${scoring}='HALF' THEN (f.fantasy_points_std+f.fantasy_points_ppr)/2.0 ELSE f.fantasy_points_ppr END)`;
  const result = await db.execute(sql`WITH pool AS (
      SELECT p.id AS player_id, p.position, f.games,
        CASE WHEN f.games>0 THEN ${points}/f.games END AS fantasy_points_pg,
        CASE WHEN f.games>0 THEN f.carries/f.games END AS carries_pg,
        CASE WHEN f.games>0 THEN f.rushing_yards/f.games END AS rushing_yards_pg,
        CASE WHEN f.games>0 THEN f.rushing_tds/f.games END AS rushing_tds_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'rushing_epa')::double precision/f.games END AS rushing_epa_pg,
        CASE WHEN f.games>0 THEN f.targets/f.games END AS targets_pg,
        CASE WHEN f.games>0 THEN f.receptions/f.games END AS receptions_pg,
        CASE WHEN f.games>0 THEN f.receiving_yards/f.games END AS receiving_yards_pg,
        CASE WHEN f.games>0 THEN f.receiving_tds/f.games END AS receiving_tds_pg,
        f.target_share,
        CASE WHEN f.games>0 THEN (f.source_row->>'receiving_epa')::double precision/f.games END AS receiving_epa_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'receiving_air_yards')::double precision/f.games END AS receiving_air_yards_pg,
        (f.source_row->>'air_yards_share')::double precision AS air_yards_share,
        (f.source_row->>'wopr')::double precision AS wopr,
        (f.source_row->>'racr')::double precision AS racr,
        CASE WHEN f.games>0 THEN (f.source_row->>'attempts')::double precision/f.games END AS attempts_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'completions')::double precision/f.games END AS completions_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'passing_yards')::double precision/f.games END AS passing_yards_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'passing_tds')::double precision/f.games END AS passing_tds_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'passing_interceptions')::double precision/f.games END AS passing_interceptions_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'passing_epa')::double precision/f.games END AS passing_epa_pg,
        CASE WHEN f.games>0 THEN (f.source_row->>'passing_air_yards')::double precision/f.games END AS passing_air_yards_pg
      FROM ff_player_season_features f
      JOIN ff_players p ON p.id=f.player_id
      WHERE f.season=${season} AND f.source='nflverse' AND p.position IN ('QB','RB','WR','TE')
        AND f.games>=${PERCENTILE_MIN_GAMES}
    ), ranked AS (
      SELECT *,
        COUNT(*) OVER (PARTITION BY position) AS position_pool_size,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY fantasy_points_pg))::numeric*100)::int AS fantasy_points_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY carries_pg))::numeric*100)::int AS carries_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY rushing_yards_pg))::numeric*100)::int AS rushing_yards_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY rushing_tds_pg))::numeric*100)::int AS rushing_tds_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY rushing_epa_pg))::numeric*100)::int AS rushing_epa_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY targets_pg))::numeric*100)::int AS targets_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY receptions_pg))::numeric*100)::int AS receptions_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY receiving_yards_pg))::numeric*100)::int AS receiving_yards_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY receiving_tds_pg))::numeric*100)::int AS receiving_tds_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY target_share))::numeric*100)::int AS target_share_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY receiving_epa_pg))::numeric*100)::int AS receiving_epa_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY receiving_air_yards_pg))::numeric*100)::int AS receiving_air_yards_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY air_yards_share))::numeric*100)::int AS air_yards_share_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY wopr))::numeric*100)::int AS wopr_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY racr))::numeric*100)::int AS racr_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY attempts_pg))::numeric*100)::int AS attempts_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY completions_pg))::numeric*100)::int AS completions_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY passing_yards_pg))::numeric*100)::int AS passing_yards_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY passing_tds_pg))::numeric*100)::int AS passing_tds_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY passing_interceptions_pg))::numeric*100)::int AS passing_interceptions_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY passing_epa_pg))::numeric*100)::int AS passing_epa_pctl,
        ROUND((PERCENT_RANK() OVER (PARTITION BY position ORDER BY passing_air_yards_pg))::numeric*100)::int AS passing_air_yards_pctl
      FROM pool
    )
    SELECT player_id AS "playerId",position,games,position_pool_size AS "positionPoolSize",
      fantasy_points_pg AS "fantasyPoints",fantasy_points_pctl AS "fantasyPointsPctl",
      carries_pg AS "carries",carries_pctl AS "carriesPctl",
      rushing_yards_pg AS "rushingYards",rushing_yards_pctl AS "rushingYardsPctl",
      rushing_tds_pg AS "rushingTds",rushing_tds_pctl AS "rushingTdsPctl",
      rushing_epa_pg AS "rushingEpa",rushing_epa_pctl AS "rushingEpaPctl",
      targets_pg AS "targets",targets_pctl AS "targetsPctl",
      receptions_pg AS "receptions",receptions_pctl AS "receptionsPctl",
      receiving_yards_pg AS "receivingYards",receiving_yards_pctl AS "receivingYardsPctl",
      receiving_tds_pg AS "receivingTds",receiving_tds_pctl AS "receivingTdsPctl",
      target_share AS "targetShare",target_share_pctl AS "targetSharePctl",
      receiving_epa_pg AS "receivingEpa",receiving_epa_pctl AS "receivingEpaPctl",
      receiving_air_yards_pg AS "receivingAirYards",receiving_air_yards_pctl AS "receivingAirYardsPctl",
      air_yards_share AS "airYardsShare",air_yards_share_pctl AS "airYardsSharePctl",
      wopr AS "wopr",wopr_pctl AS "woprPctl",
      racr AS "racr",racr_pctl AS "racrPctl",
      attempts_pg AS "attempts",attempts_pctl AS "attemptsPctl",
      completions_pg AS "completions",completions_pctl AS "completionsPctl",
      passing_yards_pg AS "passingYards",passing_yards_pctl AS "passingYardsPctl",
      passing_tds_pg AS "passingTds",passing_tds_pctl AS "passingTdsPctl",
      passing_interceptions_pg AS "passingInterceptions",passing_interceptions_pctl AS "passingInterceptionsPctl",
      passing_epa_pg AS "passingEpa",passing_epa_pctl AS "passingEpaPctl",
      passing_air_yards_pg AS "passingAirYards",passing_air_yards_pctl AS "passingAirYardsPctl"
    FROM ranked WHERE player_id=${playerId}`);
  const row = queryRows<Record<string, number | string | null>>(result)[0];
  if (!row) {
    return {
      playerId, position, season, games: 0, positionPoolSize: 0, eligible: false,
      reason: `No ${season} nflverse stats with at least ${PERCENTILE_MIN_GAMES} games for this player.`,
      stats: {},
    };
  }
  const statKeys = [
    "fantasyPoints", "carries", "rushingYards", "rushingTds", "rushingEpa",
    "targets", "receptions", "receivingYards", "receivingTds", "targetShare",
    "receivingEpa", "receivingAirYards", "airYardsShare", "wopr", "racr",
    "attempts", "completions", "passingYards", "passingTds", "passingInterceptions",
    "passingEpa", "passingAirYards",
  ];
  const stats: Record<string, FantasyPercentileStat> = {};
  for (const key of statKeys) {
    const rawValue = row[key];
    const rawPercentile = row[`${key}Pctl`];
    const value = typeof rawValue === "number" ? rawValue : rawValue === null ? null : Number(rawValue);
    // PERCENT_RANK() still returns a number (0) when every row in the
    // partition is NULL for this column -- e.g. before the next nflverse
    // refresh backfills source_row for older seasons (see
    // ingest/ff_independent.py::save_history). A percentile is only
    // meaningful when we actually have this player's raw value; never show
    // a computed percentile next to a missing value, since that reads as a
    // real (and misleadingly bad) 0th-percentile result.
    stats[key] = {
      value,
      percentile: value === null ? null : typeof rawPercentile === "number" ? rawPercentile : rawPercentile === null ? null : Number(rawPercentile),
    };
  }
  return {
    playerId, position, season,
    games: Number(row.games ?? 0),
    positionPoolSize: Number(row.positionPoolSize ?? 0),
    eligible: true,
    reason: null,
    stats,
  };
}

export type TeammateCorrelationRow = {
  playerAId: number;
  playerBId: number;
  relationshipType: string;
  sampleWeeks: number;
  shrunkCorrelation: number;
};

// Reflects real historical teammate history (see ingest/ff_teammate_correlation.py) --
// a player who has since changed teams simply has no row with their new teammates.
export async function getTeammateCorrelations(playerIds: number[]): Promise<TeammateCorrelationRow[]> {
  if (playerIds.length < 2) return [];
  await ensureFantasyFootballTables();
  // drizzle's sql`` tag already wraps an interpolated array in its own "(...)"
  // (renders "(${$1}, ${$2}, ...)"), valid for IN <array> -- an extra explicit
  // "(...)" around it double-wraps into an invalid row-value comparison, and
  // Postgres has no bare ANY(...) form for a plain param list either.
  // DISTINCT ON collapses to the latest season per pair once multiple seasons exist.
  // player_a_id/player_b_id are bigint columns -- raw sql`` execute (unlike the
  // typed drizzle query builder) skips the schema's mode:"number" mapping, so
  // the driver returns them as strings. Cast to int here so TeammateCorrelationRow's
  // declared `number` type is honest at runtime (a prior version of this query
  // returned strings here, which broke a strict === identity check downstream).
  const result = await db.execute(sql`SELECT DISTINCT ON (player_a_id, player_b_id)
    player_a_id::int AS "playerAId",player_b_id::int AS "playerBId",
    relationship_type AS "relationshipType",sample_weeks AS "sampleWeeks",
    shrunk_correlation AS "shrunkCorrelation"
    FROM ff_teammate_correlations
    WHERE player_a_id IN ${playerIds} AND player_b_id IN ${playerIds}
    ORDER BY player_a_id, player_b_id, season DESC`);
  return queryRows<TeammateCorrelationRow>(result);
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
  ourProjectedPoints: number | null;
  projectionLow: number | null;
  projectionHigh: number | null;
};

export type FantasyDraftState = {
  draft: {
    id: string; name: string; season: number; status: string; teamCount: number;
    controlledSlot: number; roundCount: number; currentPick: number; revision: number;
    rankingSetId: number; rosterConfig: Record<string, number>; scoringConfig: Record<string, number | string>;
    recommendationConfig: Record<string, unknown>;
  };
  board: DraftBoardSlot[];
  available: FantasyRankingRow[];
};

export async function getFantasyDraftState(draftId: string): Promise<FantasyDraftState | null> {
  await ensureFantasyFootballTables();
  const draftResult = await db.execute(sql`SELECT id::text,name,season,status,team_count AS "teamCount",
    controlled_slot AS "controlledSlot",round_count AS "roundCount",current_pick AS "currentPick",
    revision,ranking_set_id::int AS "rankingSetId",roster_config AS "rosterConfig",scoring_config AS "scoringConfig",
    recommendation_config AS "recommendationConfig"
    FROM ff_draft_sessions WHERE id=${draftId}::uuid`);
  const draft = queryRows<FantasyDraftState["draft"]>(draftResult)[0];
  if (!draft) return null;
  const boardResult = await db.execute(sql`WITH active_picks AS (
      SELECT e.* FROM ff_draft_events e LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=e.id
      WHERE e.draft_id=${draftId}::uuid AND e.event_type='pick_made' AND reversal.id IS NULL
    ) SELECT s.overall_pick AS "overallPick",s.round,s.pick_in_round AS "pickInRound",
      t.id::int AS "teamId",t.slot AS "teamSlot",t.name AS "teamName",t.is_controlled AS "isControlled",
      e.id::int AS "eventId",p.id::int AS "playerId",p.canonical_name AS "playerName",p.position,
      p.team_abbrev AS "nflTeam",r.our_projected_points AS "ourProjectedPoints",
      r.projection_low AS "projectionLow",r.projection_high AS "projectionHigh"
      FROM ff_draft_slots s JOIN ff_draft_teams t ON t.id=s.draft_team_id
      JOIN ff_draft_sessions d ON d.id=s.draft_id
      LEFT JOIN active_picks e ON e.overall_pick=s.overall_pick LEFT JOIN ff_players p ON p.id=e.player_id
      LEFT JOIN ff_player_rankings r ON r.ranking_set_id=d.ranking_set_id AND r.player_id=p.id
      WHERE s.draft_id=${draftId}::uuid ORDER BY s.overall_pick`);
  const all = await getFantasyRankings(draft.rankingSetId);
  const drafted = new Set(queryRows<DraftBoardSlot>(boardResult).flatMap((slot) => slot.playerId ? [slot.playerId] : []));
  
  // Apply roster-aware ADP adjustments to available players
  const rosterConfig = draft.rosterConfig as any;
  const leagueSize = draft.teamCount;
  const availableWithAdjustedAdp = all
    .filter((player) => !drafted.has(player.playerId))
    .map((player) => ({
      ...player,
      // Store original ADP and add adjusted version (computed client-side for now, could be server-side)
      baseAdp: player.adp,
    }));
  
  return { draft, board: queryRows<DraftBoardSlot>(boardResult), available: availableWithAdjustedAdp };
}
