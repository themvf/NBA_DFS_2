import { createHash } from 'node:crypto';
import { sameNflTeam } from './availability';

export type DefensiveMode = 'off' | 'experimental' | 'approved';
export type CaptureProfile = 'pfr-efficiency' | 'allowed-rushing-volume';
export type DefensiveProfile = CaptureProfile | 'gpp-integrated';
export type DefensiveSettings = { mode: DefensiveMode; profile: DefensiveProfile };
export type ForecastSummary = {
  mean: number; p10: number; p50: number; p90: number; boom: number;
  stat_means: Record<string, number>;
};
export type DefensiveForecastBundle = {
  version: 'nfl-defensive-forecast-v1'; profile: DefensiveProfile; mode: DefensiveMode;
  status: 'applied' | 'baseline'; reason: string;
  baselineRunId: string; candidateRunId: string | null; capturedAt: string | null;
  artifactDigest: string | null; modelArtifactHash: string | null; matchupManifestHash: string | null;
  baseline: ForecastSummary; selected: ForecastSummary;
  ledger: Array<{ component: string; factor: number; points_delta: number }>;
  digest: string;
};

export type FrozenDefensiveCandidate = {
  player_id: number; dk_player_id: number; game_id: string; kickoff: string;
  baseline?: { model_proj_fpts?: number; floor_fpts?: number; median_fpts?: number; ceiling_fpts?: number; boom_rate?: number; stat_means?: Record<string, number> };
  shadow?: { status?: string; reason?: string; reproduction?: { passed?: boolean };
    baseline?: ForecastSummary; candidate?: ForecastSummary; ledger?: Array<{ component?: string; factor?: number; points_delta?: number }>;
    model_artifact_hash?: string; matchup_manifest_hash?: string };
};
export type DefensivePlayerInput = {
  dkPlayerId: number; ffPlayerId: number | null; gameKey: string | null; isOut: boolean;
  ourProj: number | null; floorFpts: number | null; medianFpts?: number | null;
  ceilingFpts: number | null; boomRate: number | null; statMeans?: Record<string, number>;
};
export type DefensiveCapture = {
  runId: string; baselineRunId: string; capturedAt: string; artifactDigest: string;
  candidate: FrozenDefensiveCandidate;
};

const numeric = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const close = (left: number, right: number) => Math.abs(left - right) <= 0.00011;
/**
 * nflverse game ids end `_{away}_{home}` in nflverse codes (LA for the Rams,
 * WAS), while the salary key uses DraftKings codes (LAR). Compare by franchise:
 * a string suffix match refused every Rams capture.
 */
const sameGame = (salaryKey: string | null, gameId: string) => {
  if (!salaryKey) return false;
  const [away, home] = salaryKey.split('@');
  const parts = gameId.split('_');
  if (!away || !home || parts.length < 2) return false;
  return sameNflTeam(parts[parts.length - 2], away) && sameNflTeam(parts[parts.length - 1], home);
};
const complete = (value: unknown): value is ForecastSummary => {
  const v = value as ForecastSummary | null;
  return !!v && [v.mean, v.p10, v.p50, v.p90, v.boom].every(numeric)
    && !!v.stat_means && Object.values(v.stat_means).every(numeric);
};
const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

/** Resolve only an immutable, fully reproduced distribution. No mean-only substitution. */
export function resolveDefensiveForecast(input: DefensivePlayerInput, baselineRunId: string,
  settings: DefensiveSettings, capture: DefensiveCapture | null): DefensiveForecastBundle {
  const saved: ForecastSummary = {
    mean: input.ourProj ?? 0, p10: input.floorFpts ?? 0,
    p50: input.medianFpts ?? 0, p90: input.ceilingFpts ?? 0,
    boom: input.boomRate ?? 0, stat_means: input.statMeans ?? {},
  };
  let reason = 'defensive_mode_off';
  let selected = saved;
  let ledger: DefensiveForecastBundle['ledger'] = [];
  const row = capture?.candidate;
  if (settings.mode !== 'off') {
    reason = !capture ? 'no_matching_pregame_capture'
      : input.isOut ? 'ineligible_or_out'
      : input.ffPlayerId == null || row?.player_id !== input.ffPlayerId || row.dk_player_id !== input.dkPlayerId ? 'identity_mismatch'
      : capture.baselineRunId !== baselineRunId ? 'baseline_run_mismatch'
      : !sameGame(input.gameKey, row.game_id) ? 'game_identity_mismatch'
      : !row?.shadow?.reproduction?.passed ? 'baseline_draws_not_reproduced'
      : row.shadow.status !== 'under_evaluation' ? row.shadow.reason ?? 'no_adjustment_for_player'
      : !complete(row.shadow.baseline) || !complete(row.shadow.candidate) ? 'incomplete_distribution'
      : ![input.ourProj, input.floorFpts, input.medianFpts, input.ceilingFpts, input.boomRate].every(numeric)
        || !close(row.shadow.baseline.mean, input.ourProj!)
        || !close(row.shadow.baseline.p10, input.floorFpts!)
        || !close(row.shadow.baseline.p50, input.medianFpts!)
        || !close(row.shadow.baseline.p90, input.ceilingFpts!)
        || !close(row.shadow.baseline.boom, input.boomRate!) ? 'saved_baseline_distribution_mismatch'
      : Object.keys(input.statMeans ?? {}).length === 0
        || Object.keys(input.statMeans ?? {}).length !== Object.keys(row.shadow.baseline.stat_means).length
        || Object.entries(input.statMeans ?? {}).some(([key, value]) => !numeric(row.shadow!.baseline!.stat_means[key]) || !close(value, row.shadow!.baseline!.stat_means[key]))
        ? 'saved_baseline_stats_mismatch'
      : 'applied';
    if (reason === 'applied') {
      selected = row!.shadow!.candidate!;
      ledger = (row!.shadow!.ledger ?? []).map(step => ({ component: step.component ?? 'unknown', factor: step.factor ?? 1, points_delta: step.points_delta ?? 0 }));
    }
  }
  const value = {
    version: 'nfl-defensive-forecast-v1' as const, profile: settings.profile, mode: settings.mode,
    status: reason === 'applied' ? 'applied' as const : 'baseline' as const, reason,
    baselineRunId, candidateRunId: capture?.runId ?? null, capturedAt: capture?.capturedAt ?? null,
    artifactDigest: capture?.artifactDigest ?? null,
    modelArtifactHash: row?.shadow?.model_artifact_hash ?? null,
    matchupManifestHash: row?.shadow?.matchup_manifest_hash ?? null,
    baseline: saved, selected, ledger,
  };
  return { ...value, digest: digest(value) };
}
