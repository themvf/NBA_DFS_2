export type JointPageReport = {
  schema_version: number;
  status: 'generated' | 'not_generated';
  authority: string;
  game?: { game_id: string; away: string; home: string; kickoff: string };
  decision_at?: string;
  generated_at?: string;
  branch?: string;
  draws?: number;
  exit_model_enabled?: boolean;
  source_manifest?: { availability_verified?: boolean };
  metrics?: Record<string, { players: Array<{ identity: string; name: string; team: string; residual: boolean;
    mean: number; p10: number; p50: number; p90: number; leader_share: number }> }>;
  exact_top_three?: Record<string, { sets: Array<{ identities: string[]; names: string[]; probability: number }>;
    unresolved_identity_mass: number; enumeration_overflow_mass: number }>;
  market_reweighting?: { accepted: boolean; ess_fraction?: number; reason?: string | null } | null;
  limits?: string[];
};

export function readJointPageReport(input: unknown): JointPageReport {
  if (!input || typeof input !== 'object') throw new Error('Missing joint report');
  const report = input as JointPageReport;
  if (report.schema_version !== 1 || !['generated', 'not_generated'].includes(report.status) || !report.authority) {
    throw new Error('Invalid joint report schema');
  }
  if (report.status === 'generated') {
    if (!report.game || !report.decision_at || !Number.isFinite(Date.parse(report.game.kickoff)) ||
      !Number.isFinite(Date.parse(report.decision_at)) || Date.parse(report.decision_at) >= Date.parse(report.game.kickoff) ||
      !report.metrics || !report.exact_top_three || !report.draws || report.draws < 2) throw new Error('Invalid joint report provenance');
    for (const metric of ['receptions', 'receiving_yards', 'rushing_yards', 'total_yards']) {
      const rows = report.metrics[metric]?.players;
      const sets = report.exact_top_three[metric]?.sets;
      if (!rows?.length || !sets || rows.some(p => !p.identity || !Number.isFinite(p.mean) ||
        !Number.isFinite(p.p10) || !Number.isFinite(p.p90) || p.p10 > p.p90 ||
        !Number.isFinite(p.leader_share) || p.leader_share < 0 || p.leader_share > 1)) {
        throw new Error('Invalid joint report probabilities/ranges');
      }
      if (Math.abs(rows.reduce((s, p) => s + p.leader_share, 0) - 1) > 1e-8 ||
        sets.some(s => s.identities.length !== 3 || s.names.length !== 3 || s.probability < 0 || s.probability > 1 || !Number.isFinite(s.probability))) {
        throw new Error('Invalid full-field report normalization');
      }
    }
  }
  return report;
}
