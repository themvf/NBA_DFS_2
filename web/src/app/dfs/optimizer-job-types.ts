import type { Sport } from "@/db/queries";
import type { OptimizerLineupAttemptDebug, OptimizerProbeDebug, OptimizerDebugInfo } from "./optimizer-debug";
import type { OptimizerPlayer, OptimizerSettings, LineupSlot } from "./optimizer";
import type { MlbOptimizerPlayer, MlbOptimizerSettings, MlbLineupSlot } from "./mlb-optimizer";
import type { NormalizedNbaRuleSelections } from "./nba-optimizer-rules";
import type { NormalizedMlbRuleSelections } from "./mlb-optimizer-rules";
import type { OptimizerMode } from "./optimizer-mode";

export type OptimizerJobStatus = "queued" | "running" | "completed" | "failed";

export type OptimizerJobSettings = OptimizerSettings | MlbOptimizerSettings;

export type OptimizerJobTerminationReason =
  | OptimizerDebugInfo["terminationReason"]
  | "stale";

export type OptimizerJobSnapshot = {
  sport: Sport;
  slateId: number;
  selectedMatchupIds: number[];
  requestedLineups: number;
  mode: OptimizerMode;
};

export type NbaPreparedOptimizerRun = {
  sport: "nba";
  mode: OptimizerMode;
  requestedLineups: number;
  maxExposureCount: number;
  eligibleCount: number;
  pool: OptimizerPlayer[];
  ruleSelections: NormalizedNbaRuleSelections;
  effectiveSettings: {
    minStack: number;
    teamStackCount: number;
    bringBackEnabled: boolean;
    bringBackSize: number;
    ceilingBoost: boolean;
    ceilingCount: number;
    maxExposure: number;
    minChanges: number;
    salaryFloor: number;
  };
  /** NBA ceiling boost by player id — serializable for job persistence. */
  ceilingBonusRecord: Record<number, number>;
  relaxedConstraints: string[];
  probeSummary: OptimizerProbeDebug[];
};

export type MlbPreparedOptimizerRun = {
  sport: "mlb";
  mode: OptimizerMode;
  requestedLineups: number;
  maxExposureCount: number;
  eligibleCount: number;
  pool: MlbOptimizerPlayer[];
  ruleSelections: NormalizedMlbRuleSelections;
  effectiveSettings: {
    minStack: number;
    bringBackThreshold: number;
    maxExposure: number;
    minChanges: number;
    antiCorrMax: number;
    pendingLineupPolicy: "ignore" | "downgrade" | "exclude";
  };
  /** HR correlation bonus by player id — serializable as plain object for job persistence. */
  hrBonusRecord: Record<number, number>;
  /** Pitcher ceiling boost by player id — serializable for job persistence. */
  pitcherCeilingBonusRecord: Record<number, number>;
  relaxedConstraints: string[];
  probeSummary: OptimizerProbeDebug[];
};

export type PreparedOptimizerRun = NbaPreparedOptimizerRun | MlbPreparedOptimizerRun;

export type MlbLineupHrSignalPlayer = {
  playerId: number;
  dkPlayerId: number | null;
  name: string;
  teamAbbrev: string;
  eligiblePositions: string;
  salary: number;
  lineupOrder: number | null;
  projection: number | null;
  projectedOwnership: number | null;
  hrProb1Plus: number | null;
  expectedHr: number | null;
  marketHrProb: number | null;
  hrEdgePct: number | null;
  hrInfluenceScore: number;
  gpp2OrderBonus: number;
  hrCorrelationBonus: number;
  isHrTarget: boolean;
  isHighHrTarget: boolean;
  isOrder23: boolean;
  isPositiveEdge: boolean;
  isTop15Hr: boolean;
  isTop15Edge: boolean;
};

export type MlbLineupHrSignal = {
  version: "mlb_dfs_hr_signal_v1";
  mode: OptimizerMode;
  hitterCount: number;
  hrTargetCount: number;
  highHrTargetCount: number;
  order23HitterCount: number;
  order23HrTargetCount: number;
  positiveEdgeCount: number;
  lowOwnedHrTargetCount: number;
  selectedTop15HrCount: number;
  selectedTop15EdgeCount: number;
  avgHrProb: number | null;
  maxHrProb: number | null;
  totalExpectedHr: number;
  avgExpectedHr: number | null;
  avgMarketHrProb: number | null;
  maxEdgePct: number | null;
  totalPositiveEdgePct: number;
  avgProjectedOwnership: number | null;
  totalHrInfluenceScore: number;
  gpp2OrderBonusTotal: number;
  hrCorrelationBonusTotal: number;
  players: MlbLineupHrSignalPlayer[];
};

export type NbaSlotPlayerIds = Record<LineupSlot, number>;
export type MlbSlotPlayerIds = Record<MlbLineupSlot, number>;
export type PersistedSlotPlayerIds = Record<string, number>;

export type PersistedOptimizerJobLineup = {
  lineupNumber: number;
  slotPlayerIds: PersistedSlotPlayerIds;
  playerIds: number[];
  totalSalary: number;
  projFpts: number;
  leverageScore: number;
  actualFpts: number | null;
  mlbHrSignal: MlbLineupHrSignal | null;
  durationMs: number;
  winningStage?: string;
  attempts: OptimizerLineupAttemptDebug[];
};

export type OptimizerJobTelemetry = {
  minMs: number | null;
  avgMs: number | null;
  maxMs: number | null;
  p95Ms: number | null;
};

export type OptimizerJobView = {
  id: number;
  sport: Sport;
  slateId: number;
  clientToken: string;
  status: OptimizerJobStatus;
  requestedLineups: number;
  builtLineups: number;
  eligibleCount: number | null;
  settings: OptimizerJobSettings;
  snapshot: OptimizerJobSnapshot;
  effectiveSettings: OptimizerDebugInfo["effectiveSettings"] | null;
  probeSummary: OptimizerProbeDebug[];
  relaxedConstraints: string[];
  selectedMatchupIds: number[];
  warning: string | null;
  error: string | null;
  terminationReason: OptimizerJobTerminationReason | null;
  workflowRunId: string | null;
  totalMs: number | null;
  probeMs: number | null;
  stale: boolean;
  createdAt: string | null;
  startedAt: string | null;
  heartbeatAt: string | null;
  finishedAt: string | null;
};

export type OptimizerJobStatusResponse = {
  ok: true;
  job: OptimizerJobView;
  debug: OptimizerDebugInfo | null;
  lineups: PersistedOptimizerJobLineup[];
  telemetry: OptimizerJobTelemetry;
};

export type OptimizerJobErrorResponse = {
  ok: false;
  error: string;
};

export type CreateOptimizerJobRequest = {
  sport: Sport;
  slateId: number;
  clientToken: string;
  selectedMatchupIds: number[];
  settings: OptimizerJobSettings;
};

export type CreateOptimizerJobResponse = {
  ok: boolean;
  jobId?: number;
  existing?: boolean;
  error?: string;
};
