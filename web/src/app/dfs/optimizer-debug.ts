import type { OptimizerMode } from "./optimizer-mode";

export type OptimizerProbeDebug = {
  label: string;
  success: boolean;
  durationMs: number;
};

export type OptimizerLineupAttemptDebug = {
  stage: string;
  success: boolean;
  durationMs: number;
  prunedCandidateCount?: number;
  templateCount?: number;
  templatesTried?: number;
  repairAttempts?: number;
  rejectedByReason?: Record<string, number>;
  failureReason?: string;
};

export type OptimizerLineupDebug = {
  lineupNumber: number;
  status: "built" | "failed";
  durationMs: number;
  winningStage?: string;
  attempts: OptimizerLineupAttemptDebug[];
};

export type OptimizerDebugInfo = {
  sport: "nba" | "mlb";
  mode: OptimizerMode;
  eligibleCount: number;
  requestedLineups: number;
  builtLineups: number;
  totalMs: number;
  probeMs: number;
  maxExposureCount: number;
  relaxedConstraints: string[];
  probeSummary: OptimizerProbeDebug[];
  lineupSummaries: OptimizerLineupDebug[];
  terminationReason: "completed" | "insufficient_pool" | "probe_infeasible" | "lineup_failed";
  stoppedAtLineup?: number;
  heuristic?: {
    prunedCandidateCount: number;
    templateCount: number;
    templatesTried: number;
    repairAttempts: number;
    rejectedByReason: Record<string, number>;
  };
  effectiveSettings: {
    minStack: number;
    teamStackCount?: number;
    bringBackEnabled?: boolean;
    bringBackSize?: number;
    bringBackThreshold?: number;
    ceilingBoost?: boolean;
    ceilingCount?: number;
    maxExposure: number;
    minChanges: number;
    salaryFloor?: number;
    antiCorrMax?: number;
    pendingLineupPolicy?: "ignore" | "downgrade" | "exclude";
  };
};
