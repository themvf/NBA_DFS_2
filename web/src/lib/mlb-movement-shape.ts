import { MLB_MEANINGFUL_MOVE_PP } from "@/lib/mlb-movement-signals";

export type MlbMovementShape = "steady" | "steam" | "reversal" | "one_book" | "quiet" | "stale";

export type MovementTrailPoint = {
  capturedAt: string;
  homeProb: number;
};

type MovementShapeInput = {
  openProbability: number;
  currentProbability: number;
  maxJumpPp: number;
  confirmingBooks: number;
  trackedBooks: number;
  closeCapturedAt: string;
  nowIso: string;
  trail: MovementTrailPoint[];
};

const STALE_AFTER_MINUTES = 35;
const STEAM_JUMP_PP = 1.5;
const STEAM_BOOKS = 3;
const REVERSAL_PP = 0.5;
const MATERIAL_RETRACE_PP = 1;

function orientedProgressPp(point: number, open: number, direction: number): number {
  return (point - open) * direction * 100;
}

export function findMovementStart(input: Pick<MovementShapeInput, "openProbability" | "currentProbability" | "trail">): string | null {
  const direction = Math.sign(input.currentProbability - input.openProbability);
  if (direction === 0) return null;
  return input.trail.find((point) => (
    orientedProgressPp(point.homeProb, input.openProbability, direction) >= MLB_MEANINGFUL_MOVE_PP
  ))?.capturedAt ?? null;
}

export function classifyMlbMovementShape(input: MovementShapeInput): MlbMovementShape {
  const capturedAt = Date.parse(input.closeCapturedAt);
  const now = Date.parse(input.nowIso);
  if (!Number.isFinite(capturedAt) || !Number.isFinite(now)
      || (now - capturedAt) / 60_000 > STALE_AFTER_MINUTES) {
    return "stale";
  }

  const signedMovePp = (input.currentProbability - input.openProbability) * 100;
  if (Math.abs(signedMovePp) < MLB_MEANINGFUL_MOVE_PP) return "quiet";

  const direction = Math.sign(signedMovePp);
  const progress = input.trail.map((point) => (
    orientedProgressPp(point.homeProb, input.openProbability, direction)
  ));
  const finalProgress = Math.abs(signedMovePp);
  const minProgress = progress.length > 0 ? Math.min(...progress) : 0;
  const maxProgress = progress.length > 0 ? Math.max(...progress) : finalProgress;
  if (minProgress <= -REVERSAL_PP || maxProgress - finalProgress >= MATERIAL_RETRACE_PP) {
    return "reversal";
  }
  if (input.maxJumpPp >= STEAM_JUMP_PP && input.confirmingBooks >= STEAM_BOOKS) {
    return "steam";
  }
  if (input.trackedBooks >= 2 && input.confirmingBooks <= 1) return "one_book";
  return "steady";
}
