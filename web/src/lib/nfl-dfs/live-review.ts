/** Recheck current evidence near lock without moving saved forecast/run records. */
export const LIVE_REVIEW_INTERVAL_MS = 60_000;
export function shouldReviewLiveSlate(firstKickoff: string | null | undefined, now: number, hasLineups: boolean): boolean {
  const kickoff = firstKickoff ? Date.parse(firstKickoff) : NaN;
  return hasLineups && Number.isFinite(now) && Number.isFinite(kickoff) && now < kickoff && kickoff - now <= 90 * 60_000;
}

export function canApplyLiveReview(input: { requestedUploadId: string; currentUploadId: string | undefined;
  responseUploadId: string; firstKickoff: string | null | undefined; now: number }): boolean {
  return input.requestedUploadId === input.currentUploadId && input.requestedUploadId === input.responseUploadId
    && shouldReviewLiveSlate(input.firstKickoff, input.now, true);
}
