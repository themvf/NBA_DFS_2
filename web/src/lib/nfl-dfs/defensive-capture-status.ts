/**
 * What happened to a slate's opponent (defensive) capture, per profile, in one
 * plain sentence. Pure: the server gathers the request row and the coverage
 * the generation reader actually sees; this decides.
 *
 * Captures used to come only from the twice-daily projection workflow, for
 * uploads that already existed when it ran. Uploads now request their own
 * (ingest/nfl_defensive_capture_requests.py), so "none yet" has several
 * distinct causes, and each needs a different action from the user.
 */

export type CaptureRequestState = "pending" | "running" | "captured" | "failed" | "ineligible";

export interface CaptureRequestRow {
  profile: "pfr-efficiency" | "allowed-rushing-volume";
  state: CaptureRequestState;
  capturedPlayers: number | null;
  lastError: string | null;
  dispatchError: string | null;
  workerRunUrl: string | null;
  updatedAt: string | null;
}

/** What the generation reader sees for the profile (opponentAdjustmentCoverage). */
export interface CaptureCoverage {
  applied: number;
  eligible: number;
  captured: number;
  error?: string | null;
}

export type CaptureStatus =
  | "applied"          // players adjust: the reader and resolver agree
  | "captured_zero"    // a capture exists, no player passed the checks
  | "pending"          // requested, worker not finished
  | "failed"           // the worker tried and failed; retry is possible pregame
  | "ineligible"       // the worker refused, with a reason (started, incomplete, rebound)
  | "not_requested"    // no capture and no request (e.g. an upload before this existed)
  | "read_error";      // the captures could not be read at all

export interface ProfileCaptureStatus {
  status: CaptureStatus;
  text: string;
  retryable: boolean;
}

export function profileCaptureStatus(label: string, request: CaptureRequestRow | null, coverage: CaptureCoverage | null,
  options: { started: boolean }): ProfileCaptureStatus {
  if (coverage?.error) return { status: "read_error", retryable: false, text: `${label}: couldn't read captures (${coverage.error}).` };
  if (coverage && coverage.applied > 0) {
    return { status: "applied", retryable: false, text: `${label}: ${coverage.applied} of ${coverage.eligible} players adjusted (${coverage.captured} captured).` };
  }
  if (coverage && coverage.captured > 0) {
    return { status: "captured_zero", retryable: false, text: `${label}: captured ${coverage.captured} players, but none passed the checks, so they keep their baseline.` };
  }
  if (!request) return { status: "not_requested", retryable: !options.started, text: `${label}: no capture has been requested for this upload.` };
  switch (request.state) {
    case "pending":
    case "running":
      return { status: "pending", retryable: false, text: request.dispatchError
        ? `${label}: capture requested, but starting it failed (${request.dispatchError}); it retries within 15 minutes.`
        : `${label}: capture ${request.state === "running" ? "running" : "requested"}; it usually takes a few minutes.` };
    case "captured":
      // The worker verified rows, but the reader sees none: every captured player has kicked off,
      // or the capture was for players no longer on this upload's eligible list.
      return { status: "captured_zero", retryable: false, text: `${label}: captured ${request.capturedPlayers ?? 0} players, but none can be used now, so they keep their baseline.` };
    case "failed":
      return { status: "failed", retryable: !options.started, text: `${label}: capture failed (${request.lastError ?? "no reason recorded"}).` };
    case "ineligible":
      return { status: "ineligible", retryable: false, text: `${label}: not captured (${request.lastError ?? "no reason recorded"}).` };
  }
}
