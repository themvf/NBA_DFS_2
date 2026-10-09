"use server";

import { dispatchNflPbpRefresh, type DispatchResult } from "@/lib/nfl/pbp-dispatch";

/**
 * Server action behind the "Ingest latest" button on /nfl/pbp.
 *
 * Runs entirely on the server, so the GITHUB_DISPATCH_TOKEN never reaches the
 * browser. It asks GitHub Actions to run the Python ingest in `stale` mode --
 * which picks up whatever nflverse has published so far -- and returns a
 * status the client renders. It does not write to Postgres itself; Python
 * remains the single writer.
 */
export async function refreshNflPbp(): Promise<DispatchResult> {
  return dispatchNflPbpRefresh();
}
