// Dispatch the NFL PBP ingest workflow in GitHub Actions.
//
// This is the ONE place that knows how to trigger the ingest, shared by the
// server action behind the /nfl/pbp button and the CRON_SECRET-gated API
// route. It does not ingest anything itself: the ingest is a Python
// single-writer, and this only asks GitHub to run it in `stale` mode -- the
// same self-healing path a push and the weekly cron take.

const OWNER = "themvf";
const REPO = "NBA_DFS_2";
const WORKFLOW = "refresh_nfl_pbp_archetypes.yml";
const REF = "main";

export type DispatchResult =
  | { ok: true; message: string; runsUrl: string }
  | { ok: false; error: string; status?: number };

export async function dispatchNflPbpRefresh(): Promise<DispatchResult> {
  const token = process.env.GITHUB_DISPATCH_TOKEN;
  if (!token) {
    // A missing dispatch token is a deployment misconfiguration, not a caller
    // error -- surface it plainly so it shows up rather than looking like the
    // workflow silently did nothing.
    return { ok: false, error: "GITHUB_DISPATCH_TOKEN is not configured on this deployment" };
  }

  const url = `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    // `stale` mode picks up whatever nflverse has published so far -- exactly
    // what a Monday-before-the-cron run wants.
    body: JSON.stringify({ ref: REF, inputs: { mode: "stale" } }),
  });

  if (!res.ok) {
    const detail = await res.text();
    return {
      ok: false,
      error: `GitHub workflow dispatch failed: ${detail || res.statusText}`,
      status: res.status,
    };
  }

  return {
    ok: true,
    message: "Ingest dispatched. It runs in GitHub Actions; refresh this page in a few minutes.",
    runsUrl: `https://github.com/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}`,
  };
}
