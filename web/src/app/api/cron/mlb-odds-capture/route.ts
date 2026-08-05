import { NextRequest, NextResponse } from "next/server";

// Vercel Cron -> GitHub Actions dispatch bridge.
//
// Why this exists: GitHub's native `schedule:` trigger for
// `.github/workflows/capture_odds_history.yml` is configured for every ~15
// minutes but in practice GitHub silently delays/drops most firings under
// platform load — observed real gaps of 60-95 minutes during active hours
// and 3-13 hours overnight, instead of 15 minutes. That starves the MLB
// "Today's movement board" (STALE_AFTER_MINUTES=35 in
// web/src/lib/mlb-movement-shape.ts) of fresh captures.
//
// Vercel Cron on a Pro plan is invoked within the configured minute, so it
// is used here purely as a reliable clock. The actual capture logic is
// untouched and keeps running inside GitHub Actions (same Python
// environment, same single-writer odds policy) - this route only fires a
// `workflow_dispatch` event; it never touches the database itself.
//
// Required Vercel project env vars (Production + Preview):
//   CRON_SECRET            - shared secret Vercel sends as `Authorization:
//                             Bearer <CRON_SECRET>` on every cron invocation
//   GITHUB_DISPATCH_TOKEN  - a GitHub fine-grained PAT scoped to this repo
//                             only, with "Actions: Read and write" permission

const GITHUB_OWNER = "themvf";
const GITHUB_REPO = "NBA_DFS_2";
const WORKFLOW_FILE = "capture_odds_history.yml";
const WORKFLOW_REF = "main";

export const dynamic = "force-dynamic";
export const maxDuration = 15;

export async function GET(request: NextRequest) {
  // Distinguish "secret missing from this deployment" (a misconfiguration we
  // need to see in the logs) from "caller sent the wrong secret" (a genuine
  // rejection). Collapsing both into one 401 hid a real deploy problem:
  // Vercel derives the Authorization header it sends from CRON_SECRET itself,
  // so a legitimate cron call can only fail when the FUNCTION cannot read that
  // variable — i.e. it is missing from the Production scope, or was added
  // after this deployment was built (env vars are captured at build time, so
  // an existing deployment needs a redeploy to pick up a new variable).
  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    console.error(
      "mlb-odds-capture cron: CRON_SECRET is not readable by this deployment. " +
        "Set it for the Production environment, then redeploy so the function picks it up.",
    );
    return NextResponse.json(
      { ok: false, error: "CRON_SECRET is not configured in this deployment" },
      { status: 500 },
    );
  }

  const authHeader = request.headers.get("authorization");
  if (authHeader !== `Bearer ${cronSecret}`) {
    // Never log either value — only whether a header arrived at all.
    console.error(
      `mlb-odds-capture cron: rejected request (authorization header ${
        authHeader ? "present but non-matching" : "missing"
      })`,
    );
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const dispatchToken = process.env.GITHUB_DISPATCH_TOKEN;
  if (!dispatchToken) {
    console.error("mlb-odds-capture cron: GITHUB_DISPATCH_TOKEN is not configured");
    return NextResponse.json(
      { ok: false, error: "GITHUB_DISPATCH_TOKEN is not configured" },
      { status: 500 },
    );
  }

  const dispatchUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`;

  try {
    const response = await fetch(dispatchUrl, {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${dispatchToken}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref: WORKFLOW_REF }),
    });

    // GitHub returns 204 No Content on a successful dispatch.
    if (response.status === 204) {
      return NextResponse.json({ ok: true, dispatchedAt: new Date().toISOString() });
    }

    const body = await response.text();
    console.error(
      `mlb-odds-capture cron: GitHub dispatch failed (${response.status}): ${body}`,
    );
    return NextResponse.json(
      { ok: false, error: `GitHub dispatch failed (${response.status})`, body },
      { status: 502 },
    );
  } catch (error) {
    console.error("mlb-odds-capture cron: dispatch request threw", error);
    return NextResponse.json(
      { ok: false, error: "Dispatch request failed" },
      { status: 502 },
    );
  }
}
