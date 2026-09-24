import { NextRequest, NextResponse } from "next/server";
import { dkPoolDispatchDue } from "@/lib/nfl-dfs/dk-pool-cadence";

// Vercel Cron -> GitHub Actions dispatch bridge for DraftKings' live NFL pool.
//
// Same pattern, and the same two env vars, as ../mlb-odds-capture/route.ts,
// which has dispatched at exactly :07 and :37 all day. GitHub's own schedule
// for refresh_nfl_dk_pool.yml skipped all four of its first Thursday slots
// (16:00-17:30 UTC, 2026-09-24) -- on the evening the feed existed to serve.
// This route only fires `workflow_dispatch`; the capture stays in Python, in
// GitHub Actions, and this never touches the database.

const GITHUB_OWNER = "themvf";
const GITHUB_REPO = "NBA_DFS_2";
const WORKFLOW_FILE = "refresh_nfl_dk_pool.yml";
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
      "nfl-dk-pool cron: CRON_SECRET is not readable by this deployment. " +
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
      `nfl-dk-pool cron: rejected request (authorization header ${
        authHeader ? "present but non-matching" : "missing"
      })`,
    );
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  // Vercel calls every half-hour as a clock; most half-hours are not worth a
  // GitHub Actions run. See dk-pool-cadence.ts for the windows.
  if (!dkPoolDispatchDue(new Date())) {
    return NextResponse.json({ ok: true, skipped: "outside a polling window" });
  }

  const dispatchToken = process.env.GITHUB_DISPATCH_TOKEN;
  if (!dispatchToken) {
    console.error("nfl-dk-pool cron: GITHUB_DISPATCH_TOKEN is not configured");
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
      `nfl-dk-pool cron: GitHub dispatch failed (${response.status}): ${body}`,
    );
    return NextResponse.json(
      { ok: false, error: `GitHub dispatch failed (${response.status})`, body },
      { status: 502 },
    );
  } catch (error) {
    console.error("nfl-dk-pool cron: dispatch request threw", error);
    return NextResponse.json(
      { ok: false, error: "Dispatch request failed" },
      { status: 502 },
    );
  }
}
