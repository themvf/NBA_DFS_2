import { NextRequest, NextResponse } from "next/server";
import { nflProjectionDispatchDue } from "@/lib/nfl-dfs/projection-cadence";

// Vercel Cron -> GitHub Actions dispatch bridge for the production NFL DFS
// projection rebuild. Same pattern and env vars as ../nfl-dk-pool/route.ts.
// GitHub skipped this workflow's 21:35 UTC slot on 2026-09-26, so Sunday's
// slate sat on a 1:22 PM projection that refused the evening's roster data.
// This route only fires `workflow_dispatch`; the build stays in Python.

const GITHUB_OWNER = "themvf";
const GITHUB_REPO = "NBA_DFS_2";
const WORKFLOW_FILE = "refresh_nfl_dfs_projections.yml";
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
      "nfl-projections cron: CRON_SECRET is not readable by this deployment. " +
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
      `nfl-projections cron: rejected request (authorization header ${
        authHeader ? "present but non-matching" : "missing"
      })`,
    );
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  // Vercel calls every half-hour as a clock; only the slots in
  // projection-cadence.ts dispatch a rebuild.
  if (!nflProjectionDispatchDue(new Date())) {
    return NextResponse.json({ ok: true, skipped: "outside a polling window" });
  }

  const dispatchToken = process.env.GITHUB_DISPATCH_TOKEN;
  if (!dispatchToken) {
    console.error("nfl-projections cron: GITHUB_DISPATCH_TOKEN is not configured");
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
      `nfl-projections cron: GitHub dispatch failed (${response.status}): ${body}`,
    );
    return NextResponse.json(
      { ok: false, error: `GitHub dispatch failed (${response.status})`, body },
      { status: 502 },
    );
  } catch (error) {
    console.error("nfl-projections cron: dispatch request threw", error);
    return NextResponse.json(
      { ok: false, error: "Dispatch request failed" },
      { status: 502 },
    );
  }
}
