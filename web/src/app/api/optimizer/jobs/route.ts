import { NextRequest, NextResponse } from "next/server";
import { start } from "workflow/api";
import {
  attachWorkflowRunId,
  createOptimizerJob,
  failOptimizerJob,
  getActiveOptimizerJobStatus,
} from "@/app/dfs/optimizer-jobs";
import type { CreateOptimizerJobRequest } from "@/app/dfs/optimizer-job-types";
import type { Sport } from "@/db/queries";
import { runOptimizerJobWorkflow } from "@/workflows/optimizer-job";

export const dynamic = "force-dynamic";

function isSport(value: unknown): value is Sport {
  return value === "nba" || value === "mlb";
}

function isValidCreateRequest(body: unknown): body is CreateOptimizerJobRequest {
  if (!body || typeof body !== "object") return false;
  const candidate = body as Record<string, unknown>;
  return (
    isSport(candidate.sport)
    && typeof candidate.slateId === "number"
    && Number.isInteger(candidate.slateId)
    && typeof candidate.clientToken === "string"
    && candidate.clientToken.length > 0
    && Array.isArray(candidate.selectedMatchupIds)
    && typeof candidate.settings === "object"
    && candidate.settings !== null
    && typeof (candidate.settings as Record<string, unknown>).mode === "string"
    && typeof (candidate.settings as Record<string, unknown>).nLineups === "number"
  );
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const clientToken = searchParams.get("clientToken");
  const sport = searchParams.get("sport");
  const slateId = Number(searchParams.get("slateId"));

  if (!clientToken || !isSport(sport) || !Number.isInteger(slateId) || slateId <= 0) {
    return NextResponse.json({ ok: false, error: "clientToken, sport, and slateId are required." }, { status: 400 });
  }

  const job = await getActiveOptimizerJobStatus(clientToken, sport, slateId);
  if (!job) {
    return NextResponse.json({ ok: true, job: null });
  }

  return NextResponse.json(job);
}

export async function POST(request: NextRequest) {
  let createdJobId: number | null = null;

  try {
    const body = await request.json();
    if (!isValidCreateRequest(body)) {
      return NextResponse.json({ ok: false, error: "Invalid optimizer job request." }, { status: 400 });
    }

    const created = await createOptimizerJob(body);
    createdJobId = created.jobId;

    if (created.existing) {
      return NextResponse.json({ ok: true, jobId: created.jobId, existing: true });
    }

    const run = await start(runOptimizerJobWorkflow, [created.jobId]);
    await attachWorkflowRunId(created.jobId, run.runId);

    return NextResponse.json({ ok: true, jobId: created.jobId, existing: false });
  } catch (error) {
    if (createdJobId != null) {
      await failOptimizerJob(
        createdJobId,
        error instanceof Error ? error.message : String(error),
      );
    }

    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : String(error) },
      { status: 500 },
    );
  }
}
