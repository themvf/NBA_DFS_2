import { NextResponse } from "next/server";
import { getOptimizerJobStatus } from "@/app/dfs/optimizer-jobs";

export const dynamic = "force-dynamic";

export async function GET(
  _request: Request,
  context: { params: Promise<{ jobId: string }> },
) {
  const { jobId: rawJobId } = await context.params;
  const jobId = Number(rawJobId);
  if (!Number.isInteger(jobId) || jobId <= 0) {
    return NextResponse.json({ ok: false, error: "Invalid optimizer job id." }, { status: 400 });
  }

  const job = await getOptimizerJobStatus(jobId);
  if (!job) {
    return NextResponse.json({ ok: false, error: "Optimizer job not found." }, { status: 404 });
  }

  return NextResponse.json(job);
}
