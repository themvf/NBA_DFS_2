import {
  buildAndPersistOptimizerJobLineup,
  finalizeOptimizerJob,
  prepareOptimizerJob,
} from "@/app/dfs/optimizer-jobs";

async function prepareOptimizerJobStep(jobId: number) {
  "use step";

  const result = await prepareOptimizerJob(jobId);
  if (!result.ok) {
    return { ok: false, requestedLineups: 0 };
  }

  return {
    ok: true,
    requestedLineups: result.prepared.requestedLineups,
  };
}

async function buildOptimizerLineupStep(jobId: number, lineupNumber: number) {
  "use step";

  return buildAndPersistOptimizerJobLineup(jobId, lineupNumber);
}

async function finalizeOptimizerJobStep(jobId: number) {
  "use step";

  await finalizeOptimizerJob(jobId);
}

export async function runOptimizerJobWorkflow(jobId: number) {
  "use workflow";

  const prepared = await prepareOptimizerJobStep(jobId);
  if (!prepared.ok) {
    return;
  }

  for (let lineupNumber = 1; lineupNumber <= prepared.requestedLineups; lineupNumber++) {
    const result = await buildOptimizerLineupStep(jobId, lineupNumber);
    if (!result.built) break;
  }

  await finalizeOptimizerJobStep(jobId);
}
