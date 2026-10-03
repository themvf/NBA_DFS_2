/** Only the paid capture stage blocks the next due checkpoint dispatch. */
export function captureStageBlocksDispatch(jobs: Array<{ name: string; status: string }>): boolean {
  const capture = jobs.find((job) => job.name === "capture");
  return !capture || capture.status !== "completed";
}
