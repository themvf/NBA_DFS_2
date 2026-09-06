import { runNflScenarioLab, type ScenarioLabRequest } from "@/lib/nfl-dfs/scenario-lab";

self.onmessage = async (event: MessageEvent<ScenarioLabRequest>) => {
  try { self.postMessage({ ok: true, result: await runNflScenarioLab(event.data) }); }
  catch (error) { self.postMessage({ ok: false, error: error instanceof Error ? error.message : "Unable to compare scenarios" }); }
};
