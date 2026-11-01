"use server";

import { buildCbbLineups } from "@/lib/cbb-dfs/optimizer";
import type { CbbEstimates } from "@/lib/cbb-dfs/projection";
import type { CbbMode } from "@/lib/cbb-dfs/salary-csv";

export async function buildBasketballLineups(csv: string, mode: CbbMode, estimates: CbbEstimates, count: number) {
  return buildCbbLineups(csv, mode, estimates, count);
}
