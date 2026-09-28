import { readUpsideGradeStatus } from "@/db/nfl-replacement-upside-grade";
import { UPSIDE_GRADE_VERSION } from "@/lib/nfl-dfs/replacement-upside-grade";
import { UpsideGradeView, type UpsideGradeStatus } from "./upside-grade-view";

/** Reads the automatic weekly grade's latest run and frozen verdict. */
export async function UpsideGradeCard() {
  let status: UpsideGradeStatus | null = null;
  let error: string | null = null;
  try { status = await readUpsideGradeStatus(UPSIDE_GRADE_VERSION); }
  catch (e) { error = e instanceof Error ? e.message : "Could not read the grade."; }
  return <UpsideGradeView status={status} error={error} />;
}
