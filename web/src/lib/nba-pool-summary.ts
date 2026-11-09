/** Separate complete benchmarks from provisional observations; never choose new players. */
export type PoolObservation = {
  game_date: string; record_sha256: string; provisional: boolean; pool_gap: number | null;
};
export function summarizePoolObservations(rows: PoolObservation[]) {
  if (new Set(rows.map(r => r.game_date)).size !== rows.length || new Set(rows.map(r => r.record_sha256)).size !== rows.length)
    throw Error("Duplicate original/date in pool summary");
  const cohort = (provisional: boolean) => {
    const values = rows.filter(r => r.provisional === provisional && r.pool_gap !== null).map(r => r.pool_gap!);
    if (values.some(v => !Number.isFinite(v) || v < 0)) throw Error("Invalid nested-pool gap");
    return {n: values.length, positive: values.filter(v => v > 0).length,
      mean: values.length ? values.reduce((a,b) => a+b,0)/values.length : null};
  };
  return {complete: cohort(false), provisional: cohort(true)};
}
