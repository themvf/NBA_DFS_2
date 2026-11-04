import type { TrioPoolAudit } from "@/db/nba-trio-math";

const points = (value: number | null) => value === null ? "Pending" : String(value);
const labels: Record<string, string> = {
  market_baseline: "Market baseline",
  joint_best_of_five: "Joint best-of-five",
  robust_best_of_five: "Robust best-of-five",
};

export function PoolAudit({ audit }: { audit: TrioPoolAudit | null }) {
  return (
    <section className="space-y-3 rounded-lg border p-4">
      <h2 className="font-semibold">Where did the trio points go?</h2>
      <p className="text-sm text-muted-foreground">
        The pool gap measures points lost at the top-12 cutoff. The selection gap measures points
        left on the table by the best of five entries inside that pool.
      </p>
      {audit === null ? (
        <p className="text-sm">Awaiting a postgame pool audit for this record.</p>
      ) : (
        <>
          {audit.provisional && (
            <p className="text-sm text-amber-700">
              Provisional benchmarks use verified scores so far. The slate or player coverage is incomplete.
            </p>
          )}
          <div className="grid gap-3 text-sm sm:grid-cols-2">
            {([["Full frozen v3 modeled pool", audit.full_pool], ["Frozen top-12 pool", audit.top_12]] as const)
              .map(([label, pool]) => (
                <div key={label} className="rounded border p-3">
                  <p className="font-medium">{label} · best trio {points(pool.best_total)}</p>
                  <p>{pool.verified} of {pool.eligible} scores verified</p>
                  {pool.trio.length > 0 && (
                    <p>{pool.trio.map(({ player, points }) => `${player} (${points})`).join(" · ")}</p>
                  )}
                </div>
              ))}
          </div>
          <p className="text-sm font-medium">Pool gap: {points(audit.pool_gap)}{audit.pool_gap === null ? "" : " points"}</p>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr>
                  <th scope="col" className="pr-3">Method</th>
                  <th scope="col" className="pr-3">Best entry</th>
                  <th scope="col" className="pr-3">Selection gap</th>
                  <th scope="col" className="pr-3">Total gap</th>
                  <th scope="col">Top-three slots</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(audit.portfolios).map(([key, result]) => (
                  <tr key={key} className="border-t">
                    <th scope="row" className="py-2 pr-3 font-medium">{labels[key] ?? key}</th>
                    <td>{points(result.best_entry)}</td>
                    <td>{points(result.selection_gap)}</td>
                    <td>{points(result.total_gap)}</td>
                    <td>{result.fractional_top_three === null
                      ? "Pending"
                      : `${Number(result.fractional_top_three.toFixed(2))}/3${result.tie_compatible_top_three ? " · all three with ties" : ""}`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-xs text-muted-foreground">
            Top-three slots are measured against the full frozen modeled pool, with fractional credit
            at the scoring boundary. Missing scores and DNPs stay unresolved.
          </p>
        </>
      )}
    </section>
  );
}
