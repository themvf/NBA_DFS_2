import { getMlbPitcherLineupReport } from "@/db/queries";
import type { MlbPitcherLineupBucketRow } from "@/db/queries";

const fmt2 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);

function BucketTable({
  title,
  subtitle,
  rows,
}: {
  title: string;
  subtitle: string;
  rows: MlbPitcherLineupBucketRow[];
}) {
  return (
    <div>
      <div className="mb-2">
        <p className="text-xs font-medium text-gray-300">{title}</p>
        <p className="text-[11px] text-gray-500">{subtitle}</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-gray-400">
              <th className="py-1 text-left">Bucket</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Avg Proj</th>
              <th className="py-1 text-right">Avg Own</th>
              <th className="py-1 text-right">20+</th>
              <th className="py-1 text-right">25+</th>
              <th className="py-1 text-right">UO 20+</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${title}-${row.bucket}`} className="border-b border-gray-50/60">
                <td className="py-1 font-medium text-gray-100">{row.bucket}</td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">{fmt2(row.avgProjection)}</td>
                <td className="py-1 text-right">{fmtPct(row.avgProjectedOwnPct)}</td>
                <td className="py-1 text-right">{fmtPct(row.hit20Rate)}</td>
                <td className="py-1 text-right">{fmtPct(row.hit25Rate)}</td>
                <td className="py-1 text-right">{fmtPct(row.underownedHit20Rate)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default async function MlbPitcherLineupPanel() {
  const report = await getMlbPitcherLineupReport();

  if (!report) return null;

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-6 rounded-lg border bg-card p-4 text-slate-900 [&_.text-gray-100]:text-slate-900 [&_.text-gray-300]:text-slate-700 [&_.text-gray-400]:text-slate-600 [&_.text-gray-500]:text-slate-700">
      <div>
        <h2 className="mb-1 text-sm font-semibold">MLB Pitcher Lineup Signals</h2>
        <p className="text-xs text-gray-500">
          Historical SP cohorts only. Player-specific pitcher badges now live on the MLB DFS page so this panel stays focused on the underlying signal.
        </p>
        <p className="mt-2 text-xs text-gray-400">
          Historical sample: {report.historical.sample.rows} active SP rows across {report.historical.sample.slates} main GPP slates
          {" · "}
          Avg proj {fmt2(report.historical.sample.avgProjection)}
          {" · "}
          Avg actual {fmt2(report.historical.sample.avgActualFpts)}
          {" · "}
          Avg proj own {fmtPct(report.historical.sample.avgProjectedOwnPct)}
        </p>
        <p className="mt-1 text-xs text-gray-400">
          Opponent implied context: {report.historical.sample.contextCoverage.oppImpliedKnownRows}/{report.historical.sample.rows} rows
          {" · "}
          Moneyline context: {report.historical.sample.contextCoverage.moneylineKnownRows}/{report.historical.sample.rows} rows
        </p>
      </div>

      {report.historical.findings.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-gray-300">What The Cohort Is Saying</p>
          <div className="grid gap-2 md:grid-cols-3">
            {report.historical.findings.map((finding) => (
              <div key={finding} className="rounded-md border border-gray-200/10 bg-black/10 p-3 text-xs text-gray-300">
                {finding}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-3">
        <BucketTable
          title="Projection Buckets"
          subtitle="Primary viability filter. Stronger buckets are where similar pitchers most often reached 20+ DK points."
          rows={report.historical.buckets.projection}
        />
        <BucketTable
          title="Value Buckets"
          subtitle="Projection per $1k. Useful for understanding which SP2 salary lanes have actually converted."
          rows={report.historical.buckets.value}
        />
        <BucketTable
          title="Projected Ownership Buckets"
          subtitle="Contrarian context. This shows where under-owned pitcher smash games have concentrated historically."
          rows={report.historical.buckets.projectedOwn}
        />
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <BucketTable
          title="Opponent Implied Total Buckets"
          subtitle="Run-environment context for pitcher ceiling. Lower opponent totals generally support cleaner paths to 20+."
          rows={report.historical.buckets.oppImplied}
        />
        <BucketTable
          title="Moneyline Buckets"
          subtitle="Win-equity tiebreaker. Useful context, but not as predictive as projection and value."
          rows={report.historical.buckets.moneyline}
        />
      </div>
    </div>
  );
}
