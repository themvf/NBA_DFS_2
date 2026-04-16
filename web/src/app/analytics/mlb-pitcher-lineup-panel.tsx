import {
  getMlbPitcherLineupReport,
} from "@/db/queries";
import type {
  MlbPitcherLineupBucketRow,
  MlbPitcherLineupCandidate,
} from "@/db/queries";

const fmt1 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(1));
const fmt2 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const fmtSalary = (v: number | null | undefined) => (v == null ? "—" : `$${v.toLocaleString()}`);
const fmtMoneyline = (v: number | null | undefined) => {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : `${v}`;
};

function CandidateTable({
  title,
  subtitle,
  rows,
  scoreLabel,
  scoreKey,
}: {
  title: string;
  subtitle: string;
  rows: MlbPitcherLineupCandidate[];
  scoreLabel: string;
  scoreKey: "lineupScore" | "contrarianScore";
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
              <th className="py-1 text-left">Pitcher</th>
              <th className="py-1 text-right">Salary</th>
              <th className="py-1 text-right">Proj</th>
              <th className="py-1 text-right">Own</th>
              <th className="py-1 text-right">Value</th>
              <th className="py-1 text-right">Opp TT</th>
              <th className="py-1 text-right">ML</th>
              <th className="py-1 text-right">{scoreLabel}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${title}-${row.name}-${row.teamAbbrev}`} className="border-b border-gray-50/60 align-top">
                <td className="py-2">
                  <div className="font-medium text-gray-100">{row.name}</div>
                  <div className="text-[11px] text-gray-500">
                    {row.teamAbbrev ?? "—"}
                    {" · "}
                    {row.projectionBucket}
                    {" · "}
                    {row.valueBucket}
                  </div>
                </td>
                <td className="py-2 text-right">{fmtSalary(row.salary)}</td>
                <td className="py-2 text-right">{fmt2(row.projection)}</td>
                <td className="py-2 text-right">{fmtPct(row.projectedOwnPct)}</td>
                <td className="py-2 text-right">{fmt2(row.projectedValueX)}x</td>
                <td className="py-2 text-right">{fmt2(row.oppImplied)}</td>
                <td className="py-2 text-right">{fmtMoneyline(row.teamMl)}</td>
                <td className="py-2 text-right font-medium text-gray-100">{fmt1(row[scoreKey])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

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
          Historical SP cohorts are used to score current slate pitchers for lineup utility and lower-owned pivot strength.
          The current scoring leans on projection, value, and under-owned hit rates rather than raw ownership alone.
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

      {report.currentSlate ? (
        <div className="grid gap-6 xl:grid-cols-2">
          <CandidateTable
            title={`Top Lineup Candidates (${report.currentSlate.slate.slateDate})`}
            subtitle="Higher lineup scores reflect stronger historical projection and ceiling buckets."
            rows={report.currentSlate.pitchers}
            scoreLabel="Lineup"
            scoreKey="lineupScore"
          />
          <CandidateTable
            title="Contrarian Pivots"
            subtitle="Lower-owned SPs sorted by under-owned hit-rate context and ceiling support."
            rows={report.currentSlate.contrarianPitchers}
            scoreLabel="Pivot"
            scoreKey="contrarianScore"
          />
        </div>
      ) : (
        <div className="rounded-md border border-gray-200/10 bg-black/10 p-3 text-xs text-gray-400">
          No current MLB main slate with active SPs was available for ranking. Historical cohort tables are still shown below.
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-3">
        <BucketTable
          title="Projection Buckets"
          subtitle="Best primary filter for identifying viable SPs before ownership decisions."
          rows={report.historical.buckets.projection}
        />
        <BucketTable
          title="Value Buckets"
          subtitle="Projection per $1k. Useful for finding usable SP2s and salary-efficient pivots."
          rows={report.historical.buckets.value}
        />
        <BucketTable
          title="Projected Ownership Buckets"
          subtitle="Shows where under-owned smashes have actually concentrated."
          rows={report.historical.buckets.projectedOwn}
        />
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <BucketTable
          title="Opponent Implied Total Buckets"
          subtitle="Context table for how much opposing run environment matters for SP ceiling."
          rows={report.historical.buckets.oppImplied}
        />
        <BucketTable
          title="Moneyline Buckets"
          subtitle="Win equity context. Useful as a tiebreaker, not a standalone filter."
          rows={report.historical.buckets.moneyline}
        />
      </div>

      <div>
        <div className="mb-2">
          <p className="text-xs font-medium text-gray-300">Top Historical Under-Owned Smashes</p>
          <p className="text-[11px] text-gray-500">
            Historical SP games with 20+ DK points and sub-5% actual ownership.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-gray-400">
                <th className="py-1 text-left">Date</th>
                <th className="py-1 text-left">Pitcher</th>
                <th className="py-1 text-right">Salary</th>
                <th className="py-1 text-right">Proj</th>
                <th className="py-1 text-right">Proj Own</th>
                <th className="py-1 text-right">Actual Own</th>
                <th className="py-1 text-right">Actual</th>
                <th className="py-1 text-right">Opp TT</th>
                <th className="py-1 text-right">ML</th>
                <th className="py-1 text-right">Value</th>
              </tr>
            </thead>
            <tbody>
              {report.historical.topUnderownedSmashes.map((row) => (
                <tr key={`${row.slateDate}-${row.name}-${row.teamAbbrev}`} className="border-b border-gray-50/60">
                  <td className="py-1">{row.slateDate}</td>
                  <td className="py-1">
                    <span className="font-medium text-gray-100">{row.name}</span>
                    <span className="text-gray-500"> {row.teamAbbrev ? `· ${row.teamAbbrev}` : ""}</span>
                  </td>
                  <td className="py-1 text-right">{fmtSalary(row.salary)}</td>
                  <td className="py-1 text-right">{fmt2(row.projection)}</td>
                  <td className="py-1 text-right">{fmtPct(row.projectedOwnPct)}</td>
                  <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
                  <td className="py-1 text-right font-medium text-gray-100">{fmt2(row.actualFpts)}</td>
                  <td className="py-1 text-right">{fmt2(row.oppImplied)}</td>
                  <td className="py-1 text-right">{fmtMoneyline(row.teamMl)}</td>
                  <td className="py-1 text-right">{fmt2(row.projectedValueX)}x</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
