import { getCachedMlbPostmortemReport } from "@/db/analytics-cache";
import type {
  MlbPostmortemOwnershipMissRow,
  MlbPostmortemProjectionMissRow,
  MlbPostmortemProjectionRow,
  MlbPostmortemRecentSlateRow,
  MlbPostmortemSignalRow,
} from "@/db/queries";

const fmt2 = (v: number | null | undefined) => (v == null ? "-" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "-" : `${v.toFixed(1)}%`);
const fmtDate = (v: string | null | undefined) => v ?? "-";
const fmtSalary = (v: number | null | undefined) => (v == null ? "-" : `$${v.toLocaleString()}`);

function SignedValue({ value, inverted = false }: { value: number | null | undefined; inverted?: boolean }) {
  if (value == null) return <span>-</span>;

  const isGood = inverted ? value < 0 : value > 0;
  const isBad = inverted ? value > 0 : value < 0;

  return (
    <span className={isGood ? "text-emerald-700" : isBad ? "text-rose-700" : ""}>
      {value > 0 ? "+" : ""}
      {value.toFixed(2)}
    </span>
  );
}

function ProjectionTable({ rows }: { rows: MlbPostmortemProjectionRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Projection Impact</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Window</th>
              <th className="py-1 text-left">Group</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Final MAE</th>
              <th className="py-1 text-right">Final Bias</th>
              <th className="py-1 text-right">LineStar MAE</th>
              <th className="py-1 text-right">Gain</th>
              <th className="py-1 text-right">Our MAE</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.windowLabel}-${row.playerGroup}`} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.windowLabel}</td>
                <td className="py-1">{row.playerGroup}</td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">{fmt2(row.finalMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.finalBias} inverted />
                </td>
                <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.finalGainVsLineStar} />
                </td>
                <td className="py-1 text-right">{fmt2(row.ourMae)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">Gain is LineStar MAE minus final/blended MAE. Positive means our blended projection was more accurate.</p>
    </div>
  );
}

function OwnershipTable({
  rows,
}: {
  rows: {
    windowLabel: string;
    rows: number;
    fieldRows: number;
    linestarRows: number;
    fieldMae: number | null;
    fieldBias: number | null;
    fieldCorr: number | null;
    linestarMae: number | null;
    fieldGainVsLineStar: number | null;
  }[];
}) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Ownership Impact</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Window</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Field Rows</th>
              <th className="py-1 text-right">Field MAE</th>
              <th className="py-1 text-right">Field Bias</th>
              <th className="py-1 text-right">Field Corr</th>
              <th className="py-1 text-right">LineStar MAE</th>
              <th className="py-1 text-right">Gain</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.windowLabel} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.windowLabel}</td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">
                  {row.fieldRows}/{row.linestarRows}
                </td>
                <td className="py-1 text-right">{fmt2(row.fieldMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.fieldBias} inverted />
                </td>
                <td className="py-1 text-right">{fmt2(row.fieldCorr)}</td>
                <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.fieldGainVsLineStar} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">Field ownership uses our live/field ownership when available; LineStar remains the external benchmark.</p>
    </div>
  );
}

function SignalTable({ rows }: { rows: MlbPostmortemSignalRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Signal Follow-Through</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Signal</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Slates</th>
              <th className="py-1 text-right">Avg Proj</th>
              <th className="py-1 text-right">Avg Actual</th>
              <th className="py-1 text-right">Beat</th>
              <th className="py-1 text-right">Avg Own</th>
              <th className="py-1 text-right">15+</th>
              <th className="py-1 text-right">20+</th>
              <th className="py-1 text-right">25+</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.signal} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.signal}</td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">{row.slates}</td>
                <td className="py-1 text-right">{fmt2(row.avgProjection)}</td>
                <td className="py-1 text-right">{fmt2(row.avgActual)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.avgBeat} />
                </td>
                <td className="py-1 text-right">{fmtPct(row.avgActualOwn)}</td>
                <td className="py-1 text-right">{fmtPct(row.hit15Rate)}</td>
                <td className="py-1 text-right">{fmtPct(row.hit20Rate)}</td>
                <td className="py-1 text-right">{fmtPct(row.hit25Rate)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RecentSlatesTable({ rows }: { rows: MlbPostmortemRecentSlateRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Recent Completed Slates</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Date</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Final MAE</th>
              <th className="py-1 text-right">Bias</th>
              <th className="py-1 text-right">LS MAE</th>
              <th className="py-1 text-right">Field Own MAE</th>
              <th className="py-1 text-right">LS Own MAE</th>
              <th className="py-1 text-right">HR Badges</th>
              <th className="py-1 text-right">HR Avg</th>
              <th className="py-1 text-right">Blowup 20+</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.slateId} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.slateDate}</td>
                <td className="py-1 text-right">{row.playerRows}</td>
                <td className="py-1 text-right">{fmt2(row.finalMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.finalBias} inverted />
                </td>
                <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                <td className="py-1 text-right">{fmt2(row.fieldOwnMae)}</td>
                <td className="py-1 text-right">{fmt2(row.linestarOwnMae)}</td>
                <td className="py-1 text-right">{row.hrBadgeRows}</td>
                <td className="py-1 text-right">{fmt2(row.hrBadgeAvgActual)}</td>
                <td className="py-1 text-right">{row.blowupHit20 ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ProjectionMisses({ rows }: { rows: MlbPostmortemProjectionMissRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Latest Projection Misses</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Player</th>
              <th className="py-1 text-right">Order</th>
              <th className="py-1 text-right">Salary</th>
              <th className="py-1 text-right">Proj</th>
              <th className="py-1 text-right">LS</th>
              <th className="py-1 text-right">Actual</th>
              <th className="py-1 text-right">Miss</th>
              <th className="py-1 text-right">Own</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.slateId}-${row.name}-${row.salary}`} className="border-b border-slate-100">
                <td className="py-1">
                  <div className="font-medium">{row.name}</div>
                  <div className="text-[11px] text-slate-500">
                    {row.teamAbbrev ?? "-"} | {row.eligiblePositions ?? "-"}
                  </div>
                </td>
                <td className="py-1 text-right">{row.lineupOrder ?? "-"}</td>
                <td className="py-1 text-right">{fmtSalary(row.salary)}</td>
                <td className="py-1 text-right">{fmt2(row.projection)}</td>
                <td className="py-1 text-right">{fmt2(row.linestarProjection)}</td>
                <td className="py-1 text-right">{fmt2(row.actualFpts)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.miss} />
                </td>
                <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function OwnershipMisses({ rows }: { rows: MlbPostmortemOwnershipMissRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Latest Ownership Misses</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Player</th>
              <th className="py-1 text-right">Order</th>
              <th className="py-1 text-right">Field</th>
              <th className="py-1 text-right">LS</th>
              <th className="py-1 text-right">Actual</th>
              <th className="py-1 text-right">Field Err</th>
              <th className="py-1 text-right">LS Err</th>
              <th className="py-1 text-right">Gain</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.slateId}-${row.name}-${row.salary}`} className="border-b border-slate-100">
                <td className="py-1">
                  <div className="font-medium">{row.name}</div>
                  <div className="text-[11px] text-slate-500">
                    {row.teamAbbrev ?? "-"} | {row.eligiblePositions ?? "-"} | {fmtSalary(row.salary)}
                  </div>
                </td>
                <td className="py-1 text-right">{row.lineupOrder ?? "-"}</td>
                <td className="py-1 text-right">{fmtPct(row.fieldOwnPct)}</td>
                <td className="py-1 text-right">{fmtPct(row.linestarOwnPct)}</td>
                <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
                <td className="py-1 text-right">{fmt2(row.fieldAbsError)}</td>
                <td className="py-1 text-right">{fmt2(row.linestarAbsError)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.fieldGainVsLineStar} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default async function MlbPostmortemPanel() {
  let report;
  try {
    report = await getCachedMlbPostmortemReport();
  } catch {
    return null;
  }

  if (!report) {
    return (
      <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
        <h2 className="mb-1 text-sm font-semibold">MLB Postmortem</h2>
        <p className="text-xs text-slate-700">No completed MLB slate sample is available yet.</p>
      </div>
    );
  }

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-6 rounded-lg border bg-card p-4 text-slate-900">
      <div>
        <h2 className="mb-1 text-sm font-semibold">MLB Postmortem</h2>
        <p className="text-xs text-slate-700">
          Recent-vs-prior readout for projection accuracy, ownership accuracy, and signal follow-through after the latest MLB model changes.
        </p>
        <p className="mt-2 text-xs text-slate-600">
          Latest completed slate {fmtDate(report.sample.latestSlateDate)}
          {" | "}
          Recent {fmtDate(report.sample.recentStartDate)} to {fmtDate(report.sample.recentEndDate)}
          {" | "}
          Prior {fmtDate(report.sample.priorStartDate)} to {fmtDate(report.sample.priorEndDate)}
          {" | "}
          {report.sample.playerRows} player rows, {report.sample.ownershipRows} ownership rows
        </p>
      </div>

      {report.findings.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">What Changed</p>
          <div className="grid gap-2 md:grid-cols-2">
            {report.findings.map((finding) => (
              <div key={finding} className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
                {finding}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-2">
        <ProjectionTable rows={report.projectionSummary} />
        <OwnershipTable rows={report.ownershipSummary} />
      </div>

      <SignalTable rows={report.signalFollowThrough} />
      <RecentSlatesTable rows={report.recentSlates} />

      <div className="grid gap-6 xl:grid-cols-2">
        <ProjectionMisses rows={report.projectionMisses} />
        <OwnershipMisses rows={report.ownershipMisses} />
      </div>
    </div>
  );
}
