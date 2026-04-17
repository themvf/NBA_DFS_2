import { getCachedMlbOwnershipModelReport } from "@/db/analytics-cache";

const fmt2 = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(2)}%`);
const fmtDt = (v: string | null | undefined) => (v ? new Date(v).toLocaleString() : "—");

function deltaClass(value: number | null | undefined) {
  if (value == null) return "text-slate-600";
  if (value > 0) return "text-emerald-700";
  if (value < 0) return "text-rose-700";
  return "text-slate-600";
}

export default async function MlbOwnershipModelPanel() {
  const report = await getCachedMlbOwnershipModelReport();

  if (!report) {
    return (
      <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
        <h2 className="mb-1 text-sm font-semibold">MLB Ownership Model Tracking</h2>
        <p className="text-xs text-slate-700">
          No completed MLB ownership snapshots yet. Load or refresh MLB slates, then upload actual ownership to start tracking LineStar vs our field model.
        </p>
      </div>
    );
  }

  const capturedAt = report.sample.latestCapturedAt
    ? new Date(report.sample.latestCapturedAt).toLocaleString()
    : null;

  return (
    <div className="mx-auto mt-8 max-w-5xl space-y-6 rounded-lg border bg-card p-4 text-slate-900">
      <div>
        <h2 className="mb-1 text-sm font-semibold">MLB Ownership Model Tracking</h2>
        <p className="text-xs text-slate-700">
          Latest run per slate, active players only. This compares raw `LS Own%` against our modeled `Field Own%` after actual ownership is imported.
        </p>
        <p className="mt-2 text-xs text-slate-600">
          Sample: {report.sample.rows} player rows across {report.sample.slates} slates
          {" · "}
          Version {report.sample.latestVersion ?? "—"}
          {" · "}
          Latest source {report.sample.latestSource ?? "—"}
          {capturedAt ? ` · Last capture ${capturedAt}` : ""}
        </p>
      </div>

      {report.findings.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">What The Tracking Is Saying</p>
          <div className="grid gap-2 md:grid-cols-2">
            {report.findings.map((finding) => (
              <div key={finding} className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
                {finding}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div>
        <p className="mb-2 text-xs font-medium text-slate-700">Source Accuracy</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-slate-600">
                <th className="py-1 text-left">Source</th>
                <th className="py-1 text-right">Rows</th>
                <th className="py-1 text-right">MAE</th>
                <th className="py-1 text-right">Bias</th>
                <th className="py-1 text-right">Corr</th>
              </tr>
            </thead>
            <tbody>
              {report.sources.map((row) => (
                <tr key={row.label} className="border-b border-slate-100">
                  <td className="py-1 font-medium">{row.label}</td>
                  <td className="py-1 text-right">{row.rows}</td>
                  <td className="py-1 text-right">{fmt2(row.mae)}</td>
                  <td className={`py-1 text-right ${deltaClass(row.bias)}`}>{fmt2(row.bias)}</td>
                  <td className="py-1 text-right">{fmt2(row.corr)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Where The Model Helps</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Segment</th>
                  <th className="py-1 text-right">Rows</th>
                  <th className="py-1 text-right">LS MAE</th>
                  <th className="py-1 text-right">Field MAE</th>
                  <th className="py-1 text-right">MAE Gain</th>
                  <th className="py-1 text-right">Field Corr</th>
                </tr>
              </thead>
              <tbody>
                {report.segments.map((row) => (
                  <tr key={row.segment} className="border-b border-slate-100">
                    <td className="py-1 font-medium">{row.segment}</td>
                    <td className="py-1 text-right">{row.rows}</td>
                    <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldMae)}</td>
                    <td className={`py-1 text-right ${deltaClass(row.maeDelta)}`}>{fmt2(row.maeDelta)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldCorr)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Field Ownership Buckets</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Bucket</th>
                  <th className="py-1 text-right">Rows</th>
                  <th className="py-1 text-right">LS Own</th>
                  <th className="py-1 text-right">Field Own</th>
                  <th className="py-1 text-right">Actual Own</th>
                  <th className="py-1 text-right">Field Bias</th>
                </tr>
              </thead>
              <tbody>
                {report.buckets.map((row) => (
                  <tr key={row.bucket} className="border-b border-slate-100">
                    <td className="py-1 font-medium">{row.bucket}</td>
                    <td className="py-1 text-right">{row.rows}</td>
                    <td className="py-1 text-right">{fmtPct(row.avgLinestarOwnPct)}</td>
                    <td className="py-1 text-right">{fmtPct(row.avgFieldOwnPct)}</td>
                    <td className="py-1 text-right">{fmtPct(row.avgActualOwnPct)}</td>
                    <td className={`py-1 text-right ${deltaClass(-1 * (row.fieldBias ?? 0))}`}>{fmtPct(row.fieldBias)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Recent Tracked Slates</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Slate</th>
                  <th className="py-1 text-left">Version</th>
                  <th className="py-1 text-right">Rows</th>
                  <th className="py-1 text-right">LS MAE</th>
                  <th className="py-1 text-right">Field MAE</th>
                  <th className="py-1 text-right">Gain</th>
                  <th className="py-1 text-right">Field Corr</th>
                </tr>
              </thead>
              <tbody>
                {report.recentSlates.map((row) => (
                  <tr key={`${row.slateId}-${row.ownershipVersion}-${row.source}`} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.slateDate}</div>
                      <div className="text-[11px] text-slate-500">{fmtDt(row.capturedAt)}</div>
                    </td>
                    <td className="py-1">
                      <div className="font-medium">{row.ownershipVersion}</div>
                      <div className="text-[11px] text-slate-500">{row.source}</div>
                    </td>
                    <td className="py-1 text-right">{row.rows}</td>
                    <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldMae)}</td>
                    <td className={`py-1 text-right ${deltaClass(row.maeGain)}`}>{fmt2(row.maeGain)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldCorr)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">Version Summary</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Version</th>
                  <th className="py-1 text-right">Slates</th>
                  <th className="py-1 text-right">Rows</th>
                  <th className="py-1 text-right">LS MAE</th>
                  <th className="py-1 text-right">Field MAE</th>
                  <th className="py-1 text-right">Gain</th>
                  <th className="py-1 text-right">Field Corr</th>
                </tr>
              </thead>
              <tbody>
                {report.versions.map((row) => (
                  <tr key={`${row.ownershipVersion}-${row.source}`} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.ownershipVersion}</div>
                      <div className="text-[11px] text-slate-500">{row.source}</div>
                    </td>
                    <td className="py-1 text-right">{row.slates}</td>
                    <td className="py-1 text-right">{row.rows}</td>
                    <td className="py-1 text-right">{fmt2(row.linestarMae)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldMae)}</td>
                    <td className={`py-1 text-right ${deltaClass(row.maeGain)}`}>{fmt2(row.maeGain)}</td>
                    <td className="py-1 text-right">{fmt2(row.fieldCorr)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {report.latestSlateMisses.length > 0 ? (
        <div>
          <p className="mb-2 text-xs font-medium text-slate-700">
            Biggest Misses On Latest Completed Tracked Slate ({report.latestSlateMisses[0].slateDate})
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b text-slate-600">
                  <th className="py-1 text-left">Player</th>
                  <th className="py-1 text-right">Salary</th>
                  <th className="py-1 text-right">Order</th>
                  <th className="py-1 text-right">LS Own</th>
                  <th className="py-1 text-right">Field Own</th>
                  <th className="py-1 text-right">Actual Own</th>
                  <th className="py-1 text-right">Field Err</th>
                  <th className="py-1 text-right">Gain</th>
                </tr>
              </thead>
              <tbody>
                {report.latestSlateMisses.map((row) => (
                  <tr key={`${row.slateId}-${row.name}-${row.salary}`} className="border-b border-slate-100">
                    <td className="py-1">
                      <div className="font-medium">{row.name}</div>
                      <div className="text-[11px] text-slate-500">{row.eligiblePositions ?? "—"}</div>
                    </td>
                    <td className="py-1 text-right">${row.salary.toLocaleString()}</td>
                    <td className="py-1 text-right">{row.lineupOrder ?? "—"}</td>
                    <td className="py-1 text-right">{fmtPct(row.linestarOwnPct)}</td>
                    <td className="py-1 text-right">{fmtPct(row.fieldOwnPct)}</td>
                    <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
                    <td className="py-1 text-right">{fmtPct(row.fieldAbsError)}</td>
                    <td className={`py-1 text-right ${deltaClass(row.errorGain)}`}>{fmtPct(row.errorGain)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </div>
  );
}
