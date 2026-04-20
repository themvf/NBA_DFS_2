import { getCachedMlbPostmortemReport } from "@/db/analytics-cache";
import MlbPostmortemCharts from "./mlb-postmortem-charts";
import type {
  MlbPostmortemDecisionCaptureRow,
  MlbPostmortemIndependenceRow,
  MlbPostmortemLeverageErrorRow,
  MlbPostmortemOwnershipChalkRow,
  MlbPostmortemOwnershipMissRow,
  MlbPostmortemOwnershipRankingRow,
  MlbPostmortemPitcherExploitRow,
  MlbPostmortemProjectionMissRow,
  MlbPostmortemProjectionRow,
  MlbPostmortemProjectionSourceRow,
  MlbPostmortemRecentSlateRow,
  MlbPostmortemSignalRow,
} from "@/db/queries";

const fmt2 = (v: number | null | undefined) => (v == null ? "-" : v.toFixed(2));
const fmtPct = (v: number | null | undefined) => (v == null ? "-" : `${v.toFixed(1)}%`);
const fmtDate = (v: string | null | undefined) => v ?? "-";
const fmtSalary = (v: number | null | undefined) => (v == null ? "-" : `$${v.toLocaleString()}`);
const fmtInt = (v: number | null | undefined) => (v == null ? "-" : v.toLocaleString());

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

function ProjectionIndependenceTable({ rows }: { rows: MlbPostmortemIndependenceRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Projection Independence</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Window</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Fallback</th>
              <th className="py-1 text-right">Final MAE</th>
              <th className="py-1 text-right">Raw MAE</th>
              <th className="py-1 text-right">Our Src MAE</th>
              <th className="py-1 text-right">Non-LS MAE</th>
              <th className="py-1 text-right">Fallback MAE</th>
              <th className="py-1 text-right">Blend Uplift</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.windowLabel} className="border-b border-slate-100">
                <td className="py-1 font-medium">
                  <div>{row.windowLabel}</div>
                  {row.warning ? <div className="text-[11px] text-amber-700">{row.warning}</div> : null}
                </td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">
                  <div>{fmtPct(row.fallbackPct)}</div>
                  <div className="text-[11px] text-slate-500">{row.fallbackRows}/{row.finalRows}</div>
                </td>
                <td className="py-1 text-right">{fmt2(row.finalMae)}</td>
                <td className="py-1 text-right">{fmt2(row.rawOurMae)}</td>
                <td className="py-1 text-right">{fmt2(row.ourSourceMae)}</td>
                <td className="py-1 text-right">{fmt2(row.nonLineStarMae)}</td>
                <td className="py-1 text-right">{fmt2(row.fallbackMae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.blendUplift} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">Fallback is the share of effective projections coming from LineStar. High fallback means final MAE is not independent validation.</p>
    </div>
  );
}

function ProjectionSourceTable({ rows }: { rows: MlbPostmortemProjectionSourceRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Projection Source Coverage</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Window</th>
              <th className="py-1 text-left">Source</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">Coverage</th>
              <th className="py-1 text-right">MAE</th>
              <th className="py-1 text-right">Bias</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.windowLabel}-${row.projectionSource}`} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.windowLabel}</td>
                <td className="py-1">{row.projectionSource}</td>
                <td className="py-1 text-right">{row.rows}</td>
                <td className="py-1 text-right">{fmtPct(row.pctRows)}</td>
                <td className="py-1 text-right">{fmt2(row.mae)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.bias} inverted />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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

function OwnershipDiagnostics({
  chalkRows,
  rankingRows,
  leverageRows,
}: {
  chalkRows: MlbPostmortemOwnershipChalkRow[];
  rankingRows: MlbPostmortemOwnershipRankingRow[];
  leverageRows: MlbPostmortemLeverageErrorRow[];
}) {
  return (
    <div className="grid gap-6 xl:grid-cols-3">
      <div>
        <p className="mb-2 text-xs font-medium text-slate-700">Chalk Capture</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-slate-600">
                <th className="py-1 text-left">Window</th>
                <th className="py-1 text-right">Thresh</th>
                <th className="py-1 text-right">Actual</th>
                <th className="py-1 text-right">Capture</th>
                <th className="py-1 text-right">False Low</th>
              </tr>
            </thead>
            <tbody>
              {chalkRows.map((row) => (
                <tr key={`${row.windowLabel}-${row.threshold}`} className="border-b border-slate-100">
                  <td className="py-1 font-medium">{row.windowLabel}</td>
                  <td className="py-1 text-right">{row.threshold}%</td>
                  <td className="py-1 text-right">{row.actualChalkRows}</td>
                  <td className="py-1 text-right">{fmtPct(row.captureRate)}</td>
                  <td className="py-1 text-right">{row.falseLowRows}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div>
        <p className="mb-2 text-xs font-medium text-slate-700">Ownership Ranking</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-slate-600">
                <th className="py-1 text-left">Window</th>
                <th className="py-1 text-right">Top</th>
                <th className="py-1 text-right">Overlap</th>
                <th className="py-1 text-right">Capture</th>
                <th className="py-1 text-right">Rank Corr</th>
              </tr>
            </thead>
            <tbody>
              {rankingRows.map((row) => (
                <tr key={`${row.windowLabel}-${row.topN}`} className="border-b border-slate-100">
                  <td className="py-1 font-medium">{row.windowLabel}</td>
                  <td className="py-1 text-right">{row.topN}</td>
                  <td className="py-1 text-right">{row.capturedRows}/{row.actualTopRows}</td>
                  <td className="py-1 text-right">{fmtPct(row.overlapPct)}</td>
                  <td className="py-1 text-right">{fmt2(row.spearman)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div>
        <p className="mb-2 text-xs font-medium text-slate-700">Leverage Error Rate</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-slate-600">
                <th className="py-1 text-left">Window</th>
                <th className="py-1 text-right">High Impact</th>
                <th className="py-1 text-right">Errors</th>
                <th className="py-1 text-right">Rate</th>
                <th className="py-1 text-right">Avg Err</th>
              </tr>
            </thead>
            <tbody>
              {leverageRows.map((row) => (
                <tr key={row.windowLabel} className="border-b border-slate-100">
                  <td className="py-1 font-medium">{row.windowLabel}</td>
                  <td className="py-1 text-right">{row.highImpactRows}</td>
                  <td className="py-1 text-right">{row.leverageErrorRows}</td>
                  <td className="py-1 text-right">{fmtPct(row.leverageErrorRate)}</td>
                  <td className="py-1 text-right">{fmt2(row.avgAbsError)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
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
              <th className="py-1 text-right">20+ Lift</th>
              <th className="py-1 text-right">25+</th>
              <th className="py-1 text-right">25+ Lift</th>
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
                <td className="py-1 text-right">
                  <SignedValue value={row.lift20Rate} />
                </td>
                <td className="py-1 text-right">{fmtPct(row.hit25Rate)}</td>
                <td className="py-1 text-right">
                  <SignedValue value={row.lift25Rate} />
                </td>
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

function PitcherExploitTable({ rows }: { rows: MlbPostmortemPitcherExploitRow[] }) {
  if (rows.length === 0) return null;

  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Pitcher Exploit Watch</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Pitcher</th>
              <th className="py-1 text-right">Salary</th>
              <th className="py-1 text-right">Proj</th>
              <th className="py-1 text-right">LS</th>
              <th className="py-1 text-right">Own</th>
              <th className="py-1 text-right">Actual Own</th>
              <th className="py-1 text-right">Actual</th>
              <th className="py-1 text-right">Value</th>
              <th className="py-1 text-right">Opp Imp</th>
              <th className="py-1 text-right">ML</th>
              <th className="py-1 text-right">Score</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.slateId}-${row.name}-${row.salary}`} className="border-b border-slate-100">
                <td className="py-1">
                  <div className="font-medium">{row.name}</div>
                  <div className="text-[11px] text-slate-500">{row.slateDate} | {row.teamAbbrev ?? "-"}</div>
                </td>
                <td className="py-1 text-right">{fmtSalary(row.salary)}</td>
                <td className="py-1 text-right">{fmt2(row.projection)}</td>
                <td className="py-1 text-right">{fmt2(row.linestarProjection)}</td>
                <td className="py-1 text-right">{fmtPct(row.fieldOwnPct)}</td>
                <td className="py-1 text-right">{fmtPct(row.actualOwnPct)}</td>
                <td className="py-1 text-right">{fmt2(row.actualFpts)}</td>
                <td className="py-1 text-right">{row.valueMultiple == null ? "-" : `${row.valueMultiple.toFixed(2)}x`}</td>
                <td className="py-1 text-right">{fmt2(row.opponentImplied)}</td>
                <td className="py-1 text-right">{row.moneyline == null ? "-" : row.moneyline}</td>
                <td className="py-1 text-right">{fmt2(row.score)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">Rule-based audit: low projected ownership, viable projection, and favorable Vegas context when available.</p>
    </div>
  );
}

function DecisionCaptureTable({ rows }: { rows: MlbPostmortemDecisionCaptureRow[] }) {
  return (
    <div>
      <p className="mb-2 text-xs font-medium text-slate-700">Decision Capture Baseline</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b text-slate-600">
              <th className="py-1 text-left">Window</th>
              <th className="py-1 text-left">Outcome</th>
              <th className="py-1 text-right">Rows</th>
              <th className="py-1 text-right">High Proj</th>
              <th className="py-1 text-right">Ceiling Pool</th>
              <th className="py-1 text-right">Leverage Pool</th>
              <th className="py-1 text-right">Avg Own</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.windowLabel}-${row.outcomeBucket}`} className="border-b border-slate-100">
                <td className="py-1 font-medium">{row.windowLabel}</td>
                <td className="py-1">{row.outcomeBucket}</td>
                <td className="py-1 text-right">{fmtInt(row.outcomeRows)}</td>
                <td className="py-1 text-right">{fmtPct(row.highProjectionCaptureRate)}</td>
                <td className="py-1 text-right">{fmtPct(row.ceilingCaptureRate)}</td>
                <td className="py-1 text-right">{fmtPct(row.leverageCaptureRate)}</td>
                <td className="py-1 text-right">{fmtPct(row.avgActualOwn)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">Measures whether current decision pools captured top actual outcomes before lineup EV is modeled.</p>
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

      {report.warnings.length > 0 ? (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-3">
          <p className="mb-2 text-xs font-semibold text-amber-900">Postmortem Warning</p>
          <div className="space-y-1">
            {report.warnings.map((warning) => (
              <p key={warning} className="text-xs text-amber-800">{warning}</p>
            ))}
          </div>
        </div>
      ) : null}

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

      <MlbPostmortemCharts
        recentSlates={report.recentSlates}
        projectionSummary={report.projectionSummary}
        ownershipSummary={report.ownershipSummary}
        decisionCapture={report.decisionCapture}
      />

      <div className="grid gap-6 xl:grid-cols-2">
        <ProjectionIndependenceTable rows={report.projectionIndependence} />
        <ProjectionSourceTable rows={report.projectionSources} />
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <ProjectionTable rows={report.projectionSummary} />
        <OwnershipTable rows={report.ownershipSummary} />
      </div>

      <OwnershipDiagnostics
        chalkRows={report.ownershipChalk}
        rankingRows={report.ownershipRanking}
        leverageRows={report.leverageErrors}
      />

      <SignalTable rows={report.signalFollowThrough} />
      <PitcherExploitTable rows={report.pitcherExploitWatch} />
      <DecisionCaptureTable rows={report.decisionCapture} />
      <RecentSlatesTable rows={report.recentSlates} />

      <div className="grid gap-6 xl:grid-cols-2">
        <ProjectionMisses rows={report.projectionMisses} />
        <OwnershipMisses rows={report.ownershipMisses} />
      </div>
    </div>
  );
}
