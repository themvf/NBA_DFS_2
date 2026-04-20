"use client";

import {
  Area,
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

// ── Minimal local types (mirrors server-side query types) ─────────────────────

type RecentSlateRow = {
  slateDate: string;
  playerRows: number;
  finalMae: number | null;
  linestarMae: number | null;
  fieldOwnMae: number | null;
};

type ProjectionRow = {
  windowLabel: string;
  windowSort: number;
  playerGroup: string;
  rows: number;
  finalMae: number | null;
  linestarMae: number | null;
  ourMae: number | null;
  finalGainVsLineStar: number | null;
};

type OwnershipRow = {
  windowLabel: string;
  windowSort: number;
  fieldMae: number | null;
  fieldCorr: number | null;
  linestarMae: number | null;
};

type DecisionCaptureRow = {
  windowLabel: string;
  windowSort: number;
  outcomeBucket: string;
  outcomeRows: number;
  highProjectionCaptureRate: number | null;
  ceilingCaptureRate: number | null;
  leverageCaptureRate: number | null;
};

type Props = {
  recentSlates: RecentSlateRow[];
  projectionSummary: ProjectionRow[];
  ownershipSummary: OwnershipRow[];
  decisionCapture: DecisionCaptureRow[];
};

// ── Shared helpers ────────────────────────────────────────────────────────────

function fmt1(v: number | null | undefined) {
  return v == null ? "—" : v.toFixed(1);
}

function ChartCard({
  title,
  insight,
  children,
}: {
  title: string;
  insight: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2 rounded-lg border border-slate-200 bg-white p-4">
      <div>
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">{title}</p>
        <p className="mt-0.5 text-xs italic text-slate-600">{insight}</p>
      </div>
      {children}
    </div>
  );
}

// ── Chart 1: Slate-by-slate MAE trend ─────────────────────────────────────────

function MaeTrendChart({ rows }: { rows: RecentSlateRow[] }) {
  // oldest → newest; disambiguate same-date slates with (n)
  const reversed = [...rows].reverse();
  const dateCounts: Record<string, number> = {};
  rows.forEach((r) => { dateCounts[r.slateDate] = (dateCounts[r.slateDate] ?? 0) + 1; });
  const dateIdx: Record<string, number> = {};

  const data = reversed.map((row) => {
    dateIdx[row.slateDate] = (dateIdx[row.slateDate] ?? 0) + 1;
    const multi = dateCounts[row.slateDate] > 1;
    const label = multi
      ? `${row.slateDate.slice(5)} (${dateIdx[row.slateDate]})`
      : row.slateDate.slice(5);
    return {
      label,
      "Final MAE": row.finalMae,
      "LineStar MAE": row.linestarMae,
      "Own MAE": row.fieldOwnMae,
      rows: row.playerRows,
    };
  });

  const latestFinal = rows[0]?.finalMae;
  const latestLs = rows[0]?.linestarMae;
  const delta = latestFinal != null && latestLs != null ? latestFinal - latestLs : null;
  const insight =
    delta == null
      ? "Slate-by-slate projection MAE. Clean early slates vs. full-day volatility."
      : delta > 0
        ? `Latest slate: our model trailed LineStar by ${Math.abs(delta).toFixed(2)} pts MAE.`
        : `Latest slate: our model beat LineStar by ${Math.abs(delta).toFixed(2)} pts MAE.`;

  return (
    <ChartCard title="Slate-by-Slate MAE Trend" insight={insight}>
      <ResponsiveContainer width="100%" height={210}>
        <ComposedChart data={data} margin={{ top: 4, right: 16, bottom: 0, left: -8 }}>
          <defs>
            <linearGradient id="finalGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#6366f1" stopOpacity={0.18} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="label" tick={{ fontSize: 9 }} interval={0} angle={-30} textAnchor="end" height={36} />
          <YAxis tick={{ fontSize: 10 }} tickFormatter={(v: number) => v.toFixed(1)} />
          <Tooltip
            formatter={(v, name) => [typeof v === "number" ? v.toFixed(2) : "—", String(name)]}
            labelFormatter={(l) => `Slate ${l}`}
          />
          <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          <Area
            type="monotone"
            dataKey="Final MAE"
            stroke="#6366f1"
            strokeWidth={2}
            fill="url(#finalGrad)"
            dot={{ r: 3, fill: "#6366f1" }}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="LineStar MAE"
            stroke="#10b981"
            strokeWidth={1.5}
            strokeDasharray="4 2"
            dot={{ r: 2, fill: "#10b981" }}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="Own MAE"
            stroke="#f59e0b"
            strokeWidth={1.5}
            strokeDasharray="6 3"
            dot={{ r: 2, fill: "#f59e0b" }}
            connectNulls={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

// ── Chart 2: Model vs LineStar by player type (Recent 5) ──────────────────────

const GROUP_ORDER = ["All Active", "Hitters", "Pitchers"];

function ModelVsLsChart({ rows }: { rows: ProjectionRow[] }) {
  const recent = rows
    .filter((r) => r.windowSort === 1)
    .sort((a, b) => GROUP_ORDER.indexOf(a.playerGroup) - GROUP_ORDER.indexOf(b.playerGroup));

  const data = recent.map((r) => ({
    group: r.playerGroup === "All Active" ? "All" : r.playerGroup,
    "Final (blended)": r.finalMae,
    LineStar: r.linestarMae,
    "Our (raw)": r.ourMae,
    gain: r.finalGainVsLineStar,
  }));

  const pitcherRow = recent.find((r) => r.playerGroup === "Pitchers");
  const pitcherGain = pitcherRow?.finalGainVsLineStar;
  const insight =
    pitcherGain != null
      ? pitcherGain >= 0
        ? `Pitchers: blended beats LineStar by ${pitcherGain.toFixed(2)} pts. Raw model (gray) shows how much LineStar blending helps.`
        : `Pitchers trail LineStar by ${Math.abs(pitcherGain).toFixed(2)} pts; hitters even further. Blending closes the gap.`
      : "Final = blended projection. Our = raw model only. Lower MAE is better.";

  return (
    <ChartCard title="Our Model vs LineStar — Recent 5 Slates" insight={insight}>
      <ResponsiveContainer width="100%" height={210}>
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, bottom: 0, left: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
          <XAxis type="number" tick={{ fontSize: 10 }} tickFormatter={(v: number) => v.toFixed(1)} />
          <YAxis type="category" dataKey="group" tick={{ fontSize: 11 }} width={44} />
          <Tooltip
            formatter={(v, name) => [typeof v === "number" ? v.toFixed(2) : "—", String(name)]}
          />
          <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          <Bar dataKey="Final (blended)" fill="#6366f1" radius={[0, 3, 3, 0]} barSize={14} />
          <Bar dataKey="LineStar" fill="#10b981" radius={[0, 3, 3, 0]} barSize={14} />
          <Bar dataKey="Our (raw)" fill="#cbd5e1" radius={[0, 3, 3, 0]} barSize={14} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

// ── Chart 3: Ownership accuracy by window ────────────────────────────────────

function OwnershipAccuracyChart({ rows }: { rows: OwnershipRow[] }) {
  const data = [...rows]
    .sort((a, b) => a.windowSort - b.windowSort)
    .map((r) => ({
      window: r.windowLabel.replace("Rolling ", "").replace(" 5", "5"),
      "Field MAE": r.fieldMae,
      "LS MAE": r.linestarMae,
      Corr: r.fieldCorr,
    }));

  const recentRow = rows.find((r) => r.windowSort === 1);
  const corrStr = recentRow?.fieldCorr != null ? recentRow.fieldCorr.toFixed(2) : "—";
  const gainStr =
    recentRow?.fieldMae != null && recentRow?.linestarMae != null
      ? (recentRow.linestarMae - recentRow.fieldMae).toFixed(2)
      : null;
  const insight =
    gainStr != null
      ? `Recent correlation: ${corrStr}. LineStar leads field ownership by ${Math.abs(Number(gainStr)).toFixed(2)} pts MAE — ownership is still our biggest gap.`
      : `Ownership MAE and rank correlation by window. Correlation (right axis) closer to 1.0 is better.`;

  return (
    <ChartCard title="Ownership Accuracy vs LineStar" insight={insight}>
      <ResponsiveContainer width="100%" height={210}>
        <ComposedChart data={data} margin={{ top: 4, right: 44, bottom: 0, left: -8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="window" tick={{ fontSize: 10 }} />
          <YAxis yAxisId="mae" tick={{ fontSize: 10 }} tickFormatter={(v: number) => v.toFixed(1)} />
          <YAxis
            yAxisId="corr"
            orientation="right"
            domain={[0, 1]}
            tick={{ fontSize: 10 }}
            tickFormatter={(v: number) => v.toFixed(2)}
          />
          <Tooltip
            formatter={(v, name) =>
              [typeof v === "number" ? v.toFixed(2) : "—", String(name)]
            }
          />
          <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          <Bar yAxisId="mae" dataKey="Field MAE" fill="#f59e0b" radius={[3, 3, 0, 0]} barSize={24} />
          <Bar yAxisId="mae" dataKey="LS MAE" fill="#10b981" radius={[3, 3, 0, 0]} barSize={24} />
          <Line
            yAxisId="corr"
            type="monotone"
            dataKey="Corr"
            name="Field Corr"
            stroke="#6366f1"
            strokeWidth={2}
            dot={{ r: 4, fill: "#6366f1" }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

// ── Chart 4: Decision pool capture — Rolling 10 ───────────────────────────────

const OUTCOME_ORDER = ["Top 1%", "Top 5%", "Top 10%"];

function DecisionPoolChart({ rows }: { rows: DecisionCaptureRow[] }) {
  const rolling10 = rows
    .filter((r) => r.windowSort === 3)
    .sort((a, b) => OUTCOME_ORDER.indexOf(a.outcomeBucket) - OUTCOME_ORDER.indexOf(b.outcomeBucket));

  const data = rolling10.map((r) => ({
    outcome: r.outcomeBucket,
    "High Proj": r.highProjectionCaptureRate,
    "Ceiling Pool": r.ceilingCaptureRate,
    Leverage: r.leverageCaptureRate,
    n: r.outcomeRows,
  }));

  const top1 = rolling10.find((r) => r.outcomeBucket === "Top 1%");
  const ceilingLead =
    top1?.ceilingCaptureRate != null && top1?.highProjectionCaptureRate != null
      ? top1.ceilingCaptureRate - top1.highProjectionCaptureRate
      : null;
  const levStr = top1?.leverageCaptureRate != null ? `${top1.leverageCaptureRate.toFixed(0)}%` : "—";
  const insight =
    ceilingLead != null
      ? `Ceiling pool leads High Proj by ${ceilingLead.toFixed(0)}pp for top 1% outcomes. Leverage barely registers (${levStr}) — ceiling is the decisive signal.`
      : "Share of top actual performers captured by each decision pool before lineup construction.";

  if (data.length === 0) return null;

  return (
    <ChartCard title="Decision Pool Capture Rate — Rolling 10" insight={insight}>
      <ResponsiveContainer width="100%" height={210}>
        <BarChart data={data} margin={{ top: 4, right: 16, bottom: 0, left: -8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis dataKey="outcome" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 10 }} tickFormatter={(v: number) => `${fmt1(v)}%`} />
          <Tooltip
            formatter={(v, name) => [typeof v === "number" ? `${v.toFixed(1)}%` : "—", String(name)]}
            labelFormatter={(l) => {
              const row = data.find((d) => d.outcome === l);
              return `${l} (n=${row?.n ?? "?"})`;
            }}
          />
          <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          <Bar dataKey="High Proj" fill="#6366f1" radius={[3, 3, 0, 0]} barSize={28} />
          <Bar dataKey="Ceiling Pool" fill="#10b981" radius={[3, 3, 0, 0]} barSize={28} />
          <Bar dataKey="Leverage" fill="#f43f5e" radius={[3, 3, 0, 0]} barSize={28} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

// ── Root export ───────────────────────────────────────────────────────────────

export default function MlbPostmortemCharts({
  recentSlates,
  projectionSummary,
  ownershipSummary,
  decisionCapture,
}: Props) {
  if (recentSlates.length === 0 && projectionSummary.length === 0) return null;

  return (
    <div className="space-y-4">
      <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">
        Visual Story
      </p>
      <MaeTrendChart rows={recentSlates} />
      <div className="grid gap-4 xl:grid-cols-2">
        <ModelVsLsChart rows={projectionSummary} />
        <OwnershipAccuracyChart rows={ownershipSummary} />
      </div>
      <DecisionPoolChart rows={decisionCapture} />
    </div>
  );
}
