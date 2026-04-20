"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import type { SlateTypePerformanceRow, Sport } from "@/db/queries";

const MIN_SLATES = 3;
const MIN_ROWS = 100;

const fmt2 = (v: number | null | undefined) =>
  v == null ? "-" : v.toFixed(2);

const fmtPct = (v: number | null | undefined) =>
  v == null ? "-" : `${v.toFixed(2)}%`;

type MetricKind = "projection" | "ownership";

type SourceMetric = {
  source: "Our" | "LineStar" | "Raw Our";
  label: string;
  shortLabel: string;
  mae: number | null;
  bias: number | null;
  corr?: number | null;
  rows: number;
  rank: number | null;
  color: string;
  kind: MetricKind;
};

type Confidence = {
  score: number;
  label: string;
  tone: string;
};

function BestBadge({ rank }: { rank: number | null }) {
  if (rank !== 1) return null;
  return (
    <span className="ml-1 rounded-full bg-emerald-50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700">
      Source Best
    </span>
  );
}

function OverallBadge() {
  return (
    <span className="ml-1 rounded-full bg-sky-50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-sky-700">
      Overall
    </span>
  );
}

function BiasChip({ bias }: { bias: number | null }) {
  if (bias == null) return <span className="text-slate-400">-</span>;
  const pos = bias > 0;
  return (
    <span className={pos ? "text-rose-700" : "text-sky-700"}>
      {pos ? "+" : ""}
      {bias.toFixed(2)}
    </span>
  );
}

function isReliable(row: SlateTypePerformanceRow, metric: SourceMetric): boolean {
  return row.slates >= MIN_SLATES && metric.rows >= MIN_ROWS && metric.mae != null;
}

function confidenceFor(row: SlateTypePerformanceRow): Confidence {
  if (row.actualRows <= 0 || row.slates <= 0) {
    return { score: 0, label: "No sample", tone: "bg-slate-100 text-slate-500" };
  }

  const slateScore = Math.min(row.slates / 12, 1);
  const rowScore = Math.min(row.actualRows / 2500, 1);
  const score = Math.round((slateScore * 0.55 + rowScore * 0.45) * 100);

  if (row.slates < MIN_SLATES || row.actualRows < MIN_ROWS) {
    return { score, label: "Thin", tone: "bg-amber-50 text-amber-700" };
  }
  if (score >= 80) {
    return { score, label: "Proven", tone: "bg-emerald-50 text-emerald-700" };
  }
  if (score >= 45) {
    return { score, label: "Useful", tone: "bg-sky-50 text-sky-700" };
  }
  return { score, label: "Emerging", tone: "bg-indigo-50 text-indigo-700" };
}

function projectionMetrics(row: SlateTypePerformanceRow): SourceMetric[] {
  return [
    {
      source: "Our",
      label: "Our final projection",
      shortLabel: "Our",
      mae: row.ourFinalProjMae,
      bias: row.ourFinalProjBias,
      rows: row.ourFinalProjRows,
      rank: row.ourFinalProjRank,
      color: "#4f46e5",
      kind: "projection",
    },
    {
      source: "LineStar",
      label: "LineStar projection",
      shortLabel: "LS",
      mae: row.linestarProjMae,
      bias: row.linestarProjBias,
      rows: row.linestarProjRows,
      rank: row.linestarProjRank,
      color: "#059669",
      kind: "projection",
    },
    {
      source: "Raw Our",
      label: "Raw our projection",
      shortLabel: "Raw",
      mae: row.ourProjMae,
      bias: row.ourProjBias,
      rows: row.ourProjRows,
      rank: row.ourProjRank,
      color: "#64748b",
      kind: "projection",
    },
  ];
}

function ownershipMetrics(row: SlateTypePerformanceRow): SourceMetric[] {
  return [
    {
      source: "Our",
      label: "Our ownership",
      shortLabel: "Our",
      mae: row.ourOwnMae,
      bias: row.ourOwnBias,
      corr: row.ourOwnCorr,
      rows: row.ourOwnRows,
      rank: row.ourOwnRank,
      color: "#f59e0b",
      kind: "ownership",
    },
    {
      source: "LineStar",
      label: "LineStar ownership",
      shortLabel: "LS",
      mae: row.linestarOwnMae,
      bias: row.linestarOwnBias,
      corr: row.linestarOwnCorr,
      rows: row.linestarOwnRows,
      rank: row.linestarOwnRank,
      color: "#0ea5e9",
      kind: "ownership",
    },
  ];
}

function pickWinner(row: SlateTypePerformanceRow, metrics: SourceMetric[]): SourceMetric | null {
  return metrics
    .filter((metric) => isReliable(row, metric))
    .sort((a, b) => (a.mae ?? Number.POSITIVE_INFINITY) - (b.mae ?? Number.POSITIVE_INFINITY))[0] ?? null;
}

function metricValue(metric: SourceMetric): string {
  if (metric.kind === "ownership") return fmtPct(metric.mae);
  return `${fmt2(metric.mae)} MAE`;
}

function metricShortValue(metric: SourceMetric): string {
  if (metric.kind === "ownership") return fmtPct(metric.mae);
  return fmt2(metric.mae);
}

function sampleRange(row: SlateTypePerformanceRow): string {
  if (!row.firstSlateDate || !row.lastSlateDate) return "No completed sample";
  return `${row.firstSlateDate.slice(5)} to ${row.lastSlateDate.slice(5)}`;
}

function coveragePct(rows: number, total: number): number {
  if (total <= 0) return 0;
  return Math.max(0, Math.min(100, (rows / total) * 100));
}

function ConfidenceBar({ confidence }: { confidence: Confidence }) {
  return (
    <div>
      <div className="flex items-center justify-between text-[11px]">
        <span className={`rounded-full px-2 py-0.5 font-semibold ${confidence.tone}`}>
          {confidence.label}
        </span>
        <span className="font-mono text-slate-500">{confidence.score}/100</span>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-100">
        <div
          className="h-full rounded-full bg-slate-800"
          style={{ width: `${confidence.score}%` }}
        />
      </div>
    </div>
  );
}

function CoverageStrip({
  label,
  rows,
  total,
  color,
}: {
  label: string;
  rows: number;
  total: number;
  color: string;
}) {
  return (
    <div>
      <div className="mb-1 flex items-center justify-between text-[10px] text-slate-500">
        <span>{label}</span>
        <span>{rows}/{total}</span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-slate-100">
        <div
          className="h-full rounded-full"
          style={{ width: `${coveragePct(rows, total)}%`, backgroundColor: color }}
        />
      </div>
    </div>
  );
}

function BiasCompass({
  label,
  bias,
  scale,
}: {
  label: string;
  bias: number | null;
  scale: number;
}) {
  const clamped = bias == null
    ? 50
    : Math.max(0, Math.min(100, 50 + (bias / scale) * 50));
  return (
    <div>
      <div className="mb-1 flex items-center justify-between text-[10px] text-slate-500">
        <span>{label}</span>
        <span><BiasChip bias={bias} /></span>
      </div>
      <div className="relative h-2 rounded-full bg-gradient-to-r from-sky-100 via-slate-100 to-rose-100">
        <div className="absolute left-1/2 top-0 h-2 w-px bg-slate-400" />
        <div
          className="absolute top-1/2 h-2.5 w-2.5 -translate-y-1/2 rounded-full border border-white bg-slate-900 shadow"
          style={{ left: `calc(${clamped}% - 5px)` }}
        />
      </div>
    </div>
  );
}

function StoryTile({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: string;
  sub: string;
  accent: string;
}) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-3">
      <div className="flex items-center gap-2">
        <span className="h-2 w-2 rounded-full" style={{ backgroundColor: accent }} />
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">{label}</p>
      </div>
      <p className="mt-2 text-lg font-bold text-slate-950">{value}</p>
      <p className="mt-0.5 text-xs text-slate-500">{sub}</p>
    </div>
  );
}

function buildStoryTiles(rows: SlateTypePerformanceRow[], sport: Sport) {
  const strongest = [...rows].sort((a, b) => confidenceFor(b).score - confidenceFor(a).score)[0];

  const projectionCandidates = rows.flatMap((row) =>
    projectionMetrics(row)
      .filter((metric) => metric.source !== "Raw Our" && isReliable(row, metric))
      .map((metric) => ({ row, metric })),
  );
  const ownershipCandidates = rows.flatMap((row) =>
    ownershipMetrics(row)
      .filter((metric) => isReliable(row, metric))
      .map((metric) => ({ row, metric })),
  );
  const projectionLeader = projectionCandidates
    .sort((a, b) => (a.metric.mae ?? Number.POSITIVE_INFINITY) - (b.metric.mae ?? Number.POSITIVE_INFINITY))[0];
  const ownershipLeader = ownershipCandidates
    .sort((a, b) => (a.metric.mae ?? Number.POSITIVE_INFINITY) - (b.metric.mae ?? Number.POSITIVE_INFINITY))[0];
  const sampleRisk = rows
    .filter((row) => row.sampleWarning)
    .sort((a, b) => confidenceFor(a).score - confidenceFor(b).score)[0];

  return [
    {
      label: "Most Reliable Context",
      value: strongest ? `${sport.toUpperCase()} ${strongest.label}` : "-",
      sub: strongest ? `${strongest.slates} slates, ${strongest.actualRows.toLocaleString()} rows` : "No slate samples yet",
      accent: "#111827",
    },
    {
      label: "Projection Leader",
      value: projectionLeader ? `${projectionLeader.metric.source} ${projectionLeader.row.label}` : "-",
      sub: projectionLeader ? `${fmt2(projectionLeader.metric.mae)} MAE across ${projectionLeader.metric.rows.toLocaleString()} rows` : "No reliable projection sample",
      accent: projectionLeader?.metric.color ?? "#4f46e5",
    },
    {
      label: "Ownership Leader",
      value: ownershipLeader ? `${ownershipLeader.metric.source} ${ownershipLeader.row.label}` : "-",
      sub: ownershipLeader ? `${fmtPct(ownershipLeader.metric.mae)} MAE, corr ${fmt2(ownershipLeader.metric.corr)}` : "No reliable ownership sample",
      accent: ownershipLeader?.metric.color ?? "#0ea5e9",
    },
    {
      label: "Sample Risk",
      value: sampleRisk ? sampleRisk.label : "Samples Stable",
      sub: sampleRisk
        ? `${sampleRisk.sampleWarning}: ${sampleRisk.slates} slates, ${sampleRisk.actualRows.toLocaleString()} rows`
        : "All visible timing buckets meet the reliability gate",
      accent: sampleRisk ? "#f59e0b" : "#059669",
    },
  ];
}

function TrustMatrix({ rows, sport }: { rows: SlateTypePerformanceRow[]; sport: Sport }) {
  return (
    <div className="grid gap-3 lg:grid-cols-4">
      {rows.map((row) => {
        const confidence = confidenceFor(row);
        const projectionWinner = pickWinner(row, projectionMetrics(row).filter((metric) => metric.source !== "Raw Our"));
        const ownershipWinner = pickWinner(row, ownershipMetrics(row));
        return (
          <div key={row.contestType} className="rounded-lg border border-slate-200 bg-white p-3">
            <div className="flex items-start justify-between gap-2">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">
                  {sport.toUpperCase()} Slate
                </p>
                <h3 className="text-base font-bold text-slate-950">{row.label}</h3>
              </div>
              {row.sampleWarning ? (
                <span className="rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-700">
                  {row.sampleWarning}
                </span>
              ) : null}
            </div>

            <div className="mt-3">
              <ConfidenceBar confidence={confidence} />
            </div>

            <div className="mt-3 grid gap-2 text-xs">
              <div className="rounded-md bg-slate-50 p-2">
                <p className="text-[10px] uppercase tracking-wide text-slate-400">Projection read</p>
                <p className="mt-0.5 font-semibold text-slate-900">
                  {projectionWinner ? `${projectionWinner.source} ${metricValue(projectionWinner)}` : "No reliable sample"}
                </p>
              </div>
              <div className="rounded-md bg-slate-50 p-2">
                <p className="text-[10px] uppercase tracking-wide text-slate-400">Ownership read</p>
                <p className="mt-0.5 font-semibold text-slate-900">
                  {ownershipWinner ? `${ownershipWinner.source} ${metricValue(ownershipWinner)}` : "No reliable sample"}
                </p>
              </div>
            </div>

            <div className="mt-3 space-y-2">
              <CoverageStrip label="Our proj coverage" rows={row.ourFinalProjRows} total={row.actualRows} color="#4f46e5" />
              <CoverageStrip label="LineStar proj coverage" rows={row.linestarProjRows} total={row.actualRows} color="#059669" />
              <BiasCompass label="Our final bias" bias={row.ourFinalProjBias} scale={6} />
            </div>
            <p className="mt-3 text-[10px] text-slate-400">{sampleRange(row)}</p>
          </div>
        );
      })}
    </div>
  );
}

function AccuracyConfidenceChart({ rows }: { rows: SlateTypePerformanceRow[] }) {
  const points = rows.flatMap((row) => {
    const confidence = confidenceFor(row);
    return projectionMetrics(row)
      .filter((metric) => metric.source !== "Raw Our" && metric.mae != null && metric.rows > 0)
      .map((metric) => ({
        slate: row.label,
        source: metric.source,
        confidence: confidence.score,
        mae: metric.mae,
        rows: metric.rows,
        bias: metric.bias,
      }));
  });

  const our = points.filter((point) => point.source === "Our");
  const linestar = points.filter((point) => point.source === "LineStar");

  if (points.length === 0) return null;

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">
          Accuracy vs Confidence
        </p>
        <p className="mt-0.5 text-xs text-slate-500">
          Projection MAE by timing bucket. Bigger bubbles have more evaluated rows.
        </p>
      </div>
      <ResponsiveContainer width="100%" height={250}>
        <ScatterChart margin={{ top: 18, right: 24, bottom: 8, left: -4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
          <XAxis
            dataKey="confidence"
            type="number"
            name="Confidence"
            domain={[0, 100]}
            tick={{ fontSize: 10 }}
            tickFormatter={(value: number) => `${value}`}
          />
          <YAxis
            dataKey="mae"
            type="number"
            name="MAE"
            tick={{ fontSize: 10 }}
            tickFormatter={(value: number) => value.toFixed(1)}
          />
          <ZAxis dataKey="rows" range={[70, 360]} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            formatter={(value, name) => {
              if (typeof value !== "number") return [String(value), String(name)];
              if (name === "rows") return [value.toLocaleString(), "Rows"];
              return [value.toFixed(2), String(name)];
            }}
          />
          <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
          <Scatter name="Our final" data={our} fill="#4f46e5">
            <LabelList dataKey="slate" position="top" fontSize={10} fill="#475569" />
          </Scatter>
          <Scatter name="LineStar" data={linestar} fill="#059669">
            <LabelList dataKey="slate" position="bottom" fontSize={10} fill="#475569" />
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

function SourceRaceBoard({ rows }: { rows: SlateTypePerformanceRow[] }) {
  const projectionMax = Math.max(
    1,
    ...rows.flatMap((row) =>
      projectionMetrics(row)
        .filter((metric) => metric.source !== "Raw Our")
        .map((metric) => metric.mae ?? 0),
    ),
  );
  const ownershipMax = Math.max(
    1,
    ...rows.flatMap((row) => ownershipMetrics(row).map((metric) => metric.mae ?? 0)),
  );

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">
          Source Race By Slate Type
        </p>
        <p className="mt-0.5 text-xs text-slate-500">
          Error bars compare absolute accuracy inside each timing bucket.
        </p>
      </div>
      <div className="space-y-4">
        {rows.map((row) => (
          <div key={row.contestType} className="rounded-md border border-slate-100 bg-slate-50 p-3">
            <div className="mb-2 flex items-center justify-between gap-2">
              <p className="font-semibold text-slate-900">{row.label}</p>
              <p className="text-[11px] text-slate-500">{row.slates} slates / {row.actualRows.toLocaleString()} rows</p>
            </div>
            <RaceGroup
              row={row}
              title="Projection MAE"
              metrics={projectionMetrics(row).filter((metric) => metric.source !== "Raw Our")}
              max={projectionMax}
            />
            <RaceGroup
              row={row}
              title="Ownership MAE"
              metrics={ownershipMetrics(row)}
              max={ownershipMax}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

function RaceGroup({
  row,
  title,
  metrics,
  max,
}: {
  row: SlateTypePerformanceRow;
  title: string;
  metrics: SourceMetric[];
  max: number;
}) {
  const winner = pickWinner(row, metrics);

  return (
    <div className="mt-3">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-slate-400">{title}</p>
      <div className="space-y-1.5">
        {metrics.map((metric) => {
          const reliable = isReliable(row, metric);
          const width = metric.mae == null ? 0 : Math.max(3, Math.min(100, (metric.mae / max) * 100));
          const isWinner = winner?.label === metric.label;
          return (
            <div key={metric.label}>
              <div className="mb-0.5 flex items-center justify-between gap-2 text-[11px]">
                <span className="font-medium text-slate-700">
                  {metric.label}
                  {isWinner ? <OverallBadge /> : null}
                  <BestBadge rank={metric.rank} />
                </span>
                <span className="font-mono text-slate-600">
                  {reliable ? metricShortValue(metric) : metric.rows > 0 ? `${metricShortValue(metric)} low n` : "-"}
                </span>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-white">
                <div
                  className={`h-full rounded-full ${reliable ? "" : "opacity-35"}`}
                  style={{ width: `${width}%`, backgroundColor: metric.color }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function Playbook({ rows, sport }: { rows: SlateTypePerformanceRow[]; sport: Sport }) {
  const strongest = [...rows].sort((a, b) => confidenceFor(b).score - confidenceFor(a).score)[0] ?? null;
  const noSample = rows.filter((row) => row.actualRows === 0).map((row) => row.label);
  const projectionGaps = rows
    .filter((row) =>
      isReliable(row, projectionMetrics(row)[0])
      && isReliable(row, projectionMetrics(row)[1])
      && row.ourFinalProjMae != null
      && row.linestarProjMae != null,
    )
    .map((row) => ({
      row,
      gap: (row.ourFinalProjMae ?? 0) - (row.linestarProjMae ?? 0),
    }))
    .sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap));
  const largestGap = projectionGaps[0] ?? null;

  return (
    <div className="grid gap-3 md:grid-cols-3">
      <div className="rounded-lg border border-slate-200 bg-white p-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Use Case</p>
        <p className="mt-2 text-sm font-semibold text-slate-950">
          {strongest ? `${sport.toUpperCase()} ${strongest.label} is the baseline read.` : "Baseline read unavailable."}
        </p>
        <p className="mt-1 text-xs text-slate-500">
          {strongest ? `${strongest.actualRows.toLocaleString()} rows gives this timing bucket the clearest signal.` : "No completed slate type has enough data yet."}
        </p>
      </div>
      <div className="rounded-lg border border-slate-200 bg-white p-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Projection Gap</p>
        <p className="mt-2 text-sm font-semibold text-slate-950">
          {largestGap
            ? largestGap.gap > 0
              ? `LineStar leads ${largestGap.row.label} by ${Math.abs(largestGap.gap).toFixed(2)} MAE.`
              : `Our model leads ${largestGap.row.label} by ${Math.abs(largestGap.gap).toFixed(2)} MAE.`
            : "No reliable head-to-head projection gap."}
        </p>
        <p className="mt-1 text-xs text-slate-500">
          {largestGap ? "Treat this as the priority context for calibration changes." : "More completed slates will unlock this comparison."}
        </p>
      </div>
      <div className="rounded-lg border border-slate-200 bg-white p-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Data Need</p>
        <p className="mt-2 text-sm font-semibold text-slate-950">
          {noSample.length > 0 ? `${noSample.join(", ")} still needs results.` : "Every timing bucket has at least one sample."}
        </p>
        <p className="mt-1 text-xs text-slate-500">
          The reliability gate is {MIN_SLATES} slates and {MIN_ROWS} rows for source-level badges.
        </p>
      </div>
    </div>
  );
}

function MetricCell({
  mae,
  bias,
  corr,
  rows,
  rank,
  format = "number",
}: {
  mae: number | null;
  bias: number | null;
  corr?: number | null;
  rows: number;
  rank: number | null;
  format?: "number" | "pct";
}) {
  return (
    <td className="py-1 text-right">
      <div className={rank === 1 ? "font-semibold text-emerald-800" : ""}>
        {format === "pct" && mae != null ? fmtPct(mae) : fmt2(mae)}
        <BestBadge rank={rank} />
      </div>
      <div className="text-[11px] text-slate-500">
        n={rows}
        {" | "}
        bias <BiasChip bias={bias} />
        {corr != null ? <> | corr {fmt2(corr)}</> : null}
      </div>
    </td>
  );
}

function DetailTable({ rows }: { rows: SlateTypePerformanceRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b text-slate-400">
            <th className="py-1 text-left">Slate</th>
            <th className="py-1 text-right">Sample</th>
            <th className="py-1 text-right">Our Proj</th>
            <th className="py-1 text-right">Raw Our</th>
            <th className="py-1 text-right">LS Proj</th>
            <th className="py-1 text-right">Our Own</th>
            <th className="py-1 text-right">LS Own</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.contestType} className="align-top border-b border-slate-100">
              <td className="py-1 font-medium">
                <div>{row.label}</div>
                {row.sampleWarning ? (
                  <div className="text-[11px] text-amber-700">{row.sampleWarning}</div>
                ) : null}
              </td>
              <td className="py-1 text-right">
                <div>{row.slates} slates</div>
                <div className="text-[11px] text-slate-500">{row.actualRows} rows</div>
                <div className="text-[11px] text-slate-500">{sampleRange(row)}</div>
              </td>
              <MetricCell
                mae={row.ourFinalProjMae}
                bias={row.ourFinalProjBias}
                rows={row.ourFinalProjRows}
                rank={row.ourFinalProjRank}
              />
              <MetricCell
                mae={row.ourProjMae}
                bias={row.ourProjBias}
                rows={row.ourProjRows}
                rank={row.ourProjRank}
              />
              <MetricCell
                mae={row.linestarProjMae}
                bias={row.linestarProjBias}
                rows={row.linestarProjRows}
                rank={row.linestarProjRank}
              />
              <MetricCell
                mae={row.ourOwnMae}
                bias={row.ourOwnBias}
                corr={row.ourOwnCorr}
                rows={row.ourOwnRows}
                rank={row.ourOwnRank}
                format="pct"
              />
              <MetricCell
                mae={row.linestarOwnMae}
                bias={row.linestarOwnBias}
                corr={row.linestarOwnCorr}
                rows={row.linestarOwnRows}
                rank={row.linestarOwnRank}
                format="pct"
              />
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function SlateTypePerformancePanel({
  rows,
  sport,
}: {
  rows: SlateTypePerformanceRow[];
  sport: Sport;
}) {
  if (rows.length === 0 || rows.every((row) => row.actualRows === 0)) return null;

  const storyTiles = buildStoryTiles(rows, sport);
  const chartData = rows
    .filter((row) => row.actualRows > 0)
    .map((row) => ({
      label: row.label,
      confidence: confidenceFor(row).score,
      slates: row.slates,
      rows: row.actualRows,
    }));

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Slate Type Performance</h2>
          <p className="mt-1 max-w-3xl text-xs text-slate-500">
            DraftKings timing buckets, source accuracy, sample strength, and coverage in one read.
          </p>
        </div>
        <div className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-[11px] font-semibold text-slate-600">
          Gate: {MIN_SLATES} slates / {MIN_ROWS} rows
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-4">
        {storyTiles.map((tile) => (
          <StoryTile key={tile.label} {...tile} />
        ))}
      </div>

      <div className="mt-5">
        <TrustMatrix rows={rows} sport={sport} />
      </div>

      <div className="mt-5 grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <AccuracyConfidenceChart rows={rows} />
        <SourceRaceBoard rows={rows} />
      </div>

      {chartData.length > 0 && (
        <div className="mt-5 rounded-lg border border-slate-200 bg-white p-4">
          <div className="mb-3">
            <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Sample Footprint</p>
            <p className="mt-0.5 text-xs text-slate-500">
              Rows explain stability; slates explain repeatability.
            </p>
          </div>
          <ResponsiveContainer width="100%" height={190}>
            <BarChart data={chartData} margin={{ top: 18, right: 16, bottom: 0, left: -8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="label" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip
                formatter={(value, name) =>
                  [typeof value === "number" ? value.toLocaleString() : "-", String(name)]
                }
              />
              <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="rows" name="Rows" fill="#4f46e5" radius={[3, 3, 0, 0]} barSize={28} />
              <Bar dataKey="slates" name="Slates" fill="#f59e0b" radius={[3, 3, 0, 0]} barSize={28} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="mt-5">
        <Playbook rows={rows} sport={sport} />
      </div>

      <div className="mt-5 rounded-lg border border-slate-200 bg-white p-3">
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Metric Detail</p>
          <p className="text-[11px] text-slate-500">
            Our Proj uses live projections when present, then raw our_proj.
          </p>
        </div>
        <DetailTable rows={rows} />
      </div>
    </div>
  );
}
