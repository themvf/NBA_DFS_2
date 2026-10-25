"use client";

import {
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import {
  CLOSE_MINUTES,
  MIN_BIN_N,
  type ReliabilityBin,
  type ReliabilitySeries,
  type ScatterData,
  type ScatterPoint,
} from "@/lib/nba-points-calibration-charts";

// Validated against the page's #0f1726 surface (dataviz validate_palette, dark mode): blue, orange.
const BLUE = "#3987e5";
const ORANGE = "#d95926";
const GRID = "#1e293b";
const AXIS = "#94a3b8";
const SURFACE = "#0f1726";

const pct = (v: number, digits = 0) => `${(v * 100).toFixed(digits)}%`;

const tooltipStyle = {
  backgroundColor: SURFACE,
  border: `1px solid ${GRID}`,
  borderRadius: 6,
  color: "#e2e8f0",
  fontFamily: "ui-monospace, monospace",
  fontSize: 11,
  padding: "6px 8px",
};

function BinTooltip({ active, payload }: { active?: boolean; payload?: { payload: ReliabilityBin }[] }) {
  if (!active || !payload?.length) return null;
  const b = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div>Books said {pct(b.lo)} to {pct(b.hi)} (mean {pct(b.stated, 1)})</div>
      <div>Happened {pct(b.observed, 1)} of {b.n}</div>
      {b.thin ? <div style={{ color: AXIS }}>Under {MIN_BIN_N} rows: read with care</div> : null}
    </div>
  );
}

/** Hollow dot below MIN_BIN_N; size grows with n, 8px floor so it is hittable. */
function BinDot(props: { cx?: number; cy?: number; payload?: ReliabilityBin }) {
  const { cx = 0, cy = 0, payload } = props;
  if (!payload) return null;
  const r = Math.max(4, Math.min(11, 3 + Math.sqrt(payload.n)));
  return (
    <circle
      cx={cx}
      cy={cy}
      r={r}
      fill={payload.thin ? SURFACE : BLUE}
      stroke={BLUE}
      strokeWidth={2}
    />
  );
}

function ReliabilityPanel({ series }: { series: ReliabilitySeries }) {
  return (
    <div className="rounded-lg border border-slate-800 p-2" style={{ borderColor: GRID }}>
      <div className="flex items-baseline justify-between font-mono text-xs">
        <span className="text-slate-200">{series.label}</span>
        <span className="text-slate-400">n {series.n}</span>
      </div>
      {series.n === 0 ? (
        <div className="flex h-[150px] items-center justify-center font-mono text-xs text-slate-500">no rows carry this reading yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={150}>
          <ScatterChart margin={{ top: 8, right: 10, bottom: 4, left: -18 }}>
            <CartesianGrid stroke={GRID} strokeDasharray="2 4" />
            <XAxis
              type="number"
              dataKey="stated"
              domain={[0, 1]}
              ticks={[0, 0.25, 0.5, 0.75, 1]}
              tickFormatter={(v: number) => pct(v)}
              tick={{ fontSize: 10, fill: AXIS }}
              axisLine={{ stroke: GRID }}
              tickLine={false}
            />
            <YAxis
              type="number"
              dataKey="observed"
              domain={[0, 1]}
              ticks={[0, 0.5, 1]}
              tickFormatter={(v: number) => pct(v)}
              tick={{ fontSize: 10, fill: AXIS }}
              axisLine={false}
              tickLine={false}
            />
            <ReferenceLine segment={[{ x: 0, y: 0 }, { x: 1, y: 1 }]} stroke={AXIS} strokeDasharray="4 4" />
            <Tooltip content={<BinTooltip />} cursor={false} />
            <Scatter data={series.bins} shape={<BinDot />} isAnimationActive={false} />
          </ScatterChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

function PointTooltip({ active, payload }: { active?: boolean; payload?: { payload: ScatterPoint }[] }) {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div className="text-white">{p.player}</div>
      <div>{p.gameDate} · line {p.line} · scored {p.actual}</div>
      <div style={{ color: AXIS }}>captured {Math.round(p.minutesBeforeTip)} min before tip</div>
    </div>
  );
}

export function CalibrationCharts({ reliability, scatter }: { reliability: ReliabilitySeries[]; scatter: ScatterData }) {
  const close = scatter.points.filter((p) => p.group === "close");
  const early = scatter.points.filter((p) => p.group === "early");
  const span = scatter.firstDate === scatter.lastDate ? scatter.lastDate : `${scatter.firstDate} to ${scatter.lastDate}`;
  return (
    <div className="mt-4 space-y-4">
      <div>
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 className="text-sm font-semibold text-white">Reliability: what the books said against what happened</h3>
          <span className="font-mono text-xs text-slate-400">
            dot at the mean stated chance of each 10-point bin · size by n · hollow under {MIN_BIN_N} rows · every graded night
          </span>
        </div>
        <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {reliability.map((s) => (
            <ReliabilityPanel key={s.key} series={s} />
          ))}
        </div>
      </div>

      <div>
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 className="text-sm font-semibold text-white">Points scored against the line</h3>
          <span className="font-mono text-xs text-slate-400">
            {scatter.points.length} player-games · {scatter.nights} {scatter.nights === 1 ? "night" : "nights"}
            {span ? ` · ${span}` : ""}
            {scatter.bias != null ? ` · actual minus line ${scatter.bias >= 0 ? "+" : ""}${scatter.bias.toFixed(1)}` : ""}
            {scatter.mae != null ? ` · mean miss ${scatter.mae.toFixed(1)}` : ""}
          </span>
        </div>
        {scatter.points.length === 0 ? (
          <div className="mt-2 flex h-[200px] items-center justify-center font-mono text-xs text-slate-500">nothing graded yet</div>
        ) : (
          <div className="mt-2 rounded-lg border p-2" style={{ borderColor: GRID }}>
            <ResponsiveContainer width="100%" height={340}>
              <ScatterChart margin={{ top: 8, right: 16, bottom: 18, left: -6 }}>
                <CartesianGrid stroke={GRID} strokeDasharray="2 4" />
                <XAxis
                  type="number"
                  dataKey="line"
                  domain={[0, scatter.maxValue]}
                  tick={{ fontSize: 10, fill: AXIS }}
                  axisLine={{ stroke: GRID }}
                  tickLine={false}
                  label={{ value: "Books' line", position: "insideBottom", offset: -10, fontSize: 11, fill: AXIS }}
                />
                <YAxis
                  type="number"
                  dataKey="actual"
                  domain={[0, scatter.maxValue]}
                  tick={{ fontSize: 10, fill: AXIS }}
                  axisLine={false}
                  tickLine={false}
                  label={{ value: "Points scored", angle: -90, position: "insideLeft", offset: 16, fontSize: 11, fill: AXIS }}
                />
                <ZAxis range={[36, 36]} />
                <ReferenceLine
                  segment={[{ x: 0, y: 0 }, { x: scatter.maxValue, y: scatter.maxValue }]}
                  stroke={AXIS}
                  strokeDasharray="4 4"
                />
                <Tooltip content={<PointTooltip />} cursor={false} />
                <Legend
                  verticalAlign="top"
                  height={24}
                  iconType="circle"
                  iconSize={8}
                  wrapperStyle={{ fontSize: 11, color: AXIS, fontFamily: "ui-monospace, monospace" }}
                />
                <Scatter
                  name={`Captured within ${CLOSE_MINUTES / 60} h of tip (${close.length})`}
                  data={close}
                  fill={BLUE}
                  fillOpacity={0.75}
                  stroke={SURFACE}
                  strokeWidth={1}
                  isAnimationActive={false}
                />
                <Scatter
                  name={`Captured earlier (${early.length})`}
                  data={early}
                  fill={ORANGE}
                  fillOpacity={0.75}
                  stroke={SURFACE}
                  strokeWidth={1}
                  isAnimationActive={false}
                />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        )}
        <p className="mt-1 font-mono text-xs text-slate-500">
          The diagonal is the line itself: a point above it went over. A player who did not play is not shown.
        </p>
      </div>
    </div>
  );
}
