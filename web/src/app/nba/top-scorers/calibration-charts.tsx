"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
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
  LINE_BAND,
  MIN_BIN_N,
  SHORT_MINUTES,
  type CoverageData,
  type ReliabilityBin,
  type ReliabilitySeries,
  type ScatterData,
  type ScatterPoint,
  type SkillRow,
  type TailPoint,
  type TailSeries,
} from "@/lib/nba-points-calibration-charts";

// Validated against the page's #0f1726 surface (dataviz validate_palette, dark mode): blue, orange.
const BLUE = "#3987e5";
const ORANGE = "#d95926";
// Neutral midpoint for the coverage bar (below / inside / above is a polarity around the interval).
const NEUTRAL = "#64748b";
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

function pointShape(color: string) {
  return function ScatterDot(props: { cx?: number; cy?: number; payload?: ScatterPoint }) {
    const { cx = 0, cy = 0, payload } = props;
    if (!payload) return null;
    return payload.short ? (
      <circle cx={cx} cy={cy} r={3.5} fill={SURFACE} stroke={color} strokeWidth={1.5} />
    ) : (
      <circle cx={cx} cy={cy} r={3.5} fill={color} fillOpacity={0.75} stroke={SURFACE} strokeWidth={1} />
    );
  };
}

function PointTooltip({ active, payload }: { active?: boolean; payload?: { payload: ScatterPoint }[] }) {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div className="text-white">{p.player}</div>
      <div>{p.gameDate} · line {p.line} · scored {p.actual}</div>
      <div style={{ color: AXIS }}>
        {p.minutes != null ? `played ${Math.round(p.minutes)} min · ` : ""}captured {Math.round(p.minutesBeforeTip)} min before tip
      </div>
      {p.short ? <div style={{ color: AXIS }}>Short night: under {SHORT_MINUTES} minutes</div> : null}
    </div>
  );
}

type CoverageRow = { name: string; below: number; inside: number; above: number; n: number | null };

function CoverageTooltip({ active, payload }: { active?: boolean; payload?: { payload: CoverageRow }[] }) {
  if (!active || !payload?.length) return null;
  const r = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div className="text-white">{r.name}{r.n != null ? ` · n ${r.n}` : ""}</div>
      <div>Below the floor {pct(r.below, 1)}</div>
      <div>Inside {pct(r.inside, 1)}</div>
      <div>Above the ceiling {pct(r.above, 1)}</div>
    </div>
  );
}

/** Two stacked bars: what the ladder claims (10 / 80 / 10) and what happened. */
function CoverageBar({ coverage }: { coverage: CoverageData }) {
  const rows: CoverageRow[] = [
    { name: "Ladder claims", below: coverage.target.below, inside: coverage.target.inside, above: coverage.target.above, n: null },
    { name: "Happened", below: coverage.below, inside: coverage.inside, above: coverage.above, n: coverage.n },
  ];
  type LabelProps = { x?: number | string; y?: number | string; width?: number | string; height?: number | string; index?: number };
  const label = (key: keyof CoverageData["target"]) =>
    function CoverageLabel(props: LabelProps) {
      const x = Number(props.x ?? 0);
      const y = Number(props.y ?? 0);
      const width = Number(props.width ?? 0);
      const height = Number(props.height ?? 0);
      const value = rows[props.index ?? 0][key];
      if (width < 34) return null;
      return (
        <text x={x + width / 2} y={y + height / 2 + 4} textAnchor="middle" fontSize={11} fill="#e2e8f0" fontFamily="ui-monospace, monospace">
          {pct(value)}
        </text>
      );
    };
  return (
    <div className="rounded-lg border p-2" style={{ borderColor: GRID }}>
      <ResponsiveContainer width="100%" height={110}>
        <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 12, bottom: 0, left: 8 }} barCategoryGap="30%">
          <XAxis type="number" domain={[0, 1]} ticks={[0, 0.25, 0.5, 0.75, 1]} tickFormatter={(v: number) => pct(v)} tick={{ fontSize: 10, fill: AXIS }} axisLine={{ stroke: GRID }} tickLine={false} />
          <YAxis type="category" dataKey="name" width={92} tick={{ fontSize: 11, fill: "#e2e8f0" }} axisLine={false} tickLine={false} />
          <Tooltip content={<CoverageTooltip />} cursor={{ fill: "rgba(148, 163, 184, 0.08)" }} />
          <Bar dataKey="below" name="Below the floor" stackId="c" fill={BLUE} stroke={SURFACE} strokeWidth={2} isAnimationActive={false} label={label("below")} />
          <Bar dataKey="inside" name="Inside" stackId="c" fill={NEUTRAL} stroke={SURFACE} strokeWidth={2} isAnimationActive={false} label={label("inside")} />
          <Bar dataKey="above" name="Above the ceiling" stackId="c" fill={ORANGE} stroke={SURFACE} strokeWidth={2} isAnimationActive={false} label={label("above")} radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
      <div className="flex flex-wrap gap-4 px-2 font-mono text-xs text-slate-400">
        <span><span className="mr-1 inline-block h-2 w-2 rounded-full align-middle" style={{ background: BLUE }} />Below the floor</span>
        <span><span className="mr-1 inline-block h-2 w-2 rounded-full align-middle" style={{ background: NEUTRAL }} />Inside the floor-to-ceiling range</span>
        <span><span className="mr-1 inline-block h-2 w-2 rounded-full align-middle" style={{ background: ORANGE }} />Above the ceiling</span>
      </div>
    </div>
  );
}

function TailTooltip({ active, payload }: { active?: boolean; payload?: { payload: TailPoint }[] }) {
  if (!active || !payload?.length) return null;
  const p = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div className="text-white">{p.date} · n {p.n}</div>
      <div>That night: said {pct(p.statedNight, 1)}, happened {pct(p.observedNight, 1)}</div>
      <div>Through then (n {p.nCum}): said {pct(p.statedCum, 1)}, happened {pct(p.observedCum, 1)}</div>
    </div>
  );
}

function TailPanel({ series }: { series: TailSeries }) {
  const last = series.points[series.points.length - 1];
  const peak = series.points.reduce((m, p) => Math.max(m, p.statedCum, p.observedCum, p.observedNight), 0);
  const maxY = Math.min(1, Math.max(0.1, Math.ceil(peak * 1.25 * 10) / 10));
  return (
    <div className="rounded-lg border p-2" style={{ borderColor: GRID }}>
      <div className="flex items-baseline justify-between font-mono text-xs">
        <span className="text-slate-200">{series.label}</span>
        <span className="text-slate-400">
          n {series.n}
          {last ? ` · said ${pct(last.statedCum, 1)} · happened ${pct(last.observedCum, 1)}` : ""}
        </span>
      </div>
      {series.points.length === 0 ? (
        <div className="flex h-[150px] items-center justify-center font-mono text-xs text-slate-500">no rows carry this reading yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={150}>
          <ComposedChart data={series.points} margin={{ top: 8, right: 10, bottom: 4, left: -18 }}>
            <CartesianGrid stroke={GRID} strokeDasharray="2 4" />
            <XAxis dataKey="date" tickFormatter={(v: string) => v.slice(5)} tick={{ fontSize: 10, fill: AXIS }} axisLine={{ stroke: GRID }} tickLine={false} minTickGap={24} />
            {/* The axis fits the reading (a 40+ chance lives under 15%), capped at 100%, so the tails are not flat lines on a 0-100 scale. */}
            <YAxis
              type="number"
              domain={[0, maxY]}
              ticks={[0, maxY / 2, maxY]}
              tickFormatter={(v: number) => pct(v)}
              tick={{ fontSize: 10, fill: AXIS }}
              axisLine={false}
              tickLine={false}
              width={44}
            />
            <Tooltip content={<TailTooltip />} cursor={{ stroke: AXIS, strokeDasharray: "2 2" }} />
            <Scatter dataKey="observedNight" name="Happened that night" fill={SURFACE} stroke={ORANGE} strokeWidth={1.5} isAnimationActive={false} />
            <Line type="monotone" dataKey="statedCum" name="Books said, running" stroke={BLUE} strokeWidth={2} dot={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="observedCum" name="Happened, running" stroke={ORANGE} strokeWidth={2} dot={false} isAnimationActive={false} />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

const signed = (v: number | null, digits = 3) => (v == null ? "n/a" : `${v >= 0 ? "+" : ""}${v.toFixed(digits)}`);
const fixed = (v: number | null, digits = 3) => (v == null ? "n/a" : v.toFixed(digits));

function SkillTable({ rows, shortNights }: { rows: SkillRow[]; shortNights: { n: number; short: number } }) {
  const nights = rows.reduce((m, r) => Math.max(m, r.nights), 0);
  return (
    <div>
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-white">Does the books&apos; reading beat simple guesses?</h3>
        <span className="font-mono text-xs text-slate-400">
          {nights} {nights === 1 ? "night" : "nights"} · Brier, lower is better · skill above 0 means the books beat the guess
          {shortNights.n ? ` · ${shortNights.short} of ${shortNights.n} graded played under ${SHORT_MINUTES} min` : ""}
        </span>
      </div>
      <div className="mt-2 overflow-x-auto rounded-lg border p-2" style={{ borderColor: GRID }}>
        <table className="w-full font-mono text-xs">
          <thead className="text-slate-400">
            <tr className="text-left">
              <th className="py-1 pr-3 font-normal">Reading</th>
              <th className="py-1 pr-3 text-right font-normal">n</th>
              <th className="py-1 pr-3 text-right font-normal">Books</th>
              <th className="py-1 pr-3 text-right font-normal">Base rate</th>
              <th className="py-1 pr-3 text-right font-normal">Skill vs base</th>
              <th className="py-1 pr-3 text-right font-normal">Paired n</th>
              <th className="py-1 pr-3 text-right font-normal">Books (paired)</th>
              <th className="py-1 pr-3 text-right font-normal">Line only</th>
              <th className="py-1 text-right font-normal">Skill vs line only</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key} className="border-t" style={{ borderColor: GRID }}>
                <td className="py-1 pr-3 text-slate-200">{r.label}</td>
                <td className="py-1 pr-3 text-right">{r.n}</td>
                <td className="py-1 pr-3 text-right text-white">{fixed(r.brier)}</td>
                <td className="py-1 pr-3 text-right">{fixed(r.brierBase)}</td>
                <td className="py-1 pr-3 text-right text-white">{signed(r.skillVsBase)}</td>
                <td className="py-1 pr-3 text-right">{r.lineN}</td>
                <td className="py-1 pr-3 text-right">{fixed(r.brierPaired)}</td>
                <td className="py-1 pr-3 text-right">{fixed(r.brierLine)}</td>
                <td className="py-1 text-right text-white">{signed(r.skillVsLine)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-1 font-mono text-xs text-slate-500">
        Base rate gives every player the pooled hit share; it knows these outcomes, so it is the best any constant can do.
        Line only gives a player the hit share of players with a line in the same {LINE_BAND}-point band on other nights (shrunk
        toward those nights&apos; pooled share), i.e. &quot;stars score more&quot; without the ladder; it never sees its own night, so the
        first night has none. The books&apos; ladder earns its keep only if it beats line only.
      </p>
    </div>
  );
}

export function CalibrationCharts({
  reliability,
  scatter,
  coverage,
  tails,
  skill,
  shortNights,
}: {
  reliability: ReliabilitySeries[];
  scatter: ScatterData;
  coverage: CoverageData;
  tails: TailSeries[];
  skill: SkillRow[];
  shortNights: { n: number; short: number };
}) {
  const close = scatter.points.filter((p) => p.group === "close");
  const early = scatter.points.filter((p) => p.group === "early");
  const span = scatter.firstDate === scatter.lastDate ? scatter.lastDate : `${scatter.firstDate} to ${scatter.lastDate}`;
  return (
    <div className="mt-4 space-y-4">
      <SkillTable rows={skill} shortNights={shortNights} />

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
                  shape={pointShape(BLUE)}
                  isAnimationActive={false}
                />
                <Scatter
                  name={`Captured earlier (${early.length})`}
                  data={early}
                  fill={ORANGE}
                  shape={pointShape(ORANGE)}
                  isAnimationActive={false}
                />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        )}
        <p className="mt-1 font-mono text-xs text-slate-500">
          The diagonal is the line itself: a point above it went over. A hollow point played under {SHORT_MINUTES} minutes (early
          exit, foul trouble, blowout rest); books settle it as played, so it stays graded. A player who did not play is not shown.
        </p>
      </div>

      <div>
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 className="text-sm font-semibold text-white">Where the score landed against the floor and ceiling</h3>
          <span className="font-mono text-xs text-slate-400">
            {coverage.n} player-games with both a floor and a ceiling · every graded night
          </span>
        </div>
        {coverage.n === 0 ? (
          <div className="mt-2 flex h-[110px] items-center justify-center font-mono text-xs text-slate-500">no rows carry both a floor and a ceiling yet</div>
        ) : (
          <div className="mt-2">
            <CoverageBar coverage={coverage} />
          </div>
        )}
        <p className="mt-1 font-mono text-xs text-slate-500">
          The floor is the score the ladder says he beats 90% of the time, the ceiling the one he beats 10% of the time, so the
          ladder claims 10% below, 80% inside, 10% above. Only rows where the ladder reached both ends count.
        </p>
      </div>

      <div>
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 className="text-sm font-semibold text-white">Each reading night by night</h3>
          <span className="font-mono text-xs text-slate-400">
            <span className="mr-3"><span className="mr-1 inline-block h-0.5 w-4 align-middle" style={{ background: BLUE }} />books said, running mean</span>
            <span className="mr-3"><span className="mr-1 inline-block h-0.5 w-4 align-middle" style={{ background: ORANGE }} />happened, running share</span>
            <span><span className="mr-1 inline-block h-2 w-2 rounded-full border-2 align-middle" style={{ borderColor: ORANGE }} />happened that night</span>
          </span>
        </div>
        <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {tails.map((s) => (
            <TailPanel key={s.key} series={s} />
          ))}
        </div>
        <p className="mt-1 font-mono text-xs text-slate-500">
          The two running lines should meet as nights accumulate; a gap that persists is a bias in that reading. Single-night dots
          are small samples and swing.
        </p>
      </div>
    </div>
  );
}
