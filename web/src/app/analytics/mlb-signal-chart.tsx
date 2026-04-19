"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";

type SignalRow = {
  signal: string;
  rows: number;
  slates: number;
  avgActual: number | null;
  avgBeat: number | null;
  hit20Rate: number | null;
  hit25Rate: number | null;
  lift20Rate: number | null;
  lift25Rate: number | null;
};

function formatSignalName(s: string): string {
  return s
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .slice(0, 28); // truncate long names
}

export default function MlbSignalChart({ signals }: { signals: SignalRow[] }) {
  const data = signals.map((s) => ({
    name: formatSignalName(s.signal),
    "20+ Lift": s.lift20Rate ?? 0,
    "25+ Lift": s.lift25Rate ?? 0,
    _rows: s.rows,
    _slates: s.slates,
    _hit20: s.hit20Rate,
    _hit25: s.hit25Rate,
  }));

  const chartHeight = Math.max(180, data.length * 42 + 60);

  return (
    <ResponsiveContainer width="100%" height={chartHeight}>
      <BarChart
        layout="vertical"
        data={data}
        margin={{ top: 4, right: 32, bottom: 4, left: 8 }}
        barCategoryGap="28%"
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" horizontal={false} />
        <XAxis
          type="number"
          tick={{ fontSize: 11 }}
          tickFormatter={(v: number) => `${v > 0 ? "+" : ""}${v.toFixed(0)}%`}
          label={{ value: "Lift vs. baseline (%)", position: "insideBottom", offset: -4, fontSize: 11 }}
        />
        <YAxis
          type="category"
          dataKey="name"
          tick={{ fontSize: 11 }}
          width={148}
        />
        <Tooltip
          formatter={(value, name, props) => {
            const row = props.payload as typeof data[0];
            const liftNum = typeof value === "number" ? value : null;
            const liftStr = liftNum != null ? liftNum.toFixed(1) : "—";
            const hit = name === "20+ Lift" ? row._hit20 : row._hit25;
            const hitStr = hit != null ? ` (hit rate: ${hit.toFixed(1)}%)` : "";
            return [`${liftNum != null && liftNum >= 0 ? "+" : ""}${liftStr}%${hitStr}`, String(name)];
          }}
          labelFormatter={(label) => {
            const row = data.find((d) => d.name === label);
            return `${label} — ${row?._rows ?? 0} rows / ${row?._slates ?? 0} slates`;
          }}
        />
        <Legend verticalAlign="top" />
        <ReferenceLine x={0} stroke="#94a3b8" strokeWidth={1.5} />
        <Bar
          dataKey="20+ Lift"
          fill="#6366f1"
          radius={[0, 3, 3, 0]}
          barSize={14}
        />
        <Bar
          dataKey="25+ Lift"
          fill="#10b981"
          radius={[0, 3, 3, 0]}
          barSize={14}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
