"use client";

import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/**
 * Drive and play stories from CFBD play-by-play (FBS, 2022-25 pooled; per-season
 * tables also show the current season to date). Rows come pre-computed from
 * model/cfb_drive_stories.py (build_tables); this file only arranges them.
 * Descriptive, not betting patterns: every row carries its sample size.
 */

export type DriveFieldPositionRow = {
  Seasons: string;
  Start: string;
  Drives: number;
  "TD (%)": number | null;
  "FG (%)": number | null;
  "Punt (%)": number | null;
  "Turnover (%)": number | null;
  "Points per drive": number | null;
  "Net points per drive": number | null;
};

export type RedZoneRow = {
  Season: string;
  Trips: number;
  "TD (%)": number | null;
  "FG (%)": number | null;
  "No points (%)": number | null;
  "Points per trip": number | null;
};

export type FourthDownRow = {
  Seasons: string;
  Distance: string;
  "Field zone": string;
  "Fourth downs": number;
  "Went for it": number;
  "Go rate (%)": number | null;
  Converted: number;
  "Conversion (%)": number | null;
};

export type FourthDownTrendRow = {
  Season: string;
  "Fourth downs": number;
  "Went for it": number;
  "Go rate (%)": number | null;
  "Short go rate (%)": number | null;
  "Conversion (%)": number | null;
};

export type ComebackRow = {
  Seasons: string;
  "Deficit entering 4th": string;
  Games: number;
  "Trailing team won": number;
  "Comeback (%)": number | null;
  "Went to overtime (%)": number | null;
};

const BLUE = "#2a78d6";
const ORANGE = "#eb6834";
const GREEN = "#1baf7a";
const ALL_ZONES = "All field";

const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const num = (v: number | null | undefined, digits = 2) => (v == null ? "—" : v.toFixed(digits));
const int = (v: number | null | undefined) => (v == null ? "—" : v.toLocaleString());
const pctTick = (v: number) => `${v}%`;

function Card({ title, note, children }: { title: string; note: string; children: React.ReactNode }) {
  return (
    <section className="space-y-3 rounded-lg border bg-card p-4">
      <div>
        <h3 className="text-sm font-semibold">{title}</h3>
        <p className="text-xs text-gray-600">{note}</p>
      </div>
      {children}
    </section>
  );
}

function Empty() {
  return <p className="text-xs text-gray-500">No play-by-play rows loaded yet.</p>;
}

function Th({ children, left }: { children: React.ReactNode; left?: boolean }) {
  return <th className={`py-1 pr-3 font-medium ${left ? "text-left" : "text-right"}`}>{children}</th>;
}

function Td({ children, left }: { children: React.ReactNode; left?: boolean }) {
  return <td className={`py-1 pr-3 tabular-nums ${left ? "text-left" : "text-right"}`}>{children}</td>;
}

function FieldPosition({ rows }: { rows: DriveFieldPositionRow[] }) {
  const data = rows.filter((r) => r.Drives > 0);
  const total = data.reduce((s, r) => s + r.Drives, 0);
  return (
    <Card
      title="What a drive is worth by where it starts"
      note={`FBS regulation drives, ${rows[0]?.Seasons ?? "2022-25"} (${int(total)} drives). Points are the offense's actual score change, so a touchdown includes the try after it. Net subtracts what the defense scored on the drive. Overtime and run-out-the-clock drives are left out.`}
    >
      {data.length === 0 ? (
        <Empty />
      ) : (
        <>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data} margin={{ top: 20, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="Start" tick={{ fontSize: 11 }} interval={0} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip formatter={(v) => num(Number(v))} />
                <Bar dataKey="Points per drive" fill={BLUE} isAnimationActive={false}>
                  <LabelList dataKey="Points per drive" position="top" fontSize={11} formatter={(v) => num(Number(v))} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[560px] text-xs">
              <thead className="border-b text-gray-600">
                <tr>
                  <Th left>Start</Th><Th>Drives</Th><Th>TD</Th><Th>FG</Th><Th>Punt</Th><Th>Turnover</Th>
                  <Th>Points / drive</Th><Th>Net / drive</Th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.Start} className="border-b last:border-0">
                    <Td left>{r.Start}</Td><Td>{int(r.Drives)}</Td><Td>{pct(r["TD (%)"])}</Td><Td>{pct(r["FG (%)"])}</Td>
                    <Td>{pct(r["Punt (%)"])}</Td><Td>{pct(r["Turnover (%)"])}</Td>
                    <Td>{num(r["Points per drive"])}</Td><Td>{num(r["Net points per drive"])}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </Card>
  );
}

function RedZone({ rows }: { rows: RedZoneRow[] }) {
  return (
    <Card
      title="Red-zone trips and how they end"
      note="A trip is a drive with at least one snap at the opponent's 20 or closer; a long touchdown from outside the 20 is not a trip. The current season is to date."
    >
      {rows.length === 0 ? (
        <Empty />
      ) : (
        <>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={rows} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="Season" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={pctTick} domain={[0, 100]} />
                <Tooltip formatter={(v) => pct(Number(v))} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="TD (%)" name="Touchdown" stackId="rz" fill={BLUE} isAnimationActive={false} />
                <Bar dataKey="FG (%)" name="Field goal" stackId="rz" fill={GREEN} isAnimationActive={false} />
                <Bar dataKey="No points (%)" name="No points" stackId="rz" fill={ORANGE} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[440px] text-xs">
              <thead className="border-b text-gray-600">
                <tr><Th left>Season</Th><Th>Trips</Th><Th>TD</Th><Th>FG</Th><Th>No points</Th><Th>Points / trip</Th></tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.Season} className="border-b last:border-0">
                    <Td left>{r.Season}</Td><Td>{int(r.Trips)}</Td><Td>{pct(r["TD (%)"])}</Td><Td>{pct(r["FG (%)"])}</Td>
                    <Td>{pct(r["No points (%)"])}</Td><Td>{num(r["Points per trip"])}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </Card>
  );
}

function FourthDown({ rows, trend }: { rows: FourthDownRow[]; trend: FourthDownTrendRow[] }) {
  const zones = [ALL_ZONES, ...new Set(rows.map((r) => r["Field zone"]).filter((z) => z !== ALL_ZONES))];
  const [zone, setZone] = useState(ALL_ZONES);
  const shown = rows.filter((r) => r["Field zone"] === zone);
  const total = rows.filter((r) => r["Field zone"] === ALL_ZONES).reduce((s, r) => s + r["Fourth downs"], 0);
  return (
    <Card
      title="Fourth down: how often teams go, and how often it works"
      note={`FBS regulation 4th downs, ${rows[0]?.Seasons ?? "2022-25"} (${int(total)} decisions). A decision is a run or pass (fakes included), a punt or a field-goal try; penalties and timeouts are not decisions. A try converts when it gains the distance or scores.`}
    >
      {rows.length === 0 ? (
        <Empty />
      ) : (
        <>
          <div className="flex flex-wrap gap-1 text-xs">
            {zones.map((z) => (
              <button
                key={z}
                onClick={() => setZone(z)}
                className={`rounded border px-2 py-1 ${zone === z ? "border-slate-800 bg-slate-800 text-white" : "bg-white text-gray-700"}`}
              >
                {z}
              </button>
            ))}
          </div>
          <div className="h-60">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={shown} margin={{ top: 20, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="Distance" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={pctTick} domain={[0, 100]} />
                <Tooltip formatter={(v) => pct(Number(v))} />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="Go rate (%)" name="Went for it" fill={BLUE} isAnimationActive={false}>
                  <LabelList dataKey="Go rate (%)" position="top" fontSize={10} formatter={(v) => `${Number(v).toFixed(0)}%`} />
                </Bar>
                <Bar dataKey="Conversion (%)" name="Converted when they went" fill={GREEN} isAnimationActive={false}>
                  <LabelList dataKey="Conversion (%)" position="top" fontSize={10} formatter={(v) => `${Number(v).toFixed(0)}%`} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[520px] text-xs">
              <thead className="border-b text-gray-600">
                <tr>
                  <Th left>Distance</Th><Th left>Field zone</Th><Th>4th downs</Th><Th>Went for it</Th><Th>Go rate</Th>
                  <Th>Converted</Th><Th>Conversion</Th>
                </tr>
              </thead>
              <tbody>
                {shown.map((r) => (
                  <tr key={`${r.Distance}-${r["Field zone"]}`} className="border-b last:border-0">
                    <Td left>{r.Distance}</Td><Td left>{r["Field zone"]}</Td><Td>{int(r["Fourth downs"])}</Td>
                    <Td>{int(r["Went for it"])}</Td><Td>{pct(r["Go rate (%)"])}</Td><Td>{int(r.Converted)}</Td>
                    <Td>{pct(r["Conversion (%)"])}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      <div className="space-y-2 border-t pt-3">
        <h4 className="text-xs font-semibold">Are coaches going for it more?</h4>
        {trend.length === 0 ? (
          <Empty />
        ) : (
          <>
            <div className="h-52">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trend} margin={{ top: 16, right: 16, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="Season" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={pctTick} domain={[0, "auto"]} />
                  <Tooltip formatter={(v) => pct(Number(v))} />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <Line dataKey="Short go rate (%)" name="4th & 1-3" stroke={ORANGE} strokeWidth={2} isAnimationActive={false} />
                  <Line dataKey="Go rate (%)" name="All 4th downs" stroke={BLUE} strokeWidth={2} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[480px] text-xs">
                <thead className="border-b text-gray-600">
                  <tr><Th left>Season</Th><Th>4th downs</Th><Th>Went for it</Th><Th>Go rate</Th><Th>4th &amp; 1-3 go rate</Th><Th>Conversion</Th></tr>
                </thead>
                <tbody>
                  {trend.map((r) => (
                    <tr key={r.Season} className="border-b last:border-0">
                      <Td left>{r.Season}</Td><Td>{int(r["Fourth downs"])}</Td><Td>{int(r["Went for it"])}</Td>
                      <Td>{pct(r["Go rate (%)"])}</Td><Td>{pct(r["Short go rate (%)"])}</Td><Td>{pct(r["Conversion (%)"])}</Td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </Card>
  );
}

function Comebacks({ rows }: { rows: ComebackRow[] }) {
  const data = rows.filter((r) => r.Games > 0);
  const total = data.reduce((s, r) => s + r.Games, 0);
  return (
    <Card
      title="How often the team trailing after three quarters wins"
      note={`FBS games, ${rows[0]?.Seasons ?? "2022-25"} (${int(total)} games not tied entering the 4th), from quarter line scores. Overtime wins count as wins.`}
    >
      {data.length === 0 ? (
        <Empty />
      ) : (
        <>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data} margin={{ top: 20, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="Deficit entering 4th" tick={{ fontSize: 11 }} tickFormatter={(v) => `Down ${v}`} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={pctTick} />
                <Tooltip formatter={(v) => pct(Number(v))} />
                <Bar dataKey="Comeback (%)" name="Trailing team won" fill={ORANGE} isAnimationActive={false}>
                  <LabelList dataKey="Comeback (%)" position="top" fontSize={11} formatter={(v) => pct(Number(v))} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[440px] text-xs">
              <thead className="border-b text-gray-600">
                <tr><Th left>Deficit entering 4th</Th><Th>Games</Th><Th>Trailing team won</Th><Th>Comeback rate</Th><Th>Went to OT</Th></tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r["Deficit entering 4th"]} className="border-b last:border-0">
                    <Td left>{r["Deficit entering 4th"]}</Td><Td>{int(r.Games)}</Td><Td>{int(r["Trailing team won"])}</Td>
                    <Td>{pct(r["Comeback (%)"])}</Td><Td>{pct(r["Went to overtime (%)"])}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </Card>
  );
}

export default function DriveStories({
  fieldPosition,
  redZone,
  fourthDown,
  fourthDownTrend,
  comebacks,
}: {
  fieldPosition: DriveFieldPositionRow[];
  redZone: RedZoneRow[];
  fourthDown: FourthDownRow[];
  fourthDownTrend: FourthDownTrendRow[];
  comebacks: ComebackRow[];
}) {
  return (
    <div className="space-y-4">
      <p className="text-xs text-gray-600">
        Evergreen stories from FBS play-by-play, 2022-25 seasons pooled; tables by season also show the current season to date.
        Descriptive only: these say how college football games tend to unfold, not which bet to make.
      </p>
      <FieldPosition rows={fieldPosition} />
      <div className="grid gap-4 lg:grid-cols-2">
        <RedZone rows={redZone} />
        <Comebacks rows={comebacks} />
      </div>
      <FourthDown rows={fourthDown} trend={fourthDownTrend} />
    </div>
  );
}
