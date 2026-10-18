"use client";

import { Fragment, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { MarketBreakdownRow } from "@/db/cfb-pattern-watch";

/**
 * Favorites and underdogs at the close, from the simplest cut to the most detailed:
 * every game; then one cut at a time (where and when, the line, form and rest);
 * the halftime score (not a pregame bet); then two cuts together.
 * One switch picks the bet (moneyline, spread at -110, total at -110) for every
 * chart, table and grid. Rows come pre-computed from model/cfb_pattern_csv.py
 * (market_breakdown); this file only arranges them. Descriptive, not study patterns.
 */

const A_COLOR = "#2a78d6";
const B_COLOR = "#eb6834";
// Below this many games a cell is shown but muted: a few games say very little.
const THIN = 20;

const signed = (v: number | null | undefined, digits = 1) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const roi = (v: number | null | undefined) => (v == null ? "—" : `${signed(v)}%`);
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");
// Pad both ends past zero so value labels on short or negative bars never sit on the axis labels.
const PADDED: [(m: number) => number, (m: number) => number] = [
  (m) => Math.floor(Math.min(0, m) * 1.25 - 1),
  (m) => Math.ceil(Math.max(0, m) * 1.25 + 1),
];
const short = (s: string | null | undefined) => (s ?? "").replace(/^Spread /, "").replace(/^Total /, "");

type Market = "ml" | "ats" | "ou";
type View = {
  name: string;
  aName: string;            // the side in blue (favorite, or over)
  bName: string;            // the side in orange (underdog, or under)
  games: (r: MarketBreakdownRow) => number;
  aRoi: (r: MarketBreakdownRow) => number | null | undefined;
  bRoi: (r: MarketBreakdownRow) => number | null | undefined;
  aUnits: (r: MarketBreakdownRow) => number | null | undefined;
  bUnits: (r: MarketBreakdownRow) => number | null | undefined;
  aRecord: (r: MarketBreakdownRow) => string | undefined;
  bRecord: (r: MarketBreakdownRow) => string | undefined;
  rateTitle: string;
  rateGap: (r: MarketBreakdownRow) => number | null | undefined;   // percentage points
  rateText: (r: MarketBreakdownRow) => string;
  note: string;
};

const flip = (rec: string | undefined) => {
  if (!rec) return rec;
  const [w, l, ...rest] = rec.split("-");
  return [l, w, ...rest].join("-");
};

const VIEWS: Record<Market, View> = {
  ml: {
    name: "Moneyline",
    aName: "Favorites",
    bName: "Underdogs",
    games: (r) => r.Games,
    aRoi: (r) => r["ROI (%)"],
    bRoi: (r) => r["Underdog ROI (%)"],
    aUnits: (r) => r.Units,
    bUnits: (r) => r["Underdog units"],
    aRecord: (r) => r.Record,
    bRecord: (r) => r["Underdog record"],
    rateTitle: "Favorites' win rate minus price-implied, percentage points",
    rateGap: (r) => r["Difference (pts)"],
    rateText: (r) => `won ${pct(r["Favorites won (%)"])} vs ${pct(r["Price implied (%)"])} implied`,
    note: "One unit on each side at its closing moneyline.",
  },
  ats: {
    name: "Spread",
    aName: "Favorites",
    bName: "Underdogs",
    games: (r) => r.Games,
    aRoi: (r) => r["Fav ATS ROI (%)"],
    bRoi: (r) => r["Dog ATS ROI (%)"],
    aUnits: (r) => r["Fav ATS units"],
    bUnits: (r) => r["Dog ATS units"],
    aRecord: (r) => r["Fav ATS record"],
    bRecord: (r) => flip(r["Fav ATS record"]),
    rateTitle: "Favorites' cover rate minus 50%, percentage points (pushes left out)",
    rateGap: (r) => (r["Fav covered (%)"] == null ? null : r["Fav covered (%)"]! - 50),
    rateText: (r) => `covered ${pct(r["Fav covered (%)"])}; 52.4% breaks even at -110`,
    note: "One unit on each side of the closing spread at -110; a push returns the stake. Records read win-loss-push.",
  },
  ou: {
    name: "Total",
    aName: "Overs",
    bName: "Unders",
    games: (r) => r["Games with total"] ?? 0,
    aRoi: (r) => r["Over ROI (%)"],
    bRoi: (r) => r["Under ROI (%)"],
    aUnits: (r) => r["Over units"],
    bUnits: (r) => r["Under units"],
    aRecord: (r) => r["Over-under record"],
    bRecord: (r) => flip(r["Over-under record"]),
    rateTitle: "Over rate minus 50%, percentage points (pushes left out)",
    rateGap: (r) => (r["Overs (%)"] == null ? null : r["Overs (%)"]! - 50),
    rateText: (r) => `overs ${pct(r["Overs (%)"])}; 52.4% breaks even at -110`,
    note: "One unit on the over and one on the under of the closing total at -110. Games without a total are left out.",
  },
};

function Step({ id, n, title, note, children }: { id: string; n: number; title: string; note: string; children: React.ReactNode }) {
  return (
    <div id={id} className="scroll-mt-28 space-y-3 border-t pt-4">
      <div className="flex items-baseline gap-2">
        <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-slate-800 text-[11px] font-semibold text-white">{n}</span>
        <div>
          <h3 className="text-sm font-semibold">{title}</h3>
          <p className="text-xs text-gray-500">{note}</p>
        </div>
      </div>
      {children}
    </div>
  );
}

function GroupHeading({ title, note }: { title: string; note: string }) {
  return (
    <div className="pt-3">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-700">{title}</div>
      <div className="text-xs text-gray-500">{note}</div>
    </div>
  );
}

function SideCard({ label, color, record, units, roiPct, sub }: {
  label: string; color: string; record: string | undefined; units: number | null | undefined;
  roiPct: number | null | undefined; sub: string;
}) {
  return (
    <div className="rounded-lg border p-4" style={{ borderLeft: `4px solid ${color}` }}>
      <div className="text-[11px] uppercase tracking-wide text-gray-500">{label}</div>
      <div className="mt-1 flex items-baseline gap-3">
        <span className={`text-2xl font-semibold tabular-nums ${tone(roiPct)}`}>{roi(roiPct)}</span>
        <span className="text-sm tabular-nums text-gray-700">{record ?? "—"}</span>
      </div>
      <div className="text-xs tabular-nums text-gray-500">{signed(units, 2)} units · {sub}</div>
    </div>
  );
}

function RoiBars({ rows, view }: { rows: MarketBreakdownRow[]; view: View }) {
  const data = rows.map((r) => ({ name: short(r.Group), games: view.games(r), a: view.aRoi(r) ?? 0, b: view.bRoi(r) ?? 0 }));
  return (
    <div>
      <div className="mb-1 flex flex-wrap items-center gap-x-3 text-xs font-medium text-gray-600">
        <span>Return per unit</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: A_COLOR }} />{view.aName}</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: B_COLOR }} />{view.bName}</span>
      </div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#ecebe7" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} />
            <YAxis domain={PADDED} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
            <ReferenceLine y={0} stroke="#52514e" />
            <Tooltip
              formatter={(v) => roi(Number(v))}
              labelFormatter={(l, items) => `${l}: ${(items?.[0]?.payload as { games?: number } | undefined)?.games ?? 0} games`}
            />
            <Bar dataKey="a" name={view.aName} fill={A_COLOR} isAnimationActive={false}>
              <LabelList dataKey="a" position="top" formatter={(v) => roi(Number(v))} style={{ fontSize: 10 }} />
            </Bar>
            <Bar dataKey="b" name={view.bName} fill={B_COLOR} isAnimationActive={false}>
              <LabelList dataKey="b" position="top" formatter={(v) => roi(Number(v))} style={{ fontSize: 10 }} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function RateBars({ rows, view }: { rows: MarketBreakdownRow[]; view: View }) {
  const data = rows.map((r) => ({ name: short(r.Group), gap: view.rateGap(r) ?? 0, text: view.rateText(r) }));
  return (
    <div>
      <div className="mb-1 text-xs font-medium text-gray-600">{view.rateTitle}</div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#ecebe7" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} />
            <YAxis domain={PADDED} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
            <ReferenceLine y={0} stroke="#52514e" />
            <Tooltip formatter={(v, _n, item) => [`${signed(Number(v))} pts (${(item?.payload as { text: string }).text})`, "Gap"]} />
            <Bar dataKey="gap" isAnimationActive={false}>
              {data.map((d) => <Cell key={d.name} fill={d.gap >= 0 ? "#1baf7a" : "#e34948"} />)}
              <LabelList dataKey="gap" position="top" formatter={(v) => `${signed(Number(v))}`} style={{ fontSize: 10 }} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function BandTable({ rows, view, market, first }: { rows: MarketBreakdownRow[]; view: View; market: Market; first: string }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b text-gray-500">
            <th className="py-1 pr-3 text-left">{first}</th>
            <th className="py-1 pr-3 text-right">Games</th>
            <th className="py-1 pr-3 text-right">{view.aName} record</th>
            {market === "ml" && <><th className="py-1 pr-3 text-right">Won</th><th className="py-1 pr-3 text-right">Implied</th></>}
            {market === "ats" && <th className="py-1 pr-3 text-right">Covered</th>}
            {market === "ou" && <th className="py-1 pr-3 text-right">Overs</th>}
            <th className="py-1 pr-3 text-right">Gap</th>
            <th className="py-1 pr-3 text-right">{view.aName} units</th>
            <th className="py-1 pr-3 text-right">{view.aName} ROI</th>
            {market === "ml" && <th className="py-1 pr-3 text-right">Fav median</th>}
            <th className="border-l py-1 pl-3 pr-3 text-right">{view.bName} units</th>
            <th className="py-1 pr-3 text-right">{view.bName} ROI</th>
            {market === "ml" && <th className="py-1 text-right">Dog median</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((m) => {
            const games = view.games(m);
            const gap = view.rateGap(m);
            return (
              <tr key={m.Split} className={`border-b border-gray-50 tabular-nums ${games < THIN ? "text-gray-400" : ""}`}>
                <td className="py-1 pr-3">{m.Split}</td>
                <td className="py-1 pr-3 text-right">{games}</td>
                <td className="py-1 pr-3 text-right">{view.aRecord(m) ?? "—"}</td>
                {market === "ml" && <><td className="py-1 pr-3 text-right">{pct(m["Favorites won (%)"])}</td><td className="py-1 pr-3 text-right">{pct(m["Price implied (%)"])}</td></>}
                {market === "ats" && <td className="py-1 pr-3 text-right">{pct(m["Fav covered (%)"])}</td>}
                {market === "ou" && <td className="py-1 pr-3 text-right">{pct(m["Overs (%)"])}</td>}
                <td className={`py-1 pr-3 text-right ${tone(gap)}`}>{gap == null ? "—" : `${signed(gap)} pts`}</td>
                <td className={`py-1 pr-3 text-right ${tone(view.aUnits(m))}`}>{signed(view.aUnits(m), 2)}</td>
                <td className={`py-1 pr-3 text-right ${tone(view.aRoi(m))}`}>{roi(view.aRoi(m))}</td>
                {market === "ml" && <td className="py-1 pr-3 text-right">{m["Median price"] ?? "—"}</td>}
                <td className={`border-l py-1 pl-3 pr-3 text-right ${tone(view.bUnits(m))}`}>{signed(view.bUnits(m), 2)}</td>
                <td className={`py-1 pr-3 text-right ${tone(view.bRoi(m))}`}>{roi(view.bRoi(m))}</td>
                {market === "ml" && <td className="py-1 text-right">{m["Underdog median price"] ?? "—"}</td>}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function OneCut({ rows, view, market, first }: { rows: MarketBreakdownRow[]; view: View; market: Market; first: string }) {
  return (
    <Fragment>
      <div className="grid gap-4 lg:grid-cols-2">
        <RoiBars rows={rows} view={view} />
        <RateBars rows={rows} view={view} />
      </div>
      <BandTable rows={rows} view={view} market={market} first={first} />
    </Fragment>
  );
}

// Diverging fill for an ROI cell: green above zero, red below, stronger with size, capped at +-40%.
function heat(v: number | null | undefined, games: number): React.CSSProperties {
  if (v == null || games === 0) return { background: "#f6f6f4" };
  const a = Math.min(Math.abs(v), 40) / 40;
  const alpha = (games < THIN ? 0.35 : 1) * (0.12 + 0.6 * a);
  return { background: v >= 0 ? `rgba(27,175,122,${alpha})` : `rgba(227,73,72,${alpha})` };
}

function Grid({ rows, view, market, rowName, colName }: {
  rows: MarketBreakdownRow[]; view: View; market: Market; rowName: string; colName: string;
}) {
  const [side, setSide] = useState<"a" | "b">("a");
  const [showTable, setShowTable] = useState(false);
  const rowVals = [...new Set(rows.map((r) => r.Group ?? ""))];
  const colVals = [...new Set(rows.map((r) => r.Column ?? ""))];
  const cell = (a: string, b: string) => rows.find((r) => r.Group === a && r.Column === b);
  const sideName = side === "a" ? view.aName : view.bName;
  return (
    <div className="space-y-2">
      <div className="flex gap-1 text-xs">
        {(["a", "b"] as const).map((k) => (
          <button
            key={k}
            onClick={() => setSide(k)}
            className={`rounded border px-2 py-1 ${side === k ? "border-slate-800 bg-slate-800 text-white" : "bg-white text-gray-700"}`}
          >
            {k === "a" ? view.aName : view.bName}
          </button>
        ))}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[520px] border-separate border-spacing-1 text-xs">
          <thead>
            <tr>
              <th className="p-1 text-left font-normal text-gray-500">{rowName} ↓ · {colName} →</th>
              {colVals.map((c) => <th key={c} className="p-1 text-center font-medium">{short(c)}</th>)}
            </tr>
          </thead>
          <tbody>
            {rowVals.map((a) => (
              <tr key={a}>
                <th className="p-1 text-left font-medium">{short(a)}</th>
                {colVals.map((b) => {
                  const c = cell(a, b);
                  const games = c ? view.games(c) : 0;
                  const v = c ? (side === "a" ? view.aRoi(c) : view.bRoi(c)) : null;
                  const rec = c ? (side === "a" ? view.aRecord(c) : view.bRecord(c)) : undefined;
                  return (
                    <td key={b} className="rounded p-2 text-center tabular-nums" style={heat(v, games)}>
                      {games === 0 ? (
                        <span className="text-gray-400">no games</span>
                      ) : (
                        <Fragment>
                          <div className={`text-sm font-semibold ${games < THIN ? "text-gray-500" : ""}`}>{roi(v)}</div>
                          <div className="text-[10px] text-gray-600">{rec} · {games} g</div>
                        </Fragment>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-[11px] text-gray-500">
        Each cell is one unit per game on {sideName.toLowerCase()}. Faded cells have fewer than {THIN} games.
      </p>
      <button className="text-xs text-blue-700 underline" onClick={() => setShowTable((v) => !v)}>
        {showTable ? "Hide" : "Show"} all {rows.length} cells as a table
      </button>
      {showTable && <BandTable rows={rows} view={view} market={market} first={`${rowName} and ${colName.toLowerCase()}`} />}
    </div>
  );
}

type Cut = { level: string; title: string; first: string; note: string; id: string };

const WHERE_WHEN: Cut[] = [
  { id: "venue", level: "By venue", title: "Home or away favorite", first: "Favorite", note: "Where the favorite played. Neutral-site games are kept apart so home and away mean what they say." },
  { id: "conference", level: "By conference game", title: "Conference or non-conference", first: "Game", note: "Non-conference games hold most of the early-season mismatches." },
  { id: "tier", level: "By matchup tier", title: "Matchup tier", first: "Matchup", note: "Power means the SEC, Big Ten, Big 12, ACC and Notre Dame; everyone else is Group of Five." },
  { id: "kickoff", level: "By kickoff", title: "Kickoff window", first: "Kickoff", note: "Eastern time. Pattern T5 lives in the Saturday 5 pm or later window." },
];
const THE_LINE: Cut[] = [
  { id: "spread", level: "By spread", title: "Spread", first: "Spread", note: "The size of the closing spread. Every game falls in exactly one band." },
  { id: "total", level: "By total", title: "Total", first: "Total", note: "The closing over/under." },
  { id: "price", level: "By price", title: "Favorite's moneyline price", first: "Price", note: "The favorite's closing American price; the favorite-longshot check." },
  { id: "move", level: "By line movement", title: "Line movement", first: "Movement", note: "The favorite's vig-free win probability from our earliest capture (at least 12 hours before kickoff) to the close. Within 2 points counts as held." },
];
const FORM_REST: Cut[] = [
  { id: "last", level: "By last result", title: "Favorite's last result", first: "Last game", note: "Whether the favorite won or lost its previous game. A team's first game has none." },
  { id: "rest", level: "By rest", title: "Rest", first: "Rest", note: "Days since each team's previous game. More rest means 3 or more extra days (usually a bye); a short week is 5 days or fewer for either team." },
];
const GRIDS = [
  { id: "venue-spread", level: "Venue and spread", title: "Home or away, by spread", row: "Favorite", col: "Spread" },
  { id: "venue-total", level: "Venue and total", title: "Home or away, by total", row: "Favorite", col: "Total" },
  { id: "spread-total", level: "Spread and total", title: "Spread and total together", row: "Spread", col: "Total" },
  { id: "tier-spread", level: "Tier and spread", title: "Matchup tier, by spread", row: "Matchup", col: "Spread" },
  { id: "kickoff-tier", level: "Kickoff and tier", title: "Kickoff window, by matchup tier", row: "Kickoff", col: "Matchup" },
];

export default function MarketBreakdown({ rows }: { rows: MarketBreakdownRow[] }) {
  const [market, setMarket] = useState<Market>("ml");
  const view = VIEWS[market];
  const level = (name: string) => rows.filter((r) => r["Level name"] === name);
  const overall = level("All games")[0];
  if (!overall) return null;
  // How many games a cut could not place (no total, no early line, first game of the season).
  const unplaced = (cut: MarketBreakdownRow[]) => view.games(overall) - cut.reduce((s, r) => s + view.games(r), 0);
  const withUnplaced = (note: string, cut: MarketBreakdownRow[]) => {
    const n = unplaced(cut);
    return n > 0 ? `${note} ${n} game${n === 1 ? "" : "s"} left out.` : note;
  };
  let n = 1;
  const steps = (cuts: Cut[]) =>
    cuts.map((c) => {
      const cut = level(c.level);
      if (cut.length === 0) return null;
      return (
        <Step key={c.id} id={`breakdown-${c.id}`} n={++n} title={c.title} note={withUnplaced(c.note, cut)}>
          <OneCut rows={cut} view={view} market={market} first={c.first} />
        </Step>
      );
    });
  const half = level("By halftime score");
  const nav = [...WHERE_WHEN, ...THE_LINE, ...FORM_REST].filter((c) => level(c.level).length > 0);

  return (
    <section className="space-y-4 rounded-lg border bg-card p-4">
      <div className="space-y-3">
        <div>
          <h2 className="text-sm font-semibold">Favorites and underdogs, season to date</h2>
          <p className="text-xs text-gray-500">
            All FBS-vs-FBS regular-season games with a closing spread and moneylines. The whole slate first, then one cut at a time,
            then two cuts together. Descriptive only: not study patterns and not graded for a verdict. With this many cuts of the same
            games, some groups will look striking by chance.
          </p>
        </div>
        <nav className="flex flex-wrap gap-1 text-[11px]">
          {[...nav.map((c) => ({ id: c.id, title: c.title })), ...(half.length ? [{ id: "half", title: "Halftime" }] : []),
            ...GRIDS.filter((g) => level(g.level).length > 0).map((g) => ({ id: g.id, title: g.title }))].map((c) => (
            <a key={c.id} href={`#breakdown-${c.id}`} className="rounded-full border px-2 py-0.5 text-gray-600 hover:bg-slate-50">{c.title}</a>
          ))}
        </nav>
      </div>
      <div className="sticky top-[57px] z-10 -mx-4 flex flex-wrap items-center gap-2 border-y bg-card px-4 py-2">
        <span className="text-xs text-gray-500">Bet</span>
        {(Object.keys(VIEWS) as Market[]).map((k) => (
          <button
            key={k}
            onClick={() => setMarket(k)}
            className={`rounded border px-3 py-1 text-xs ${market === k ? "border-slate-800 bg-slate-800 text-white" : "bg-white text-gray-700"}`}
          >
            {VIEWS[k].name}
          </button>
        ))}
        <span className="text-[11px] text-gray-500">{view.note}</span>
      </div>

      <Step id="breakdown-all" n={1} title="Every game" note={`${view.games(overall)} games. The starting point every cut below adds up to.`}>
        <div className="grid gap-3 sm:grid-cols-2">
          <SideCard label={view.aName} color={A_COLOR} record={view.aRecord(overall)} units={view.aUnits(overall)}
            roiPct={view.aRoi(overall)} sub={view.rateText(overall)} />
          <SideCard label={view.bName} color={B_COLOR} record={view.bRecord(overall)} units={view.bUnits(overall)}
            roiPct={view.bRoi(overall)} sub={market === "ml" ? `median price ${overall["Underdog median price"] ?? "—"}` : "same games, other side"} />
        </div>
      </Step>

      <GroupHeading title="Where and when" note="Who is home, what kind of game, and when it kicks off." />
      {steps(WHERE_WHEN)}

      <GroupHeading title="The line" note="What the closing market said, and how it got there." />
      {steps(THE_LINE)}

      <GroupHeading title="Form and rest" note="What each team did last week and how long it has had since." />
      {steps(FORM_REST)}

      {half.length > 0 && (
        <Fragment>
          <GroupHeading title="In the game" note="Not a pregame bet: the halftime score is only known at halftime. Results are still graded at the closing price." />
          <Step id="breakdown-half" n={++n} title="Halftime score" note={withUnplaced("How often favorites finished the job from each halftime position. Context for live betting.", half)}>
            <OneCut rows={half} view={view} market={market} first="At half" />
          </Step>
        </Fragment>
      )}

      <GroupHeading title="Two cuts together" note="The most detailed views and the smallest samples." />
      {GRIDS.map((g) => {
        const cut = level(g.level);
        if (cut.length === 0) return null;
        return (
          <Step key={g.id} id={`breakdown-${g.id}`} n={++n} title={g.title} note={`Every ${g.row.toLowerCase()} group crossed with every ${g.col.toLowerCase()} group.`}>
            <Grid rows={cut} view={view} market={market} rowName={g.row} colName={g.col} />
          </Step>
        );
      })}
    </section>
  );
}
