"use client";

import { useMemo, useState } from "react";
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

/**
 * Team against-the-spread standings and conference report cards, season to date.
 * Rows come pre-computed from model/cfb_team_ats.py (team_rows, conference_rows):
 * closing spreads and totals graded at -110, regular season, every game an FBS
 * team played with a closing spread (FCS opponents count for the FBS team).
 * Descriptive, not study patterns and not a recommendation.
 */

export type TeamAtsRow = {
  Team: string;
  Conference: string | null;
  Games: number;
  "Vs FCS": number;
  "SU record": string;
  "ATS record": string;
  "Covered (%)": number | null;
  "ATS units": number;
  "Avg cover margin": number;
  "Over-under record": string;
  "Overs (%)": number | null;
  "Favorite ATS": string;
  "Favorite covered (%)": number | null;
  "Favorite games": number;
  "Underdog ATS": string;
  "Underdog covered (%)": number | null;
  "Underdog games": number;
  "Home ATS": string;
  "Home covered (%)": number | null;
  "Home games": number;
  "Away ATS": string;
  "Away covered (%)": number | null;
  "Away games": number;
};

export type ConferenceRow = {
  Conference: string;
  Teams: number;
  "Team-games": number;
  "SU record": string;
  "ATS record": string;
  "Covered (%)": number | null;
  "Overs (%)": number | null;
  "Non-conference games": number;
  "Non-conference SU": string;
  "Non-conference ATS": string;
  "Non-conference covered (%)": number | null;
  "Non-conference ATS units": number;
  "Non-conference overs (%)": number | null;
  "Road favorite ATS": string;
  "Road favorite covered (%)": number | null;
  "Road favorite games": number;
  "Home underdog ATS": string;
  "Home underdog covered (%)": number | null;
  "Home underdog games": number;
};

const A_COLOR = "#2a78d6";
const B_COLOR = "#eb6834";
// Faded below these sample sizes: a handful of games says very little.
const THIN_TEAM = 4;
const THIN_SPLIT = 3;
const THIN_CONF = 20;

const signed = (v: number | null | undefined, digits = 1) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
const pct = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}%`);
const tone = (v: number | null | undefined) => (v == null || v === 0 ? "" : v > 0 ? "text-emerald-700" : "text-rose-700");
const toneVs50 = (v: number | null | undefined) => tone(v == null ? v : v - 50);

type Col = {
  key: string;
  label: string;
  sort: (r: TeamAtsRow) => number | string | null;
  cell: (r: TeamAtsRow) => React.ReactNode;
  left?: boolean;
  divider?: boolean;
};

function SplitCell({ record, rate, games }: { record: string; rate: number | null; games: number }) {
  if (!games) return <span className="text-gray-300">—</span>;
  return (
    <span className={games < THIN_SPLIT ? "text-gray-400" : toneVs50(rate)} title={`${pct(rate)} covered over ${games} games`}>
      {record}
    </span>
  );
}

const split = (prefix: "Favorite" | "Underdog" | "Home" | "Away", label: string, divider = false): Col => ({
  key: prefix,
  label,
  divider,
  sort: (r) => r[`${prefix} covered (%)`],
  cell: (r) => <SplitCell record={r[`${prefix} ATS`]} rate={r[`${prefix} covered (%)`]} games={r[`${prefix} games`]} />,
});

const COLUMNS: Col[] = [
  { key: "Team", label: "Team", left: true, sort: (r) => r.Team, cell: (r) => <span className="font-medium">{r.Team}</span> },
  { key: "Conference", label: "Conference", left: true, sort: (r) => r.Conference ?? "", cell: (r) => r.Conference ?? "—" },
  { key: "Games", label: "Games", sort: (r) => r.Games, cell: (r) => (r["Vs FCS"] ? `${r.Games} (${r["Vs FCS"]} FCS)` : r.Games) },
  { key: "SU", label: "SU", sort: (r) => Number(r["SU record"].split("-")[0]) / Math.max(1, r.Games), cell: (r) => r["SU record"] },
  { key: "ATS", label: "ATS", divider: true, sort: (r) => r["Covered (%)"], cell: (r) => r["ATS record"] },
  { key: "Cover", label: "Covered", sort: (r) => r["Covered (%)"], cell: (r) => <span className={toneVs50(r["Covered (%)"])}>{pct(r["Covered (%)"])}</span> },
  { key: "Units", label: "Units", sort: (r) => r["ATS units"], cell: (r) => <span className={tone(r["ATS units"])}>{signed(r["ATS units"], 2)}</span> },
  { key: "Margin", label: "Avg cover margin", sort: (r) => r["Avg cover margin"], cell: (r) => <span className={tone(r["Avg cover margin"])}>{signed(r["Avg cover margin"])}</span> },
  { key: "OU", label: "O-U", divider: true, sort: (r) => r["Overs (%)"], cell: (r) => r["Over-under record"] },
  { key: "Overs", label: "Overs", sort: (r) => r["Overs (%)"], cell: (r) => pct(r["Overs (%)"]) },
  split("Favorite", "As favorite", true),
  split("Underdog", "As underdog"),
  split("Home", "Home"),
  split("Away", "Away"),
];

function compare(a: number | string | null, b: number | string | null, dir: 1 | -1) {
  if (a == null && b == null) return 0;
  if (a == null) return 1;   // missing values always last
  if (b == null) return -1;
  if (typeof a === "string" || typeof b === "string") return String(a).localeCompare(String(b)) * dir;
  return (a - b) * dir;
}

function TeamTable({ teams }: { teams: TeamAtsRow[] }) {
  const [query, setQuery] = useState("");
  const [conf, setConf] = useState("all");
  const [sortKey, setSortKey] = useState("Cover");
  const [dir, setDir] = useState<1 | -1>(-1);
  const conferences = useMemo(
    () => Array.from(new Set(teams.map((t) => t.Conference).filter((c): c is string => !!c))).sort(),
    [teams],
  );
  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    const col = COLUMNS.find((c) => c.key === sortKey) ?? COLUMNS[5];
    return teams
      .filter((t) => (conf === "all" || t.Conference === conf) && (!q || t.Team.toLowerCase().includes(q)))
      .slice()
      .sort((a, b) => compare(col.sort(a), col.sort(b), dir) || compare(a["ATS units"], b["ATS units"], -1) || a.Team.localeCompare(b.Team));
  }, [teams, query, conf, sortKey, dir]);

  const onSort = (key: string, left?: boolean) => {
    if (key === sortKey) setDir(dir === 1 ? -1 : 1);
    else {
      setSortKey(key);
      setDir(left ? 1 : -1);
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search team"
          className="h-8 w-48 rounded-md border px-2 text-xs"
          aria-label="Search team"
        />
        <select value={conf} onChange={(e) => setConf(e.target.value)} className="h-8 rounded-md border px-2 text-xs" aria-label="Conference">
          <option value="all">All conferences</option>
          {conferences.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <span className="text-xs text-gray-500">{rows.length} of {teams.length} teams · click a header to sort</span>
      </div>
      <div className="max-h-[32rem] overflow-auto">
        <table className="min-w-full text-xs">
          <thead className="sticky top-0 bg-card">
            <tr className="border-b text-gray-500">
              <th className="py-1 pr-2 text-right">#</th>
              {COLUMNS.map((c) => (
                <th
                  key={c.key}
                  className={`cursor-pointer select-none whitespace-nowrap py-1 pr-3 ${c.left ? "text-left" : "text-right"} ${c.divider ? "border-l pl-3" : ""} ${sortKey === c.key ? "text-gray-900" : ""}`}
                  onClick={() => onSort(c.key, c.left)}
                  aria-sort={sortKey === c.key ? (dir === 1 ? "ascending" : "descending") : "none"}
                >
                  {c.label}{sortKey === c.key ? (dir === 1 ? " ▲" : " ▼") : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={`${r.Team}-${r.Conference}`} className={`border-b border-gray-50 tabular-nums ${r.Games < THIN_TEAM ? "text-gray-400" : ""}`}>
                <td className="py-1 pr-2 text-right text-gray-400">{i + 1}</td>
                {COLUMNS.map((c) => (
                  <td key={c.key} className={`whitespace-nowrap py-1 pr-3 ${c.left ? "text-left" : "text-right"} ${c.divider ? "border-l pl-3" : ""}`}>
                    {c.cell(r)}
                  </td>
                ))}
              </tr>
            ))}
            {!rows.length && (
              <tr><td colSpan={COLUMNS.length + 1} className="py-3 text-center text-gray-500">No teams match.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ConferenceChart({ conferences }: { conferences: ConferenceRow[] }) {
  const data = conferences
    .filter((c) => c["Non-conference covered (%)"] != null)
    .map((c) => ({
      name: c.Conference.replace("Conference USA", "C-USA").replace("American Athletic", "American").replace("FBS Independents", "Independents"),
      games: c["Non-conference games"],
      cover: c["Non-conference covered (%)"] as number,
      over: c["Non-conference overs (%)"] ?? 0,
    }));
  if (!data.length) return null;
  return (
    <div>
      <div className="mb-1 flex flex-wrap items-center gap-x-3 text-xs font-medium text-gray-600">
        <span>Non-conference games</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: A_COLOR }} />Covered</span>
        <span className="flex items-center gap-1 font-normal"><span className="h-2 w-2 rounded-sm" style={{ background: B_COLOR }} />Went over</span>
      </div>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 18, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#ecebe7" vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} interval={0} angle={-25} textAnchor="end" height={52} />
            <YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
            <ReferenceLine y={50} stroke="#52514e" strokeDasharray="4 3" />
            <Tooltip
              formatter={(v) => pct(Number(v))}
              labelFormatter={(l, items) => `${l}: ${(items?.[0]?.payload as { games?: number } | undefined)?.games ?? 0} non-conference games`}
            />
            <Bar dataKey="cover" name="Covered" isAnimationActive={false}>
              {data.map((d) => <Cell key={d.name} fill={A_COLOR} fillOpacity={d.games < THIN_CONF ? 0.45 : 1} />)}
              <LabelList dataKey="cover" position="top" formatter={(v) => `${Number(v).toFixed(0)}`} style={{ fontSize: 10 }} />
            </Bar>
            <Bar dataKey="over" name="Went over" isAnimationActive={false}>
              {data.map((d) => <Cell key={d.name} fill={B_COLOR} fillOpacity={d.games < THIN_CONF ? 0.45 : 1} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function ConfSplit({ record, rate, games }: { record: string; rate: number | null; games: number }) {
  if (!games) return <span className="text-gray-300">—</span>;
  return <span className={games < 8 ? "text-gray-400" : toneVs50(rate)}>{record} <span className="text-gray-400">({pct(rate)})</span></span>;
}

function ConferenceTable({ conferences }: { conferences: ConferenceRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b text-gray-500">
            <th className="py-1 pr-3 text-left">Conference</th>
            <th className="py-1 pr-3 text-right">Teams</th>
            <th className="border-l py-1 pl-3 pr-3 text-right font-semibold text-gray-700">Non-conf games</th>
            <th className="py-1 pr-3 text-right font-semibold text-gray-700">Non-conf SU</th>
            <th className="py-1 pr-3 text-right font-semibold text-gray-700">Non-conf ATS</th>
            <th className="py-1 pr-3 text-right font-semibold text-gray-700">Covered</th>
            <th className="py-1 pr-3 text-right font-semibold text-gray-700">Units</th>
            <th className="py-1 pr-3 text-right font-semibold text-gray-700">Overs</th>
            <th className="border-l py-1 pl-3 pr-3 text-right">Road favorite ATS</th>
            <th className="py-1 pr-3 text-right">Home underdog ATS</th>
            <th className="border-l py-1 pl-3 pr-3 text-right">All team-games</th>
            <th className="py-1 pr-3 text-right">All SU</th>
            <th className="py-1 pr-3 text-right">All ATS</th>
            <th className="py-1 text-right">All overs</th>
          </tr>
        </thead>
        <tbody>
          {conferences.map((c) => (
            <tr key={c.Conference} className={`border-b border-gray-50 tabular-nums ${c["Non-conference games"] < THIN_CONF ? "text-gray-400" : ""}`}>
              <td className="whitespace-nowrap py-1 pr-3 font-medium">{c.Conference}</td>
              <td className="py-1 pr-3 text-right">{c.Teams}</td>
              <td className="border-l py-1 pl-3 pr-3 text-right">{c["Non-conference games"]}</td>
              <td className="py-1 pr-3 text-right">{c["Non-conference SU"]}</td>
              <td className="py-1 pr-3 text-right">{c["Non-conference ATS"]}</td>
              <td className={`py-1 pr-3 text-right ${toneVs50(c["Non-conference covered (%)"])}`}>{pct(c["Non-conference covered (%)"])}</td>
              <td className={`py-1 pr-3 text-right ${tone(c["Non-conference ATS units"])}`}>{signed(c["Non-conference ATS units"], 2)}</td>
              <td className="py-1 pr-3 text-right">{pct(c["Non-conference overs (%)"])}</td>
              <td className="whitespace-nowrap border-l py-1 pl-3 pr-3 text-right">
                <ConfSplit record={c["Road favorite ATS"]} rate={c["Road favorite covered (%)"]} games={c["Road favorite games"]} />
              </td>
              <td className="whitespace-nowrap py-1 pr-3 text-right">
                <ConfSplit record={c["Home underdog ATS"]} rate={c["Home underdog covered (%)"]} games={c["Home underdog games"]} />
              </td>
              <td className="border-l py-1 pl-3 pr-3 text-right">{c["Team-games"]}</td>
              <td className="py-1 pr-3 text-right">{c["SU record"]}</td>
              <td className="py-1 pr-3 text-right">{c["ATS record"]} <span className="text-gray-400">({pct(c["Covered (%)"])})</span></td>
              <td className="py-1 text-right">{pct(c["Overs (%)"])}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function TeamAts({ teams, conferences }: { teams: TeamAtsRow[]; conferences: ConferenceRow[] }) {
  const teamGames = teams.reduce((s, t) => s + t.Games, 0);
  const fcsGames = teams.reduce((s, t) => s + t["Vs FCS"], 0);
  return (
    <div className="space-y-4">
      <div className="rounded-lg border bg-card p-4 space-y-3">
        <div>
          <h3 className="text-sm font-semibold">Team against-the-spread standings</h3>
          <p className="text-xs text-gray-500">
            Season to date, regular season, closing spread and total at -110; a push returns the stake and is left out of
            the cover rate. {teamGames ? `${teams.length} FBS teams, ${teamGames} team-games (${fcsGames} against FCS opponents, which count for the FBS team).` : ""}{" "}
            Avg cover margin is the team&apos;s margin plus its line. Records read win-loss-push. Faded: fewer than {THIN_TEAM} games.
          </p>
        </div>
        {teams.length ? <TeamTable teams={teams} /> : <p className="text-xs text-gray-500">No graded games with a closing spread yet.</p>}
      </div>
      <div className="rounded-lg border bg-card p-4 space-y-3">
        <div>
          <h3 className="text-sm font-semibold">Conference report cards</h3>
          <p className="text-xs text-gray-500">
            A conference game puts one of its teams on each side of the same line, so in-conference records always come out
            near 50%. The non-conference columns, where a conference&apos;s teams face outsiders, are the ones that say something.
            Overs are counted per team-game. Faded: fewer than {THIN_CONF} non-conference games.
          </p>
        </div>
        {conferences.length ? (
          <>
            <ConferenceChart conferences={conferences} />
            <ConferenceTable conferences={conferences} />
          </>
        ) : (
          <p className="text-xs text-gray-500">No conference rows yet.</p>
        )}
      </div>
    </div>
  );
}
