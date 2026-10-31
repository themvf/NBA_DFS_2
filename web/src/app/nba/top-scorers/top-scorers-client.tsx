"use client";

import { useMemo, useState } from "react";
import type { GameSummary, ScorerRow } from "@/lib/nba-top-scorers";

// The app-wide `* { border-color: var(--border) }` rule beats border utilities, so the
// dark panel sets the variable itself.
const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;

type Pulled = { player: string; eventId: string; lastSeenAt: string };

const BOOK_NAMES: Record<string, string> = {
  pinnacle: "Pinnacle",
  draftkings: "DK",
  fanduel: "FanDuel",
  betmgm: "BetMGM",
  williamhill_us: "Caesars",
  fanatics: "Fanatics",
};

const one = (v: number) => v.toFixed(1);
const pct = (v: number | null) => (v == null ? "-" : `${(v * 100).toFixed(1)}%`);
const signed = (v: number | null) => (v == null ? "" : `${v >= 0 ? "+" : ""}${v.toFixed(1)}`);
const american = (p: number | null) => (p == null ? "-" : p > 0 ? `+${p}` : `${p}`);

function tip(value: string) {
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(
    new Date(value),
  );
}

function stamp(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

function shortGame(g: Pick<GameSummary, "awayTeam" | "homeTeam">) {
  const last = (name: string) => name.split(" ").slice(-1)[0];
  return `${last(g.awayTeam)} @ ${last(g.homeTeam)}`;
}

// Position badge: guard / forward / center families, each with its own hue.
function positionStyle(pos: string | null) {
  const p = (pos ?? "").toUpperCase();
  if (p.startsWith("C")) return { badge: "border-amber-400/60! text-amber-300", stripe: "bg-amber-400" };
  if (p.startsWith("F")) return { badge: "border-emerald-400/60! text-emerald-300", stripe: "bg-emerald-400" };
  if (p.startsWith("G")) return { badge: "border-rose-400/60! text-rose-300", stripe: "bg-rose-400" };
  return { badge: "border-slate-500! text-slate-400", stripe: "bg-slate-600" };
}

type SortKey = "line" | "ceiling" | "p25" | "p30" | "p40" | "ppg" | "delta" | "books" | "rank";

const SORTERS: Record<SortKey, (r: ScorerRow) => number> = {
  line: (r) => r.line,
  ceiling: (r) => r.distribution?.ceiling ?? -1,
  p25: (r) => r.distribution?.atLeast[25] ?? -1,
  p30: (r) => r.distribution?.atLeast[30] ?? -1,
  p40: (r) => r.distribution?.atLeast[40] ?? -1,
  ppg: (r) => r.lastSeasonPpg ?? -1,
  delta: (r) => r.vsLastSeason ?? -999,
  books: (r) => r.books.length,
  rank: (r) => -r.rank,
};

function MiniBar({ value, max, tone }: { value: number | null; max: number; tone: string }) {
  const width = value == null || max <= 0 ? 0 : Math.max(2, Math.min(100, (value / max) * 100));
  return (
    <div className="ml-auto mt-1 h-1 w-16 rounded-full bg-slate-700/70">
      <div className={`h-1 rounded-full ${tone}`} style={{ width: `${width}%` }} />
    </div>
  );
}

/**
 * Floor-ceiling from the alternate ladder (P10-P90) when one was captured; otherwise
 * the spread of main lines across books, drawn hatched and labelled so it is not
 * mistaken for a floor and ceiling. A side the ladder does not reach shows "<"/">".
 */
function RangeBar({ row, scaleMax }: { row: ScorerRow; scaleMax: number }) {
  const x = (v: number) => Math.max(0, Math.min(100, (v / scaleMax) * 100));
  const d = row.distribution;
  const curve = d?.curve ?? [];
  const low = d ? d.floor ?? curve[0]?.point ?? row.lineLow : row.lineLow;
  const high = d ? d.ceiling ?? curve[curve.length - 1]?.point ?? row.lineHigh : row.lineHigh;
  const lowLabel = d ? `${d.floor == null ? "<" : ""}${one(low)}` : one(low);
  const highLabel = d ? `${d.ceiling == null ? ">" : ""}${one(high)}` : one(high);
  const lo = x(low);
  const hi = x(high);
  return (
    <div className="flex items-center gap-3" title={d ? "Floor (P10) to ceiling (P90) from the alternate points ladder" : "No ladder captured: spread of main lines across books"}>
      <span className="w-10 text-right font-mono text-xs text-slate-400">{lowLabel}</span>
      <div className="relative h-4 min-w-[10rem] flex-1">
        <div className="absolute inset-x-0 top-1/2 h-1.5 -translate-y-1/2 rounded-full bg-slate-700/80" />
        <div
          className={`absolute top-1/2 h-1.5 -translate-y-1/2 rounded-full ${d ? "bg-sky-500/80" : "bg-[repeating-linear-gradient(90deg,#64748b_0_3px,transparent_3px_6px)]"}`}
          style={{ left: `${lo}%`, width: `max(10px, ${hi - lo}%)`, marginLeft: hi === lo ? -5 : 0 }}
        />
        {row.lastSeasonPpg != null && (
          <span
            className="absolute top-1/2 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-slate-300! bg-transparent"
            style={{ left: `${x(row.lastSeasonPpg)}%` }}
            title={`2025-26 regular season: ${one(row.lastSeasonPpg)} PPG across ${row.lastSeasonGames ?? "?"} appearances`}
          />
        )}
        <span
          className="absolute top-1/2 h-3.5 w-3.5 -translate-x-1/2 -translate-y-1/2 rounded-full bg-white ring-2 ring-[#0f1726]"
          style={{ left: `${x(row.line)}%` }}
        />
      </div>
      <span className="w-10 font-mono text-xs text-slate-400">{highLabel}</span>
    </div>
  );
}

export function TopScorersClient({
  scorers,
  games,
  pulled,
  lastSeason,
}: {
  scorers: ScorerRow[];
  games: GameSummary[];
  pulled: Pulled[];
  lastSeason: string;
}) {
  const [tab, setTab] = useState<"players" | "games" | "pulled">("players");
  const [game, setGame] = useState<string>("all");
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: "line", desc: true });
  const [open, setOpen] = useState<string | null>(null);

  const shown = useMemo(() => {
    const base = game === "all" ? scorers : scorers.filter((s) => s.eventId === game);
    const f = SORTERS[sort.key];
    return [...base].sort((a, b) => (sort.desc ? f(b) - f(a) : f(a) - f(b)) || a.rank - b.rank);
  }, [scorers, game, sort]);

  const scaleMax = Math.max(
    40,
    Math.ceil(
      (Math.max(0, ...scorers.map((s) => Math.max(s.lineHigh, s.lastSeasonPpg ?? 0, s.distribution?.ceiling ?? 0))) + 2) / 5,
    ) * 5,
  );
  const withLadder = scorers.filter((s) => s.distribution).length;
  const maxPpg = Math.max(1, ...scorers.map((s) => s.lastSeasonPpg ?? 0));
  const gameName = new Map(games.map((g) => [g.eventId, shortGame(g)]));

  const rise = [...scorers].filter((s) => s.vsLastSeason != null).sort((a, b) => b.vsLastSeason! - a.vsLastSeason!)[0];
  const best30 = [...scorers]
    .filter((s) => s.distribution?.atLeast[30] != null)
    .sort((a, b) => b.distribution!.atLeast[30]! - a.distribution!.atLeast[30]!)[0];

  const header = (key: SortKey, label: string, align = "text-right") => (
    <th className={`px-3 py-3 font-medium ${align}`}>
      <button
        type="button"
        onClick={() => setSort((s) => ({ key, desc: s.key === key ? !s.desc : true }))}
        className={`uppercase tracking-wider hover:text-slate-200 ${sort.key === key ? "text-slate-100" : ""}`}
      >
        {label}
        {sort.key === key ? (sort.desc ? " ↓" : " ↑") : ""}
      </button>
    </th>
  );

  return (
    <div className="overflow-hidden rounded-xl border bg-[#0f1726] text-slate-200" style={DARK_BORDER}>
      <div className="grid grid-cols-1 border-b border-slate-800 sm:grid-cols-3">
        <div className="border-b border-slate-800 px-4 py-3 sm:border-b-0 sm:border-r">
          <div className="font-mono text-[11px] uppercase tracking-widest text-slate-500">Top line</div>
          <div className="mt-1 text-lg">
            <span className="font-mono font-semibold text-white">{one(scorers[0].line)}</span>{" "}
            <span className="text-slate-300">{scorers[0].player}</span>
          </div>
        </div>
        <div className="border-b border-slate-800 px-4 py-3 sm:border-b-0 sm:border-r">
          <div className="font-mono text-[11px] uppercase tracking-widest text-slate-500">Largest line–season gap</div>
          <div className="mt-1 text-lg">
            {rise ? (
              <>
                <span className="font-mono font-semibold text-white">{signed(rise.vsLastSeason)}</span>{" "}
                <span className="text-slate-300">{rise.player}</span>
              </>
            ) : (
              <span className="text-slate-500">No matched players</span>
            )}
          </div>
        </div>
        <div className="px-4 py-3">
          <div className="font-mono text-[11px] uppercase tracking-widest text-slate-500">Best chance of 30+</div>
          <div className="mt-1 text-lg">
            {best30 ? (
              <>
                <span className="font-mono font-semibold text-white">{pct(best30.distribution!.atLeast[30])}</span>{" "}
                <span className="text-slate-300">{best30.player}</span>
              </>
            ) : (
              <span className="text-slate-500">No ladder captured yet</span>
            )}
          </div>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-x-6 border-b border-slate-800 px-4 text-sm">
        {(
          [
            ["players", "Players", scorers.length],
            ["games", "By game", games.length],
            ["pulled", "Pulled", pulled.length],
          ] as const
        ).map(([key, label, n]) => (
          <button
            key={key}
            type="button"
            onClick={() => setTab(key)}
            className={`py-3 ${tab === key ? "text-white shadow-[inset_0_-2px_0_#38bdf8]" : "text-slate-400 hover:text-slate-200"}`}
          >
            {label} <span className="ml-1 rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-slate-400">{n}</span>
          </button>
        ))}
      </div>

      {tab === "players" && (
        <>
          <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 text-xs">
            <div className="flex flex-wrap items-center gap-4 font-mono uppercase tracking-wider text-slate-400">
              <span className="flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-white" />Line</span>
              <span className="flex items-center gap-1.5"><span className="h-1.5 w-5 rounded-full bg-sky-500/80" />Floor–ceiling (P10–P90)</span>
              <span className="flex items-center gap-1.5"><span className="h-1.5 w-5 rounded-full bg-[repeating-linear-gradient(90deg,#64748b_0_3px,transparent_3px_6px)]" />Book range (no ladder)</span>
              <span className="flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full border-2 border-slate-300!" />{lastSeason} reg. PPG</span>
            </div>
            <div className="flex items-center gap-3">
              <select
                value={game}
                onChange={(e) => setGame(e.target.value)}
                className="rounded border border-slate-700 bg-[#0b1220] px-2 py-1 text-slate-200"
                aria-label="Filter by game"
              >
                <option value="all">All games</option>
                {games.map((g) => (
                  <option key={g.eventId} value={g.eventId}>
                    {shortGame(g)} · {tip(g.commenceTime)}
                  </option>
                ))}
              </select>
              <span className="hidden text-slate-500 sm:inline">
                Ladder for {withLadder}/{scorers.length} · click a column to sort
              </span>
            </div>
          </div>
          <div className="overflow-x-auto px-4 pb-4">
            <table className="w-full min-w-[64rem] border-separate border-spacing-0 overflow-hidden rounded-lg border border-slate-800 text-sm">
              <thead className="bg-[#111b2d] font-mono text-[11px] text-slate-400">
                <tr>
                  <th className="px-3 py-3 text-left font-medium uppercase tracking-wider">Player</th>
                  {header("line", "Line")}
                  {header("ceiling", "Floor – Ceiling", "text-left")}
                  {header("p25", "25+")}
                  {header("p30", "30+")}
                  {header("p40", "40+")}
                  {header("ppg", `${lastSeason} reg. PPG`)}
                  {header("delta", "vs PPG")}
                  {header("books", "Books")}
                  {header("rank", "Rank")}
                </tr>
              </thead>
              <tbody>
                {shown.map((s) => {
                  const key = `${s.eventId}-${s.player}`;
                  const pos = positionStyle(s.position);
                  const split = s.lineHigh > s.lineLow;
                  return (
                    <FragmentRow key={key} expanded={open === key} row={s}>
                      <tr
                        className="cursor-pointer border-t border-slate-800 hover:bg-slate-800/40"
                        onClick={() => setOpen(open === key ? null : key)}
                      >
                        <td className="relative border-t border-slate-800 py-2.5 pl-4 pr-3">
                          <span className={`absolute inset-y-0 left-0 w-1 ${pos.stripe}`} />
                          <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                            <span className="font-semibold text-white">{s.player}</span>
                            {s.position && (
                              <span className={`whitespace-nowrap rounded border px-1.5 py-px font-mono text-[10px] ${pos.badge}`}>{s.position}</span>
                            )}
                            {s.rank <= 3 && (
                              <span className="rounded border border-amber-400/60! px-1.5 py-px font-mono text-[10px] text-amber-300">TOP {s.rank}</span>
                            )}
                            {split && (
                              <span className="rounded border border-sky-400/50! px-1.5 py-px font-mono text-[10px] text-sky-300" title="Books quote different lines">SPLIT</span>
                            )}
                          </div>
                          <div className="mt-0.5 font-mono text-xs text-slate-400">
                            {s.teamAbbrev ? `${s.teamAbbrev} (prior season) · vs ${s.opponentAbbrev ?? s.opponent}` : `${gameName.get(s.eventId)} · current team unverified`} · {tip(s.commenceTime)}
                          </div>
                        </td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-base font-semibold text-white">{one(s.line)}</td>
                        <td className="border-t border-slate-800 px-3"><RangeBar row={s} scaleMax={scaleMax} /></td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-amber-300">
                          {pct(s.distribution?.atLeast[25] ?? null)}
                          <MiniBar value={s.distribution?.atLeast[25] ?? null} max={1} tone="bg-amber-400" />
                        </td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-orange-300">
                          {pct(s.distribution?.atLeast[30] ?? null)}
                          <MiniBar value={s.distribution?.atLeast[30] ?? null} max={1} tone="bg-orange-400" />
                        </td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-rose-300">
                          {pct(s.distribution?.atLeast[40] ?? null)}
                          <MiniBar value={s.distribution?.atLeast[40] ?? null} max={1} tone="bg-rose-400" />
                        </td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-sky-300">
                          {s.lastSeasonPpg == null ? <span className="text-slate-500">-</span> : one(s.lastSeasonPpg)}
                          <MiniBar value={s.lastSeasonPpg} max={maxPpg} tone="bg-sky-400" />
                        </td>
                        <td className={`border-t border-slate-800 px-3 text-right font-mono ${s.vsLastSeason == null ? "text-slate-500" : "text-slate-200"}`}>
                          {s.vsLastSeason == null ? "-" : signed(s.vsLastSeason)}
                        </td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-slate-300">{s.books.length}</td>
                        <td className="border-t border-slate-800 px-3 text-right font-mono text-slate-300" title="Slate rank / rank among prior-season team matches; current roster unverified">
                          {s.rank} / {s.teamRank ?? "-"}
                        </td>
                      </tr>
                    </FragmentRow>
                  );
                })}
              </tbody>
            </table>
            <p className="mt-2 font-mono text-[11px] text-slate-500">
              Click a row for every book&apos;s line and price and the ladder&apos;s chances. Rank = slate / prior-season team matches. Current team and availability are unverified. 25+ / 30+ / 40+ and
              floor–ceiling come from the alternate points ladder; &quot;-&quot; means no ladder or a threshold past its last rung.
            </p>
          </div>
        </>
      )}

      {tab === "games" && (
        <div className="grid gap-3 p-4 sm:grid-cols-2 lg:grid-cols-3">
          {games.map((g) => {
            const inGame = scorers.filter((s) => s.eventId === g.eventId);
            const side = (team: string) => inGame.filter((s) => s.team === team).slice(0, 4);
            return (
              <div key={g.eventId} className="rounded-lg border border-slate-800 bg-[#111b2d] p-3">
                <div className="mb-2 flex items-baseline justify-between">
                  <span className="font-semibold text-white">{shortGame(g)}</span>
                  <span className="font-mono text-xs text-slate-400">{tip(g.commenceTime)} ET</span>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  {[g.awayTeam, g.homeTeam].map((team) => (
                    <div key={team}>
                      <div className="mb-1 truncate font-mono text-[11px] uppercase tracking-wider text-slate-500">{team}</div>
                      {side(team).length === 0 ? (
                        <div className="text-xs text-slate-500">No prior-season matches</div>
                      ) : (
                        side(team).map((s) => (
                          <div key={s.player} className="flex justify-between gap-2 text-sm">
                            <span className="truncate text-slate-200">{s.player}</span>
                            <span className="font-mono text-white">{one(s.line)}</span>
                          </div>
                        ))
                      )}
                    </div>
                  ))}
                </div>
                <div className="mt-2 font-mono text-[11px] text-slate-500">
                  {g.players} players · lines sum to {one(g.lineSum)}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {tab === "pulled" && (
        <div className="p-4 text-sm">
          {pulled.length === 0 ? (
            <p className="text-slate-400">No line has been pulled since an earlier capture today.</p>
          ) : (
            <>
              <p className="mb-2 text-slate-400">
                Quoted earlier but missing from the game&apos;s latest capture. Books usually pull a line on injury or rest news.
              </p>
              <ul className="space-y-1">
                {pulled.map((p) => (
                  <li key={`${p.eventId}-${p.player}`} className="text-slate-200">
                    {p.player}{" "}
                    <span className="font-mono text-xs text-slate-500">
                      {gameName.get(p.eventId) ?? "game"} · last seen {stamp(p.lastSeenAt)}
                    </span>
                  </li>
                ))}
              </ul>
            </>
          )}
        </div>
      )}
    </div>
  );
}

function FragmentRow({
  children,
  expanded,
  row,
}: {
  children: React.ReactNode;
  expanded: boolean;
  row: ScorerRow;
}) {
  const d = row.distribution;
  return (
    <>
      {children}
      {expanded && (
        <tr className="bg-[#0b1220]">
          <td colSpan={9} className="border-t border-slate-800 px-4 py-2 font-mono text-xs text-slate-300">
            <div className="mb-1">
              {d ? (
                <>
                  <span className="text-slate-500">Ladder ({d.ladderBooks} book{d.ladderBooks === 1 ? "" : "s"})</span>{" "}
                  {([20, 25, 30, 35, 40] as const).map((n) => (
                    <span key={n} className="mr-4">
                      {n}+ <span className="text-orange-300">{pct(d.atLeast[n])}</span>
                    </span>
                  ))}
                  <span className="text-slate-500">
                    floor {d.floor == null ? "below ladder" : one(d.floor)} · ceiling {d.ceiling == null ? "above ladder" : one(d.ceiling)}
                  </span>
                </>
              ) : (
                <span className="text-slate-500">No alternate ladder captured for this player.</span>
              )}
            </div>
            {row.books.map((b) => (
              <span key={b.book} className="mr-5 inline-block">
                <span className="text-slate-500">{BOOK_NAMES[b.book] ?? b.book}</span> {one(b.line)}{" "}
                <span className="text-slate-400">
                  o{american(b.over)} / u{american(b.under)}
                </span>
                {b.pOver != null && <span className="text-amber-300"> · {Math.round(b.pOver * 100)}%</span>}
              </span>
            ))}
          </td>
        </tr>
      )}
    </>
  );
}
