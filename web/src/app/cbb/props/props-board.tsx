"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { CBB_PROP_MARKETS, cbbPropFreshness, type CbbPropBoard, type CbbPropEvent } from "@/lib/cbb-props/model";
import { loadCbbPropGame, loadCbbPropSchedule } from "./actions";

const control = "rounded-lg border bg-background px-3 py-2 text-sm disabled:opacity-50";
const time = (value: string) => new Date(value).toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
const odds = (value: number | null) => value === null ? "—" : value > 0 ? `+${value}` : String(value);

export default function CbbPropsBoard({ embedded = false }: { embedded?: boolean }) {
  const [events, setEvents] = useState<CbbPropEvent[]>([]);
  const [eventId, setEventId] = useState("");
  const [board, setBoard] = useState<CbbPropBoard | null>(null);
  const [error, setError] = useState("");
  const [scheduleTime, setScheduleTime] = useState("");
  const [scheduleBusy, setScheduleBusy] = useState(true);
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState("");
  const [market, setMarket] = useState("all");
  const [book, setBook] = useState("all");
  const [now, setNow] = useState(() => Date.now());
  const request = useRef(0);
  const mounted = useRef(false);

  async function loadGame(id: string) {
    const token = ++request.current;
    setBusy(true); setError(""); setBoard(null); setBook("all");
    try {
      const result = await loadCbbPropGame(id);
      if (!mounted.current || token !== request.current) return;
      if (result.ok) { setBoard(result.data); setNow(Date.now()); }
      else setError(result.error);
    } catch { if (mounted.current && token === request.current) setError("Could not load player props. Please try again."); }
    finally { if (mounted.current && token === request.current) setBusy(false); }
  }
  async function loadSchedule() {
    const token = ++request.current;
    setScheduleBusy(true); setBusy(false); setError(""); setBoard(null);
    try {
      const result = await loadCbbPropSchedule();
      if (!mounted.current || token !== request.current) return;
      if (!result.ok) { setError(result.error); return; }
      const available = result.data.events.filter(e => Date.parse(e.start) > Date.now() - 6 * 60 * 60_000);
      setEvents(available); setScheduleTime(result.data.capturedAt);
      const first = available.find(e => e.id === eventId)?.id ?? available[0]?.id ?? ""; setEventId(first);
      setScheduleBusy(false);
      if (first) void loadGame(first);
    } catch { if (mounted.current && token === request.current) setError("Could not load the CBB schedule. Please try again."); }
    finally { if (mounted.current && token === request.current) setScheduleBusy(false); }
  }
  useEffect(() => {
    mounted.current = true;
    void loadSchedule();
    const timer = setInterval(() => setNow(Date.now()), 30_000);
    return () => { mounted.current = false; clearInterval(timer); };
    // Load once per mount; user refreshes explicitly. Avoid polling paid odds endpoints.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const books = useMemo(() => [...new Map((board?.lines ?? []).map(row => [row.book, row.bookName])).entries()].sort((a, b) => a[0] === "draftkings" ? -1 : b[0] === "draftkings" ? 1 : a[1].localeCompare(b[1])), [board]);
  const filtered = useMemo(() => (board?.lines ?? []).filter(row => (market === "all" || row.market === market) && (book === "all" || row.book === book) && row.player.toLowerCase().includes(query.toLowerCase())), [board, market, book, query]);
  const missing = board ? Object.entries(CBB_PROP_MARKETS).filter(([key]) => !board.lines.some(row => row.market === key)).map(([, label]) => label.toLowerCase()) : [];

  function downloadSnapshot() {
    if (!board) return;
    const content = JSON.stringify({ version: "cbb-player-props-v1", exportedAt: new Date().toISOString(), ...board }, null, 2);
    const url = URL.createObjectURL(new Blob([content], { type: "application/json" }));
    const a = document.createElement("a"); a.href = url; a.download = `cbb-props-${board.event.id}.json`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return <section className="space-y-4 rounded-xl border bg-card p-4 md:p-5" aria-label="College basketball player props">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h2 className="text-xl font-semibold">Vegas player props</h2><p className="mt-1 text-sm text-muted-foreground">Individual player points, rebounds, and assists · US sportsbooks · Times in ET</p></div>{embedded && <Link href="/cbb/props" className="text-sm font-medium text-blue-600 underline">Open full board →</Link>}</div>
    <div className="flex flex-wrap items-end gap-3">
      <label className="grid min-w-0 flex-1 gap-1 text-sm">Game<select aria-label="CBB prop game" className={`${control} w-full min-w-0 md:min-w-72`} value={eventId} disabled={scheduleBusy || !events.length} onChange={e => { const id = e.target.value; setEventId(id); void loadGame(id); }}>
        {!events.length && <option value="">{scheduleBusy ? "Loading games…" : "No games available"}</option>}
        {events.map(e => <option key={e.id} value={e.id}>{e.away} @ {e.home} · {time(e.start)}{Date.parse(e.start) <= now ? " · scheduled start passed" : ""}</option>)}
      </select></label>
      <button className={control} disabled={busy || scheduleBusy || !eventId} onClick={() => void loadGame(eventId)}>{busy ? "Loading lines…" : "Refresh lines"}</button>
      <button className={control} disabled={busy || scheduleBusy} onClick={() => void loadSchedule()}>Reload games</button>
    </div>
    <p className="text-xs text-muted-foreground">Source: The Odds API. Lines are cached for about 1 minute; games for 5 minutes. {scheduleTime && `Schedule fetched ${time(scheduleTime)} ET.`}</p>
    {error && <p role="alert" className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-800">{error}</p>}
    {(busy || scheduleBusy) && <p role="status" className="text-sm text-muted-foreground">{scheduleBusy ? "Loading college basketball games…" : "Loading player-specific lines for this game…"}</p>}
    {!scheduleBusy && !error && !events.length && <p className="rounded-lg bg-muted/40 p-4 text-sm">No recent or upcoming CBB games were returned by the provider. Reload later.</p>}
    {board && <>
      <div className="rounded-lg bg-muted/40 p-3 text-sm"><p className="font-semibold">{board.event.away} @ {board.event.home}</p><p className="mt-1 text-muted-foreground">Scheduled tipoff {time(board.event.start)} ET · {new Set(board.lines.map(row => row.player)).size} named players · {books.length} books · Feed fetched {time(board.capturedAt)} ET</p>
        {Date.parse(board.event.start) <= now && <p className="mt-2 font-medium text-amber-800">The scheduled start has passed. Returned lines may be in-game; this is not a frozen pregame snapshot.</p>}
        {!books.some(([key]) => key === "draftkings") && <p className="mt-2 text-muted-foreground">No DraftKings lines were returned for this game in this feed.</p>}
        {missing.length > 0 && <p className="mt-1 text-muted-foreground">No {missing.join(", ")} lines returned. Missing coverage is not a zero.</p>}
        {board.skipped > 0 && <p className="mt-2 text-amber-800">{board.skipped} malformed or conflicting selections were omitted. Coverage may be incomplete.</p>}
      </div>
      {board.lines.length ? <>
        <div className="flex flex-wrap gap-3"><input aria-label="Search prop players" className={`${control} min-w-0 flex-1`} placeholder="Search player" value={query} onChange={e => setQuery(e.target.value)} />
          <select aria-label="Prop stat" className={control} value={market} onChange={e => setMarket(e.target.value)}><option value="all">All stats</option>{Object.entries(CBB_PROP_MARKETS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select>
          <select aria-label="Prop sportsbook" className={control} value={book} onChange={e => setBook(e.target.value)}><option value="all">All sportsbooks</option>{books.map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select>
          <button className={control} onClick={downloadSnapshot}>Download source snapshot</button>
        </div>
        <div className="overflow-x-auto rounded-lg border"><table className="w-full whitespace-nowrap text-left text-sm"><thead className="border-b bg-muted/40"><tr>{["Player", "Stat", "Line", "Over", "Under", "Sportsbook", "Book updated (ET)"].map(label => <th key={label} className="px-3 py-3 text-xs font-medium text-muted-foreground">{label}</th>)}</tr></thead><tbody>{filtered.map(row => {
          const freshness = cbbPropFreshness(row.updatedAt, now);
          return <tr key={JSON.stringify([row.player, row.market, row.book, row.line])} className="border-b last:border-0"><td className="px-3 py-3 font-medium">{row.player}</td><td className="px-3 py-3">{CBB_PROP_MARKETS[row.market]}</td><td className="px-3 py-3 font-semibold">{row.line}</td><td className="px-3 py-3 tabular-nums">{odds(row.over)}</td><td className="px-3 py-3 tabular-nums">{odds(row.under)}</td><td className="px-3 py-3">{row.bookName}</td><td className="px-3 py-3 text-xs"><span>{row.updatedAt ? time(row.updatedAt) : "Unknown"}</span>{freshness !== "recent" && <span className="ml-2 rounded bg-amber-100 px-2 py-1 text-amber-900">{freshness === "stale" ? "Older than 15m" : "Time unverified"}</span>}</td></tr>;
        })}</tbody></table></div>
        {!filtered.length && <p className="text-sm text-muted-foreground">No lines match these player, stat, and sportsbook filters.</p>}
      </> : <p className="rounded-lg border border-dashed p-6 text-sm text-muted-foreground">No player props are currently returned for this game. This does not mean the game has no props at every sportsbook. Choose another game or refresh closer to tipoff.</p>}
      <p className="text-xs leading-5 text-muted-foreground">Over and under prices are American odds. A dash means that side was not returned at the same threshold. Book update time is separate from feed fetch time. Props stay separate from DFS projections: an over/under line is not an expected stat average or a full DraftKings fantasy-point projection.</p>
    </>}
  </section>;
}

export function CbbDfsProps() {
  const [open, setOpen] = useState(false);
  return <details className="rounded-xl border p-4" onToggle={e => setOpen(e.currentTarget.open)}><summary className="cursor-pointer font-semibold">Vegas player props <span className="ml-2 text-xs font-normal text-muted-foreground">Points · Rebounds · Assists</span></summary>{open && <div className="mt-4"><CbbPropsBoard embedded /></div>}</details>;
}
