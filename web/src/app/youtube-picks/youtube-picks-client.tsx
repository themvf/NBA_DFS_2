"use client";

import { useMemo, useState, useTransition } from "react";
import { addYoutubeChannel } from "./actions";
import type { YoutubePickRow } from "./queries";
import type { YoutubePickChannel } from "@/db/schema";

const SPORT_ICON: Record<string, string> = {
  nba: "🏀",
  mlb: "⚾",
  nfl: "🏈",
  nhl: "🏒",
  wnba: "🏀",
  soccer: "⚽",
  tennis: "🎾",
  f1: "🏎️",
  other: "🎲",
};

const BET_TYPE_LABEL: Record<string, string> = {
  moneyline: "Moneyline",
  spread: "Spread",
  total: "Total",
  prop: "Prop",
  futures: "Futures",
  other: "Other",
};

const STATUS_STYLE: Record<string, string> = {
  won: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
  lost: "bg-rose-500/10 text-rose-600 dark:text-rose-400",
  pending: "bg-muted text-muted-foreground",
  unsettleable: "bg-muted text-muted-foreground/60",
};

function fmtOdds(odds: number | null): string {
  if (odds == null) return "—";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

function fmtDate(d: Date | null): string {
  if (!d) return "—";
  return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function ManageChannels({
  channels,
  onAdded,
}: {
  channels: YoutubePickChannel[];
  onAdded: (channel: YoutubePickChannel) => void;
}) {
  const [input, setInput] = useState("");
  const [isPending, startTransition] = useTransition();
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);
  const [open, setOpen] = useState(false);

  function handleAdd() {
    if (!input.trim()) return;
    startTransition(async () => {
      setMessage(null);
      const res = await addYoutubeChannel(input.trim());
      setMessage({ ok: res.ok, text: res.message });
      if (res.ok && res.channel) {
        setInput("");
        onAdded({
          id: -Date.now(),
          channelId: res.channel.channelId,
          channelName: res.channel.channelName,
          handle: null,
          active: true,
          addedAt: new Date(),
        });
      }
    });
  }

  return (
    <div className="rounded-lg border bg-card p-3">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between text-sm font-semibold"
      >
        <span>📺 Tracked Channels ({channels.length})</span>
        <span className="text-xs text-muted-foreground">{open ? "hide" : "manage"}</span>
      </button>

      {open && (
        <div className="mt-3 space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              placeholder="@handle or youtube.com/@handle"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleAdd()}
              className="min-w-[220px] flex-1 rounded border bg-background px-2 py-1.5 text-sm"
            />
            <button
              onClick={handleAdd}
              disabled={isPending || !input.trim()}
              className="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
            >
              {isPending ? "Adding…" : "Add Channel"}
            </button>
          </div>

          {message && (
            <p className={`text-xs ${message.ok ? "text-emerald-600 dark:text-emerald-400" : "text-rose-600 dark:text-rose-400"}`}>
              {message.text}
            </p>
          )}

          <p className="text-xs text-muted-foreground">
            New channels are picked up by the scheduled scraper on its next run — adding one here
            doesn&apos;t fetch videos immediately.
          </p>

          <ul className="space-y-1">
            {channels.map((c) => (
              <li key={c.channelId} className="flex items-center justify-between text-sm">
                <span>{c.channelName}{c.handle ? ` (${c.handle})` : ""}</span>
                <span className={`text-xs ${c.active ? "text-emerald-600 dark:text-emerald-400" : "text-muted-foreground"}`}>
                  {c.active ? "active" : "paused"}
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export function YoutubePicksClient({
  picks,
  initialChannels,
}: {
  picks: YoutubePickRow[];
  initialChannels: YoutubePickChannel[];
}) {
  const [channels, setChannels] = useState(initialChannels);
  const [channelFilter, setChannelFilter] = useState("all");
  const [sport, setSport] = useState("all");
  const [betType, setBetType] = useState("all");
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  const sports = useMemo(
    () => Array.from(new Set(picks.map((p) => p.sport))).sort(),
    [picks],
  );
  const betTypes = useMemo(
    () => Array.from(new Set(picks.map((p) => p.betType))).sort(),
    [picks],
  );
  const pickChannelNames = useMemo(
    () => Array.from(new Set(picks.map((p) => p.channelName))).sort(),
    [picks],
  );

  const filtered = picks.filter((p) => {
    if (channelFilter !== "all" && p.channelName !== channelFilter) return false;
    if (sport !== "all" && p.sport !== sport) return false;
    if (betType !== "all" && p.betType !== betType) return false;
    if (search) {
      const q = search.toLowerCase();
      const haystack = `${p.subject} ${p.opponent ?? ""} ${p.selection}`.toLowerCase();
      if (!haystack.includes(q)) return false;
    }
    return true;
  });

  function toggleExpanded(id: number) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <main className="mx-auto max-w-5xl space-y-4 p-4">
      <div>
        <h1 className="text-xl font-bold">🎯 YouTube Picks Tracker</h1>
        <p className="text-sm text-muted-foreground">
          Betting picks extracted from tracked YouTube channels. Every pick links back to its
          source quote and video.
        </p>
      </div>

      <ManageChannels
        channels={channels}
        onAdded={(c) => setChannels((prev) => [c, ...prev.filter((x) => x.channelId !== c.channelId)])}
      />

      <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-300">
        ⚠ Settlement only covers <span className="font-medium">moneyline picks for MLB,
        soccer, and tennis</span> so far — those grade automatically once the game finishes.
        Spread/total bets and every other sport (WNBA, NFL, F1, etc.) show as{" "}
        <span className="font-medium">unsettleable</span> for now, not silently ignored.
      </div>

      <div className="flex flex-wrap items-center gap-2 text-sm">
        <input
          type="text"
          placeholder="Search team / player..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="rounded border bg-background px-2 py-1.5 text-sm placeholder:text-muted-foreground w-52"
        />
        <label className="flex items-center gap-1">
          <span className="text-muted-foreground">Channel</span>
          <select value={channelFilter} onChange={(e) => setChannelFilter(e.target.value)}
            className="rounded border bg-background px-1.5 py-1.5">
            <option value="all">All</option>
            {pickChannelNames.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-1">
          <span className="text-muted-foreground">Sport</span>
          <select value={sport} onChange={(e) => setSport(e.target.value)}
            className="rounded border bg-background px-1.5 py-1.5">
            <option value="all">All</option>
            {sports.map((s) => (
              <option key={s} value={s}>
                {SPORT_ICON[s] ?? ""} {s.toUpperCase()}
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-1">
          <span className="text-muted-foreground">Bet Type</span>
          <select value={betType} onChange={(e) => setBetType(e.target.value)}
            className="rounded border bg-background px-1.5 py-1.5">
            <option value="all">All</option>
            {betTypes.map((b) => (
              <option key={b} value={b}>{BET_TYPE_LABEL[b] ?? b}</option>
            ))}
          </select>
        </label>
        <span className="text-xs text-muted-foreground">
          {filtered.length} of {picks.length} picks
        </span>
      </div>

      {filtered.length === 0 ? (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No picks match these filters.
        </div>
      ) : (
        <div className="space-y-2">
          {filtered.map((p) => (
            <div key={p.id} className="rounded-lg border bg-card p-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span aria-hidden="true">{SPORT_ICON[p.sport] ?? "🎲"}</span>
                  <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
                    {BET_TYPE_LABEL[p.betType] ?? p.betType}
                  </span>
                  <span className="font-medium">
                    {p.subject}
                    {p.opponent ? ` vs ${p.opponent}` : ""}
                  </span>
                  {p.confidenceLabel && (
                    <span className="rounded-full bg-blue-500/10 px-2 py-0.5 text-[10px] font-medium text-blue-600 dark:text-blue-400">
                      {p.confidenceLabel}
                    </span>
                  )}
                </div>
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium uppercase ${STATUS_STYLE[p.status] ?? "bg-muted text-muted-foreground"}`}>
                  {p.status}
                </span>
              </div>

              {p.resultDetail && (
                <p className="mt-1 text-xs text-muted-foreground">{p.resultDetail}</p>
              )}

              <div className="mt-1 flex flex-wrap items-baseline gap-2">
                <span className="text-sm">{p.selection}</span>
                {p.oddsAmerican != null && (
                  <span className="text-sm font-medium tabular-nums text-emerald-600 dark:text-emerald-400">
                    {fmtOdds(p.oddsAmerican)}
                  </span>
                )}
              </div>

              <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <span className="font-medium">{p.channelName}</span>
                <span>·</span>
                <span>{fmtDate(p.publishedAt)}</span>
                <span>·</span>
                <a
                  href={`https://www.youtube.com/watch?v=${p.youtubeVideoId}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline hover:text-foreground"
                >
                  {p.videoTitle}
                </a>
                {p.gameContext && (
                  <>
                    <span>·</span>
                    <span>{p.gameContext}</span>
                  </>
                )}
                <button
                  onClick={() => toggleExpanded(p.id)}
                  className="ml-auto underline hover:text-foreground"
                >
                  {expanded.has(p.id) ? "hide quote" : "show quote"}
                </button>
              </div>

              {expanded.has(p.id) && (
                <blockquote className="mt-2 border-l-2 border-muted-foreground/30 pl-3 text-xs italic text-muted-foreground">
                  &quot;{p.quote}&quot;
                </blockquote>
              )}
            </div>
          ))}
        </div>
      )}
    </main>
  );
}
