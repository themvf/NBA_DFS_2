"use client";

/**
 * Boom/bust profile from real weekly scores.
 *
 * This panel answers "who put up the biggest weeks" -- a question about the
 * past. It deliberately does not present itself as a forecast, because a
 * year-over-year screen of these exact metrics found that last season's
 * ceiling, once you already know the player's points per game, does not
 * reliably predict next season's ceiling (see UPSIDE_IS_DESCRIPTIVE).
 */

import { useMemo, useState } from "react";
import type { WeeklyUpsideRow } from "@/db/queries-fantasy-football";
import {
  POSITION_COLORS,
  SPIKE_THRESHOLDS,
  VIEW_LABELS,
  VIEW_POSITIONS,
  WEEKLY_SEASONS,
  type ScatterView,
} from "@/lib/fantasy-football/projection-scatter";

type Sort = "top3" | "best" | "spikeRate" | "streak" | "floor";

/**
 * Draft-cost windows. The question this panel usually gets asked -- "who is a
 * good late upside stash" -- is not answerable from a raw ceiling sort, which
 * just returns the first round. Filtering by where a player actually goes is.
 */
const ADP_WINDOWS: { key: string; label: string; min: number }[] = [
  { key: "all", label: "Any pick", min: 0 },
  { key: "60", label: "After 60", min: 60 },
  { key: "100", label: "After 100", min: 100 },
  { key: "150", label: "After 150", min: 150 },
];

/**
 * Streakiness is spread divided by the player's own average, so it explodes as
 * the average approaches zero: a WR averaging 5.8 with one 28-point afternoon
 * scores higher than any real boom-bust starter. Sorting by it without a
 * scoring floor returns arithmetic, not players worth drafting. The floor is
 * a share of the position's spike threshold rather than a fixed number, so it
 * scales with what the position actually scores.
 */
const STREAK_FLOOR_SHARE = 0.4;

const f1 = (n: number) => n.toFixed(1);
const pct = (n: number) => `${Math.round(n * 100)}%`;

export default function UpsideTable({
  rows,
  view,
  seasons,
  seasonMode,
  onSeasonModeChange,
}: {
  rows: WeeklyUpsideRow[];
  view: ScatterView;
  seasons: number[];
  seasonMode: "recent" | "all";
  onSeasonModeChange: (mode: "recent" | "all") => void;
}) {
  const [sort, setSort] = useState<Sort>("top3");
  const [draftedOnly, setDraftedOnly] = useState(true);
  const [minGames, setMinGames] = useState(8);
  const [adpWindow, setAdpWindow] = useState("all");

  const label = VIEW_LABELS[view];
  const multiPosition = VIEW_POSITIONS[view].length > 1;

  const spikeThreshold = SPIKE_THRESHOLDS[VIEW_POSITIONS[view][0]] ?? 16;
  const streakFloor = spikeThreshold * STREAK_FLOOR_SHARE;

  const prepared = useMemo(() => {
    const wanted = new Set(VIEW_POSITIONS[view]);
    const minAdp = ADP_WINDOWS.find((w) => w.key === adpWindow)?.min ?? 0;
    return rows
      .filter((r) => wanted.has(r.position))
      .filter((r) => (draftedOnly ? r.adp != null : true))
      .filter((r) => (minAdp > 0 ? r.adp != null && r.adp >= minAdp : true))
      .filter((r) => r.games >= minGames)
      // Only the streak sort needs the scoring floor; it is the one metric
      // that is mechanically inflated by a low average.
      .filter((r) => (sort === "streak" ? r.ppg >= streakFloor : true))
      .map((r) => ({
        ...r,
        spikeRate: r.games ? r.spikes / r.games : 0,
        // Spread relative to the player's own scoring level. A 6-point swing
        // means something different for a WR3 than for a WR1.
        streak: r.ppg > 0 ? r.sd / r.ppg : 0,
      }))
      .sort((a, b) =>
        sort === "best"
          ? b.best - a.best
          : sort === "spikeRate"
            ? b.spikeRate - a.spikeRate
            : sort === "streak"
              ? b.streak - a.streak
              : sort === "floor"
                ? b.floor - a.floor
                : b.top3 - a.top3,
      )
      .slice(0, 60);
  }, [rows, view, draftedOnly, minGames, adpWindow, sort, streakFloor]);

  const threshold = spikeThreshold;
  const sortButton = (key: Sort, text: string, title: string) => (
    <button
      type="button"
      onClick={() => setSort(key)}
      aria-pressed={sort === key}
      title={title}
      className={`rounded-md px-2 py-1 text-xs ${
        sort === key ? "bg-slate-900 font-bold text-white" : "border hover:bg-muted"
      }`}
    >
      {text}
    </button>
  );

  return (
    <section className="space-y-3 rounded-2xl border bg-card p-4">
      <div className="flex flex-wrap items-end gap-x-6 gap-y-3">
        <div>
          <h2 className="font-bold">{label} boom &amp; bust &mdash; who had the big weeks</h2>
          <p className="text-xs text-muted-foreground">
            Weeks 1&ndash;17 of {seasons.length > 1 ? `${Math.min(...seasons)}–${Math.max(...seasons)}` : seasons[0]}.
            A spike week is {threshold}+ points for a {multiPosition ? "RB/WR/TE" : label}.
          </p>
        </div>
        <div className="ml-auto flex flex-wrap items-center gap-3">
          <div className="flex overflow-hidden rounded-lg border">
            {(
              [
                ["recent", `${WEEKLY_SEASONS[0]}`],
                ["all", "3 seasons"],
              ] as const
            ).map(([value, text]) => (
              <button
                key={value}
                type="button"
                aria-pressed={seasonMode === value}
                onClick={() => onSeasonModeChange(value)}
                className={`px-2.5 py-1 text-xs ${
                  seasonMode === value ? "bg-slate-900 font-bold text-white" : "hover:bg-muted"
                }`}
              >
                {text}
              </button>
            ))}
          </div>
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={draftedOnly}
              onChange={(event) => setDraftedOnly(event.target.checked)}
              className="size-3.5 accent-slate-900"
            />
            Drafted only
          </label>
          <label className="flex items-center gap-1.5 text-xs">
            Drafted
            <select
              value={adpWindow}
              onChange={(event) => setAdpWindow(event.target.value)}
              className="rounded-md border px-1.5 py-1 text-xs"
            >
              {ADP_WINDOWS.map((w) => (
                <option key={w.key} value={w.key}>
                  {w.label}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-xs">
            Min games
            <select
              value={minGames}
              onChange={(event) => setMinGames(Number(event.target.value))}
              className="rounded-md border px-1.5 py-1 text-xs"
            >
              {[4, 8, 12, 17].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-1.5">
        <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
          Sort
        </span>
        {sortButton("top3", "Ceiling", "Average of the player's three best weeks")}
        {sortButton("best", "Best week", "Single highest week")}
        {sortButton("spikeRate", "Spike rate", `Share of weeks at ${threshold}+ points`)}
        {sortButton(
          "streak",
          "Streakiness",
          `Week-to-week spread relative to the player's own average (needs ${f1(streakFloor)}+ PPG)`,
        )}
        {sortButton("floor", "Floor", "25th-percentile week — the typical bad game")}
      </div>

      {sort === "streak" && (
        <p className="text-xs text-muted-foreground">
          Streakiness divides spread by the player&rsquo;s own average, so it runs away at low
          scoring. Players under {f1(streakFloor)} points per game are held out of this sort &mdash;
          otherwise a {label} averaging 5 points with one big afternoon outranks every real
          boom-bust starter.
        </p>
      )}

      <div className="max-h-[520px] overflow-auto rounded-lg border">
        <table className="w-full text-sm tabular-nums">
          <thead className="sticky top-0 z-10 bg-card text-[10px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="p-2 text-left">Player</th>
              <th className="p-2 text-right">ADP</th>
              <th className="p-2 text-right">G</th>
              <th className="p-2 text-right">PPG</th>
              <th className="p-2 text-right" title="Average of the three best weeks">
                Ceiling
              </th>
              <th className="p-2 text-right">Best</th>
              <th className="p-2 text-right" title={`Weeks at ${threshold}+ points`}>
                Spikes
              </th>
              <th className="p-2 text-right">Spike %</th>
              <th className="p-2 text-right" title="25th-percentile week">
                Floor
              </th>
              <th className="p-2 text-right">Worst</th>
              <th className="p-2 text-right" title="Spread relative to the player's own average">
                Streak
              </th>
            </tr>
          </thead>
          <tbody>
            {prepared.length === 0 && (
              <tr>
                <td colSpan={11} className="p-6 text-center text-muted-foreground">
                  No {label} clears these filters &mdash; try fewer minimum games or a wider draft
                  range.
                </td>
              </tr>
            )}
            {prepared.map((row) => (
              <tr key={row.playerId} className="border-b hover:bg-muted/60">
                <td className="p-2 font-semibold">
                  <span
                    className="mr-2 inline-block size-2 rounded-full align-middle"
                    style={{ background: POSITION_COLORS[row.position]?.light }}
                  />
                  {row.name}
                  <span className="ml-1.5 text-[10px] font-normal uppercase tracking-wide text-muted-foreground">
                    {multiPosition ? `${row.position} · ` : ""}
                    {row.team}
                  </span>
                </td>
                <td className="p-2 text-right">{row.adp != null ? row.adp.toFixed(1) : "—"}</td>
                <td className="p-2 text-right text-muted-foreground">{row.games}</td>
                <td className="p-2 text-right">{f1(row.ppg)}</td>
                <td className="p-2 text-right font-bold">{f1(row.top3)}</td>
                <td className="p-2 text-right">{f1(row.best)}</td>
                <td className="p-2 text-right">{row.spikes}</td>
                <td className="p-2 text-right font-semibold">{pct(row.spikeRate)}</td>
                <td className="p-2 text-right">{f1(row.floor)}</td>
                <td className="p-2 text-right text-muted-foreground">{f1(row.worst)}</td>
                <td className="p-2 text-right">{row.streak.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="space-y-2 rounded-xl border border-amber-300 bg-amber-50 p-3 text-xs text-amber-950">
        <p className="font-bold">This is what happened, not what will happen.</p>
        <p>
          These metrics were screened year over year (2023&rarr;2024 and 2024&rarr;2025, players with
          8+ games in both). <strong>Ceiling barely survives the screen:</strong> once you already
          know a player&rsquo;s points per game, last season&rsquo;s three best weeks add almost
          nothing about next season&rsquo;s &mdash; the correlation flips sign between the two year
          pairs (WR +0.17 then &minus;0.10; TE +0.27 then &minus;0.07), which is what noise looks
          like. A big ceiling mostly just means a good player.
        </p>
        <p>
          <strong>Streakiness holds up better</strong> for RB/WR/TE (+0.02 to +0.48 beyond points per
          game), and spike rate a little (+0.05 to +0.25, one negative). So &ldquo;he is boom-bust&rdquo;
          is a somewhat more durable trait than &ldquo;he has a high ceiling&rdquo; &mdash; but both are
          modest, and QB, K and DEF samples here are too small (20&ndash;32 players) to claim anything.
        </p>
        <p>
          Use the 3-season view when you can: a 17-game sample is a thin base for a variance estimate,
          and the min-games filter exists because a 4-game player with one huge afternoon will
          otherwise top every ceiling sort.
        </p>
      </div>
    </section>
  );
}
