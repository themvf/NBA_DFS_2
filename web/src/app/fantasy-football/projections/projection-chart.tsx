"use client";

/**
 * 2025 actual vs 2026 projection, by position.
 *
 * Both axes share one domain so the dashed parity line sits at a true 45
 * degrees: above it the model expects more than last year, below it less. The
 * domain is deliberately not anchored at zero -- this compares two like
 * measures by position, not magnitude, and a zero origin would push every
 * point into one corner.
 *
 * Players with no 2025 sample (rookies, plus anyone who never took a snap)
 * cannot sit on an x-axis of 2025 points, so they get a separate strip at the
 * left rather than being dropped or drawn at a fake zero.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ProjectionScatterRow } from "@/db/queries-fantasy-football";
import {
  BASELINE_GAMES,
  POSITION_COLORS,
  VIEW_LABELS,
  VIEW_NOTES,
  VIEW_MOVER_NOTES,
  VIEW_ORDER,
  VIEW_POSITIONS,
  type ScatterView,
} from "@/lib/fantasy-football/projection-scatter";

type Mode = "pg" | "total";
type Scope = "adp" | "all";

type Point = ProjectionScatterRow & {
  ppg2025: number | null;
  projPpg: number;
  proj: number;
  hasHistory: boolean;
};

type Placed = { point: Point; cx: number; cy: number; label: { x: number; y: number } | null };

const fmt1 = (n: number) => n.toFixed(1);
const signed = (n: number) => `${n >= 0 ? "+" : "−"}${Math.abs(n).toFixed(1)}`;

function niceTicks(lo: number, hi: number, count: number): number[] {
  const raw = (hi - lo) / count;
  if (!Number.isFinite(raw) || raw <= 0) return [0, 1];
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? 10 * mag;
  const start = Math.floor(lo / step) * step;
  const end = Math.ceil(hi / step) * step;
  const out: number[] = [];
  for (let v = start; v <= end + 1e-9; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

export default function ProjectionChart({ rows }: { rows: ProjectionScatterRow[] }) {
  const [view, setView] = useState<ScatterView>("QB");
  const [mode, setMode] = useState<Mode>("pg");
  const [scope, setScope] = useState<Scope>("adp");
  const [hover, setHover] = useState<Placed | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(900);

  useEffect(() => {
    const node = wrapRef.current;
    if (!node) return;
    const observer = new ResizeObserver((entries) => {
      const next = Math.round(entries[0]?.contentRect.width ?? 0);
      if (next > 0) setWidth(next);
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const points = useMemo<Point[]>(() => {
    const wanted = new Set(VIEW_POSITIONS[view]);
    return rows
      .filter((row) => wanted.has(row.position) && row.ourProjectedPoints != null)
      .filter((row) => (scope === "adp" ? row.adp != null : true))
      .map((row) => {
        const proj = row.ourProjectedPoints as number;
        const pts = row.fantasyPoints2025;
        const games = row.games2025;
        const ppg = pts != null && games ? pts / games : null;
        return { ...row, proj, projPpg: proj / BASELINE_GAMES, ppg2025: ppg, hasHistory: ppg != null };
      });
  }, [rows, view, scope]);

  const xOf = useCallback((p: Point) => (mode === "pg" ? p.ppg2025 : p.fantasyPoints2025), [mode]);
  const yOf = useCallback((p: Point) => (mode === "pg" ? p.projPpg : p.proj), [mode]);
  const deltaOf = useCallback(
    (p: Point) => (p.hasHistory ? p.projPpg - (p.ppg2025 as number) : null),
    [],
  );

  const layout = useMemo(() => {
    const W = Math.max(560, width || 900);
    const H = Math.round(Math.min(560, Math.max(400, W * 0.54)));
    // Below 560 CSS px the viewBox is scaled down, so SVG type and label
    // geometry are scaled back up by k to stay legible on a phone.
    const k = W / Math.max(width || W, 1);
    const narrow = (width || W) < 560;
    const M = { top: 30, right: 24, bottom: 56, left: 58 };
    // The rail only exists when someone actually lacks a 2025 sample. DEF and
    // TE have full coverage, so reserving its width there would just be a gap.
    const needsRail = points.some((p) => !p.hasHistory);
    const railW = needsRail ? 58 : 0;
    const railGap = needsRail ? 22 : 0;
    const px0 = M.left + railW + railGap;
    const px1 = W - M.right;
    const py0 = H - M.bottom;
    const py1 = M.top;

    const values: number[] = [];
    for (const p of points) {
      const x = xOf(p);
      if (x != null) values.push(x);
      values.push(yOf(p));
    }
    if (!values.length) values.push(0, 1);
    const lo = Math.min(...values);
    const hi = Math.max(...values);
    const pad = Math.max((hi - lo) * 0.09, 0.5);
    const ticks = niceTicks(Math.max(0, lo - pad), hi + pad, 5);
    const d0 = ticks[0];
    const span = Math.max(ticks[ticks.length - 1] - d0, 1e-6);
    const sx = (v: number) => px0 + ((v - d0) / span) * (px1 - px0);
    const sy = (v: number) => py0 - ((v - d0) / span) * (py0 - py1);
    const railX = M.left + railW / 2;

    const positioned = points
      .map((point) => {
        const x = xOf(point);
        return { point, cx: x == null ? railX : sx(x), cy: sy(yOf(point)) };
      })
      .sort((a, b) => a.cy - b.cy);

    // Label the players a drafter is actually choosing between, plus the
    // leading rookies -- rookies are half the reason to open this chart, but
    // they all share the narrow no-history rail, so labelling all 20 WR rookies
    // produces a wall of overlapping text instead of information.
    const ranked = [...points].sort((a, b) => b.proj - a.proj);
    const labelCount = narrow ? 5 : scope === "adp" ? 14 : 10;
    const rookieLabelCount = narrow ? 3 : 6;
    const labelled = new Set(ranked.slice(0, labelCount).map((p) => p.playerId));
    for (const p of ranked.filter((p) => p.rookie).slice(0, rookieLabelCount)) {
      labelled.add(p.playerId);
    }

    // Every mark is an obstacle, so a label never lands on another player's dot.
    const boxes = positioned.map((p) => ({ x: p.cx - 8, y: p.cy - 8, w: 16, h: 16 }));
    const hits = (box: { x: number; y: number; w: number; h: number }) =>
      boxes.some(
        (b) =>
          !(box.x + box.w < b.x || b.x + b.w < box.x || box.y + box.h < b.y || b.y + b.h < box.y),
      );

    const placed: Placed[] = positioned.map(({ point, cx, cy }) => {
      if (!labelled.has(point.playerId)) return { point, cx, cy, label: null };
      const w = (point.name.length * 6.15 + 4) * k;
      const lh = 13 * k;
      let label: { x: number; y: number } | null = null;
      for (const dy of [0, -12, 12, -23, 23, -34, 34, -45, 45].map((v) => v * k)) {
        for (const side of [1, -1]) {
          const lx = side === 1 ? cx + 11 : cx - 11 - w;
          const ly = cy + 4 * k + dy;
          if (lx < 4 || lx + w > W - 4 || ly < py1 + lh || ly > py0 - 2) continue;
          const box = { x: lx - 2, y: ly - lh * 0.78, w: w + 4, h: lh };
          if (!hits(box)) {
            label = { x: lx, y: ly };
            boxes.push(box);
            break;
          }
        }
        if (label) break;
      }
      return { point, cx, cy, label };
    });

    return { W, H, k, px0, px1, py0, py1, railX, railW, needsRail, ticks, d0, sx, sy, placed, marginLeft: M.left };
    // mode is not listed: xOf/yOf are already keyed to it and change identity with it.
  }, [points, width, scope, xOf, yOf]);

  const movers = useMemo(() => {
    const withHistory = points.filter((p) => p.hasHistory);
    const sorted = [...withHistory].sort((a, b) => (deltaOf(b) ?? 0) - (deltaOf(a) ?? 0));
    return { up: sorted.slice(0, 6), down: sorted.slice(-6).reverse(), n: withHistory.length };
  }, [points, deltaOf]);

  const noHistory = layout.placed.filter((p) => !p.point.hasHistory).length;
  const unit = mode === "pg" ? "points per game" : "fantasy points";
  const note = VIEW_NOTES[view];
  const colorOf = (position: string) => POSITION_COLORS[position]?.light ?? "#2a78d6";
  const seriesPositions = VIEW_POSITIONS[view];
  // No defense is ever a rookie, so the hollow-mark legend would be noise on DEF.
  const hasRookies = points.some((p) => p.rookie);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-2">
        {VIEW_ORDER.map((key) => {
          const active = key === view;
          const accent = key === "FLEX" ? null : POSITION_COLORS[VIEW_POSITIONS[key][0]];
          return (
            <button
              key={key}
              type="button"
              onClick={() => {
                setView(key);
                setHover(null);
              }}
              aria-pressed={active}
              className={`rounded-lg border px-4 py-2 text-sm font-bold transition ${
                active ? "border-slate-900 bg-slate-900 text-white" : "hover:bg-muted"
              }`}
            >
              <span className="flex items-center gap-2">
                {accent && (
                  <span
                    className="inline-block size-2.5 rounded-full"
                    style={{ background: accent.light }}
                  />
                )}
                {VIEW_LABELS[key]}
              </span>
            </button>
          );
        })}
      </div>

      <div className="flex flex-wrap items-end gap-x-7 gap-y-4 rounded-2xl border bg-card p-4">
        <div className="space-y-1.5">
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
            Compare on
          </p>
          <div className="flex overflow-hidden rounded-lg border">
            {(
              [
                ["pg", "Per game"],
                ["total", "Season total"],
              ] as const
            ).map(([value, label]) => (
              <button
                key={value}
                type="button"
                aria-pressed={mode === value}
                onClick={() => {
                  setMode(value);
                  setHover(null);
                }}
                className={`px-3 py-1.5 text-sm ${
                  mode === value ? "bg-slate-900 font-bold text-white" : "hover:bg-muted"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        <div className="space-y-1.5">
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
            Who to show
          </p>
          <div className="flex overflow-hidden rounded-lg border">
            {(
              [
                ["adp", "Drafted"],
                ["all", "Everyone"],
              ] as const
            ).map(([value, label]) => (
              <button
                key={value}
                type="button"
                aria-pressed={scope === value}
                onClick={() => {
                  setScope(value);
                  setHover(null);
                }}
                className={`px-3 py-1.5 text-sm ${
                  scope === value ? "bg-slate-900 font-bold text-white" : "hover:bg-muted"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        <p className="ml-auto max-w-[38ch] text-xs text-muted-foreground">
          {scope === "adp"
            ? `${points.length} with a Fantasy Football Calculator ADP — the ones actually being drafted.`
            : `All ${points.length} on the board, backups included.`}
          {noHistory > 0 && ` ${noHistory} have no 2025 sample.`}
        </p>
      </div>

      {note && (
        <p className="rounded-xl border border-amber-300 bg-amber-50 p-3 text-sm text-amber-950">
          {note}
        </p>
      )}

      <div className="rounded-2xl border bg-card p-4">
        <div className="mb-2 flex flex-wrap items-baseline gap-x-6 gap-y-2">
          <h2 className="font-bold">
            {VIEW_LABELS[view]} — {mode === "pg" ? "points per game" : "season points"}, 2025 actual
            vs 2026 projection
          </h2>
          <div className="ml-auto flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
            {seriesPositions.map((position) => (
              <span key={position} className="flex items-center gap-1.5">
                <span
                  className="inline-block size-2.5 rounded-full"
                  style={{ background: colorOf(position) }}
                />
                {position === "DST" ? "DEF" : position}
              </span>
            ))}
            {hasRookies && (
              <span className="flex items-center gap-1.5">
                <span
                  className="inline-block size-2.5 rounded-full border-2 bg-card"
                  style={{ borderColor: colorOf(seriesPositions[0]) }}
                />
                Rookie
              </span>
            )}
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-0 w-4 border-t-2 border-dashed border-slate-400" />
              Same as 2025
            </span>
          </div>
        </div>

        <div ref={wrapRef} className="relative w-full">
          <svg
            viewBox={`0 0 ${layout.W} ${layout.H}`}
            className="block h-auto w-full"
            role="img"
            aria-label={`${VIEW_LABELS[view]} 2025 actual against 2026 projection, ${unit}`}
            style={{ "--k": layout.k.toFixed(3) } as React.CSSProperties}
          >
            <polygon
              points={`${layout.px0},${layout.py0} ${layout.px1},${layout.py1} ${layout.px0},${layout.py1}`}
              fill="rgba(42,120,214,0.055)"
            />
            <polygon
              points={`${layout.px0},${layout.py0} ${layout.px1},${layout.py1} ${layout.px1},${layout.py0}`}
              fill="rgba(227,73,72,0.045)"
            />

            {layout.ticks.map((t) =>
              t === layout.d0 ? null : (
                <g key={`grid-${t}`} stroke="currentColor" className="text-border">
                  <line x1={layout.px0} x2={layout.px1} y1={layout.sy(t)} y2={layout.sy(t)} />
                  <line y1={layout.py1} y2={layout.py0} x1={layout.sx(t)} x2={layout.sx(t)} />
                </g>
              ),
            )}

            {layout.needsRail && (
              <g>
                <rect
                  x={layout.marginLeft}
                  y={layout.py1}
                  width={layout.railW}
                  height={layout.py0 - layout.py1}
                  rx={4}
                  className="fill-muted"
                />
                <line
                  x1={layout.marginLeft + layout.railW + 11}
                  x2={layout.marginLeft + layout.railW + 11}
                  y1={layout.py1}
                  y2={layout.py0}
                  strokeDasharray="3 4"
                  className="stroke-muted-foreground/50"
                />
                {["no 2025", "sample"].map((text, i) => (
                  <text
                    key={text}
                    x={layout.railX}
                    y={layout.py0 + 17 + i * 13}
                    textAnchor="middle"
                    className="fill-muted-foreground"
                    style={{ fontSize: "calc(11px * var(--k, 1))" }}
                  >
                    {text}
                  </text>
                ))}
              </g>
            )}

            <line
              x1={layout.px0}
              x2={layout.px1}
              y1={layout.py0}
              y2={layout.py0}
              className="stroke-muted-foreground/40"
            />
            <line
              x1={layout.px0}
              x2={layout.px0}
              y1={layout.py1}
              y2={layout.py0}
              className="stroke-muted-foreground/40"
            />

            {layout.ticks.map((t) => (
              <g key={`tick-${t}`} className="fill-muted-foreground" style={{ fontSize: "calc(11px * var(--k, 1))" }}>
                <text x={layout.sx(t)} y={layout.py0 + 17} textAnchor="middle">
                  {t}
                </text>
                <text x={layout.px0 - 10} y={layout.sy(t) + 4} textAnchor="end">
                  {t}
                </text>
              </g>
            ))}

            <text
              x={(layout.px0 + layout.px1) / 2}
              y={layout.H - 12}
              textAnchor="middle"
              className="fill-muted-foreground font-semibold"
              style={{ fontSize: "calc(12px * var(--k, 1))" }}
            >
              {`2025 actual — ${unit}`}
            </text>
            <text
              textAnchor="middle"
              transform={`translate(15 ${(layout.py0 + layout.py1) / 2}) rotate(-90)`}
              className="fill-muted-foreground font-semibold"
              style={{ fontSize: "calc(12px * var(--k, 1))" }}
            >
              {`2026 projected — ${unit}`}
            </text>

            <line
              x1={layout.px0}
              y1={layout.py0}
              x2={layout.px1}
              y2={layout.py1}
              strokeWidth={1.5}
              strokeDasharray="5 4"
              className="stroke-muted-foreground/60"
            />
            <text
              x={layout.px0 + 14}
              y={layout.py1 + 17}
              className="fill-muted-foreground font-semibold"
              style={{ fontSize: "calc(11.5px * var(--k, 1))" }}
            >
              {"↑ model projects growth"}
            </text>
            <text
              x={layout.px1 - 6}
              y={layout.py0 - 12}
              textAnchor="end"
              className="fill-muted-foreground font-semibold"
              style={{ fontSize: "calc(11.5px * var(--k, 1))" }}
            >
              {"model projects decline ↓"}
            </text>

            {layout.placed.map((entry) => {
              const dimmed = hover != null && hover.point.playerId !== entry.point.playerId;
              const color = colorOf(entry.point.position);
              return (
                <g key={entry.point.playerId} opacity={dimmed ? 0.26 : 1}>
                  {entry.label && (
                    <text
                      x={entry.label.x}
                      y={entry.label.y}
                      className={hover?.point.playerId === entry.point.playerId ? "fill-foreground font-bold" : "fill-muted-foreground"}
                      style={{ fontSize: "calc(11.5px * var(--k, 1))" }}
                    >
                      {entry.point.name}
                    </text>
                  )}
                  <circle
                    cx={entry.cx}
                    cy={entry.cy}
                    r={5.5}
                    fill={entry.point.rookie ? "var(--card)" : color}
                    stroke={entry.point.rookie ? color : "var(--card)"}
                    strokeWidth={entry.point.rookie ? 2.5 : 2}
                  />
                  <circle
                    cx={entry.cx}
                    cy={entry.cy}
                    r={13}
                    fill="transparent"
                    tabIndex={0}
                    role="img"
                    aria-label={describe(entry.point, deltaOf(entry.point))}
                    className="cursor-pointer outline-none focus-visible:stroke-foreground"
                    onMouseEnter={() => setHover(entry)}
                    onMouseLeave={() => setHover(null)}
                    onFocus={() => setHover(entry)}
                    onBlur={() => setHover(null)}
                  />
                </g>
              );
            })}
          </svg>

          {hover && (
            <Tooltip entry={hover} layout={layout} width={width} delta={deltaOf(hover.point)} />
          )}
        </div>
      </div>

      {VIEW_MOVER_NOTES[view] && (
        <p className="text-xs text-muted-foreground">{VIEW_MOVER_NOTES[view]}</p>
      )}

      <div className="grid gap-5 md:grid-cols-2">
        <MoverList
          title="Biggest projected gains"
          subtitle={`Per-game change, ${movers.n} with a 2025 sample`}
          rows={movers.up}
          deltaOf={deltaOf}
        />
        <MoverList
          title="Biggest projected drops"
          subtitle="Per-game change, same pool"
          rows={movers.down}
          deltaOf={deltaOf}
        />
      </div>

      <details className="rounded-2xl border bg-card">
        <summary className="cursor-pointer p-4 font-bold">
          Full table — {points.length} {VIEW_LABELS[view]}
        </summary>
        <div className="max-h-[460px] overflow-auto border-t">
          <table className="w-full text-sm tabular-nums">
            <thead className="sticky top-0 bg-card text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="p-2 text-left">Player</th>
                <th className="p-2 text-left">Team</th>
                <th className="p-2 text-right">Pos rk</th>
                <th className="p-2 text-right">ADP</th>
                <th className="p-2 text-right">2025 G</th>
                <th className="p-2 text-right">2025 pts</th>
                <th className="p-2 text-right">2025 PPG</th>
                <th className="p-2 text-right">2026 proj</th>
                <th className="p-2 text-right">2026 PPG</th>
                <th className="p-2 text-right">Δ PPG</th>
              </tr>
            </thead>
            <tbody>
              {[...points]
                .sort((a, b) => b.proj - a.proj)
                .map((p) => {
                  const delta = deltaOf(p);
                  return (
                    <tr key={p.playerId} className="border-b hover:bg-muted/60">
                      <td className="p-2 font-semibold">
                        <span
                          className="mr-2 inline-block size-2 rounded-full align-middle"
                          style={{ background: colorOf(p.position) }}
                        />
                        {p.name}
                        {p.rookie && (
                          <span className="ml-1.5 rounded bg-slate-900 px-1 py-0.5 text-[9px] font-bold text-white">
                            R
                          </span>
                        )}
                      </td>
                      <td className="p-2">{p.team ?? "—"}</td>
                      <td className="p-2 text-right">
                        {p.positionRank ? `${p.position === "DST" ? "DEF" : p.position}${p.positionRank}` : "—"}
                      </td>
                      <td className="p-2 text-right">{p.adp != null ? p.adp.toFixed(1) : "—"}</td>
                      <td className="p-2 text-right">{p.games2025 ?? "—"}</td>
                      <td className="p-2 text-right">
                        {p.fantasyPoints2025 != null ? fmt1(p.fantasyPoints2025) : "—"}
                      </td>
                      <td className="p-2 text-right">{p.ppg2025 != null ? fmt1(p.ppg2025) : "—"}</td>
                      <td className="p-2 text-right">{fmt1(p.proj)}</td>
                      <td className="p-2 text-right">{fmt1(p.projPpg)}</td>
                      <td
                        className={`p-2 text-right font-semibold ${
                          delta == null ? "" : delta >= 0 ? "text-emerald-700" : "text-rose-700"
                        }`}
                      >
                        {delta != null ? signed(delta) : "—"}
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      </details>

    </div>
  );
}

function describe(point: Point, delta: number | null) {
  const history = point.hasHistory
    ? `${fmt1(point.ppg2025 as number)} points per game over ${point.games2025} games`
    : "no 2025 games";
  return `${point.name}, ${point.team ?? "free agent"}. 2025: ${history}. 2026 projection: ${fmt1(
    point.projPpg,
  )} per game, ${fmt1(point.proj)} for the season.${delta != null ? ` Change ${signed(delta)} per game.` : ""}`;
}

function Tooltip({
  entry,
  layout,
  width,
  delta,
}: {
  entry: Placed;
  layout: { W: number; H: number };
  width: number;
  delta: number | null;
}) {
  const scale = (width || layout.W) / layout.W;
  const left = entry.cx * scale;
  const top = entry.cy * scale;
  const flip = left > (width || layout.W) * 0.62;
  const p = entry.point;
  return (
    <div
      role="tooltip"
      className="pointer-events-none absolute z-10 w-[200px] rounded-lg border bg-card p-2.5 text-xs shadow-lg"
      style={{
        left: flip ? undefined : Math.max(left + 16, 0),
        right: flip ? Math.max((width || layout.W) - left + 16, 0) : undefined,
        top: Math.min(Math.max(top - 14, 0), Math.max(layout.H * scale - 150, 0)),
      }}
    >
      <p className="text-sm font-bold">
        {p.name}
        {p.rookie && (
          <span className="ml-1.5 rounded bg-slate-900 px-1 py-0.5 text-[9px] font-bold text-white">
            ROOKIE
          </span>
        )}
      </p>
      <p className="mb-1.5 text-[11px] text-muted-foreground">
        {p.team ?? "FA"} · {p.position === "DST" ? "DEF" : p.position}
        {p.positionRank ?? ""} · {p.adp != null ? `ADP ${p.adp.toFixed(1)}` : "undrafted"}
        {p.byeWeek ? ` · bye ${p.byeWeek}` : ""}
      </p>
      <dl className="grid grid-cols-[auto_auto] justify-between gap-x-4 gap-y-0.5 tabular-nums">
        <dt className="text-muted-foreground">2025 PPG</dt>
        <dd className="text-right font-semibold">
          {p.ppg2025 != null ? fmt1(p.ppg2025) : "—"}
        </dd>
        <dt className="text-muted-foreground">2026 PPG</dt>
        <dd className="text-right font-semibold">{fmt1(p.projPpg)}</dd>
        {delta != null && (
          <>
            <dt className="text-muted-foreground">Change</dt>
            <dd
              className={`text-right font-semibold ${delta >= 0 ? "text-emerald-700" : "text-rose-700"}`}
            >
              {signed(delta)}
            </dd>
          </>
        )}
        <dt className="text-muted-foreground">2025 total</dt>
        <dd className="text-right font-semibold">
          {p.fantasyPoints2025 != null ? `${fmt1(p.fantasyPoints2025)} (${p.games2025}g)` : "—"}
        </dd>
        <dt className="text-muted-foreground">2026 total</dt>
        <dd className="text-right font-semibold">{fmt1(p.proj)}</dd>
      </dl>
    </div>
  );
}

function MoverList({
  title,
  subtitle,
  rows,
  deltaOf,
}: {
  title: string;
  subtitle: string;
  rows: Point[];
  deltaOf: (p: Point) => number | null;
}) {
  return (
    <section className="rounded-2xl border bg-card p-4">
      <h3 className="font-bold">{title}</h3>
      <p className="mb-2 text-xs text-muted-foreground">{subtitle}</p>
      <ol className="tabular-nums">
        {rows.length === 0 && <li className="py-2 text-sm text-muted-foreground">No 2025 sample.</li>}
        {rows.map((p) => {
          const delta = deltaOf(p) ?? 0;
          return (
            <li
              key={p.playerId}
              className="grid grid-cols-[1fr_auto_auto] items-baseline gap-3 border-t py-1.5 text-sm first:border-t-0"
            >
              <span>
                <span className="font-semibold">{p.name}</span>{" "}
                <span className="text-[11px] uppercase tracking-wide text-muted-foreground">
                  {p.team}
                </span>
              </span>
              <span className="text-xs text-muted-foreground">
                {fmt1(p.ppg2025 as number)} {"→"} {fmt1(p.projPpg)}
              </span>
              <span
                className={`min-w-[54px] text-right font-bold ${
                  delta >= 0 ? "text-emerald-700" : "text-rose-700"
                }`}
              >
                {signed(delta)}
              </span>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
