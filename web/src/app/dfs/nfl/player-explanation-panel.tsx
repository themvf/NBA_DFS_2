"use client";

import { useEffect, useMemo, useState, useTransition, type ReactNode } from "react";
import {
  X, Activity, TrendingUp, Dice5, Gauge, AlertTriangle, Users, Coins, Calculator,
} from "lucide-react";
import {
  PASS_YARD_PTS, PASS_TD_PTS, INTERCEPTION_PTS, RUSH_YARD_PTS, RUSH_TD_PTS,
  REC_YARD_PTS, REC_TD_PTS, RECEPTION_PTS, FUMBLE_LOST_PTS, TWO_POINT_CONVERSION_PTS,
  PASS_YARD_BONUS_THRESHOLD, RUSH_YARD_BONUS_THRESHOLD, YARDAGE_BONUS_PTS,
} from "@/lib/nfl-dfs/scoring";
import { explainNflPlayerProjection, type NflProjectionExplanation, type NflWorkspacePlayer } from "./actions";

/*
 * Chart palette. Validated for the light surface (#ffffff) with the dataviz
 * validator: ACCENT x UP and ACCENT x DOWN each clear every gate (lightness
 * band, chroma floor, CVD separation, normal-vision floor, 3:1 contrast).
 *
 * UP and DOWN are a diverging pair and are deliberately never drawn in the
 * same chart -- a waterfall step is a boost or a drag, never both -- because
 * emerald x orange sits below the CVD separation floor. Where both directions
 * can appear at once (the environment multipliers) the direction is carried by
 * which side of a centre line the bar sits on, not by that pair.
 */
const ACCENT = "#2563eb"; // our model -- the subject of every chart in this panel
const UP = "#059669";     // a step that raises the projection
const DOWN = "#c2410c";   // a step that lowers it
const MUTED = "#94a3b8";  // de-emphasised context marks (slate peers, baselines)
const TRACK = "#dbeafe";  // meter track: a lighter step of the accent ramp

const fmt = (v: number | null | undefined, digits = 1) =>
  v === null || v === undefined ? "—" : v.toFixed(digits);
const pct = (v: number | null | undefined) =>
  v === null || v === undefined ? "—" : `${Math.round(v * 100)}%`;
const signed = (v: number, digits = 1) =>
  `${v >= 0 ? "+" : "−"}${Math.abs(v).toFixed(digits)}`;

/**
 * Per-stat display metadata plus its DK points-per-unit, imported from the
 * scoring module rather than re-typed, so a rules change cannot leave this
 * panel quoting stale values. `pts: null` means the stat is volume context
 * DK does not score directly (targets, carries, pass attempts).
 */
const STAT_SPECS: Record<string, { label: string; group: string; pts: number | null }> = {
  attempts: { label: "Pass attempts", group: "Volume", pts: null },
  carries: { label: "Carries", group: "Volume", pts: null },
  targets: { label: "Targets", group: "Volume", pts: null },
  receptions: { label: "Receptions", group: "Volume", pts: RECEPTION_PTS },
  passing_yards: { label: "Passing yards", group: "Yards", pts: PASS_YARD_PTS },
  rushing_yards: { label: "Rushing yards", group: "Yards", pts: RUSH_YARD_PTS },
  receiving_yards: { label: "Receiving yards", group: "Yards", pts: REC_YARD_PTS },
  passing_tds: { label: "Passing TDs", group: "Scoring", pts: PASS_TD_PTS },
  rushing_tds: { label: "Rushing TDs", group: "Scoring", pts: RUSH_TD_PTS },
  receiving_tds: { label: "Receiving TDs", group: "Scoring", pts: REC_TD_PTS },
  passing_interceptions: { label: "Interceptions", group: "Scoring", pts: INTERCEPTION_PTS },
  fumbles_lost_total: { label: "Fumbles lost", group: "Scoring", pts: FUMBLE_LOST_PTS },
  passing_2pt_conversions: { label: "2-pt (pass)", group: "Scoring", pts: TWO_POINT_CONVERSION_PTS },
};
const spec = (key: string) =>
  STAT_SPECS[key] ?? { label: key.replace(/_/g, " "), group: "Other", pts: null };
const GROUP_ORDER = ["Volume", "Yards", "Scoring", "Other"];

/* -- Chart primitives ---------------------------------------------------
 * Plain HTML/CSS marks, no charting dependency. Shared specs: bars are 14px
 * (under the 24px cap), 4px rounded at the data end and square at the
 * baseline, separated by surface-coloured gaps rather than strokes.       */

/** One labelled plot row: label . plot area . value at the tip. */
function Row({ label, sub, value, children }: {
  label: ReactNode; sub?: ReactNode; value?: ReactNode; children: ReactNode;
}) {
  return (
    <div className="grid grid-cols-[6.5rem_1fr_3.5rem] items-center gap-2 py-0.5">
      <div className="text-[11px] leading-tight text-slate-600">
        {label}
        {sub ? <div className="text-[10px] text-slate-400">{sub}</div> : null}
      </div>
      <div className="relative h-4">{children}</div>
      <div className="text-right text-[11px] font-bold tabular-nums text-slate-900">{value}</div>
    </div>
  );
}

/** Recessive hairline gridlines at 0 / 50% / 100% of the plot width. */
function Grid() {
  return (
    <div aria-hidden className="pointer-events-none absolute inset-0">
      {[0, 50, 100].map((left) => (
        <div key={left} className="absolute top-0 h-full w-px bg-slate-200" style={{ left: `${left}%` }} />
      ))}
    </div>
  );
}

/** A bar growing from the left baseline, or floating between two values. */
function Bar({ from = 0, to, max, color, title }: {
  from?: number; to: number; max: number; color: string; title: string;
}) {
  const lo = Math.max(0, Math.min(from, to)) / max * 100;
  const hi = Math.max(0, Math.max(from, to)) / max * 100;
  const width = Math.max(hi - lo, 0.6); // keep a hairline visible for tiny steps
  const floating = from !== 0;
  return (
    <div
      title={title}
      className="absolute top-1/2 h-3.5 -translate-y-1/2"
      style={{
        left: `${lo}%`, width: `${width}%`, backgroundColor: color,
        borderRadius: floating ? 4 : "0 4px 4px 0",
      }}
    />
  );
}

/** A fill-against-track meter. The track is a lighter step of the same ramp. */
function Meter({ value, title }: { value: number; title: string }) {
  return (
    <div title={title} className="h-2 w-full overflow-hidden rounded" style={{ backgroundColor: TRACK }}>
      <div
        className="h-full rounded-r"
        style={{ width: `${Math.max(0, Math.min(1, value)) * 100}%`, backgroundColor: ACCENT }}
      />
    </div>
  );
}

function Card({ icon, title, hint, children }: {
  icon: ReactNode; title: string; hint?: ReactNode; children: ReactNode;
}) {
  return (
    <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <h3 className="flex items-center gap-2 text-sm font-bold text-slate-900">{icon}{title}</h3>
      {hint ? <p className="mt-1 text-[11px] leading-snug text-slate-500">{hint}</p> : null}
      <div className="mt-3">{children}</div>
    </section>
  );
}

/** Legend key: a colour swatch beside text-token text. Text never wears the mark colour. */
function Key({ color, children }: { color: string; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] text-slate-600">
      <span className="h-2 w-2 rounded-sm" style={{ backgroundColor: color }} />{children}
    </span>
  );
}

function Tile({ label, value, sub }: { label: string; value: ReactNode; sub?: ReactNode }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <div className="text-[10px] font-bold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="text-base font-black leading-tight text-slate-900">{value}</div>
      {sub ? <div className="text-[10px] leading-tight text-slate-500">{sub}</div> : null}
    </div>
  );
}

type Props = {
  uploadId: string;
  player: NflWorkspacePlayer | null;
  /** The whole slate pool, read only for same-position peer context. */
  slatePlayers: readonly NflWorkspacePlayer[];
  onClose: () => void;
};

export default function PlayerExplanationPanel({ uploadId, player, slatePlayers, onClose }: Props) {
  // Keyed by the player id we last fetched for, so a stale response from a
  // previously-opened player can never be painted under this one's header.
  const [state, setState] = useState<{ id: number | null; data: NflProjectionExplanation | null }>({ id: null, data: null });
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    if (!player) return;
    const id = player.id;
    startTransition(async () => {
      try {
        const result = await explainNflPlayerProjection(uploadId, id);
        setState({ id, data: result });
      } catch (err) {
        setState({ id, data: { ok: false, error: err instanceof Error ? err.message : "Explanation unavailable." } });
      }
    });
  }, [uploadId, player]);

  useEffect(() => {
    if (!player) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [player, onClose]);

  // Same-position peers on this slate that carry a model projection. Read from
  // the pool already in memory -- no extra query.
  const peers = useMemo(() => {
    if (!player) return [] as { id: number; name: string; proj: number }[];
    return slatePlayers
      .filter((p) => p.position === player.position && p.ourProj != null && !p.isOut)
      .map((p) => ({ id: p.id, name: p.name, proj: p.ourProj as number }))
      .sort((a, b) => b.proj - a.proj);
  }, [slatePlayers, player]);

  const data = player && state.id === player.id ? state.data : null;
  const e = data && data.ok ? data : null;

  if (!player) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end" role="dialog" aria-modal="true"
         aria-label={`Why ${player.name} carries this projection`}>
      <div className="absolute inset-0 bg-slate-900/40" onClick={onClose} />
      <div className="relative flex h-full w-full max-w-xl flex-col overflow-y-auto bg-slate-50 shadow-xl">
        <div className="sticky top-0 z-20 flex items-start justify-between border-b border-slate-200 bg-white p-5">
          <div>
            <h2 className="text-lg font-black text-slate-900">{player.name}</h2>
            <p className="text-xs text-slate-500">
              {player.position} &middot; {player.team}{player.opponent ? ` vs ${player.opponent}` : ""}
              {player.salary ? ` · $${player.salary.toLocaleString()}` : ""}
            </p>
          </div>
          <button onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-400 hover:bg-slate-100">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="space-y-3 p-4">
          {pending && !e && <p className="text-sm text-slate-500">Loading the projection breakdown&hellip;</p>}

          {data && !data.ok && (
            <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
              <AlertTriangle className="h-4 w-4 shrink-0" />{data.error}
            </div>
          )}

          {e && <Breakdown e={e} player={player} peers={peers} />}
        </div>
      </div>
    </div>
  );
}

function Breakdown({ e, player, peers }: {
  e: Extract<NflProjectionExplanation, { ok: true }>;
  player: NflWorkspacePlayer;
  peers: { id: number; name: string; proj: number }[];
}) {
  const proj = e.projection ?? 0;
  const baseline = e.baseline;
  const factor = e.environmentFactor;

  /* -- Waterfall ------------------------------------------------------
   * baseline -> environment -> everything the simulation adds. The third
   * step is a genuine residual, not a fudge: the sims re-score draws under
   * DK rules (including the threshold yardage bonuses) and apply any
   * availability ruling, so projection != baseline x factor by design. It
   * is drawn rather than hidden so the parts always reconcile to the whole. */
  const afterEnv = baseline != null && factor != null ? baseline * factor : null;
  const envDelta = baseline != null && afterEnv != null ? afterEnv - baseline : null;
  const simDelta = afterEnv != null ? proj - afterEnv : null;

  // One scale shared by the waterfall and the outcome range: both describe this
  // player in DK points, so sharing an axis makes the spread directly readable
  // against the steps. The peer strip gets its own scale -- folding the slate's
  // top scorer into this one would shrink this player's own bars for no gain,
  // chart carries its own labelled axis anyway.
  const scaleMax = Math.max(proj, baseline ?? 0, afterEnv ?? 0, e.ceiling ?? 0, 1) * 1.06;
  const peerScale = Math.max(peers[0]?.proj ?? 0, proj, 1) * 1.06;

  const salary = player.salary || null;
  const value = salary ? (proj / salary) * 1000 : null;
  const peerRank = peers.findIndex((p) => p.id === player.id);

  /* -- Linear DK points by stat ---------------------------------------
   * `scoring.ts` is explicit that E[score(stats)] != score(E[stats]): the
   * three yardage bonuses are step functions. So this chart is labelled as
   * the LINEAR part only, and the gap to the simulated projection is drawn
   * as its own row rather than silently absorbed into the bars. */
  const contributions = Object.entries(e.statMeans)
    .map(([key, mean]) => ({ key, mean, ...spec(key) }))
    .filter((r) => r.pts != null)
    .map((r) => ({ ...r, points: r.mean * (r.pts as number) }))
    .filter((r) => Math.abs(r.points) >= 0.05)
    .sort((a, b) => Math.abs(b.points) - Math.abs(a.points));
  const linearTotal = contributions.reduce((sum, r) => sum + r.points, 0);
  // Only offensive skill positions are covered by STAT_SPECS; for K/DST the
  // linear subtotal would be ~0 and the chart would claim nothing.
  const showContributions = contributions.length > 0
    && ["QB", "RB", "WR", "TE"].includes(player.position);
  const contribMax = Math.max(...contributions.map((r) => Math.abs(r.points)), 1);
  const bonusGap = proj - linearTotal;

  const sources = ([
    { label: "Our model", value: proj, accent: true },
    { label: "DK average", value: player.avgFptsDk },
    { label: "FantasyPros", value: player.fantasyprosProj },
    { label: "LineStar", value: player.linestarProj },
  ].filter((s) => s.value != null)) as { label: string; value: number; accent?: boolean }[];
  const sourceMax = Math.max(...sources.map((s) => s.value), 1) * 1.06;

  const statGroups = GROUP_ORDER
    .map((group) => ({
      group,
      rows: Object.entries(e.statMeans)
        .map(([key, mean]) => ({ key, mean, ...spec(key) }))
        .filter((r) => r.group === group)
        .sort((a, b) => b.mean - a.mean),
    }))
    .filter((g) => g.rows.length > 0);

  return (
    <>
      {/* Hero figure plus its supporting tiles. The number is the chart here. */}
      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="text-[10px] font-bold uppercase tracking-wide text-slate-500">Projection</div>
        <div className="mt-0.5 flex items-baseline gap-2">
          <span className="text-5xl font-black leading-none tracking-tight text-slate-900">{fmt(proj)}</span>
          <span className="text-sm font-bold text-slate-400">DK pts</span>
        </div>
        <p className="mt-1.5 text-[11px] text-slate-500">
          Mean of {e.draws ? e.draws.toLocaleString() : "—"} simulated games &middot;{" "}
          {e.status.replace(/_/g, " ")} &middot; model confidence {pct(e.confidence)}
        </p>
        <div className="mt-3 grid grid-cols-3 gap-2">
          <Tile label="Value" value={value == null ? "—" : fmt(value, 2)} sub="pts per $1K" />
          <Tile
            label={`${player.position} rank`}
            value={peerRank >= 0 ? `#${peerRank + 1}` : "—"}
            sub={peerRank >= 0 ? `of ${peers.length} on slate` : "not ranked"}
          />
          <Tile label="Boom rate" value={pct(e.boomRate)} sub="sims with a big game" />
        </div>
      </section>

      {e.availabilityNote && (
        <div className="flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-900">
          <AlertTriangle className="h-4 w-4 shrink-0" />{e.availabilityNote}
        </div>
      )}

      {/* Waterfall */}
      <Card
        icon={<TrendingUp className="h-4 w-4" style={{ color: ACCENT }} />}
        title="How the number is built"
        hint="Each step starts where the last one finished. All four bars share one scale."
      >
        {baseline == null ? (
          <p className="text-xs text-slate-500">
            This run stored no historical baseline, so the steps cannot be shown.
          </p>
        ) : (
          <>
            <div className="space-y-1">
              <Row label="Historical baseline" sub={`${e.historyGames ?? 0} recent games`} value={fmt(baseline)}>
                <Grid />
                <Bar to={baseline} max={scaleMax} color={MUTED}
                     title={`Recency-weighted baseline ${fmt(baseline)} DK pts`} />
              </Row>

              {envDelta != null && afterEnv != null && (
                <Row label="Vegas environment" sub={factor == null ? undefined : `×${fmt(factor, 3)}`} value={signed(envDelta)}>
                  <Grid />
                  {/* A step that moves nothing gets no mark. Painting a zero-width
                      bar in the "raises it" colour would claim a lift that is not there. */}
                  {Math.abs(envDelta) >= 0.05 && (
                    <Bar from={baseline} to={afterEnv} max={scaleMax} color={envDelta > 0 ? UP : DOWN}
                         title={`Team total ${fmt(e.teamImpliedTotal)} pts scales the baseline by x${fmt(factor, 3)} (${signed(envDelta)} DK pts)`} />
                  )}
                </Row>
              )}

              {simDelta != null && afterEnv != null && (
                <Row label="Simulation &amp; DK scoring" sub="bonuses, variance, availability" value={signed(simDelta)}>
                  <Grid />
                  {Math.abs(simDelta) >= 0.05 && (
                    <Bar from={afterEnv} to={proj} max={scaleMax} color={simDelta > 0 ? UP : DOWN}
                         title={`Re-scoring the simulated draws under DK rules moves the mean ${signed(simDelta)} DK pts`} />
                  )}
                </Row>
              )}

              <div className="my-1 border-t border-slate-200" />

              <Row label={<span className="font-bold text-slate-900">Projection</span>} value={fmt(proj)}>
                <Grid />
                <Bar to={proj} max={scaleMax} color={ACCENT} title={`Projection ${fmt(proj)} DK pts`} />
              </Row>
            </div>

            <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 border-t border-slate-100 pt-2">
              <Key color={MUTED}>Starting point</Key>
              <Key color={UP}>Raises it</Key>
              <Key color={DOWN}>Lowers it</Key>
              <Key color={ACCENT}>Result</Key>
            </div>
          </>
        )}
      </Card>

      {/* Outcome range, on the same scale as the waterfall */}
      <Card
        icon={<Dice5 className="h-4 w-4" style={{ color: ACCENT }} />}
        title="Outcome range"
        hint="P10 to P90 of the simulated games, drawn on the same scale as the steps above."
      >
        {e.floor == null || e.ceiling == null ? (
          <p className="text-xs text-slate-500">This run stored no simulated distribution.</p>
        ) : (
          <>
            <div className="relative h-8">
              <Grid />
              {/* The P10-P90 band: a wash, never a saturated block. */}
              <div
                className="absolute top-1/2 h-4 -translate-y-1/2 rounded"
                style={{
                  left: `${(e.floor / scaleMax) * 100}%`,
                  width: `${Math.max(((e.ceiling - e.floor) / scaleMax) * 100, 0.6)}%`,
                  backgroundColor: ACCENT, opacity: 0.16,
                }}
                title={`Middle 80% of simulated games: ${fmt(e.floor)} to ${fmt(e.ceiling)} DK pts`}
              />
              {e.median != null && (
                <div
                  className="absolute top-1/2 h-4 w-0.5 -translate-y-1/2"
                  style={{ left: `${(e.median / scaleMax) * 100}%`, backgroundColor: ACCENT }}
                  title={`Median simulated game ${fmt(e.median)} DK pts`}
                />
              )}
              {/* Mean marker: >=8px with a 2px surface ring, so it stays legible
                  where it overlaps the median line. */}
              <div
                className="absolute top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full ring-2 ring-white"
                style={{ left: `${(proj / scaleMax) * 100}%`, backgroundColor: ACCENT }}
                title={`Mean, which is the projection: ${fmt(proj)} DK pts`}
              />
            </div>
            <div className="mt-1 grid grid-cols-3 text-center">
              <div>
                <div className="text-[10px] uppercase text-slate-500">Floor P10</div>
                <div className="text-sm font-black text-slate-900">{fmt(e.floor)}</div>
              </div>
              <div>
                <div className="text-[10px] uppercase text-slate-500">Median P50</div>
                <div className="text-sm font-black text-slate-900">{fmt(e.median)}</div>
              </div>
              <div>
                <div className="text-[10px] uppercase text-slate-500">Ceiling P90</div>
                <div className="text-sm font-black text-slate-900">{fmt(e.ceiling)}</div>
              </div>
            </div>
            {e.boomRate != null && (
              <div className="mt-3">
                <div className="flex items-baseline justify-between text-[11px]">
                  <span className="text-slate-600">Boom rate</span>
                  <span className="font-bold tabular-nums text-slate-900">{pct(e.boomRate)}</span>
                </div>
                <div className="mt-1">
                  <Meter value={e.boomRate}
                         title={`${pct(e.boomRate)} of simulated games cleared a big game for a ${player.position}`} />
                </div>
              </div>
            )}
          </>
        )}
      </Card>

      {/* Where this player sits among the same-position players on this slate */}
      {peers.length > 1 && peerRank >= 0 && (
        <Card
          icon={<Users className="h-4 w-4" style={{ color: ACCENT }} />}
          title={`Against the other ${player.position}s on this slate`}
          hint={`Every projected ${player.position} on the slate. The highlighted mark is ${player.name}.`}
        >
          <div className="relative h-7">
            <Grid />
            {peers.map((p) => {
              const isHim = p.id === player.id;
              return (
                <div
                  key={p.id}
                  title={`${p.name}: ${fmt(p.proj)} DK pts`}
                  className={`absolute top-1/2 -translate-x-1/2 -translate-y-1/2 rounded-full ring-2 ring-white ${isHim ? "z-10 h-3 w-3" : "h-2 w-2"}`}
                  style={{ left: `${(p.proj / peerScale) * 100}%`, backgroundColor: isHim ? ACCENT : MUTED }}
                />
              );
            })}
          </div>
          <div className="flex justify-between text-[10px] text-slate-400">
            <span>0</span><span>{fmt(peerScale, 0)} DK pts</span>
          </div>
          <p className="mt-2 text-[11px] text-slate-600">
            <b className="text-slate-900">#{peerRank + 1} of {peers.length}</b> &mdash; above{" "}
            {pct(peers.length > 1 ? (peers.length - 1 - peerRank) / (peers.length - 1) : 0)} of the
            projected {player.position}s here.
          </p>
          <div className="mt-2 flex gap-3">
            <Key color={ACCENT}>{player.name}</Key>
            <Key color={MUTED}>Other {player.position}s</Key>
          </div>
        </Card>
      )}

      {/* Environment detail. Polarity is carried by the centre line, not by a
          red/green pair -- emerald x orange fails the CVD separation gate. */}
      <Card
        icon={<Coins className="h-4 w-4" style={{ color: ACCENT }} />}
        title="This week's environment"
        hint={<>
          Vegas team total <b className="text-slate-700">{fmt(e.teamImpliedTotal)}</b> pts.
          Bars run from the &times;1.00 line: right of it lifts production, left of it cuts it.
        </>}
      >
        {[
          { label: "Yardage", f: e.yardageFactor },
          { label: "Touchdowns", f: e.touchdownFactor },
          { label: "Combined", f: e.environmentFactor },
        ].filter((r) => r.f != null).map((r) => {
          const f = r.f as number;
          // Centre at 1.00; +/-25% spans the half-width, clamped so an extreme
          // factor still renders inside the plot rather than overflowing it.
          const off = Math.max(-1, Math.min(1, (f - 1) / 0.25)) * 50;
          return (
            <Row key={r.label} label={r.label} value={`×${fmt(f, 3)}`}>
              <div aria-hidden className="pointer-events-none absolute inset-0">
                <div className="absolute left-1/2 top-0 h-full w-px bg-slate-300" />
              </div>
              <div
                title={`${r.label} production is multiplied by ${fmt(f, 3)} this week`}
                className="absolute top-1/2 h-3.5 -translate-y-1/2 rounded"
                style={{
                  left: `${off >= 0 ? 50 : 50 + off}%`,
                  width: `${Math.max(Math.abs(off), 0.5)}%`,
                  backgroundColor: ACCENT,
                }}
              />
            </Row>
          );
        })}
        <div className="mt-1 grid grid-cols-[6.5rem_1fr_3.5rem] gap-2">
          <span />
          <div className="flex justify-between text-[10px] text-slate-400">
            <span>&times;0.75</span><span>&times;1.00</span><span>&times;1.25</span>
          </div>
          <span />
        </div>
      </Card>

      {/* Trust: this player's own history against position peers */}
      <Card icon={<Gauge className="h-4 w-4" style={{ color: ACCENT }} />} title="How much we trust this player's own history">
        <Meter value={e.playerWeight ?? 0}
               title={`${pct(e.playerWeight)} of simulated games were drawn from this player's own history`} />
        <p className="mt-2 text-[11px] leading-snug text-slate-600">
          <b className="text-slate-900">{pct(e.playerWeight)}</b> of simulations drew from this player&rsquo;s own{" "}
          {e.historyGames ?? 0} games; the rest from {e.priorGames ?? 0} position-peer games, through{" "}
          {e.cutoffSeason ?? "—"} wk{(e.cutoffWeek ?? 1) - 1}.
          {e.status === "position_prior" ? " Too few games of their own — this is mostly a position prior." : ""}
        </p>
      </Card>

      {/* Linear DK points by stat */}
      {showContributions && (
        <Card
          icon={<Calculator className="h-4 w-4" style={{ color: ACCENT }} />}
          title="Where the points come from"
          hint={<>
            The <b className="text-slate-700">linear</b> DK points in the mean stat line. The three
            yardage bonuses ({PASS_YARD_BONUS_THRESHOLD}+ passing, {RUSH_YARD_BONUS_THRESHOLD}+
            rushing or receiving, {YARDAGE_BONUS_PTS} pts) are threshold events, so they cannot be
            read off a mean &mdash; the simulation catches them, and they land in the second-last row.
          </>}
        >
          <div className="space-y-1">
            {contributions.map((r) => (
              <Row key={r.key} label={r.label} sub={`${fmt(r.mean, 1)} × ${r.pts}`} value={signed(r.points)}>
                <div aria-hidden className="pointer-events-none absolute inset-0">
                  <div className="absolute left-0 top-0 h-full w-px bg-slate-200" />
                </div>
                <div
                  title={`${fmt(r.mean, 1)} x ${r.pts} DK pts = ${signed(r.points)}`}
                  className="absolute top-1/2 h-3.5 -translate-y-1/2"
                  style={{
                    left: 0,
                    width: `${Math.max((Math.abs(r.points) / contribMax) * 100, 0.6)}%`,
                    backgroundColor: r.points >= 0 ? ACCENT : DOWN,
                    borderRadius: "0 4px 4px 0",
                  }}
                />
              </Row>
            ))}
            <div className="my-1 border-t border-slate-200" />
            <Row label="Linear subtotal" value={fmt(linearTotal)}><span /></Row>
            <Row label="Bonuses &amp; distribution" sub="threshold events" value={signed(bonusGap)}><span /></Row>
            <Row label={<span className="font-bold text-slate-900">Projection</span>} value={fmt(proj)}><span /></Row>
          </div>
          <div className="mt-2 flex gap-3 border-t border-slate-100 pt-2">
            <Key color={ACCENT}>Adds points</Key><Key color={DOWN}>Costs points</Key>
          </div>
        </Card>
      )}

      {/* Our model against the other numbers on the slate */}
      {sources.length > 1 && (
        <Card
          icon={<TrendingUp className="h-4 w-4" style={{ color: ACCENT }} />}
          title="Against the other projections"
          hint="Comparison only. None of these sources feeds our number."
        >
          <div className="space-y-1">
            {sources.map((s) => (
              <Row key={s.label} label={s.label} value={fmt(s.value)}>
                <Grid />
                <Bar to={s.value} max={sourceMax} color={s.accent ? ACCENT : MUTED}
                     title={`${s.label}: ${fmt(s.value)} DK pts${s.accent ? "" : ` (${signed(proj - s.value)} vs ours)`}`} />
              </Row>
            ))}
          </div>
          <div className="mt-2 flex gap-3 border-t border-slate-100 pt-2">
            <Key color={ACCENT}>Our model</Key><Key color={MUTED}>Other sources</Key>
          </div>
          {player.linestarOwnPct != null && (
            <p className="mt-2 text-[11px] text-slate-600">
              LineStar projected ownership <b className="text-slate-900">{player.linestarOwnPct.toFixed(1)}%</b>.
            </p>
          )}
        </Card>
      )}

      {/* Projected stat line. Each group carries its own scale: the units are
          not comparable across groups, so one shared scale would mislead. */}
      {statGroups.length > 0 && (
        <Card
          icon={<Activity className="h-4 w-4" style={{ color: ACCENT }} />}
          title="Projected stat line"
          hint="Per-game means. Bars are scaled within each group; units differ across groups."
        >
          <div className="space-y-3">
            {statGroups.map(({ group, rows }) => {
              const groupMax = Math.max(...rows.map((r) => Math.abs(r.mean)), 0.01);
              return (
                <div key={group}>
                  <div className="text-[10px] font-bold uppercase tracking-wide text-slate-400">{group}</div>
                  <div className="mt-1 space-y-1">
                    {rows.map((r) => (
                      <Row key={r.key} label={r.label} value={fmt(r.mean, 1)}>
                        <div
                          title={`${r.label}: ${fmt(r.mean, 1)} per game`}
                          className="absolute top-1/2 h-3.5 -translate-y-1/2"
                          style={{
                            left: 0,
                            width: `${Math.max((Math.abs(r.mean) / groupMax) * 100, 0.6)}%`,
                            backgroundColor: MUTED, borderRadius: "0 4px 4px 0",
                          }}
                        />
                      </Row>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </Card>
      )}

      <p className="px-1 pb-2 text-[10px] leading-snug text-slate-400">
        Descriptive historical + Vegas-environment baseline. Props are a separate overlay and are
        not shown here. Not a betting claim.
      </p>
    </>
  );
}
