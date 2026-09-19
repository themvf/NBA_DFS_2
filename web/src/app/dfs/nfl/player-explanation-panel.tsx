"use client";

import { useEffect, useState, useTransition } from "react";
import { X, Activity, TrendingUp, Dice5, Gauge, AlertTriangle } from "lucide-react";
import { explainNflPlayerProjection, type NflProjectionExplanation, type NflWorkspacePlayer } from "./actions";

const fmt = (v: number | null | undefined, digits = 1) =>
  v === null || v === undefined ? "—" : v.toFixed(digits);
const pct = (v: number | null | undefined) =>
  v === null || v === undefined ? "—" : `${Math.round(v * 100)}%`;

// Human labels for the raw stat_means keys.
const STAT_LABELS: Record<string, string> = {
  attempts: "Pass attempts", passing_yards: "Passing yards", passing_tds: "Passing TDs",
  passing_interceptions: "Interceptions", passing_2pt_conversions: "2-pt (pass)",
  carries: "Carries", rushing_yards: "Rushing yards", rushing_tds: "Rushing TDs",
  receptions: "Receptions", receiving_yards: "Receiving yards", receiving_tds: "Receiving TDs",
  targets: "Targets", fumbles_lost_total: "Fumbles lost",
};
const statLabel = (k: string) => STAT_LABELS[k] ?? k.replace(/_/g, " ");

type Props = { uploadId: string; player: NflWorkspacePlayer | null; onClose: () => void };

export default function PlayerExplanationPanel({ uploadId, player, onClose }: Props) {
  // Keyed by the player id we last fetched for, so we can show a stale-free view
  // without synchronously resetting state inside the effect.
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

  // Only show data that matches the currently-open player.
  const data = player && state.id === player.id ? state.data : null;

  // Close on Escape.
  useEffect(() => {
    if (!player) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [player, onClose]);

  if (!player) return null;
  const e = data && data.ok ? data : null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end" role="dialog" aria-modal="true"
         aria-label={`Why ${player.name} is projected as he is`}>
      <div className="absolute inset-0 bg-slate-900/40" onClick={onClose} />
      <div className="relative flex h-full w-full max-w-md flex-col overflow-y-auto bg-white shadow-xl">
        {/* Header */}
        <div className="sticky top-0 flex items-start justify-between border-b bg-white p-5">
          <div>
            <h2 className="text-lg font-black">{player.name}</h2>
            <p className="text-xs text-slate-500">
              {player.position} · {player.team}{player.opponent ? ` vs ${player.opponent}` : ""}
              {player.salary ? ` · $${player.salary.toLocaleString()}` : ""}
            </p>
          </div>
          <button onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-400 hover:bg-slate-100">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="space-y-4 p-5">
          {pending && <p className="text-sm text-slate-500">Loading the projection breakdown…</p>}

          {data && !data.ok && (
            <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
              <AlertTriangle className="h-4 w-4 shrink-0" />{data.error}
            </div>
          )}

          {e && (
            <>
              {/* Headline */}
              <section className="rounded-xl border bg-white p-4 shadow-sm">
                <div className="text-[10px] font-bold uppercase text-slate-500">Projection</div>
                <div className="mt-1 text-3xl font-black">{fmt(e.projection)} <span className="text-base font-bold text-slate-400">DK pts</span></div>
                <p className="mt-1 text-xs text-slate-500">
                  Mean of {e.draws ? e.draws.toLocaleString() : "—"} simulated games · confidence {pct(e.confidence)} · {e.status.replace(/_/g, " ")}
                </p>
              </section>

              {e.availabilityNote && (
                <div className="flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-900">
                  <AlertTriangle className="h-4 w-4 shrink-0" />{e.availabilityNote}
                </div>
              )}

              {/* How it's built: baseline -> environment */}
              <section className="rounded-xl border bg-white p-4 shadow-sm">
                <h3 className="flex items-center gap-2 font-bold"><TrendingUp className="h-4 w-4 text-emerald-600" />How the number is built</h3>
                <ol className="mt-3 space-y-3 text-sm">
                  <li>
                    <div className="flex justify-between"><span className="font-semibold">1. Historical baseline</span><b>{fmt(e.baseline)}</b></div>
                    <p className="text-xs text-slate-500">Recency-weighted average of his {e.historyGames ?? 0} most-recent games (through {e.cutoffSeason} wk{(e.cutoffWeek ?? 1) - 1}). Newer games count more.</p>
                  </li>
                  <li>
                    <div className="flex justify-between"><span className="font-semibold">2. This week&rsquo;s environment</span><b>×{fmt(e.environmentFactor, 3)}</b></div>
                    <p className="text-xs text-slate-500">
                      Vegas team total {fmt(e.teamImpliedTotal)} pts scales production
                      {e.environmentFactor && e.environmentFactor > 1 ? " up" : e.environmentFactor && e.environmentFactor < 1 ? " down" : ""}.
                      Yards ×{fmt(e.yardageFactor, 3)}, TDs ×{fmt(e.touchdownFactor, 3)}.
                    </p>
                  </li>
                  <li>
                    <div className="flex justify-between"><span className="font-semibold">3. Simulated & DK-scored</span><b>{fmt(e.projection)}</b></div>
                    <p className="text-xs text-slate-500">Each simulated game is scored with real DraftKings rules; the mean is the projection above.</p>
                  </li>
                </ol>
              </section>

              {/* Player weight / trust */}
              <section className="rounded-xl border bg-white p-4 shadow-sm">
                <h3 className="flex items-center gap-2 font-bold"><Gauge className="h-4 w-4 text-blue-600" />How much we trust his own history</h3>
                <div className="mt-3 h-2 rounded bg-slate-100">
                  <div className="h-full rounded bg-blue-600" style={{ width: `${Math.round((e.playerWeight ?? 0) * 100)}%` }} />
                </div>
                <p className="mt-2 text-xs text-slate-500">
                  {pct(e.playerWeight)} of simulations drew from <b>his own</b> {e.historyGames ?? 0} games; the rest from {e.priorGames ?? 0} position-peer games.
                  {e.status === "position_prior" ? " Too few of his own games — leaning on position peers." : ""}
                </p>
              </section>

              {/* Distribution */}
              <section className="rounded-xl border bg-white p-4 shadow-sm">
                <h3 className="flex items-center gap-2 font-bold"><Dice5 className="h-4 w-4 text-purple-600" />Outcome range (from the sims)</h3>
                <div className="mt-3 grid grid-cols-3 gap-2 text-center">
                  <div><div className="text-[10px] font-bold uppercase text-slate-500">Floor P10</div><div className="text-lg font-black">{fmt(e.floor)}</div></div>
                  <div><div className="text-[10px] font-bold uppercase text-slate-500">Median P50</div><div className="text-lg font-black">{fmt(e.median)}</div></div>
                  <div><div className="text-[10px] font-bold uppercase text-slate-500">Ceiling P90</div><div className="text-lg font-black">{fmt(e.ceiling)}</div></div>
                </div>
                <p className="mt-2 text-xs text-slate-500">Boom rate {pct(e.boomRate)} — how often the sims cleared a big game for the position.</p>
              </section>

              {/* Projected stat line */}
              {Object.keys(e.statMeans).length > 0 && (
                <section className="rounded-xl border bg-white p-4 shadow-sm">
                  <h3 className="flex items-center gap-2 font-bold"><Activity className="h-4 w-4 text-slate-600" />Projected stat line (per-game mean)</h3>
                  <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                    {Object.entries(e.statMeans).sort((a, b) => b[1] - a[1]).map(([k, v]) => (
                      <div key={k} className="flex justify-between border-b border-slate-100 py-0.5">
                        <dt className="text-slate-600">{statLabel(k)}</dt><dd className="font-semibold">{fmt(v, 1)}</dd>
                      </div>
                    ))}
                  </dl>
                </section>
              )}

              <p className="text-[10px] text-slate-400">
                Descriptive historical + environment baseline (props are a separate overlay, not shown here). Not a betting claim.
              </p>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
