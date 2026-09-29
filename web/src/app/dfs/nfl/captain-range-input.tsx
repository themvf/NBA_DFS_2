"use client";

import type { CaptainTarget } from "@/lib/nfl-dfs/generation-settings";

/**
 * Showdown captain range for one player: minimum and maximum share of lineups
 * in which he is the 1.5x Captain. Blank means no bound.
 *
 * Without this, the captain slot was decided by the optimizer's ceiling
 * objective alone -- on the Thursday ATL@GB slate that put Tucker Kraft at
 * captain in 52-60% of lineups and the chalk captain, Bijan Robinson, in
 * 15-20%. The overall cap is preserved when a range is set; see
 * `captainExposurePolicies`.
 */
export default function CaptainRangeInput({ name, value, nLineups, disabled, onChange }: {
  name: string;
  value: CaptainTarget | undefined;
  nLineups: number;
  disabled: boolean;
  onChange: (next: CaptainTarget) => void;
}) {
  const parse = (raw: string) => (raw === "" ? null : Math.max(0, Math.min(100, Number(raw))));
  const min = value?.min ?? null, max = value?.max ?? null;
  const invalid = min != null && max != null && min > max;
  const count = (pct: number | null) => (pct == null ? null : Math.round((pct / 100) * nLineups));
  return <div className="mt-1">
    <div className="inline-flex items-center gap-0.5 text-[10px]">
      <span className="font-bold text-violet-800">CPT</span>
      <input aria-label={`${name} captain minimum percentage`} disabled={disabled} type="number" min={0} max={100} step={5}
        placeholder="min" value={min ?? ""} onChange={(e) => onChange({ min: parse(e.target.value), max })}
        className={`h-7 w-11 rounded border px-1 text-right disabled:bg-slate-100 ${invalid ? "border-red-500" : ""}`} />
      <span className="text-slate-400">–</span>
      <input aria-label={`${name} captain maximum percentage`} disabled={disabled} type="number" min={0} max={100} step={5}
        placeholder="max" value={max ?? ""} onChange={(e) => onChange({ min, max: parse(e.target.value) })}
        className={`h-7 w-11 rounded border px-1 text-right disabled:bg-slate-100 ${invalid ? "border-red-500" : ""}`} />
      <span className="text-slate-500">%</span>
    </div>
    {min != null || max != null ? <div className={`text-[9px] font-bold ${invalid ? "text-red-700" : "text-violet-700"}`}>
      {invalid ? "min exceeds max" : `CPT ${count(min) ?? 0}–${count(max) ?? nLineups} of ${nLineups}`}
    </div> : null}
  </div>;
}

/**
 * A player's overall exposure: minimum and maximum share of lineups, captain
 * and flex combined. A max alone is a cap and is always honoured exactly, in
 * every plan (decided 2026-09-28); the old single box could only express an
 * exact target, so "cap Swift at 70%" had no way in. Blank means automatic.
 */
export function ExposureRangeInput({ label, name, value, nLineups, disabled, onChange }: {
  label: string;
  name: string;
  value: CaptainTarget | undefined;
  nLineups: number;
  disabled: boolean;
  onChange: (next: CaptainTarget) => void;
}) {
  const parse = (raw: string) => (raw === "" ? null : Math.max(0, Math.min(100, Number(raw))));
  const min = value?.min ?? null, max = value?.max ?? null;
  const invalid = min != null && max != null && min > max;
  const lo = min == null ? null : Math.ceil((min / 100) * nLineups - 1e-9);
  const hi = max == null ? null : Math.floor((max / 100) * nLineups + 1e-9);
  const summary = invalid ? "min exceeds max"
    : min != null && max != null && min === max ? `exactly ${Math.round((min / 100) * nLineups)} of ${nLineups}`
    : lo != null && hi != null ? `${lo}–${hi} of ${nLineups}`
    : hi != null ? `at most ${hi} of ${nLineups}`
    : lo != null ? `at least ${lo} of ${nLineups}` : null;
  return <div>
    <div className="inline-flex items-center gap-0.5 text-[10px]">
      <span className="font-bold text-slate-700">{label}</span>
      <input aria-label={`${name} exposure minimum percentage`} disabled={disabled} type="number" min={0} max={100} step={5}
        placeholder="min" value={min ?? ""} onChange={(e) => onChange({ min: parse(e.target.value), max })}
        className={`h-7 w-11 rounded border px-1 text-right disabled:bg-slate-100 ${invalid ? "border-red-500" : ""}`} />
      <span className="text-slate-400">–</span>
      <input aria-label={`${name} exposure maximum percentage`} disabled={disabled} type="number" min={0} max={100} step={5}
        placeholder="max" value={max ?? ""} onChange={(e) => onChange({ min, max: parse(e.target.value) })}
        className={`h-7 w-11 rounded border px-1 text-right disabled:bg-slate-100 ${invalid ? "border-red-500" : ""}`} />
      <span className="text-slate-500">%</span>
    </div>
    {summary ? <div className={`text-[9px] font-bold ${invalid ? "text-red-700" : "text-blue-700"}`}>{summary}</div> : null}
  </div>;
}
