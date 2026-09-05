"use client";

import { useId, useState } from "react";
import type { MovementInsight } from "@/lib/movement-intelligence";
import s from "./movement-intelligence.module.css";

function stamp(at: number) {
  return new Intl.DateTimeFormat("en-US", { hour: "numeric", minute: "2-digit", timeZone: "America/New_York" }).format(at) + " ET";
}
function value(v: number, market: MovementInsight["market"]) {
  return market === "moneyline" ? `${(v * 100).toFixed(1)}%` : `${v > 0 && market === "spread" ? "+" : ""}${v.toFixed(1)}`;
}
function Sparkline({ item }: { item: MovementInsight }) {
  const points = item.trail;
  if (points.length < 2) return <span className={s.noTrail}>Comparable trail unavailable</span>;
  const first = points[0], last = points[points.length - 1];
  const lo = Math.min(...points.map(p => p.value)), hi = Math.max(...points.map(p => p.value));
  const x = (time: number) => 4 + (time - first.time) / (last.time - first.time || 1) * 232;
  const y = (n: number) => hi === lo ? 22 : 38 - (n - lo) / (hi - lo) * 32;
  return <span className={s.sparkWrap}>
    <span className={s.trailLabel}>{item.trailLabel} · {value(first.value, item.market)} → {value(last.value, item.market)}</span>
    <svg viewBox="0 0 240 44" preserveAspectRatio="none" className={s.spark} role="img" aria-label={`${item.trailLabel}, ${value(first.value, item.market)} to ${value(last.value, item.market)}; ${stamp(first.time)} to ${stamp(last.time)}. Independently scaled; gaps over 30 minutes dashed.`}>
      {points.slice(1).map((p, i) => <line key={`${p.time}-${i}`} x1={x(points[i].time)} y1={y(points[i].value)} x2={x(p.time)} y2={y(p.value)} stroke="currentColor" strokeWidth="1.8" vectorEffect="non-scaling-stroke" strokeDasharray={p.time - points[i].time > 30 * 60_000 ? "4 4" : undefined} />)}
      <circle cx={x(last.time)} cy={y(last.value)} r="2.5" fill="currentColor" />
    </svg>
  </span>;
}

export default function MovementIntelligence({ items, selectedKey, onSelect, loading = false }: {
  items: MovementInsight[]; selectedKey: string; onSelect: (item: MovementInsight) => void; loading?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  const shown = expanded ? items : items.slice(0, 4);
  return <section className={s.strip} aria-label="Movement intelligence">
    <div className={s.heading}><h2>MOVEMENT INTELLIGENCE</h2><span>{items.length} SIGNAL{items.length === 1 ? "" : "S"} · RECENT PREMATCH</span>
      {items.length > 4 && <button type="button" aria-expanded={expanded} aria-controls={listId} onClick={() => setExpanded(v => !v)}>{expanded ? "SHOW TOP 4" : `VIEW ALL · ${items.length}`}</button>}
    </div>
    <p className={s.policy}>Recorded in the last 30m · ordered by recorded book support, then recency · research, not validated picks</p>
    <div className={s.cards} id={listId}>
      {shown.map((item, index) => <button type="button" key={item.key} className={s.card} aria-pressed={item.key === selectedKey} onClick={() => onSelect(item)}>
        <span className={s.cardTop}><span>{String(index + 1).padStart(2, "0")} / {item.label}</span><span>{item.market.toUpperCase()}</span></span>
        <strong className={s.fixture}>{item.fixture}</strong>
        <span className={s.direction}>{item.label === "MIXED DIRECTION" ? "OPPOSING SIGNALS" : `TOWARD ${item.selection}`} <span>{item.metric}</span></span>
        <Sparkline item={item} />
        <span className={s.explanation}>{item.explanation}</span>
        {item.types.length > 1 && <span className={s.related}>ALSO RECORDED · {item.types.filter(t => t !== item.label).join(" / ")}</span>}
        <span className={s.cardFoot}><span>{item.supportLabel}</span><span>OBS {stamp(item.observedAt)} →</span></span>
      </button>)}
    </div>
    {!items.length && <p className={s.empty}>{loading ? "Checking observation times…" : "No recent qualifying movement signals for upcoming games in this view. Earlier observations remain in the signal tape."}</p>}
  </section>;
}
