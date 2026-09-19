"use client";

import { ChevronDown, ChevronUp, ChevronsUp, HelpCircle, Minus } from "lucide-react";
import {
  ABSOLUTE_VALUE_FLOOR, VALUE_TIER_LABEL, rosterPointsAtMultiple,
  type ValueAssessment, type ValueTier,
} from "@/lib/nfl-dfs/salary-value";

/*
 * Value is a POLARITY scale -- good value through neutral to overpriced -- so
 * it is drawn as a diverging pair with a neutral midpoint: blue for value,
 * slate for mid-pack, orange for overpriced. Blue x orange clears every
 * colourblind gate (deutan/protan/tritan dE 31+); the emerald x orange pair a
 * red/green instinct reaches for first does NOT (dE 5.8), which is why it is
 * not used here even though "green = good" is the obvious choice.
 *
 * The two good tiers are two steps of one hue rather than two hues, because
 * elite and strong are ORDERED, not different kinds of thing. Every chip also
 * carries an ordinal icon and the number itself, so the tier never rests on
 * colour alone.
 *
 * `unproven` is deliberately not a colour at all -- it is an unfilled, dashed
 * chip. It is not a point on the good/bad scale; it is the absence of a claim,
 * and a hue would imply we had made one.
 */
const TIER_STYLE: Record<ValueTier, { className: string; Icon: typeof ChevronsUp | null }> = {
  elite: { className: "bg-blue-600 text-white ring-blue-600", Icon: ChevronsUp },
  strong: { className: "bg-blue-100 text-blue-900 ring-blue-200", Icon: ChevronUp },
  fair: { className: "bg-transparent text-slate-600 ring-transparent", Icon: null },
  poor: { className: "bg-orange-50 text-orange-800 ring-orange-200", Icon: ChevronDown },
  unproven: { className: "border border-dashed border-slate-300 bg-transparent text-slate-400 ring-transparent", Icon: HelpCircle },
  unknown: { className: "bg-transparent text-slate-300 ring-transparent", Icon: null },
};

const fmtX = (v: number | null) => (v === null ? "—" : `${v.toFixed(2)}×`);

/** The full sentence a hover should give, assembled once so both surfaces agree. */
export function valueTooltip(a: ValueAssessment, position: string): string {
  if (a.multiple === null) return a.reason;
  const parts = [
    `${fmtX(a.multiple)} = ${VALUE_TIER_LABEL[a.tier]}.`,
    a.reason,
    `A whole roster at this rate would score ${rosterPointsAtMultiple(a.multiple).toFixed(0)} from the $50,000 cap.`,
  ];
  if (a.ceilingMultiple !== null) parts.push(`Ceiling ${fmtX(a.ceilingMultiple)}.`);
  if (a.positionP90 !== null) {
    parts.push(`Top-10% bar for a ${position} on this slate is ${fmtX(a.positionP90)}; median ${fmtX(a.positionMedian)}.`);
  }
  return parts.join(" ");
}

/** Compact chip for a table row: icon + the multiple. */
export function ValueChip({ assessment, position }: { assessment: ValueAssessment; position: string }) {
  const { className, Icon } = TIER_STYLE[assessment.tier];
  return (
    <span
      title={valueTooltip(assessment, position)}
      className={`inline-flex items-center gap-0.5 rounded px-1.5 py-0.5 text-[11px] font-bold tabular-nums ring-1 ${className}`}
    >
      {Icon ? <Icon className="h-3 w-3 shrink-0" aria-hidden /> : null}
      {fmtX(assessment.multiple)}
      <span className="sr-only"> — {VALUE_TIER_LABEL[assessment.tier]}</span>
    </span>
  );
}

/** One-line key for the pool header. Names the per-position rule explicitly. */
export function ValueLegend({ className = "" }: { className?: string }) {
  return (
    <p className={`text-[11px] leading-snug text-slate-500 ${className}`}>
      <b className="text-slate-700">Value</b> is projected DK points per $1,000 of salary.
      Tiers are graded against <b className="text-slate-700">the same position on this slate</b>, not a flat
      number: quarterbacks return far more per dollar than running backs, so one bar would just rank positions.
      A tier also needs {ABSOLUTE_VALUE_FLOOR.toFixed(1)}× in absolute terms &mdash; a whole roster at
      that rate scores only {rosterPointsAtMultiple(ABSOLUTE_VALUE_FLOOR).toFixed(0)} from the cap.
      {" "}
      <span className="whitespace-nowrap"><ChevronsUp className="inline h-3 w-3" aria-hidden /> top 10%</span>{" · "}
      <span className="whitespace-nowrap"><ChevronUp className="inline h-3 w-3" aria-hidden /> top 25%</span>{" · "}
      <span className="whitespace-nowrap"><Minus className="inline h-3 w-3" aria-hidden /> mid</span>{" · "}
      <span className="whitespace-nowrap"><ChevronDown className="inline h-3 w-3" aria-hidden /> bottom 25%</span>{" · "}
      <span className="whitespace-nowrap"><HelpCircle className="inline h-3 w-3" aria-hidden /> projection is a position prior, so the multiple is not evidence</span>.
    </p>
  );
}
