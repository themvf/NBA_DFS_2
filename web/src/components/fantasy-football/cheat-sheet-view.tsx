import {
  buildCheatSheet,
  CHEAT_SHEET_VARIANTS,
  SIGNAL_GLYPH,
  SIGNAL_LABEL,
  type CheatSheetColumn,
  type CheatSheetEntry,
  type CheatSheetVariant,
} from "@/lib/fantasy-football/cheat-sheet";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";

function num(value: number | null, digits = 0) {
  return value === null ? "—" : value.toFixed(digits);
}

function signed(value: number | null) {
  if (value === null) return "";
  const rounded = Math.round(value);
  if (rounded === 0) return "0";
  return rounded > 0 ? `+${rounded}` : String(rounded);
}

function Row({ entry, showComparison }: { entry: CheatSheetEntry; showComparison: boolean }) {
  return <tr className={entry.startsNewTier ? "border-t-2 border-slate-900" : ""}>
    <td className="pr-1 text-right tabular-nums text-slate-500">{entry.positionRank}</td>
    <td className="max-w-[9rem] truncate pr-1 font-semibold">{entry.name}</td>
    <td className="pr-1 text-slate-600">{entry.team ?? "FA"}</td>
    <td className="pr-1 text-right tabular-nums text-slate-600">{entry.byeWeek ?? "—"}</td>
    <td className="pr-1 text-right tabular-nums">{num(entry.projectedPoints)}</td>
    <td className="pr-1 text-right tabular-nums text-slate-500">
      {showComparison ? signed(entry.comparisonDelta) : signed(entry.adpDelta)}
    </td>
    <td className="w-3 text-center font-bold text-slate-700">
      {entry.signal ? SIGNAL_GLYPH[entry.signal] : ""}
    </td>
  </tr>;
}

function Column({ column }: { column: CheatSheetColumn }) {
  // DST is the one column where the comparison number is FantasyPros' rank
  // rather than ADP: our DST order is pure prior-season carry-forward, so the
  // gap against their forward-looking number is the decision-relevant signal.
  const showComparison = column.position === "DST";
  return <section className="break-inside-avoid">
    <h2 className="mb-0.5 flex items-baseline justify-between border-b-2 border-slate-900 pb-0.5">
      <span className="text-[11px] font-black uppercase tracking-wide">
        {column.label}{column.continued && <span className="font-semibold text-slate-500"> cont.</span>}
      </span>
      <span className="text-[7px] font-semibold uppercase text-slate-500">
        {showComparison ? "vs FP" : "vs ADP"}
      </span>
    </h2>
    <table className="w-full text-[7.5px] leading-[1.3]">
      <tbody>
        {column.entries.map((entry) => (
          <Row key={entry.playerId} entry={entry} showComparison={showComparison} />
        ))}
      </tbody>
    </table>
    {column.tiersSuppressed && !column.continued && <p className="mt-1 text-[6.5px] leading-tight text-slate-500">
      No tiers: our defensive projections are shrunk hard toward the league
      prior, so all 32 fall in one tier. The ORDER is real (2025 actuals carried
      forward) but weak. &ldquo;vs FP&rdquo; = FantasyPros&rsquo; projection rank minus ours;
      positive = we rank them higher.
    </p>}
  </section>;
}

export default function CheatSheetView({
  rankings,
  setName,
  createdAt,
  scoring,
  season,
  variant = "rankings",
}: {
  rankings: FantasyRankingRow[];
  setName: string;
  createdAt: string;
  scoring: string;
  season: number;
  variant?: CheatSheetVariant;
}) {
  const config = CHEAT_SHEET_VARIANTS[variant];
  const columns = buildCheatSheet(rankings, variant);
  return <div className="cheat-sheet bg-white text-slate-900">
    <header className="mb-1.5 flex items-baseline justify-between border-b-2 border-slate-900 pb-1">
      <h1 className="text-sm font-black uppercase tracking-tight">
        {season} {config.label} · {scoring}
      </h1>
      <p className="text-[7px] text-slate-600">
        {config.context} · {setName} · generated{" "}
        {new Date(createdAt).toLocaleString()} · proj = our model; bye = week off
      </p>
    </header>

    <div
      className="grid gap-x-2.5"
      style={{ gridTemplateColumns: `repeat(${columns.length}, minmax(0, 1fr))` }}
    >
      {columns.map((column, index) => (
        <Column key={`${column.position}-${index}`} column={column} />
      ))}
    </div>

    <footer className="mt-1.5 border-t border-slate-300 pt-1 text-[6.5px] text-slate-600">
      {Object.entries(SIGNAL_GLYPH).map(([signal, glyph]) => (
        <span key={signal} className="mr-3">
          <strong>{glyph}</strong> {SIGNAL_LABEL[signal as keyof typeof SIGNAL_LABEL]}
        </span>
      ))}
      <span className="mr-3">Horizontal rule = tier break.</span>
      <span>
        FantasyPros is comparison data only and never feeds our projections.
      </span>
    </footer>
  </div>;
}
