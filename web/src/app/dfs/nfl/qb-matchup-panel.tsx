import type { NflWorkspacePlayer } from "./actions";

const display = (value: number | null, digits = 1) => value == null ? "—" : value.toFixed(digits);
const percent = (value: number | null) => value == null ? "—" : `${(value * 100).toFixed(0)}%`;

/** Descriptive context beside the optimizer pool; never modifies player points. */
export default function QbMatchupPanel({ players, onSelect }: {
  players: NflWorkspacePlayer[];
  onSelect: (player: NflWorkspacePlayer) => void;
}) {
  const starterRank = (player: NflWorkspacePlayer) => player.availability?.role?.startsWith("Expected starter") ? 0
    : player.availability?.role?.startsWith("Backup") ? 2 : 1;
  const qbs = players.filter(player => player.position === "QB" && !player.isOut && player.ourProj != null && player.ourProj > 0)
    .sort((a, b) => starterRank(a) - starterRank(b) || (b.ourProj! / b.salary) - (a.ourProj! / a.salary));
  if (!qbs.length) return null;
  return <section className="rounded-xl border border-indigo-200 bg-white p-4 shadow-sm" aria-label="Quarterback matchup evidence">
    <h2 className="font-bold text-indigo-950">QB matchup evidence</h2>
    <p className="mt-1 text-xs text-slate-600">Grouped by roster role, then historical projected points per $1,000 salary. Market, game script and opponent-adjusted PbP are decision aids only; they do not change projected points or lineup scores. Confirm starting roles before locking a QB.</p>
    <div className="mt-3 overflow-x-auto"><table className="w-full min-w-[850px] text-left text-xs">
      <thead><tr className="border-b text-slate-600"><th className="p-2">QB</th><th className="p-2 text-right">Base value</th><th className="p-2 text-right">Team implied</th><th className="p-2 text-right">Spread / total</th><th className="p-2 text-right">Close pass rate</th><th className="p-2 text-right">Opponent adj. pass EPA</th><th className="p-2 text-right">TD drives allowed</th><th className="p-2">Evidence</th></tr></thead>
      <tbody>{qbs.map(player => { const context = player.qbMatchupContext; const backup = player.availability?.role?.startsWith("Backup");
        return <tr key={player.dkPlayerId} className="border-b align-top last:border-0">
          <td className="p-2"><button type="button" className="font-semibold text-blue-700 underline" onClick={() => onSelect(player)}>{player.name}</button><div className="text-slate-500">{player.team} vs {player.opponent} · ${player.salary.toLocaleString()}</div>{backup ? <span className="text-amber-700">Listed backup</span> : starterRank(player) === 1 ? <span className="text-amber-700">Starting role unresolved</span> : null}</td>
          <td className="p-2 text-right font-semibold">{display(player.ourProj! / player.salary * 1000, 2)}x</td>
          <td className="p-2 text-right">{display(context?.impliedPoints ?? null)}</td>
          <td className="p-2 text-right">{context?.teamSpread == null ? "—" : `${context.teamSpread > 0 ? "+" : ""}${display(context.teamSpread)}`} / {display(context?.gameTotal ?? null)}</td>
          <td className="p-2 text-right">{percent(context?.closeDropbackRate ?? null)}<div className="text-slate-400">{context?.closePlays ?? 0} plays</div></td>
          <td className="p-2 text-right">{context?.opponentAdjustedEpa == null ? "—" : `${context.opponentAdjustedEpa > 0 ? "+" : ""}${display(context.opponentAdjustedEpa, 3)}`}<div className="text-slate-400">{context?.opponentDropbacks ?? 0} dropbacks</div></td>
          <td className="p-2 text-right">{context?.opponentCompetitiveDrives ? `${context.opponentTouchdownDrives}/${context.opponentCompetitiveDrives}` : "—"}</td>
          <td className="p-2">{context?.status === "ready" ? <span className="text-emerald-700">Complete</span> : <span className="text-amber-700">{context?.reason ?? "No as-of matchup evidence"}</span>}<div className="text-slate-400">{context?.oddsCapturedAt ? `Odds ${new Date(context.oddsCapturedAt).toLocaleString()} · ${context.bookmakerCount ?? 0} books` : ""}</div></td>
        </tr>; })}</tbody>
    </table></div>
    <p className="mt-2 text-xs text-slate-500">Positive opponent-adjusted EPA means opponents passed more efficiently against that defense than in their other games. Early-season samples are small. Close means score within seven points; TD drives exclude clock and kneel endings.</p>
  </section>;
}
