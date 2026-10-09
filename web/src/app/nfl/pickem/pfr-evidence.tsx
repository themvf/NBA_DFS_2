import React from "react";
import type { PfrEvidence, PfrPlayer } from "@/lib/nfl/pickem-pfr";

const value = (r: PfrPlayer, key: string) => r.stats[key] == null ? "—" : String(r.stats[key]);
function ChartingTable({ rows, columns }: { rows: PfrPlayer[]; columns: Array<[string, string]> }) {
  if (!rows.length) return <p className="text-muted-foreground">No player observations available.</p>;
  return <div className="overflow-x-auto"><table className="w-full text-left text-xs">
    <thead><tr><th className="p-1">Player</th>{columns.map(([key, title]) => <th className="p-1 text-right" key={key}>{title}</th>)}</tr></thead>
    <tbody>{rows.map(r => <tr key={`${r.team}-${r.id}`} className="border-t"><td className="p-1">{r.name}</td>
      {columns.map(([key]) => <td key={key} className="p-1 text-right tabular-nums">{value(r, key)}</td>)}</tr>)}</tbody>
  </table></div>;
}

export function PfrEvidencePanel({ evidence }: { evidence?: PfrEvidence[] }) {
  return <section className="space-y-2 border-t pt-2" aria-label="Advanced matchup stats">
    <h3 className="font-semibold">Pressure, protection & contact</h3>
    <p className="text-muted-foreground">PFR charting via nflverse · up to four prior completed games. Descriptive evidence; win probabilities stay market-based.</p>
    {!evidence?.length && <p>Advanced stats are unavailable for this matchup.</p>}
    {evidence?.map(team => <div key={team.team} className="space-y-2">
      <h4 className="font-semibold">{team.team} · {team.games.filter(g => !g.missing.length).length}/{team.games.length} prior games with all four advanced sections</h4>
      {!team.games.length && <p>No prior completed games available.</p>}
      {team.games.map(game => <details key={game.gameId} className="rounded border p-2">
        <summary className="cursor-pointer">Week {game.week} · {game.gameId.split("_").slice(2).join(" at ")}{game.missing.length ? " · incomplete" : ""}</summary>
        <div className="mt-2 space-y-3">
          <p className="text-muted-foreground">Captured: {game.capturedAt ? new Date(game.capturedAt).toLocaleString("en-US", {timeZone: "America/New_York", timeZoneName: "short"}) : "not available before this matchup"}.
            {game.sourceUrl && <> <a className="underline" href={game.sourceUrl} target="_blank" rel="noreferrer">Source</a></>}</p>
          {game.manifest && <details className="text-muted-foreground"><summary>Saved source and identity</summary>
            <p>Snapshot {game.manifest.snapshotId ?? "unavailable"} · {game.manifest.provider ?? "provider unavailable"} · player identity {game.manifest.identityResolution}.</p>
            <p>Recorded {game.manifest.recordedAt ?? "unknown"}. Parser {game.manifest.parserVersion ?? "unknown"}. Schema {game.manifest.schemaVersion ?? "unknown"}.</p>
          </details>}
          {!!game.missing.length && <p className="text-amber-700 dark:text-amber-400">Missing: {game.missing.map(s => s.replace("_advanced", "")).join(", ")}. Missing values are not zero.</p>}
          {([false, true] as const).map(opponent => <div key={String(opponent)}>
            <h5 className="font-medium">{opponent ? `Opposing quarterbacks — pressure created by ${team.team}` : `${team.team} quarterbacks — pressure faced`}</h5>
            <ChartingTable rows={game.players.filter(r => r.section === "passing_advanced" && (opponent ? r.team !== team.team : r.team === team.team))}
              columns={[["times_pressured", "Pressures"], ["times_pressured_pct", "Pressure %"], ["times_sacked", "Sacks"], ["times_blitzed", "Blitzes faced"], ["passing_drops", "Drops"], ["passing_bad_throws", "Bad throws"]]} />
          </div>)}
          <div><h5 className="font-medium">Rushing contact</h5><ChartingTable rows={game.players.filter(r => r.section === "rushing_advanced" && r.team === team.team)} columns={[["carries", "Carries"], ["rushing_yards_before_contact", "Yards before contact"], ["rushing_yards_after_contact", "Yards after contact"]]} /></div>
          <details><summary className="cursor-pointer">Receiving & defensive details</summary>
            <ChartingTable rows={game.players.filter(r => r.section === "receiving_advanced" && r.team === team.team)} columns={[["receiving_drop", "Drops"], ["receiving_broken_tackles", "Broken tackles"]]} />
            <ChartingTable rows={game.players.filter(r => r.section === "defense_advanced" && r.team === team.team)} columns={[["def_pressures", "Pressure credits"], ["def_targets", "Targets in coverage"], ["def_missed_tackles", "Missed tackles"]]} />
          </details>
        </div>
      </details>)}
    </div>)}
    <p className="text-muted-foreground">Small samples, not adjusted for opponent strength. Pressure rates remain per quarterback/game; defender credits are not unique team pressures. Starters and snap counts are not included. These totals cannot identify which individual plays were pressured.</p>
  </section>;
}
