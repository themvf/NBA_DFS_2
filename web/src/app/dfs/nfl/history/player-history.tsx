"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { contextPoints, historicalRange, participationLabel, type PlayerContext } from "@/lib/nfl-dfs/player-context";
import WorkloadScenarios from "./workload-scenarios";
import styles from "./player-history.module.css";

export default function PlayerHistory({ data, selectedId }: { data: PlayerContext; selectedId: string }) {
  const player = data.players.find(p => p.id === selectedId)!;
  const [query, setQuery] = useState("");
  const [opponent, setOpponent] = useState("");
  const [gameKey, setGameKey] = useState(data.rows[0]?.gameKey ?? "");
  const [fullRoster, setFullRoster] = useState(false);
  const rows = data.rows.filter(r => !opponent || data.games[r.gameKey].opponent === opponent);
  const selected = rows.find(r => r.gameKey === gameKey) ?? rows[0];
  const game = selected ? data.games[selected.gameKey] : null;
  const range = historicalRange(rows);
  const options = useMemo(() => data.players.filter(p => p.id === selectedId || `${p.name} ${p.position}`.toLowerCase().includes(query.toLowerCase())), [data.players, selectedId, query]);
  const max = Math.max(1, ...rows.map(r => Math.abs(contextPoints(r) ?? 0)));
  const qbs = game?.roster.filter(m => m.position === "QB" && (m.recordedPlays ?? 0) > 0) ?? [];
  return <main className={styles.page}>
    <nav><Link href="/dfs/nfl">← NFL DFS</Link><Link href="/dfs/nfl/model">Model Lab</Link><Link href="/dfs/nfl/scenarios">Scenario Lab</Link></nav>
    <header className={styles.hero}><span>2025 REGULAR SEASON · HISTORICAL CONTEXT</span><h1>Who was on the field?</h1><p>Explore the workload behind a player’s fantasy points, with the quarterback and teammates recorded in that game.</p><small>Review past outcomes alongside roster context. Historical results are not projected lineup floors or ceilings.</small></header>
    <section className={styles.audit} aria-label="Data coverage">
      <div><strong>{data.audit.scheduledGames}</strong><span>games · all 32 teams</span></div><div><strong>{data.audit.recordedPlays.toLocaleString()} / {data.audit.scrimmagePlays.toLocaleString()}</strong><span>scrimmage plays matched to personnel</span></div><div><strong>{data.audit.scoredRows.toLocaleString()}</strong><span>QB / WR / TE game rows with scoring inputs</span></div><div><strong>Not available</strong><span>verified individual routes & injury reasons</span></div>
    </section>
    <section className={styles.panel}>
      <form action="/dfs/nfl/history" className={styles.filters}><label>Find a player<input type="search" value={query} onChange={e => setQuery(e.target.value)} placeholder="Name or position" /></label><label>Player<select name="player" defaultValue={selectedId}>{options.map(p => <option key={p.id} value={p.id}>{p.name} · {p.position}</option>)}</select></label><button type="submit">View history →</button></form>
      <div className={styles.heading}><div><span>PLAYER GAME LOG</span><h2>{player.name} <small>{player.position}</small></h2></div><label>Opponent<select value={opponent} onChange={e => setOpponent(e.target.value)}><option value="">All opponents</option>{[...new Set(data.rows.map(r => data.games[r.gameKey].opponent))].sort().map(o => <option key={o}>{o}</option>)}</select></label></div>
      <p className={styles.note}>Historical scored-game range: {range ? <><b>P10 {range.p10.toFixed(1)} · P50 {range.p50.toFixed(1)} · P90 {range.p90.toFixed(1)}</b> DK points from {range.n} scored rows.</> : "No scored rows."} Missing stat rows are excluded, not treated as zero. These sample percentiles are not the next game’s floor or ceiling.</p>
      <div className={styles.chart} aria-label="Weekly DraftKings points"><div className={styles.chartBars}>{rows.map(r => { const score = contextPoints(r); const g = data.games[r.gameKey]; return <button key={r.gameKey} aria-pressed={selected?.gameKey === r.gameKey} onClick={() => setGameKey(r.gameKey)} aria-label={`Week ${g.week} vs ${g.opponent}: ${score === null ? "score unavailable" : `${score.toFixed(1)} DK points`}`}><b>{score === null ? "—" : score.toFixed(1)}</b><span className={styles.barSpace}><i style={{ height: `${Math.max(2, Math.abs(score ?? 0) / max * 100)}%`, background: score === null ? "#cbd5e1" : score < 0 ? "#dc2626" : undefined }} /></span><span>W{g.week}</span><small>{g.opponent}</small></button>; })}</div></div>
      <p className={styles.note}>Select a week to inspect its roster. Gray means unavailable; red means a negative score. Bar heights show score magnitude.</p>
    </section>
    {player.position === "WR" && <WorkloadScenarios data={data} playerId={selectedId} />}
    {game && selected && <section className={styles.panel}>
      <div className={styles.heading}><div><span>WEEK {game.week} · {game.date}</span><h2>{game.team} vs {game.opponent}</h2></div><strong>{contextPoints(selected)?.toFixed(1) ?? "—"} DK points</strong></div>
      <div className={styles.metrics}><div><b>{(player.position === "QB" ? selected.attempts : selected.targets) ?? "—"}</b>{player.position === "QB" ? "Pass attempts" : "Targets"}</div><div><b>{(player.position === "QB" ? selected.stats?.rushYds : selected.stats?.receptions) ?? "—"}</b>{player.position === "QB" ? "Rushing yards" : "Receptions"}</div><div><b>{(player.position === "QB" ? selected.stats?.passYds : selected.stats?.recYds) ?? "—"}</b>{player.position === "QB" ? "Passing yards" : "Receiving yards"}</div><div><b>{selected.stats ? selected.stats.passTds + selected.stats.rushTds + selected.stats.recTds : "—"}</b>Pass / rush / receiving TDs</div></div>
      <p><b>Quarterbacks recorded on scrimmage plays:</b> {qbs.map(q => `${q.name} (${q.recordedPlays})`).join(" · ") || "None observed"}. This identifies participation, not the designated starter or injury timing.</p>
      <div className={styles.heading}><h3>Teammate participation</h3><label><input type="checkbox" checked={fullRoster} onChange={e => setFullRoster(e.target.checked)} /> Include full roster</label></div>
      <p className={styles.note}>Counts cover matched pass/run plays, including kneels and spikes. They are not official snap counts or routes. No recorded participation does not establish an injury. Weekly roster status is retrospective.</p>
      <div className={styles.tableWrap}><table><thead><tr><th>Player</th><th>Position</th><th>Weekly roster status</th><th>Recorded offensive plays</th></tr></thead><tbody>{game.roster.filter(m => fullRoster || ["QB", "RB", "FB", "WR", "TE"].includes(m.position)).map(m => <tr key={m.id} className={m.id === selectedId ? styles.selected : undefined}><td>{data.players.some(p => p.id === m.id) ? <Link href={`/dfs/nfl/history?player=${encodeURIComponent(m.id)}`}>{m.name}</Link> : m.name}{m.id === selectedId ? " · selected" : ""}</td><td>{m.position}</td><td>{({ ACT: "Active", INA: "Inactive", RES: "Reserve", DEV: "Practice squad", CUT: "Cut", RET: "Retired" } as Record<string, string>)[m.status] ?? m.status}</td><td><div className={styles.participation}><span>{participationLabel(m, game)}</span>{m.recordedPlays !== null && m.recordedPlays > 0 && <i style={{ width: `${m.recordedPlays / Math.max(1, game.covered) * 100}%` }} />}</div></td></tr>)}</tbody></table></div>
    </section>}
    <details className={styles.panel}><summary>Coverage, scoring and source audit</summary><p>{data.audit.playerRows.toLocaleString()} roster-game rows for QB/WR/TE; {data.audit.scoredRows.toLocaleString()} have complete DK scoring inputs. {data.audit.unknownRosterStatuses} roster entries have unknown status. Full-roster display includes positions beyond QB/WR/TE; their player histories are outside this increment.</p><p>DK points use the app’s shared NFL scorer, including yardage bonuses, conversions, return touchdowns and lost fumbles. The dataset was acquired after the games and is retrospective evidence; it does not prove what was known before lineup lock. It does not change optimizer projections.</p>{Object.entries(data.sources).map(([key, s]) => <div className={styles.source} key={key}><a href={s.url}>{key}</a><span>Retrieved {s.fetchedAt} · Published {s.sourcePublishedAt ?? "unknown"}</span><code>SHA-256 {s.responseHash}</code></div>)}</details>
  </main>;
}
