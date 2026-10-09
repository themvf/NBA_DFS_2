export const dynamic = "force-dynamic";

import Link from "next/link";
import { getCurrentNflAvailabilityCoverage } from "@/db/nfl-context";
import { getNflIdentityGames, getNflIdentityOptions, type NflIdentityGame } from "@/db/nfl-team-identity";
import { aggregateNflIdentityGames, buildNflIdentityWeeks, identitySummary, settleNflIdentityMarket, signed, type NflIdentityMetrics } from "@/lib/nfl/team-identity";
import s from "./team-identity.module.css";

export const metadata = { title: "NFL Team Identity" };

const percent = (value: number | null, digits = 1) => value == null ? "—" : `${(value * 100).toFixed(digits)}%`;
const decimal = (value: number | null, digits = 2) => value == null ? "—" : signed(value, digits);
const plain = (value: number | null, digits = 1) => value == null ? "—" : value.toFixed(digits);
const ppChange = (a: number | null, b: number | null) => a == null || b == null ? "—" : `${signed((a - b) * 100, 1)} pp`;
const epaChange = (a: number | null, b: number | null) => a == null || b == null ? "—" : signed(a - b, 3);
const quoteTime = (value: string) => new Date(value).toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" });

type MetricRow = { label: string; key: keyof NflIdentityMetrics; format: (value: number | null) => string; note: string };
const metrics: MetricRow[] = [
  { label: "Neutral early-down dropbacks", key: "neutralDropbackRate", format: percent, note: "1st/2nd down, first 3 quarters, score within 8; QB dropbacks / eligible run-pass plays" },
  { label: "Pass rate over expectation", key: "passOe", format: value => value == null ? "—" : `${signed(value, 1)} pp`, note: "Average nflverse pass_oe on run-pass plays with a value" },
  { label: "Throw depth", key: "airYards", format: value => plain(value, 1), note: "Mean air yards per throw with air_yards recorded, including incompletions" },
  { label: "Offensive EPA / play", key: "offenseEpa", format: value => decimal(value, 3), note: "Offensive EPA on run-pass plays with EPA recorded" },
  { label: "Offensive success", key: "offenseSuccessRate", format: percent, note: "Successful run-pass plays / all offensive run-pass plays" },
  { label: "Explosive offense", key: "offenseExplosiveRate", format: percent, note: "Offensive run-pass plays gaining at least 20 yards / offensive run-pass plays" },
  { label: "Defensive EPA allowed / play", key: "defenseEpaAllowed", format: value => decimal(value, 3), note: "Opponent EPA on run-pass plays; lower is better for the defense" },
  { label: "Defensive success allowed", key: "defenseSuccessAllowed", format: percent, note: "Successful opponent run-pass plays / opponent run-pass plays" },
  { label: "Explosives allowed", key: "defenseExplosiveRate", format: percent, note: "Opponent run-pass plays gaining at least 20 yards / opponent run-pass plays" },
  { label: "Touchdown drives", key: "touchdownDriveRate", format: percent, note: "Unique offensive touchdown drives / all labeled offensive drives, including kneels and clock endings" },
  { label: "Three-and-outs", key: "threeAndOutRate", format: percent, note: "Unique three-and-out drives / all labeled offensive drives" },
];

async function latestPregameAvailability(game: NflIdentityGame | undefined, team: string): Promise<string | null> {
  if (!game) return null;
  try {
    const coverage = await getCurrentNflAvailabilityCoverage(game.gameId, new Date(game.kickoff));
    const teamRows = coverage.players.filter(row => String(row.measurement.payload.team ?? "") === team);
    if (!teamRows.length) return null;
    const limited = teamRows.filter(row => ["OUT", "DOUBTFUL", "QUESTIONABLE", "CONFLICT", "STALE"].includes(String(row.measurement.payload.resolved_availability_state ?? "")));
    return `${teamRows.length} players had eligible pregame availability records; ${limited.length} were flagged out, doubtful, questionable, conflicted, or stale.`;
  } catch {
    return null;
  }
}

export default async function NflTeamIdentityPage({ searchParams }: {
  searchParams: Promise<{ team?: string; season?: string }>;
}) {
  const params = await searchParams;
  const options = await getNflIdentityOptions();
  const selectedTeam = options.teams.find(row => row.abbreviation === params.team)
    ?? options.teams.find(row => row.abbreviation === "NYJ") ?? options.teams[0];
  const seasonNumber = Number(params.season);
  const season = options.seasons.includes(seasonNumber) ? seasonNumber : options.seasons[0];
  if (!selectedTeam || !season) return <main className={s.page}><p>No NFL play-by-play seasons are available.</p></main>;

  const { throughWeek, games } = await getNflIdentityGames(selectedTeam.abbreviation, season);
  const currentGames = games.filter(row => row.season === season && row.week <= throughWeek);
  const priorGames = games.filter(row => row.season === season - 1);
  const matchedPrior = priorGames.filter(row => row.week <= throughWeek);
  const current = aggregateNflIdentityGames(currentGames);
  const matched = aggregateNflIdentityGames(matchedPrior);
  const fullPrior = aggregateNflIdentityGames(priorGames);
  const weeks = buildNflIdentityWeeks(currentGames, priorGames);
  const latest = weeks.at(-1);
  const availability = await latestPregameAvailability(latest?.game, selectedTeam.abbreviation);
  const hasMissingScheme = current.formationCoverage === 0 || current.personnelCoverage === 0 || current.pressureCoverage === 0;

  return <main className={s.page}>
    <div className={s.shell}>
      <header className={s.header}>
        <div>
          <p className={s.eyebrow}>NFL / TEAM IDENTITY</p>
          <h1>{selectedTeam.name}</h1>
          <p className={s.subtitle}>How this team is playing · {season} through Week {throughWeek || "—"}</p>
        </div>
        <div className={s.headerActions}>
          <Link href="/nfl/pbp">Explore play by play ↗</Link>
          <Link href="/nfl">NFL market board ↗</Link>
        </div>
      </header>

      <form className={s.controls} action="/nfl/team-identity" method="get">
        <label>Team<select name="team" defaultValue={selectedTeam.abbreviation}>{options.teams.map(row => <option key={row.abbreviation} value={row.abbreviation}>{row.name}</option>)}</select></label>
        <label>Season<select name="season" defaultValue={season}>{options.seasons.map(value => <option key={value} value={value}>{value}</option>)}</select></label>
        <button type="submit">View team</button>
        <span>{current.games} completed {current.games === 1 ? "game" : "games"} · {current.offensePlays} offensive run-pass plays</span>
      </form>

      {!latest ? <section className={s.empty}><h2>No completed games yet</h2><p>The summary will appear after this team&apos;s first completed game has been labeled.</p></section> : <>
        <section className={s.hero} aria-labelledby="identity-summary">
          <div className={s.heroCopy}>
            <p className={s.eyebrow}>ROLLING SUMMARY · THROUGH WEEK {throughWeek}</p>
            <h2 id="identity-summary">What has changed?</h2>
            <p className={s.summary}>{identitySummary(selectedTeam.abbreviation, latest, season - 1)}</p>
            {latest.previous && <p className={s.since}>Since the previous completed game: neutral early-down dropback rate {ppChange(current.neutralDropbackRate, latest.previous.neutralDropbackRate)}; offensive EPA/play {epaChange(current.offenseEpa, latest.previous.offenseEpa)}.</p>}
            <p className={s.caution}>{current.games < 6 ? "EARLY SAMPLE" : "DESCRIPTIVE PROFILE"} · Same weeks compared with {season - 1} · Coaching, opponent, roster, and game-state effects are not isolated.</p>
          </div>
          <div className={s.heroNumbers}>
            <div><span>Neutral dropbacks</span><strong>{percent(current.neutralDropbackRate)}</strong><small>{ppChange(current.neutralDropbackRate, matched.neutralDropbackRate)} vs {season - 1}</small></div>
            <div><span>Offense EPA/play</span><strong>{decimal(current.offenseEpa, 3)}</strong><small>{epaChange(current.offenseEpa, matched.offenseEpa)} vs {season - 1}</small></div>
            <div><span>Defense EPA allowed</span><strong>{decimal(current.defenseEpaAllowed, 3)}</strong><small>Lower is better</small></div>
          </div>
        </section>

        <section className={s.section} aria-labelledby="comparison-heading">
          <div className={s.sectionHead}><div><p className={s.eyebrow}>MATCHED WINDOW</p><h2 id="comparison-heading">Season comparison</h2></div><p>{season} Weeks 1–{throughWeek} vs the same {season - 1} weeks. Full {season - 1} is a separate reference.</p></div>
          <div className={s.tableScroll}><table className={s.metricsTable}><thead><tr><th>Measure</th><th>{season} through W{throughWeek}<small>{current.games} games · {current.offensePlays} offense plays</small></th><th>{season - 1} through W{throughWeek}<small>{matched.games} games · {matched.offensePlays} offense plays</small></th><th>Full {season - 1}<small>{fullPrior.games} regular-season games</small></th></tr></thead><tbody>{metrics.map(row => <tr key={row.label}><th scope="row"><span>{row.label}</span><small>{row.note}</small></th><td>{row.format(current[row.key] as number | null)}</td><td>{row.format(matched[row.key] as number | null)}</td><td>{row.format(fullPrior[row.key] as number | null)}</td></tr>)}</tbody></table></div>
          <p className={s.footnote}>Drive percentages include all labeled offensive drives. Play percentages use run/pass plays. A dash means the required source field or denominator is unavailable.</p>
        </section>

        <section className={s.section} aria-labelledby="timeline-heading">
          <div className={s.sectionHead}><div><p className={s.eyebrow}>HOW THE SUMMARY DEVELOPED</p><h2 id="timeline-heading">Week by week</h2></div><p>Each row recomputes the profile through that completed game.</p></div>
          <div className={s.timeline}>{weeks.map(week => <article key={week.game.gameId} className={s.weekCard}>
            <div className={s.weekTop}><span className={s.weekBadge}>WEEK {week.week}</span><strong>{week.game.isHome ? "vs" : "at"} {week.game.opponent}</strong><span>{week.game.teamScore ?? "—"}–{week.game.opponentScore ?? "—"}</span><Link href={`/nfl/pbp?game=${encodeURIComponent(week.game.gameId)}`}>View plays ↗</Link></div>
            <p>{identitySummary(selectedTeam.abbreviation, week, season - 1)}</p>
            <div className={s.weekStats}><span>Neutral dropbacks <b>{percent(week.current.neutralDropbackRate)}</b></span><span>Offense EPA/play <b>{decimal(week.current.offenseEpa, 3)}</b></span><span>Defense EPA allowed <b>{decimal(week.current.defenseEpaAllowed, 3)}</b></span></div>
          </article>)}</div>
        </section>

        <section className={s.section} aria-labelledby="context-heading">
          <div className={s.sectionHead}><div><p className={s.eyebrow}>GAME CONDITIONS</p><h2 id="context-heading">Pregame expectations, postgame results</h2></div><p>Market quotes are the latest stored observations strictly before kickoff; they are not necessarily a sportsbook&apos;s official close.</p></div>
          <p className={s.marketExplainer}><strong>How to read this:</strong> Moneyline is the American price on an outright win: a negative price is the amount risked to win $100, while a positive price is the profit on a $100 stake. The spread adds the listed points to this team&apos;s final margin; exactly zero is a push. The total compares both teams&apos; combined points with the pregame line; an exact match is a push. The win chance removes the bookmaker&apos;s margin when both moneylines are available. Final team points can include defense or special teams scoring.</p>
          <div className={s.gameGrid}>{currentGames.map(game => {
            const settled = settleNflIdentityMarket(game);
            return <article key={game.gameId} className={s.gameCard}>
            <div className={s.gameTitle}><span>W{game.week} · {game.date}</span><h3>{game.isHome ? "vs" : "at"} {game.opponent}</h3><strong>{game.teamScore ?? "—"}–{game.opponentScore ?? "—"}</strong></div>
            <div className={s.marketReadout} aria-label={`Pregame market and postgame result for Week ${game.week}`}>
              <div className={s.marketColumns}><span>Pregame quote</span><span>Postgame result</span></div>
              <div className={s.marketRow}><div><span>Moneyline</span><strong>{game.market?.moneyline == null ? "Unavailable" : signed(game.market.moneyline, 0)}</strong><small>{game.market?.winProbability == null ? "Win chance unavailable" : `${percent(game.market.winProbability)} chance to win, margin removed`}</small></div><div><strong data-tone={settled.winner === "Won" ? "good" : settled.winner === "Lost" ? "bad" : "neutral"}>{settled.winner == null ? "Pending" : settled.winner === "Tied" ? "Tied" : `${settled.winner} outright`}</strong><small>{settled.margin == null ? "Final score pending" : `Final margin ${signed(settled.margin, 0)}`}</small></div></div>
              <div className={s.marketRow}><div><span>Team spread</span><strong>{game.market?.spread == null ? "Unavailable" : signed(game.market.spread, 1)}</strong><small>Team margin + listed points</small></div><div><strong data-tone={settled.spreadResult === "Covered" ? "good" : settled.spreadResult === "Missed" ? "bad" : "neutral"}>{settled.spreadResult == null ? "No quoted result" : settled.spreadResult === "Push" ? "Push — landed exactly" : `${settled.spreadResult} by ${plain(Math.abs(settled.spreadEdge!), 1)}`}</strong><small>{settled.spreadEdge == null ? "Spread comparison unavailable" : `Adjusted margin ${signed(settled.spreadEdge, 1)}`}</small></div></div>
              <div className={s.marketRow}><div><span>Game total</span><strong>{game.market?.total == null ? "Unavailable" : plain(game.market.total)}</strong><small>Both teams combined</small></div><div><strong data-tone="neutral">{settled.totalResult == null ? "No quoted result" : settled.totalResult === "Push" ? "Push — landed exactly" : `${settled.totalResult} by ${plain(Math.abs(settled.totalEdge!), 1)}`}</strong><small>{settled.totalPoints == null ? "Final score pending" : `${settled.totalPoints} final points`}</small></div></div>
            </div>
            <dl><dt>Team implied points</dt><dd>{game.market?.impliedPoints == null ? "Unavailable" : `${plain(game.market.impliedPoints, 2)} · ${settled.impliedPointsEdge == null ? "—" : `${signed(settled.impliedPointsEdge, 2)} vs final`}`}</dd><dt>Offense EPA/play</dt><dd>{game.offenseEpaCount ? decimal(game.offenseEpaSum / game.offenseEpaCount, 3) : "—"}</dd><dt>TD drives</dt><dd>{game.touchdownDrives} / {game.drives}</dd><dt>Rest / roof</dt><dd>{game.restDays == null ? "—" : `${game.restDays}d`} / {game.roof ?? "—"}</dd><dt>Temp / wind</dt><dd>{game.temp == null ? "—" : `${game.temp}°F`} / {game.wind == null ? "—" : `${game.wind} mph`}</dd></dl>
            <div className={s.gameLinks}><Link href={`/nfl/pbp?game=${encodeURIComponent(game.gameId)}`}>Open PbP evidence ↗</Link><Link href={`/nfl?date=${encodeURIComponent(game.date)}`}>Market board ↗</Link></div>
            <p className={s.source}>{game.market ? `${game.market.source === "archive" ? "Historical archive" : "Odds capture"} #${game.market.snapshotId} · ${quoteTime(game.market.observedAt)}` : "No verified pregame market snapshot"}</p>
          </article>})}</div>
          {matchedPrior.length > 0 && <div className={s.priorMarket}>
            <h3>{season - 1} same-week market reference</h3>
            <p>Archived pregame spreads are shown where the historical event join is verified. Comparable historical game totals and implied points are not stored here.</p>
            <div>{matchedPrior.map(game => <span key={game.gameId}>W{game.week} {game.isHome ? "vs" : "at"} {game.opponent}: {game.market?.spread == null ? "line unavailable" : `team spread ${signed(game.market.spread, 1)}`} <Link href={`/nfl/pbp?game=${encodeURIComponent(game.gameId)}`}>plays ↗</Link></span>)}</div>
          </div>}
          <div className={s.coverage}>{availability ? <p><strong>Latest-game availability:</strong> {availability} <Link href={`/nfl/pbp?game=${encodeURIComponent(latest.game.gameId)}`}>Inspect game context ↗</Link></p> : <p><strong>Latest-game availability:</strong> no eligible pregame player snapshot is available for this team. Injury listings are not treated as proof of who played.</p>}{hasMissingScheme && <p><strong>Scheme detail:</strong> formation, personnel, or pressure coverage is incomplete for this season. Those comparisons are withheld until the participation feed is available.</p>}</div>
        </section>

        <details className={s.method}><summary>Definitions, coverage, and source versions</summary><p>These are descriptive observations from completed regular-season games. Neutral early downs are first or second down in the first three quarters with the score within eight points. Drive rates use distinct game/drive pairs. Explosives are gains of at least 20 yards. Odds are selected before kickoff through the canonical `nfl_season_games` bridge; older archived lines supply spreads where current-game odds history is absent.</p><p>{season} coverage: formation {percent(current.formationCoverage)}, personnel {percent(current.personnelCoverage)}, pressure {percent(current.pressureCoverage)}. Pregame implied points available for {current.marketGames} of {current.games} games. Labels: {latest.game.playVersion} / {latest.game.driveVersion}.</p></details>
      </>}
    </div>
  </main>;
}
