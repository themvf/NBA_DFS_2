"use client";

import styles from "@/app/cfb/cfb-terminal.module.css";
import { FAVORITE_WATCH_MAX_PROB, FAVORITE_WATCH_MIN_DROP_PP, FAVORITE_WATCH_MIN_PROB, CFB_FAVORITE_WATCH_VERSION, favoriteWatchLabel, type FavoriteWatchHistory, type FavoriteWatchResult } from "@/lib/cfb-favorite-watch";

/**
 * Favorite Watch tab body, shared by the CFB and NFL line terminals. The rule,
 * the version string and the grading live in lib/cfb-favorite-watch.ts; this
 * file only renders. The motivation paragraph is per sport because the rule was
 * derived on college data and the NFL tab is a transfer, not a second finding.
 */
const MOTIVATION: Record<"CFB" | "NFL", string> = {
  CFB: "Why this list exists: across 262 completed 2026 FBS games with a verified close, favorites beat their closing price in every band (+4.8% ROI at close), and the 35 favorites whose probability fell 2+ points went 27-8 against a 65.8% expectation. That is one partial season and a 2.6-SD pattern of the kind that regresses. The thresholds are frozen so this list can be graded forward, not tuned. Nothing here is a recommendation.",
  NFL: "Why this list exists: the rule was derived on 2026 college football, where favorites beat their closing price in every band and the ones that cheapened 2+ points went 27-8. It is applied to the NFL unchanged as a transfer test, with no NFL evidence behind it yet; the results below are the only NFL record. The thresholds are frozen so the list can be graded forward, not tuned. Nothing here is a recommendation.",
};

function pct(value: number | null): string { return value == null ? "—" : `${(value * 100).toFixed(1)}%`; }
function signed(value: number, digits = 1): string { return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`; }
function american(value: number | null | undefined): string { return value == null ? "—" : `${value > 0 ? "+" : ""}${Math.round(value)}`; }
function fmtEt(value: string, compact = false): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", ...(compact ? { hour: "numeric", minute: "2-digit" } : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" }) }).format(new Date(value));
}

export default function FavoriteWatchPanel({ sport, watch, history, gameDate, asOf, boardStatusDetail, scheduled, onOpenGame }: { sport: "CFB" | "NFL"; watch: FavoriteWatchResult; history: FavoriteWatchHistory | null; gameDate: string; asOf: string; boardStatusDetail: string; scheduled: number; onOpenGame: (id: number) => void }) {
  const motivation = MOTIVATION[sport];
  const exclusionLabels: Record<string, string> = { completed: "final", kicked_off: "kicked off", no_opening: "no opening capture", no_current: "no current capture", no_anchor_book: "neither Pinnacle nor DraftKings quoted both sides at open and now", favorite_flipped: "favorite flipped", outside_band: `favorite outside ${Math.round(FAVORITE_WATCH_MIN_PROB * 100)}-${Math.round(FAVORITE_WATCH_MAX_PROB * 100)}%`, did_not_cheapen: `favorite did not cheapen ${FAVORITE_WATCH_MIN_DROP_PP}pp` };
  const excludedText = Object.entries(watch.excluded).filter(([, n]) => n > 0).map(([key, n]) => `${n} ${exclusionLabels[key] ?? key}`).join(", ") || "none";
  return <section className={styles.favoritePane} aria-label={`${sport} favorite watch`}>
    <div className={styles.sectionTitle}><span>FAVORITE WATCH · UPCOMING ONLY</span><span>{favoriteWatchLabel(sport, CFB_FAVORITE_WATCH_VERSION)} · DESCRIPTIVE · NO EDGE CLAIM</span></div>
    <p className={styles.favoriteRule}>Upcoming games on {gameDate} where the current moneyline favorite is priced {Math.round(FAVORITE_WATCH_MIN_PROB * 100)}-{Math.round(FAVORITE_WATCH_MAX_PROB * 100)}% (no-vig lower-median consensus across the selected sportsbooks) and that probability has fallen at least {FAVORITE_WATCH_MIN_DROP_PP} points since the opening capture: the market walked toward the underdog and the favorite got cheaper. Pinnacle or DraftKings must quote both sides at the open and now. A game drops off at kickoff.</p>
    <p className={styles.favoriteContext}>{motivation}</p>
    {!watch.rows.length ? <div className={styles.empty}>{scheduled ? `No upcoming game on this date meets the filter. Excluded: ${excludedText}.` : boardStatusDetail}</div>
      : <div className={styles.favoriteTableWrap}><table><thead><tr><th>Kick (ET)</th><th>Game</th><th>Favorite</th><th>Open</th><th>Now</th><th>Drop</th><th>Pinnacle now</th><th>Best price now</th><th>Books</th><th>Captured</th><th></th></tr></thead><tbody>
        {watch.rows.map((row) => <tr key={row.matchupId}>
          <td>{row.commenceTime ? fmtEt(row.commenceTime, true) : "TBD"}</td>
          <td>{row.awayTeam} @ {row.homeTeam}{row.network ? <em> · {row.network}</em> : null}</td>
          <td><strong>{row.favoriteTeam}</strong><em> vs {row.underdogTeam}</em></td>
          <td>{pct(row.openProb)}</td>
          <td>{pct(row.currentProb)}</td>
          <td className={styles.negative}>-{row.dropPp.toFixed(1)}pp</td>
          <td>{pct(row.pinnacleProb)}</td>
          <td>{row.bestPrice ? `${american(row.bestPrice.price)} ${row.bestPrice.book}` : "—"}</td>
          <td>{row.openBooks} → {row.currentBooks}</td>
          <td>{row.openingCapturedAt ? fmtEt(row.openingCapturedAt) : "—"} → {row.latestCapturedAt ? fmtEt(row.latestCapturedAt, true) : "—"}</td>
          <td><button type="button" className={styles.favoriteOpen} onClick={() => onOpenGame(row.matchupId)}>Open in terminal</button></td>
        </tr>)}
      </tbody></table></div>}
    <p className={styles.researchDisclosure}>{watch.rows.length} of {scheduled} scheduled games qualify as of {fmtEt(asOf)}. Excluded: {excludedText}. Open and Now are favorite win probabilities with the vig removed; Best price is the highest favorite moneyline among the selected sportsbooks at the latest capture and may already be gone.</p>
    <FavoriteWatchResults sport={sport} history={history} />
  </section>;
}

function FavoriteWatchResults({ sport, history }: { sport: "CFB" | "NFL"; history: FavoriteWatchHistory | null }) {
  if (!history) return <div className={styles.favoriteResults}><div className={styles.sectionTitle}><span>RESULTS · GRADED AT THE VERIFIED CLOSE</span><span>UNAVAILABLE</span></div><div className={styles.empty} role="alert">Favorite Watch history could not be loaded. Results are hidden rather than shown partially.</div></div>;
  const s = history.summary;
  const excludedText = Object.entries(history.excluded).filter(([, n]) => n > 0).map(([key, n]) => `${n} ${key.replaceAll("_", " ")}`).join(", ") || "none";
  return <div className={styles.favoriteResults}>
    <div className={styles.sectionTitle}><span>RESULTS · GRADED AT THE VERIFIED CLOSE</span><span>{favoriteWatchLabel(sport, history.version)} · {s.settled} SETTLED · {s.pending} PENDING</span></div>
    <p className={styles.favoriteContext}>Every game this season is re-run through the same rule using its opening capture and its verified pre-kickoff close, so the record is the frozen state at kickoff, not whatever this tab showed during the day, and it does not depend on anyone having opened the page. A game that qualified mid-day but drifted out by the close is not counted. Units assume one unit on the favorite at the best selected-book price in the close capture.</p>
    <div className={styles.favoriteSummary}>
      <div><span>Qualified</span><strong>{s.qualified}</strong><em>of {history.gamesConsidered} games</em></div>
      <div><span>Record</span><strong>{s.won}-{s.lost}</strong><em>{s.pending} pending</em></div>
      <div><span>Favorite win rate</span><strong>{pct(s.winRate)}</strong><em>close expected {pct(s.expectedWinRate)}</em></div>
      <div><span>Units</span><strong className={s.units == null ? "" : s.units >= 0 ? styles.positive : styles.negative}>{s.units == null ? "—" : signed(s.units, 2)}</strong><em>{s.roiPerBet == null ? "—" : `${signed(s.roiPerBet * 100, 1)}% per bet`}</em></div>
      <div><span>Span</span><strong>{s.firstGameDate ?? "—"}</strong><em>to {s.lastGameDate ?? "—"}</em></div>
    </div>
    <p className={styles.researchDisclosure}>{s.settled < 30 ? `Fewer than 30 settled games: this is a tally, not a rate anyone should trust yet. ` : ""}Excluded from the record: {excludedText}. No edge claim; the motivating pattern is one partial season.</p>
    {!history.rows.length ? <div className={styles.empty}>No game this season has met the rule at its verified close yet.</div>
      : <div className={styles.favoriteTableWrap}><table><thead><tr><th>Date</th><th>Game</th><th>Favorite</th><th>Open</th><th>Close</th><th>Drop</th><th>Best close price</th><th>Score</th><th>Result</th><th>Units</th></tr></thead><tbody>
        {history.rows.map((row) => <tr key={row.matchupId}>
          <td>{row.gameDate}</td>
          <td>{row.awayTeam} @ {row.homeTeam}</td>
          <td><strong>{row.favoriteTeam}</strong><em> vs {row.underdogTeam}</em></td>
          <td>{pct(row.openProb)}</td>
          <td>{pct(row.currentProb)}</td>
          <td className={styles.negative}>-{row.dropPp.toFixed(1)}pp</td>
          <td>{row.bestPrice ? `${american(row.bestPrice.price)} ${row.bestPrice.book}` : "—"}</td>
          <td>{row.score ?? "—"}</td>
          <td className={row.outcome === "won" ? styles.positive : row.outcome === "lost" ? styles.negative : styles.neutral}>{row.outcome.toUpperCase()}</td>
          <td className={row.pnlUnits == null ? "" : row.pnlUnits >= 0 ? styles.positive : styles.negative}>{row.pnlUnits == null ? "—" : signed(row.pnlUnits, 2)}</td>
        </tr>)}
      </tbody></table></div>}
  </div>;
}
