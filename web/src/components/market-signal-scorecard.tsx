import type { MarketSignalScorecardRow } from "@/db/queries";
import styles from "./market-signal-scorecard.module.css";

const LABELS: Record<string, string> = {
  steam: "STEAM", walking: "WALKING", reversal: "REVERSAL",
  reference_led: "REFERENCE LED", price_pressure: "PRICE PRESSURE",
  pinnacle_divergence: "PINNACLE GAP", dk_value: "DK VALUE", book_disagreement: "BOOK DISAGREEMENT",
  market_convergence: "CONVERGENCE", late_move: "LATE MOVE",
  favorite_flip: "FAVORITE FLIP", key_cross: "KEY CROSS",
  all_moneyline: "ALL ALERTS", steam_short: "STEAM ≤30m", steam_medium: "STEAM >30–180m",
  steam_long: "STEAM >180m", steam_unknown: "STEAM INTERVAL UNKNOWN",
};

const number = (value: number | null, suffix = "") => value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(1)}${suffix}`;
const rate = (value: number | null) => value == null ? "—" : `${(value * 100).toFixed(1)}%`;

export default function MarketSignalScorecard({ rows, sport }: { rows: MarketSignalScorecardRow[]; sport: "CFB" | "TENNIS" | "NHL" }) {
  if (sport === "NHL") {
    const interval = (value: [number,number] | null | undefined, roi = false) => value ? `${(value[0]*(roi?100:1)).toFixed(1)}% to ${(value[1]*(roi?100:1)).toFixed(1)}%` : "insufficient games";
    return <section className={styles.scorecard} aria-label="NHL prospective signal scorecard">
      <header><div><strong>NHL SIGNAL EVIDENCE</strong><span>PROSPECTIVE OBSERVATIONS · PAPER RETURNS</span></div><p>Market only · goalie and injury inputs unavailable</p></header>
      <div className={styles.scroll}><table><thead><tr><th>Signal / interval</th><th>Alerts / games</th><th>Repeated / opposing games</th><th>Settled / priced / missing price</th><th>W-L-V</th><th>Fresh / stale / unknown</th><th>Probability CLV</th><th>Price CLV</th><th>Paper units</th><th>Paper ROI</th><th>Fresh quote ROI</th><th>95% game-cluster ROI range</th><th>95% game-cluster price CLV range</th></tr></thead>
        <tbody>{rows.map(row=><tr key={row.alertType}>
          <td>{LABELS[row.alertType]??row.alertType}</td><td>{row.observations} / {row.uniqueGames??0}</td>
          <td>{row.repeatedAlerts??0} / {row.opposingGames??0}</td><td>{row.settled} / {row.pricedSettled??0} / {row.missingPrices??0}</td>
          <td>{row.wins}-{row.losses}-{row.voids}</td><td>{row.fresh??0} / {row.stale??0} / {row.unknownTiming??0}</td>
          <td>{number(row.avgClvPp,"pp")} (n={row.nClv})</td><td>{number(row.avgPriceClvPct??null,"%")} (n={row.priceClvN??0})</td>
          <td>{number(row.units,"u")}</td><td>{row.roiPerBet==null?"—":number(row.roiPerBet*100,"%")}</td>
          <td>{row.freshRoi==null?"—":number(row.freshRoi*100,"%")} (n={row.freshPricedSettled??0})</td>
          <td>{interval(row.roiInterval,true)}</td><td>{interval(row.priceClvInterval)}</td>
        </tr>)}</tbody></table></div>
      <footer>ROI divides by priced settled alerts. Repeated and opposing observations are retained; the all-alert row is not a portfolio. Steam interval rows are subsets of STEAM. Freshness means a quote was observed within five minutes, not a verified fill. Ranges resample whole games (2,000 fixed-seed samples); they describe this sample and do not establish predictive accuracy or profitability.</footer>
    </section>;
  }
  return <section className={styles.scorecard} aria-label={`${sport} prospective signal scorecard`}>
    <header><div><strong>{rows.length === 10 ? "TOP 10 " : ""}PROSPECTIVE SIGNAL SCORECARD</strong><span>{sport} · RETROSPECTIVE ROWS EXCLUDED</span></div><p>Evidence only · no signal is labeled predictive</p></header>
    <div className={styles.scroll}><table><thead><tr><th>Signal</th><th>Stage</th><th>Obs</th><th>Pending</th><th>Settled</th><th>W-L-V</th><th>Median CLV</th><th>Avg CLV</th><th>Beat close</th><th>Units</th><th>ROI / settled</th></tr></thead>
      <tbody>{rows.map(row => <tr key={row.alertType}>
        <td>{LABELS[row.alertType] ?? row.alertType.replaceAll("_", " ").toUpperCase()}</td>
        <td><span className={styles.stage} data-stage={row.stage}>{row.stage === "validation_ready" ? "100+ SAMPLE" : row.stage === "initial_review" ? "EARLY REVIEW" : "COLLECTING"}</span></td>
        <td>{row.observations}</td><td>{row.pending}</td><td>{row.settled}</td><td>{row.wins}-{row.losses}-{row.voids}</td>
        <td>{number(row.medianClvPp, "pp")}</td><td>{number(row.avgClvPp, "pp")}</td><td>{rate(row.beatClose)}</td>
        <td>{number(row.units, "u")}</td><td>{row.roiPerBet == null ? "—" : number(row.roiPerBet * 100, "%")}</td>
      </tr>)}</tbody></table></div>
    <footer>Stages describe sample size only. Promotion still requires prospective CLV, stable results, and out-of-sample review.</footer>
  </section>;
}
