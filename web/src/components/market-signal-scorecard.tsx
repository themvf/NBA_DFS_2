import type { MarketSignalScorecardRow } from "@/db/queries";
import styles from "./market-signal-scorecard.module.css";

const LABELS: Record<string, string> = {
  steam: "STEAM", walking: "WALKING", reversal: "REVERSAL",
  reference_led: "REFERENCE LED", price_pressure: "PRICE PRESSURE",
  pinnacle_divergence: "PINNACLE GAP", book_disagreement: "BOOK DISAGREEMENT",
  market_convergence: "CONVERGENCE", late_move: "LATE MOVE",
  favorite_flip: "FAVORITE FLIP", key_cross: "KEY CROSS",
};

const number = (value: number | null, suffix = "") => value == null ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(1)}${suffix}`;
const rate = (value: number | null) => value == null ? "—" : `${(value * 100).toFixed(1)}%`;

export default function MarketSignalScorecard({ rows, sport }: { rows: MarketSignalScorecardRow[]; sport: "CFB" | "TENNIS" }) {
  return <section className={styles.scorecard} aria-label={`${sport} prospective signal scorecard`}>
    <header><div><strong>TOP 10 PROSPECTIVE SIGNAL SCORECARD</strong><span>{sport} · RETROSPECTIVE ROWS EXCLUDED</span></div><p>Evidence only · no signal is labeled predictive</p></header>
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
