import { getLineAlerts, getLineAlertBacktest } from "@/db/queries";
import MlbPropsClient from "./mlb-props-client";

// Player-prop alerts (dk_prop_value = DK price beats Pinnacle's vig-free fair
// value at the same line; prop_line_gap = DK's line itself sits stale vs
// Pinnacle's) on pitcher K/hits-allowed/earned-runs/outs and batter total
// bases. These fire from the same MLB scanner as the game-line alerts but
// stopped being rendered anywhere once the MLB Vegas board moved to its own
// dedicated route (2026-07-12) and started filtering to game-line types only
// — this page restores their visibility on its own dedicated route, same
// pattern as /vegas/wimbledon.
const PROP_ALERT_TYPES = ["dk_prop_value", "prop_line_gap"];

export default async function MlbPropsContent() {
  const [alerts, backtestAll] = await Promise.all([
    getLineAlerts("mlb", 200, PROP_ALERT_TYPES),
    getLineAlertBacktest("mlb"),
  ]);
  const backtest = backtestAll.filter((b) => PROP_ALERT_TYPES.includes(b.alertType));

  return <MlbPropsClient alerts={alerts} backtest={backtest} />;
}
