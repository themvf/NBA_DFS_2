import { getLineAlerts, getLineAlertBacktest, getMlbPropProgram } from "@/db/queries";
import {
  ENROLLED_DETECTOR_VERSION,
  PROP_ALERT_TYPES,
} from "@/lib/mlb-prop-board";
import MlbPropsV2Client from "./mlb-props-v2-client";

/**
 * Redesign of `/vegas/mlb-props`, built alongside it rather than replacing it
 * so the two can be compared on the same live data. Same three fetches the old
 * page made, plus program enrollment; every difference is in presentation and
 * in `@/lib/mlb-prop-board`, not in what is queried.
 *
 * `evaluatedAt` is stamped on the server and passed down so the live/started
 * split is computed against one clock. Letting the client call `new Date()`
 * during render would make first-pitch classification differ between the
 * server HTML and the hydrated tree — a hydration mismatch on the one field
 * that decides whether a row is bettable.
 */
export default async function MlbPropsV2Content() {
  const [alerts, backtestAll, program] = await Promise.all([
    getLineAlerts("mlb", 300, PROP_ALERT_TYPES),
    getLineAlertBacktest("mlb"),
    getMlbPropProgram(ENROLLED_DETECTOR_VERSION),
  ]);
  const backtest = backtestAll.filter((b) => PROP_ALERT_TYPES.includes(b.alertType));

  return (
    <MlbPropsV2Client
      alerts={alerts}
      backtest={backtest}
      program={program}
      evaluatedAt={new Date().toISOString()}
    />
  );
}
