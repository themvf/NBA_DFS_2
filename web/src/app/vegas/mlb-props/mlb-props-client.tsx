"use client";

import type { LineAlertRow, LineAlertBacktestRow } from "@/db/queries";
import LineAlertsPanel from "../line-alerts-panel";
import MlbVegasTabs from "../mlb-tabs";

export default function MlbPropsClient({
  alerts,
  backtest,
}: {
  alerts: LineAlertRow[];
  backtest: LineAlertBacktestRow[];
}) {
  return (
    <div className="space-y-4 p-6 max-w-5xl mx-auto">
      <MlbVegasTabs active="props-original" />
      <div>
        <h1 className="text-xl font-bold">MLB Vegas Props</h1>
        <p className="text-sm text-gray-500 mt-1">
          Player-prop alerts only (pitcher strikeouts, hits allowed, earned runs, outs recorded,
          batter total bases) — DraftKings vs Pinnacle. Game-line alerts (moneyline/total) stay on
          the main <a href="/vegas?sport=mlb" className="underline">MLB Vegas</a> board.
        </p>
      </div>
      <LineAlertsPanel alerts={alerts} backtest={backtest} />
    </div>
  );
}
