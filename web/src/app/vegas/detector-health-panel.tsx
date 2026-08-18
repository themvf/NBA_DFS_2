import type { DetectorHealthRow } from "@/db/queries";

// Catches a detector that has run since it shipped but never fired once —
// distinct from "fired before, quiet lately" and from "no eligible games
// right now" (e.g. soccer between World Cups). Found the hard way
// (2026-08-17): scan_tennis_totals ran every cycle since 2026-07-02 and
// produced zero alerts because DraftKings has never once carried a
// total_line for tennis — a "ran fine, found nothing" cycle and a "ran
// fine, is structurally incapable of finding anything" cycle look identical
// in logs. This panel is the difference, made visible without another ad
// hoc DB audit. Mirrors check_detector_health() in model/line_alerts.py.

const STATUS_LABEL: Record<DetectorHealthRow["status"], string> = {
  dead: "DEAD",
  active: "active",
  too_new: "too new to judge",
  no_opportunity: "no recent games",
};

const STATUS_STYLE: Record<DetectorHealthRow["status"], string> = {
  dead: "bg-red-100 text-red-700",
  active: "bg-emerald-50 text-emerald-700",
  too_new: "bg-gray-100 text-gray-500",
  no_opportunity: "bg-gray-100 text-gray-500",
};

const typeLabel = (t: string) =>
  t === "pinnacle_divergence" ? "Pin divergence"
  : t === "pinnacle_polymarket_delta" ? "Pin/Poly gap"
  : t === "steam" ? "Steam"
  : t === "walking" ? "Walking"
  : t === "dk_value" ? "DK value"
  : t === "dk_prop_value" ? "DK prop value"
  : t === "prop_line_gap" ? "Prop line gap"
  : t === "total_steam" ? "Total steam"
  : t === "spread_steam" ? "Spread steam"
  : t === "total_walking" ? "Total walking"
  : t === "spread_walking" ? "Spread walking"
  : t;

export default function DetectorHealthPanel({ health }: { health: DetectorHealthRow[] }) {
  if (health.length === 0) return null;
  const dead = health.filter((h) => h.status === "dead");
  const rest = health.filter((h) => h.status !== "dead");

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Detector Health
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        Has each detector below fired at least once since it shipped? <span className="font-medium">DEAD</span> means
        it&rsquo;s had {14}+ days and real games to work with and has never once produced an alert — that&rsquo;s a
        structural bug (a field/market the detector expects isn&rsquo;t actually in the captured data), not a quiet
        market. A detector deployed too recently, or a sport with no games in the last 14 days, is withheld from
        judgment rather than mislabeled either way.
      </p>

      {dead.length > 0 && (
        <div className="mb-3 rounded border border-red-200 bg-red-50 px-3 py-2">
          {dead.map((h) => (
            <div key={`${h.sport}-${h.alertType}`} className="text-xs text-red-800">
              <span className="font-semibold">DEAD</span> — {typeLabel(h.alertType)}: deployed {h.deployedAt}{" "}
              ({h.daysDeployed}d ago), 0 alerts ever, {h.opportunityDays}d of games captured in the last 14d.
            </div>
          ))}
        </div>
      )}

      <table className="w-full text-xs border-collapse">
        <thead>
          <tr className="border-b text-gray-500">
            <th className="py-1 text-left">Detector</th>
            <th className="py-1 text-right">Deployed</th>
            <th className="py-1 text-right">Alerts ever</th>
            <th className="py-1 text-right">Last alert</th>
            <th className="py-1 text-right">Status</th>
          </tr>
        </thead>
        <tbody>
          {[...dead, ...rest].map((h) => (
            <tr key={`${h.sport}-${h.alertType}`} className="border-b border-gray-50">
              <td className="py-1 font-medium">{typeLabel(h.alertType)}</td>
              <td className="py-1 text-right text-gray-500">{h.deployedAt}</td>
              <td className="py-1 text-right tabular-nums">{h.alertsEver}</td>
              <td className="py-1 text-right text-gray-500">{h.lastAlertAt?.slice(0, 10) ?? "—"}</td>
              <td className="py-1 text-right">
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${STATUS_STYLE[h.status]}`}>
                  {STATUS_LABEL[h.status]}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
