export const dynamic = "force-dynamic";

import { getAllDetectorHealth } from "@/db/queries";
import type { DetectorHealthRow } from "@/db/queries";

// Single cross-sport status page — the "check this first" destination the
// per-sport Detector Health panels don't provide on their own. Those panels
// stay (they sit next to the actual alerts/backtest for that sport, which is
// useful context when you're already there); this page exists so knowing
// "is anything broken, anywhere" doesn't require visiting all four sport
// pages in rotation. Deliberately three states, not a literal red/green
// binary — too_new and no_opportunity both exist specifically so a young
// detector or a dormant sport (soccer between World Cups) is never confused
// with a broken one, and collapsing that to a binary would throw away the
// exact distinction the health check was built to make.

const SPORT_LABEL: Record<string, string> = {
  mlb: "MLB ⚾",
  nfl: "NFL 🏈",
  soccer: "Soccer ⚽",
  tennis: "Tennis 🎾",
};

const SPORT_ORDER = ["mlb", "nfl", "soccer", "tennis"];

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

const DOT: Record<DetectorHealthRow["status"], string> = {
  dead: "bg-red-500",
  active: "bg-emerald-500",
  too_new: "bg-gray-300",
  no_opportunity: "bg-gray-300",
};

const CARD: Record<DetectorHealthRow["status"], string> = {
  dead: "border-red-200 bg-red-50",
  active: "border-emerald-100 bg-white",
  too_new: "border-gray-200 bg-gray-50",
  no_opportunity: "border-gray-200 bg-gray-50",
};

const STATUS_TEXT: Record<DetectorHealthRow["status"], string> = {
  dead: "DEAD — 0 alerts ever, opportunity existed",
  active: "active",
  too_new: "too new to judge (<14d deployed)",
  no_opportunity: "no games captured in the last 14d",
};

function DetectorTile({ h }: { h: DetectorHealthRow }) {
  const detail =
    h.status === "dead"
      ? `Deployed ${h.deployedAt} (${h.daysDeployed}d ago). 0 alerts ever despite ${h.opportunityDays}d of eligible games in the last 14d — the field/market this detector reads likely isn't actually present in the captured books.`
      : h.status === "active"
      ? `${h.alertsEver} alert${h.alertsEver === 1 ? "" : "s"} ever. Last fired ${h.lastAlertAt?.slice(0, 10) ?? "—"}.`
      : h.status === "too_new"
      ? `Deployed ${h.deployedAt}, only ${h.daysDeployed}d ago — needs 14d before a dead/active verdict is meaningful.`
      : `0 eligible game captures for this sport in the last 14d — the sport is dormant right now, not the detector.`;

  return (
    <div
      className={`flex items-center gap-2 rounded-lg border px-3 py-2 ${CARD[h.status]}`}
      title={`${typeLabel(h.alertType)} — ${STATUS_TEXT[h.status]}\n${detail}`}
    >
      <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${DOT[h.status]}`} aria-hidden="true" />
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-medium text-gray-800">{typeLabel(h.alertType)}</div>
        <div className="truncate text-[11px] text-gray-500">
          {h.status === "dead"
            ? `0 alerts, ${h.daysDeployed}d deployed`
            : h.status === "active"
            ? `${h.alertsEver} ever · last ${h.lastAlertAt?.slice(0, 10) ?? "—"}`
            : h.status === "too_new"
            ? `deployed ${h.deployedAt}`
            : "no recent games"}
        </div>
      </div>
    </div>
  );
}

export default async function DetectorsStatusPage() {
  const health = await getAllDetectorHealth();
  const dead = health.filter((h) => h.status === "dead");
  const active = health.filter((h) => h.status === "active");
  const monitoring = health.filter((h) => h.status === "too_new" || h.status === "no_opportunity");

  return (
    <div className="space-y-6 p-6 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-xl font-bold">Detector Status</h1>
        <span className="text-xs text-muted-foreground">{health.length} detectors tracked</span>
      </div>

      <p className="text-sm text-muted-foreground">
        Has each sharp line-alert detector fired at least once since it shipped? <span className="font-medium text-red-600">Red</span> means
        it&rsquo;s had 14+ days and real games to work with and has never once produced an alert — a structural bug (a field/market the
        detector expects isn&rsquo;t actually in the captured data), not a quiet market. <span className="font-medium text-emerald-600">Green</span> means
        it has fired at least once. <span className="font-medium text-gray-500">Gray</span> means no verdict is possible yet — either the
        detector is too new to judge, or the sport has had no eligible games in the last 14 days (e.g. soccer between World Cups). Hover any
        tile for the exact numbers behind it.
      </p>

      <div className="flex flex-wrap gap-3 rounded-lg border bg-white p-4">
        <div className="flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-red-500" />
          <span className="text-sm font-medium">{dead.length} dead</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />
          <span className="text-sm font-medium">{active.length} active</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-gray-300" />
          <span className="text-sm font-medium">{monitoring.length} not yet judged</span>
        </div>
      </div>

      {dead.length > 0 && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-4">
          <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-red-800">Needs attention</h2>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            {dead.map((h) => (
              <DetectorTile key={`${h.sport}-${h.alertType}`} h={h} />
            ))}
          </div>
        </div>
      )}

      {SPORT_ORDER.filter((sport) => health.some((h) => h.sport === sport)).map((sport) => {
        const rows = health
          .filter((h) => h.sport === sport)
          .sort((a, b) => {
            const order = { dead: 0, active: 1, too_new: 2, no_opportunity: 2 };
            return order[a.status] - order[b.status];
          });
        return (
          <div key={sport} className="rounded-lg border bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-gray-700">
              {SPORT_LABEL[sport] ?? sport}
            </h2>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              {rows.map((h) => (
                <DetectorTile key={`${h.sport}-${h.alertType}`} h={h} />
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
