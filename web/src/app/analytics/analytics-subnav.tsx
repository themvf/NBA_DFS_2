import Link from "next/link";
import type { Sport } from "@/db/queries";

type AnalyticsTabKey = "overview" | "ownership" | "postmortem" | "perfect-lineups";

const MLB_TABS: Array<{ key: AnalyticsTabKey; label: string; href: string }> = [
  { key: "overview", label: "Overview", href: "/analytics?sport=mlb" },
  { key: "ownership", label: "Ownership", href: "/analytics/ownership?sport=mlb" },
  { key: "postmortem", label: "Postmortem", href: "/analytics/postmortem?sport=mlb" },
  { key: "perfect-lineups", label: "Perfect Lineups", href: "/analytics/perfect-lineups?sport=mlb" },
];

const NBA_TABS: Array<{ key: AnalyticsTabKey; label: string; href: string }> = [
  { key: "overview", label: "Overview", href: "/analytics?sport=nba" },
  { key: "perfect-lineups", label: "Perfect Lineups", href: "/analytics/perfect-lineups?sport=nba" },
];

export default function AnalyticsSubnav({
  sport,
  active,
}: {
  sport: Sport;
  active: AnalyticsTabKey;
}) {
  const tabs = sport === "mlb" ? MLB_TABS : NBA_TABS;

  return (
    <div className="mx-auto mb-6 max-w-5xl">
      <div className="flex flex-wrap gap-2 rounded-lg border bg-card p-2">
        {tabs.map((tab) => {
          const isActive = tab.key === active;
          return (
            <Link
              key={tab.key}
              href={tab.href}
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                isActive
                  ? "bg-slate-900 text-white"
                  : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
              }`}
            >
              {tab.label}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
