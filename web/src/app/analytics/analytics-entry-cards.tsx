import Link from "next/link";
import type { Sport } from "@/db/queries";

export default function AnalyticsEntryCards({ sport }: { sport: Sport }) {
  const cards = sport === "mlb"
    ? [
        {
          title: "Ownership Tracking",
          href: "/analytics/ownership?sport=mlb",
          body: "Field ownership vs LineStar, slate-level misses, bucket bias, and model gain.",
        },
        {
          title: "Postmortem",
          href: "/analytics/postmortem?sport=mlb",
          body: "Projection independence, ownership gaps, signal follow-through, and latest misses.",
        },
        {
          title: "Perfect Lineups",
          href: "/analytics/perfect-lineups?sport=mlb",
          body: "Historical optimal lineup structure, stack shapes, salary left, and opponent context.",
        },
      ]
    : [
        {
          title: "Perfect Lineups",
          href: "/analytics/perfect-lineups?sport=nba",
          body: "Historical optimal lineup construction by slate size, shape, and team concentration.",
        },
      ];

  return (
    <div className="mx-auto mb-8 max-w-5xl space-y-3">
      <div>
        <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
        <p className="mt-1 text-sm text-slate-700">
          Overview on this page, with heavier analysis moved into dedicated deep-dive views.
        </p>
      </div>
      <div className={`grid gap-3 ${cards.length > 1 ? "md:grid-cols-3" : "md:grid-cols-1"}`}>
        {cards.map((card) => (
          <Link
            key={card.href}
            href={card.href}
            className="rounded-lg border bg-card p-4 transition-colors hover:border-slate-400"
          >
            <div className="text-sm font-semibold text-slate-900">{card.title}</div>
            <p className="mt-2 text-xs text-slate-600">{card.body}</p>
            <div className="mt-3 text-xs font-medium text-sky-700">Open</div>
          </Link>
        ))}
      </div>
    </div>
  );
}
