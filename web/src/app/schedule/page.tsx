export const dynamic = "force-dynamic";

import { getRecentSchedule } from "@/db/queries";

function mlToStr(ml: number | null): string {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : `${ml}`;
}

function fmt1(v: number | null): string {
  return v != null ? v.toFixed(1) : "—";
}

function fmtProb(v: number | null): string {
  return v != null ? `${(v * 100).toFixed(0)}%` : "—";
}

function groupByDate(rows: Awaited<ReturnType<typeof getRecentSchedule>>) {
  const map = new Map<string, typeof rows>();
  for (const r of rows) {
    const arr = map.get(r.gameDate) ?? [];
    arr.push(r);
    map.set(r.gameDate, arr);
  }
  return map;
}

function formatDate(d: string): string {
  const dt = new Date(d + "T12:00:00");
  return dt.toLocaleDateString("en-US", { weekday: "long", month: "short", day: "numeric" });
}

export default async function SchedulePage() {
  const games = await getRecentSchedule(7);

  if (games.length === 0) {
    return (
      <div className="text-center py-20 text-gray-400">
        <p className="text-lg font-medium">No schedule data yet</p>
        <p className="text-sm mt-1">Run the Daily Stats workflow in GitHub Actions to populate data.</p>
      </div>
    );
  }

  const byDate = groupByDate(games);
  const dates  = Array.from(byDate.keys()).sort().reverse();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Schedule</h1>
        <p className="text-sm text-gray-500 mt-1">Last 7 days · Vegas lines where available</p>
      </div>

      {dates.map((d) => (
        <div key={d} className="rounded-lg border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b bg-gray-50">
            <h2 className="text-sm font-semibold">{formatDate(d)}</h2>
          </div>
          <div className="divide-y divide-gray-100">
            {byDate.get(d)!.map((g) => (
              <div key={g.id} className="px-4 py-3 flex items-center gap-4">
                {/* Away team */}
                <div className="flex items-center gap-2 w-44">
                  {g.awayLogo && <img src={g.awayLogo} alt="" className="h-8 w-8 object-contain" />}
                  <div>
                    <div className="font-medium text-sm">{g.awayName ?? g.awayAbbrev ?? "—"}</div>
                    <div className="text-xs text-gray-400">{mlToStr(g.awayMl)}</div>
                  </div>
                </div>

                <div className="text-gray-300 font-medium">@</div>

                {/* Home team */}
                <div className="flex items-center gap-2 w-44">
                  {g.homeLogo && <img src={g.homeLogo} alt="" className="h-8 w-8 object-contain" />}
                  <div>
                    <div className="font-medium text-sm">{g.homeName ?? g.homeAbbrev ?? "—"}</div>
                    <div className="text-xs text-gray-400">{mlToStr(g.homeMl)}</div>
                  </div>
                </div>

                {/* Vegas info */}
                <div className="ml-auto flex items-center gap-6 text-sm text-right">
                  <div>
                    <div className="text-xs text-gray-400">Total</div>
                    <div className="font-medium">{fmt1(g.vegasTotal)}</div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-400">Home Win%</div>
                    <div className="font-medium">{fmtProb(g.homeWinProb)}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
