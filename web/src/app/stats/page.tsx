export const dynamic = "force-dynamic";

import { getTeamStats } from "@/db/queries";

function fmt1(v: number | null): string {
  return v != null ? v.toFixed(1) : "—";
}

function bar(value: number | null, min: number, max: number, color: string): React.ReactNode {
  if (value == null) return null;
  const pct = Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
  return (
    <div className="flex items-center gap-2">
      <span className="w-10 text-right text-xs">{fmt1(value)}</span>
      <div className="flex-1 h-2 rounded bg-gray-100">
        <div className={`h-2 rounded ${color}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

export default async function StatsPage() {
  const teams = await getTeamStats();

  if (teams.length === 0) {
    return (
      <div className="text-center py-20 text-gray-400">
        <p className="text-lg font-medium">No team stats yet</p>
        <p className="text-sm mt-1">Run the Daily Stats workflow in GitHub Actions to populate data.</p>
      </div>
    );
  }

  const paces   = teams.map((t) => t.pace).filter((v): v is number => v != null);
  const offRtgs = teams.map((t) => t.offRtg).filter((v): v is number => v != null);
  const defRtgs = teams.map((t) => t.defRtg).filter((v): v is number => v != null);

  const minPace = Math.min(...paces),   maxPace = Math.max(...paces);
  const minOff  = Math.min(...offRtgs), maxOff  = Math.max(...offRtgs);
  const minDef  = Math.min(...defRtgs), maxDef  = Math.max(...defRtgs);

  const east = teams.filter((t) => t.conference === "East").sort((a, b) => (b.offRtg ?? 0) - (a.offRtg ?? 0));
  const west = teams.filter((t) => t.conference === "West").sort((a, b) => (b.offRtg ?? 0) - (a.offRtg ?? 0));
  const other = teams.filter((t) => t.conference !== "East" && t.conference !== "West");
  const grouped = [
    { label: "Eastern Conference", rows: east },
    { label: "Western Conference", rows: west },
    ...(other.length > 0 ? [{ label: "Other", rows: other }] : []),
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Team Stats</h1>
        <p className="text-sm text-gray-500 mt-1">
          {teams.length} teams · 2025-26 season rolling averages
        </p>
      </div>

      {grouped.map(({ label, rows }) => (
        <div key={label} className="rounded-lg border bg-card overflow-hidden">
          <div className="px-4 py-3 border-b bg-gray-50">
            <h2 className="text-sm font-semibold">{label}</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b">
                <tr className="text-xs text-gray-500">
                  <th className="px-4 py-2 text-left">Team</th>
                  <th className="px-4 py-2 text-left min-w-[180px]">Pace</th>
                  <th className="px-4 py-2 text-left min-w-[180px]">Off Rtg</th>
                  <th className="px-4 py-2 text-left min-w-[180px]">Def Rtg</th>
                  <th className="px-4 py-2 text-right">Net</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {rows.map((t) => {
                  const net = t.offRtg != null && t.defRtg != null ? t.offRtg - t.defRtg : null;
                  return (
                    <tr key={t.teamId} className="hover:bg-gray-50">
                      <td className="px-4 py-2">
                        <div className="flex items-center gap-2">
                          {t.logoUrl && (
                            <img src={t.logoUrl} alt="" className="h-6 w-6 object-contain" />
                          )}
                          <div>
                            <div className="font-medium">{t.name}</div>
                            <div className="text-xs text-gray-400">{t.abbreviation}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-2">{bar(t.pace, minPace, maxPace, "bg-blue-400")}</td>
                      <td className="px-4 py-2">{bar(t.offRtg, minOff, maxOff, "bg-green-400")}</td>
                      <td className="px-4 py-2">{bar(t.defRtg, minDef, maxDef, "bg-red-400")}</td>
                      <td className={`px-4 py-2 text-right font-medium text-sm ${
                        net == null ? "text-gray-400" : net >= 0 ? "text-green-600" : "text-red-500"
                      }`}>
                        {net != null ? (net >= 0 ? "+" : "") + net.toFixed(1) : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}
