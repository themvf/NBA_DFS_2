import Link from "next/link";
import type { FantasyAdpMover, FantasyAdpMovers } from "@/db/queries-fantasy-football";

const WINDOWS = [
  { hours: 12, label: "12h" },
  { hours: 24, label: "24h" },
  { hours: 72, label: "3d" },
  { hours: 168, label: "7d" },
];

function timeAgo(iso: string | null): string {
  if (!iso) return "never";
  const ms = Date.now() - new Date(iso).getTime();
  const hours = ms / (60 * 60 * 1000);
  if (hours < 1) return `${Math.max(1, Math.round(ms / 60000))}m ago`;
  if (hours < 48) return `${Math.round(hours)}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function MoverRow({ player, direction }: { player: FantasyAdpMover; direction: "up" | "down" }) {
  const tone = direction === "up" ? "text-emerald-700" : "text-red-700";
  const arrow = direction === "up" ? "↑" : "↓";
  return (
    <div className="flex items-center justify-between gap-3 border-b p-2.5 text-sm last:border-b-0">
      <div className="min-w-0">
        <p className="truncate font-bold">{player.name} <span className="text-xs font-normal text-muted-foreground">{player.position} · {player.team ?? "FA"}</span></p>
        <p className="text-xs text-muted-foreground">{player.baselineAdp.toFixed(1)} <span aria-hidden>&rarr;</span> {player.currentAdp.toFixed(1)}</p>
      </div>
      <p className={`shrink-0 font-black ${tone}`}>{arrow} {Math.abs(player.delta).toFixed(1)}</p>
    </div>
  );
}

export default function AdpMoversPanel({ movers, scoring }: { movers: FantasyAdpMovers; scoring: string }) {
  return (
    <section className="rounded-2xl border bg-card p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-black">ADP Movers</h2>
          <p className="text-xs text-muted-foreground">
            Fantasy Football Calculator ADP, captured every 12h · latest capture {timeAgo(movers.latestCapturedAt)}
          </p>
        </div>
        <div className="flex gap-1.5">
          {WINDOWS.map((w) => (
            <Link
              key={w.hours}
              href={`/fantasy-football/rankings?scoring=${scoring}&window=${w.hours}`}
              className={`rounded-lg px-2.5 py-1.5 text-xs font-bold ${movers.sinceHours === w.hours ? "bg-slate-900 text-white" : "border hover:bg-muted"}`}
            >
              {w.label}
            </Link>
          ))}
        </div>
      </div>

      {!movers.hasEnoughHistory ? (
        <p className="mt-4 rounded-xl bg-muted p-4 text-sm text-muted-foreground">
          {movers.latestCapturedAt
            ? `Not enough ADP history yet for a ${WINDOWS.find((w) => w.hours === movers.sinceHours)?.label ?? `${movers.sinceHours}h`} window (history starts ${timeAgo(movers.earliestCapturedAt)}). Check back after the next 12-hour capture.`
            : "No ADP history captured yet. The 12-hour capture job (refresh_ff_adp_snapshot.yml) hasn't run, or ff_players isn't populated for this season."}
        </p>
      ) : (
        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <div className="rounded-xl border">
            <p className="border-b bg-emerald-50 p-2.5 text-xs font-bold uppercase tracking-wide text-emerald-800">Risers · being drafted earlier</p>
            {movers.risers.length ? movers.risers.map((player) => <MoverRow key={player.playerId} player={player} direction="up" />) : <p className="p-3 text-sm text-muted-foreground">No risers over this window.</p>}
          </div>
          <div className="rounded-xl border">
            <p className="border-b bg-red-50 p-2.5 text-xs font-bold uppercase tracking-wide text-red-800">Fallers · being drafted later</p>
            {movers.fallers.length ? movers.fallers.map((player) => <MoverRow key={player.playerId} player={player} direction="down" />) : <p className="p-3 text-sm text-muted-foreground">No fallers over this window.</p>}
          </div>
        </div>
      )}
    </section>
  );
}
