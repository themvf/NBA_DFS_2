import {
  latestChance,
  outcomeOf,
  teamStyle,
  type BoardGame,
} from "@/lib/nfl/pickem-live-board";

const pct = (v: number) => `${Math.round(v * 100)}%`;

export function kickoffLabel(iso: string): string {
  return `${new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "2-digit",
  }).format(new Date(iso))} ET`;
}

/** Our pick's pregame chance from the first capture to the last, with the frozen value dashed. */
function Sparkline({ g }: { g: BoardGame }) {
  const t = g.trail;
  if (t.length < 2) {
    return <p className="text-xs text-muted-foreground">No pregame line trail captured.</p>;
  }
  const W = 300, H = 44;
  const vals = [...t, g.frozen];
  const lo = Math.min(...vals) - 0.02, hi = Math.max(...vals) + 0.02;
  const X = (i: number) => 4 + (i * (W - 44)) / (t.length - 1);
  const Y = (v: number) => 4 + ((hi - v) / (hi - lo)) * (H - 8);
  const pts = t.map((v, i) => `${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(" ");
  const lx = X(t.length - 1), ly = Y(t[t.length - 1]);
  const line = g.ourFlip ? "stroke-orange-600 dark:stroke-orange-400" : "stroke-sky-600 dark:stroke-sky-400";
  const fill = g.ourFlip ? "fill-orange-600 dark:fill-orange-400" : "fill-sky-600 dark:fill-sky-400";
  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="block h-auto w-full"
      role="img"
      aria-label={`${g.pick} win chance moved from ${pct(t[0])} to ${pct(t[t.length - 1])}; frozen at ${pct(g.frozen)}`}
    >
      <line x1={4} x2={W - 40} y1={Y(g.frozen)} y2={Y(g.frozen)}
        className="stroke-muted-foreground" strokeDasharray="4 3" strokeWidth={1} />
      <polygon points={`${X(0)},${H} ${pts} ${lx},${H}`} className={fill} fillOpacity={0.12} />
      <polyline points={pts} fill="none" className={line} strokeWidth={2}
        strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lx} cy={ly} r={4} className={fill} />
      <text x={lx + 8} y={ly + 4} fontSize={12} className="fill-foreground">{pct(t[t.length - 1])}</text>
    </svg>
  );
}

function TeamRow({ g, abbr, score }: { g: BoardGame; abbr: string; score: number | null }) {
  const [bg, fg, name] = teamStyle(abbr);
  const picked = g.pick === abbr;
  return (
    <>
      <div className="flex min-w-0 items-center gap-2">
        <span className="min-w-[46px] rounded px-1.5 py-0.5 text-center text-sm font-bold tracking-wide"
          style={{ background: bg, color: fg }}>{abbr}</span>
        <span className={`truncate text-sm ${picked ? "font-semibold text-foreground" : "text-muted-foreground"}`}>
          {name}
        </span>
        {picked && (
          <span className={`rounded px-1.5 text-[11px] font-semibold ring-1 ring-inset ${g.ourFlip
            ? "text-orange-600 ring-orange-600 dark:text-orange-400 dark:ring-orange-400"
            : "text-sky-600 ring-sky-600 dark:text-sky-400 dark:ring-sky-400"}`}>PICK</span>
        )}
      </div>
      <div className={`text-right text-2xl font-bold tabular-nums leading-none ${score == null ? "text-muted-foreground" : ""}`}>
        {score == null ? "–" : score}
      </div>
    </>
  );
}

export function BoardCard({ g }: { g: BoardGame }) {
  const o = outcomeOf(g);
  const now = latestChance(g);
  const homeP = g.pick === g.home ? now : 1 - now;
  const move = g.trail.length ? Math.round((now - g.trail[0]) * 1000) / 10 : 0;

  let status = kickoffLabel(g.kickoff);
  let statusCls = "bg-muted text-muted-foreground";
  if (g.state === "post") {
    statusCls = o === "won" ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400"
      : o === "lost" ? "bg-rose-500/15 text-rose-700 dark:text-rose-400" : "bg-muted text-muted-foreground";
    status = `${o === "won" ? "Won" : o === "lost" ? "Lost" : "Tie"} · Final`;
  } else if (g.state === "pending") {
    statusCls = "bg-amber-500/15 text-amber-700 dark:text-amber-400";
    status = `${kickoffLabel(g.kickoff)} · awaiting final`;
  }

  const [awayBg] = teamStyle(g.away), [homeBg] = teamStyle(g.home);

  return (
    <article className={`flex min-w-0 flex-col gap-2.5 rounded-xl border bg-card p-3.5 ${g.ourFlip
      ? "ring-2 ring-inset ring-orange-600 dark:ring-orange-400" : ""}`}>
      <div className="flex items-center justify-between gap-2">
        <span className={`whitespace-nowrap rounded-md px-2 py-0.5 text-xs font-semibold ${statusCls}`}>{status}</span>
        <span className="text-xs text-muted-foreground">{g.spread ?? ""}</span>
      </div>

      {(g.ourFlip || g.vegasFlip) && (
        <div className="flex flex-wrap gap-1.5">
          {g.ourFlip && (
            <span className="rounded-md bg-orange-500/15 px-2 py-0.5 text-xs font-semibold text-orange-700 dark:text-orange-400">
              Our flip · {g.ourFlipReason === "model" ? `our model favours ${g.pick}` : "underdog by choice"}
            </span>
          )}
          {g.vegasFlip && (
            <span className="rounded-md bg-violet-500/15 px-2 py-0.5 text-xs font-semibold text-violet-700 dark:text-violet-400">
              Vegas flip · {g.openFavorite} opened favoured, now {g.favorite}
            </span>
          )}
        </div>
      )}

      <div className="grid grid-cols-[1fr_auto] items-center gap-y-1.5">
        <TeamRow g={g} abbr={g.away} score={g.awayScore} />
        <TeamRow g={g} abbr={g.home} score={g.homeScore} />
      </div>

      <div>
        <div className="relative flex h-2 gap-0.5 overflow-hidden rounded bg-muted">
          <div style={{ width: `${(1 - homeP) * 100}%`, background: awayBg, opacity: g.pick === g.away ? 1 : 0.3 }} />
          <div style={{ flex: 1, background: homeBg, opacity: g.pick === g.home ? 1 : 0.3 }} />
          <div className="absolute inset-y-0 left-1/2 w-0.5 bg-muted-foreground" />
        </div>
        <div className="mt-0.5 flex justify-between text-xs tabular-nums text-muted-foreground">
          <span>{g.away} {pct(1 - homeP)}</span>
          <span>pregame win chance</span>
          <span>{g.home} {pct(homeP)}</span>
        </div>
      </div>

      <Sparkline g={g} />

      <p className="text-xs text-muted-foreground">
        {g.pick} {move >= 0 ? "+" : ""}{move.toFixed(1)} pts this week · frozen at {pct(g.frozen)}
        {g.confidence > 1 ? ` · confidence ${g.confidence}` : ""}
      </p>
    </article>
  );
}
