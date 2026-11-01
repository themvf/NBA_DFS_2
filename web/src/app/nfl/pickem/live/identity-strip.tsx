import Link from "next/link";
import { teamStyle } from "@/lib/nfl/pickem-live-board";
import type { TeamIdentity } from "@/lib/nfl/team-identity-ranks";

const pct = (v: number | null) => (v == null ? "–" : `${Math.round(v * 100)}%`);

/** 1-32 shaded best (green) to worst (red), by quarter of the ranked league. */
function rankTone(rank: number, of: number): string {
  const q = rank / Math.max(of, 1);
  if (q <= 0.25) return "bg-emerald-500/20 text-emerald-800 dark:text-emerald-300";
  if (q <= 0.5) return "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400";
  if (q <= 0.75) return "bg-amber-500/15 text-amber-800 dark:text-amber-300";
  return "bg-rose-500/20 text-rose-800 dark:text-rose-300";
}

function RankChip({ side, rank, of }: { side: "Off" | "Def"; rank: number; of: number }) {
  return (
    <span className={`whitespace-nowrap rounded px-1.5 py-0.5 text-[11px] font-semibold tabular-nums ${rankTone(rank, of)}`}
      title={`${side === "Off" ? "Offense EPA/play" : "Defense EPA/play allowed"} rank ${rank} of ${of}`}>
      {side} #{rank}
    </span>
  );
}

function teamHref(team: string, season: number) {
  return `/nfl/team-identity?team=${encodeURIComponent(team)}&season=${season}`;
}

function TeamLine({ team, id, season }: { team: string; id: TeamIdentity | undefined; season: number }) {
  const [bg, fg] = teamStyle(team);
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <Link href={teamHref(team, season)} className="min-w-[40px] rounded px-1 py-0.5 text-center text-xs font-bold hover:opacity-80"
        style={{ background: bg, color: fg }} title={`${team} team identity`}>
        {team}
      </Link>
      {id?.enough && id.offenseRank != null && id.defenseRank != null ? (
        <>
          <RankChip side="Off" rank={id.offenseRank} of={id.rankedTeams} />
          <RankChip side="Def" rank={id.defenseRank} of={id.rankedTeams} />
          {id.style && <span className="rounded bg-muted px-1.5 py-0.5 text-[11px] text-muted-foreground">{id.style}</span>}
          <span className="text-[11px] tabular-nums text-muted-foreground"
            title="Explosive plays (20+ yards) per scrimmage play; touchdown and three-and-out drives per drive">
            expl {pct(id.explosiveRate)} · TD {pct(id.touchdownDriveRate)} · 3&amp;out {pct(id.threeAndOutRate)}
          </span>
        </>
      ) : (
        <span className="text-[11px] text-muted-foreground">not enough games</span>
      )}
    </div>
  );
}

export function IdentityStrip({ away, home, pick, identities, season }: {
  away: string; home: string; pick: string;
  identities: Record<string, TeamIdentity>; season: number;
}) {
  const opp = pick === home ? away : home;
  const mine = identities[pick], theirs = identities[opp];
  const matchup = mine?.offenseRank != null && theirs?.defenseRank != null
    ? `${pick} offense #${mine.offenseRank} vs ${opp} defense #${theirs.defenseRank}`
    : null;
  return (
    <div className="space-y-1.5 rounded-lg bg-muted/40 p-2">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground"
        title="EPA is the strongest separator of who wins, and exactly what the line already prices; week-to-week form predicted nothing in the pick'em studies.">
        Team identity · context, not a pick signal
      </div>
      <TeamLine team={away} id={identities[away]} season={season} />
      <TeamLine team={home} id={identities[home]} season={season} />
      {matchup && <p className="text-xs font-medium">{matchup}</p>}
    </div>
  );
}
