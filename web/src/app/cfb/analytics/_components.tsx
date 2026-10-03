import Link from "next/link";
import type { ReactNode } from "react";
import type { CfbAnalyticsFeature, CfbAnalyticsGame, CfbAnalyticsTeam } from "@/db/cfb-analytics";
import styles from "./analytics.module.css";

export function AnalyticsShell({ eyebrow, title, description, children }: {
  eyebrow: string; title: string; description: string; children: ReactNode;
}) {
  return <div className={styles.shell}>
    <header className={styles.hero}>
      <p className={styles.eyebrow}>{eyebrow}</p>
      <h1>{title}</h1>
      <p>{description}</p>
      <nav className={styles.localNav} aria-label="CFB analytics">
        <Link href="/cfb/analytics">Overview</Link>
        <Link href="/cfb/analytics/teams">Teams</Link>
        <Link href="/cfb/analytics/methods">Methods & coverage</Link>
        <Link href="/cfb">Line Terminal</Link>
      </nav>
    </header>
    {children}
  </div>;
}

export function formatEt(value: string | null, includeTime = false): string {
  if (!value) return "Kickoff TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "short", day: "numeric", year: "numeric",
    ...(includeTime ? { hour: "numeric", minute: "2-digit", timeZoneName: "short" } : {}),
  }).format(date);
}

export function signed(value: number | null, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(digits)}`;
}

export function metric(feature: CfbAnalyticsFeature | null, section: string, key: string): number | null {
  const group = feature?.values[section];
  if (!group || typeof group !== "object" || Array.isArray(group)) return null;
  const raw = (group as Record<string, unknown>)[key];
  if (raw == null) return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

export function returningPpa(feature: CfbAnalyticsFeature | null): number | null {
  const roster = feature?.values.roster;
  if (!roster || typeof roster !== "object" || Array.isArray(roster)) return null;
  const returning = (roster as Record<string, unknown>).returning_production;
  if (!returning || typeof returning !== "object" || Array.isArray(returning)) return null;
  const raw = (returning as Record<string, unknown>).percentPPA;
  if (raw == null) return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

export function TeamFeaturePanel({ team }: { team: CfbAnalyticsTeam }) {
  const feature = team.feature;
  const items = [
    ["Blended points scored", metric(feature, "blended", "points_for"), "pts/game"],
    ["Blended points allowed", metric(feature, "blended", "points_against"), "pts/game"],
    ["Blended scoring margin", metric(feature, "blended", "margin"), "pts/game"],
    ["Opponent-adjusted margin", metric(feature, "blended", "opponent_adjusted_margin"), "rating"],
  ] as const;
  const continuity = metric(feature, "roster", "roster_continuity_pct");
  const ppa = returningPpa(feature);
  return <article className={styles.panel}>
    <div className={styles.panelHeading}>
      <div><p className={styles.eyebrow}>TEAM CONTEXT</p><h3><Link href={`/cfb/analytics/teams/${team.id}`}>{team.name}</Link></h3></div>
      <span className={styles.tag}>{team.conference ?? team.classification ?? "CFB"}</span>
    </div>
    {feature ? <>
      <div className={styles.metricGrid}>{items.map(([label, value, unit]) => <div key={label} className={styles.metric}>
        <span>{label}</span><strong>{signed(value)}</strong><small>{unit}</small>
      </div>)}</div>
      <div className={styles.facts}>
        <span>Roster continuity <strong>{continuity == null ? "—" : `${(continuity * 100).toFixed(0)}%`}</strong></span>
        <span>Returning PPA share <strong>{ppa == null ? "—" : `${(ppa * 100).toFixed(0)}%`}</strong></span>
      </div>
      <p className={styles.provenance}>{feature.gamesPlayed} current-season FBS games · {(feature.currentWeight * 100).toFixed(0)}% season weight · {feature.completeness == null ? "unknown" : `${(feature.completeness * 100).toFixed(0)}%`} source completeness · as of {formatEt(feature.asOf, true)}</p>
    </> : <p className={styles.empty}>No eligible pregame team feature is available for this matchup.</p>}
  </article>;
}

export function GameCard({ game }: { game: CfbAnalyticsGame }) {
  const state = game.completed ? "Final" : game.kickoffTbd ? "Time TBD" : formatEt(game.kickoff, true);
  return <article className={styles.gameCard}>
    <div className={styles.cardTop}><span>{state}</span><span>{game.bookmakerCount ? `${game.bookmakerCount} observed books` : game.oddsEventMapped ? "Mapped · awaiting odds" : "Provider event unavailable"}</span></div>
    <div className={styles.gameNames}><strong>{game.away.name}</strong><span>at</span><strong>{game.home.name}</strong></div>
    <div className={styles.gameNumbers}>
      <span>Home spread <strong>{signed(game.homeSpread)}</strong></span>
      <span>Total <strong>{game.total == null ? "—" : game.total.toFixed(1)}</strong></span>
      <span>Home ML <strong>{game.homeMoneyline == null ? "—" : signed(game.homeMoneyline, 0)}</strong></span>
    </div>
    <Link className={styles.textLink} href={`/cfb/analytics/games/${game.id}`}>Open game analysis →</Link>
  </article>;
}
