"use client";

/**
 * Global sport-aware navigation bar.
 *
 * Sport tabs link to {currentPath}?sport={sport} so switching sports
 * stays on the same page. Page links carry the current sport param
 * forward so MLB → Analytics stays on MLB.
 *
 * Adding a new sport: append an entry to SPORTS below.
 */

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import type { Sport } from "@/db/queries";

const SPORTS: { sport: Sport; label: string; icon: string }[] = [
  { sport: "nba", label: "NBA", icon: "🏀" },
  { sport: "mlb", label: "MLB", icon: "⚾" },
  { sport: "nfl", label: "NFL", icon: "🏈" },
  { sport: "soccer", label: "World Cup", icon: "⚽" },
  { sport: "tennis", label: "Tennis", icon: "🎾" },
];

// Soccer currently only has the Vegas model wired up; DFS/analytics pages are
// NBA/MLB-only until the soccer DFS phase lands.
const PAGE_LINKS: Array<{
  href: string;
  label: string;
  sports?: Sport[];
}> = [
  { href: "/dfs", label: "DFS", sports: ["nba", "mlb"] },
  { href: "/nfl", label: "NFL Board", sports: ["nfl"] },
  { href: "/fantasy-football", label: "Fantasy Football", sports: ["nfl"] },
  { href: "/fantasy-football/best-ball", label: "NFL Best Ball", sports: ["nfl"] },
  { href: "/fantasy-football/redraft", label: "NFL Redraft", sports: ["nfl"] },
  { href: "/homerun", label: "Homeruns", sports: ["mlb"] },
  { href: "/analytics", label: "Analytics", sports: ["nba", "mlb"] },
  { href: "/vegas", label: "Vegas" },
  { href: "/vegas/mlb-props", label: "Vegas Props", sports: ["mlb"] },
  { href: "/vegas/wimbledon", label: "Wimbledon", sports: ["tennis"] },
  { href: "/elo", label: "Elo / Power", sports: ["soccer"] },
  { href: "/stats", label: "Team Stats", sports: ["nba", "mlb"] },
  { href: "/schedule", label: "Schedule", sports: ["nba", "mlb"] },
  { href: "/video-analysis", label: "Video Analysis" },
  { href: "/youtube-picks", label: "YouTube Picks" },
];

export function SportNav() {
  const pathname    = usePathname();
  const searchParams = useSearchParams();
  const currentSport = (pathname === "/nfl" || pathname.startsWith("/nfl/") || pathname.startsWith("/fantasy-football")
    ? "nfl"
    : searchParams.get("sport") ?? "nba") as Sport;
  const visiblePageLinks = PAGE_LINKS.filter((link) => !link.sports || link.sports.includes(currentSport));

  return (
    <header className="sticky top-0 z-50 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-2 px-4">

        {/* Logo */}
        <Link
          href={currentSport === "nfl" ? "/nfl" : currentSport === "soccer" || currentSport === "tennis" ? `/vegas?sport=${currentSport}` : `/dfs?sport=${currentSport}`}
          className="mr-3 shrink-0 font-bold text-lg tracking-tight"
        >
          DFS
        </Link>

        {/* Sport selector — primary navigation */}
        <div className="flex items-center gap-1">
          {SPORTS.map(({ sport, label, icon }) => {
            const active = currentSport === sport;
            return (
              <Link
                key={sport}
                href={sport === "nfl" ? "/nfl" : currentSport === "nfl" ? `/vegas?sport=${sport}` : `${pathname}?sport=${sport}`}
                className={`flex items-center gap-1.5 rounded px-3 py-1.5 text-sm font-medium transition-colors ${
                  active
                    ? "bg-blue-600 text-white"
                    : "text-muted-foreground hover:bg-accent hover:text-foreground"
                }`}
              >
                <span aria-hidden="true">{icon}</span>
                <span>{label}</span>
              </Link>
            );
          })}
        </div>

        {/* Divider */}
        <div className="mx-2 h-5 w-px bg-border" />

        {/* Page links — carry current sport forward */}
        <nav className="flex items-center gap-1 text-sm">
          {visiblePageLinks.map((l) => {
            const href = currentSport === "nfl" ? l.href : `${l.href}?sport=${currentSport}`;
            // Prefer the most specific matching href so nested routes (e.g.
            // /vegas/wimbledon under /vegas) don't also highlight their parent.
            const matches = (p: string) => pathname === p || pathname.startsWith(`${p}/`);
            const isActive =
              matches(l.href) &&
              !visiblePageLinks.some((o) => o.href !== l.href && o.href.length > l.href.length && matches(o.href));
            return (
              <Link
                key={l.href}
                href={href}
                className={`rounded px-3 py-1.5 transition-colors ${
                  isActive
                    ? "font-medium text-foreground"
                    : "text-muted-foreground hover:bg-accent hover:text-foreground"
                }`}
              >
                {l.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
