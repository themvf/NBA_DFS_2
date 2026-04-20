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
];

const PAGE_LINKS = [
  { href: "/dfs",       label: "DFS" },
  { href: "/homerun",   label: "Homeruns" },
  { href: "/analytics", label: "Analytics" },
  { href: "/vegas",     label: "Vegas" },
  { href: "/stats",     label: "Team Stats" },
  { href: "/schedule",  label: "Schedule" },
];

export function SportNav() {
  const pathname    = usePathname();
  const searchParams = useSearchParams();
  const currentSport = (searchParams.get("sport") ?? "nba") as Sport;

  return (
    <header className="sticky top-0 z-50 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-2 px-4">

        {/* Logo */}
        <Link
          href={`/dfs?sport=${currentSport}`}
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
                href={`${pathname}?sport=${sport}`}
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
          {PAGE_LINKS.map((l) => {
            const href = `${l.href}?sport=${currentSport}`;
            const isActive = pathname === l.href;
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
