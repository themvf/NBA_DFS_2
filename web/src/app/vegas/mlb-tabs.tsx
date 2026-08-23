"use client";

import Link from "next/link";

/**
 * Tab strip across the MLB Vegas surfaces.
 *
 * Deliberately LINKS, not client-side tab state. Each tab is its own route with
 * its own scoped server fetch, so switching tabs does not make the line-movement
 * page pay for the prop query (or the reverse), the URL stays shareable, and the
 * back button behaves. Client state would have forced one component to fetch
 * every tab's data on every load.
 *
 * `active` is passed in rather than derived from usePathname() because the
 * board tab lives at /vegas?sport=mlb — a pathname shared with three other
 * sports — so pathname alone cannot identify it.
 */
export type MlbVegasTab = "board" | "props" | "props-original";

const TABS: { id: MlbVegasTab; href: string; label: string; hint: string }[] = [
  {
    id: "board",
    href: "/vegas?sport=mlb",
    label: "Line Movement",
    hint: "Game lines — market movement versus the model edge",
  },
  {
    id: "props",
    href: "/vegas/mlb-props-v2",
    label: "Props",
    hint: "Player props — live board, measurement program, running audit",
  },
  {
    id: "props-original",
    href: "/vegas/mlb-props",
    label: "Props (original)",
    hint: "The previous prop alerts table, kept for side-by-side comparison",
  },
];

export default function MlbVegasTabs({ active }: { active: MlbVegasTab }) {
  return (
    <nav aria-label="MLB Vegas views" className="flex flex-wrap items-center gap-1 border-b border-slate-200">
      {TABS.map((t) => {
        const on = t.id === active;
        return (
          <Link
            key={t.id}
            href={t.href}
            title={t.hint}
            aria-current={on ? "page" : undefined}
            className={`-mb-px border-b-2 px-3 py-2 text-sm font-medium transition ${
              on
                ? "border-slate-900 text-slate-900"
                : "border-transparent text-slate-500 hover:border-slate-300 hover:text-slate-800"
            }`}
          >
            {t.label}
          </Link>
        );
      })}
    </nav>
  );
}
