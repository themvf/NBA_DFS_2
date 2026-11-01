"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

/**
 * Re-renders the board every few minutes while a game is live or still to
 * come, and only while the tab is visible. The server caches the database and
 * ESPN reads for five minutes, so a refresh costs a cache hit, not a query.
 */
export default function AutoRefresh({ active, seconds = 300 }: { active: boolean; seconds?: number }) {
  const router = useRouter();
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => {
      if (document.visibilityState === "visible") router.refresh();
    }, seconds * 1000);
    return () => window.clearInterval(id);
  }, [active, seconds, router]);
  return null;
}
