"use client";

import { useEffect, useState, useSyncExternalStore } from "react";
import { isFrozenDeploymentHost } from "@/lib/nfl-dfs/lock-countdown";

/**
 * Warns when this tab is not on the newest code: either the live site has
 * deployed since the tab loaded, or the tab is on a per-deployment address
 * that never updates. A build made from old code misses fixes (2026-11-15).
 */
export default function StaleBuildBanner({ sha, productionHost }: { sha: string; productionHost: string | null }) {
  const [live, setLive] = useState<string | null>(null);
  // The address is only known in the browser; the server render says "not frozen".
  const frozen = useSyncExternalStore(() => () => {}, () => isFrozenDeploymentHost(window.location.host, productionHost), () => false);
  useEffect(() => {
    let stopped = false;
    const check = () => {
      if (document.visibilityState === "hidden") return;
      fetch("/api/build-version", { cache: "no-store" }).then((r) => (r.ok ? r.json() : null))
        .then((body) => { if (!stopped && body && typeof body.sha === "string") setLive(body.sha); })
        .catch(() => { /* offline or mid-deploy: try again on the next tick */ });
    };
    check();
    const timer = setInterval(check, 60_000);
    document.addEventListener("visibilitychange", check);
    return () => { stopped = true; clearInterval(timer); document.removeEventListener("visibilitychange", check); };
  }, []);

  const stale = live != null && live !== sha;
  if (!stale && !frozen) return null;
  return <div role="alert" className="sticky top-0 z-50 mb-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-950">
    <span>{frozen
      ? <><b>This address never updates.</b> You are on a fixed copy of the site, so builds here can miss fixes. Use the live address.</>
      : <><b>A newer version of the site is live.</b> This tab is on an older one; reload before you build or export.</>}</span>
    {frozen && productionHost
      ? <a href={`https://${productionHost}/dfs/nfl`} className="min-h-10 content-center rounded-lg bg-red-700 px-3 font-bold text-white">Open the live site</a>
      : <button type="button" onClick={() => window.location.reload()} className="min-h-10 rounded-lg bg-red-700 px-3 font-bold text-white">Reload now</button>}
  </div>;
}
