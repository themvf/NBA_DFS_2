export const dynamic = "force-dynamic";

import { Suspense } from "react";
import WimbledonContent from "./wimbledon-content";

export default function WimbledonPage() {
  return (
    <Suspense
      fallback={
        <div className="space-y-6 p-6 max-w-5xl mx-auto">
          <h1 className="text-xl font-bold">Wimbledon 🎾 — Analytics &amp; Ledger</h1>
          <div className="rounded-lg border bg-card p-6 text-sm text-gray-400">
            Loading Wimbledon ledger and analytics…
          </div>
        </div>
      }
    >
      <WimbledonContent />
    </Suspense>
  );
}
