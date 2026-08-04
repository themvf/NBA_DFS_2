export const dynamic = "force-dynamic";

import { Suspense } from "react";
import MlbPropsContent from "./mlb-props-content";

export default function MlbPropsPage() {
  return (
    <Suspense
      fallback={
        <div className="space-y-6 p-6 max-w-5xl mx-auto">
          <h1 className="text-xl font-bold">MLB Vegas Props</h1>
          <div className="rounded-lg border bg-card p-6 text-sm text-gray-400">
            Loading prop alerts…
          </div>
        </div>
      }
    >
      <MlbPropsContent />
    </Suspense>
  );
}
