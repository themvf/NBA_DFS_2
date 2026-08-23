export const dynamic = "force-dynamic";

import { Suspense } from "react";
import MlbPropsV2Content from "./mlb-props-v2-content";

export default function MlbPropsV2Page() {
  return (
    <Suspense
      fallback={
        <div className="mx-auto max-w-6xl space-y-6 p-6">
          <h1 className="text-2xl font-bold tracking-tight">MLB Prop Board</h1>
          <div className="rounded-xl border bg-card p-6 text-sm text-gray-400">
            Loading prop board…
          </div>
        </div>
      }
    >
      <MlbPropsV2Content />
    </Suspense>
  );
}
