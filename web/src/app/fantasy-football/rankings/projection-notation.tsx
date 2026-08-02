"use client";

import { useState } from "react";
import { buildProjectionExplanation } from "@/lib/fantasy-football/projection-explanation";

export default function ProjectionNotation({ details }: { details: Record<string, unknown> | null }) {
  const [open, setOpen] = useState(false);
  return <details open={open} onToggle={(event) => setOpen(event.currentTarget.open)} className="mt-1 text-xs font-normal text-muted-foreground">
    <summary className="cursor-pointer select-none font-semibold text-blue-700 hover:underline">How projected</summary>
    {open && <ProjectionExplanation details={details} />}
  </details>;
}

function ProjectionExplanation({ details }: { details: Record<string, unknown> | null }) {
  const explanation = buildProjectionExplanation(details);
  return <div className="mt-2 w-72 space-y-1 rounded-lg border bg-background p-3 text-left leading-relaxed shadow-sm">
      <p className="font-bold text-foreground">{explanation.method}</p>
      {explanation.lines.map((line) => <p key={line}>{line}</p>)}
      {explanation.notModeled.length > 0 && <p className="border-t pt-2 text-amber-700"><b>Not yet modeled:</b> {explanation.notModeled.join(", ")}.</p>}
    </div>;
}
